// Example: events/consumer-group (master-table variant #5)
//
// Consumer-group load-balancing over KubeMQ **Events** with the native
// github.com/Azure/go-amqp client (NO KubeMQ SDK).
//
// The x-opt-kubemq-group receiver link property places a subscriber in a named
// load-balancing group. Within ONE group, the connector round-robins the event
// stream across the group's members (no duplication). A DISTINCT group is an
// independent virtual-topic subscriber that gets the FULL stream.
//
// This example opens:
//   - g1a, g1b — two receivers in group "g1" → together they receive every
//     event with NO body delivered to both (the group splits the stream).
//   - g2       — one receiver in group "g2" → gets EVERY event (independent).
//
// Each receiver runs on its own *Session and goroutine: go-amqp sessions/links
// are NOT concurrency-safe, so we never share one across goroutines.
//
// Grounded in connector test TestEventsPubSubGroupFanout
// (connectors/amqp10/integration_pubsub_test.go).
//
// Run:
//
//	export KUBEMQ_AMQP_URL=amqp://localhost:5672
//	go run ./events/consumer-group
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	amqp "github.com/Azure/go-amqp"
)

// A per-run-unique suffix isolates this run's consumer groups: concurrent
// runners that joined the SAME group on a shared channel would load-balance
// the stream across processes, so group g1's split would span runners and the
// "0 duplicates" / full-stream assertions would false-fail. A fresh channel
// per run keeps the group semantics scoped to this process.
var channel = fmt.Sprintf("amqp10.examples.consumergroup.%d", time.Now().UnixNano())

const total = 30

const groupProp = "x-opt-kubemq-group"

func amqpURL() string {
	if v := os.Getenv("KUBEMQ_AMQP_URL"); v != "" {
		return v
	}
	return "amqp://localhost:5672"
}

// subscriber drives one group receiver on its own session + goroutine. It drains
// up to `max` events within `window`, returning the bodies it received.
type subscriber struct {
	label string // human label for the transcript (e.g. "g1a")
	group string // x-opt-kubemq-group value
	got   map[string]struct{}
}

func (s *subscriber) run(ctx context.Context, conn *amqp.Conn, addr string, max int, window time.Duration, started, done *sync.WaitGroup) {
	defer done.Done()
	s.got = make(map[string]struct{})

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		log.Fatalf("[%s] new session: %v", s.label, err)
	}

	rcv, err := session.NewReceiver(ctx, addr, &amqp.ReceiverOptions{
		Credit:     100,
		Properties: map[string]any{groupProp: s.group},
	})
	if err != nil {
		log.Fatalf("[%s] new receiver (group=%s): %v", s.label, s.group, err)
	}
	started.Done() // signal: this subscriber's link is attached

	deadline := time.Now().Add(window)
	for len(s.got) < max && time.Now().Before(deadline) {
		rcvCtx, rcvCancel := context.WithTimeout(ctx, time.Until(deadline))
		msg, err := rcv.Receive(rcvCtx, nil)
		rcvCancel()
		if err != nil {
			break // window elapsed / no more messages
		}
		_ = rcv.AcceptMessage(ctx, msg) // no-op for pre-settled fan-out
		s.got[string(msg.GetData())] = struct{}{}
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = rcv.Close(closeCtx)
	closeCancel()
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	addr := "events/" + channel
	fmt.Printf("Broker:  %s\n", amqpURL())
	fmt.Printf("Address: %s  (KubeMQ pattern=events, channel=%s)\n\n", addr, channel)

	conn, err := amqp.Dial(ctx, amqpURL(), nil)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Three group subscribers, each on its own session/goroutine.
	g1a := &subscriber{label: "g1a", group: "g1"}
	g1b := &subscriber{label: "g1b", group: "g1"}
	g2 := &subscriber{label: "g2", group: "g2"}
	subs := []*subscriber{g1a, g1b, g2}

	var started, done sync.WaitGroup
	started.Add(len(subs))
	done.Add(len(subs))
	for _, s := range subs {
		go s.run(ctx, conn, addr, total, 30*time.Second, &started, &done)
	}

	// Wait until all three links are attached, then let the subscription pumps go
	// live (events have no replay — a publish that races a subscription is lost).
	started.Wait()
	time.Sleep(750 * time.Millisecond)
	fmt.Println("[recv] 3 subscribers attached: g1a+g1b (group g1), g2 (group g2)")

	// Publish on a dedicated session. Pre-settled fire-and-forget.
	prodSess, err := conn.NewSession(ctx, nil)
	if err != nil {
		log.Fatalf("producer session: %v", err)
	}
	sender, err := prodSess.NewSender(ctx, addr, &amqp.SenderOptions{
		SettlementMode: amqp.SenderSettleModeSettled.Ptr(),
	})
	if err != nil {
		log.Fatalf("new sender: %v", err)
	}
	for i := 0; i < total; i++ {
		body := fmt.Sprintf("event-%03d", i)
		sendCtx, sendCancel := context.WithTimeout(ctx, 15*time.Second)
		err := sender.Send(sendCtx, amqp.NewMessage([]byte(body)), nil)
		sendCancel()
		if err != nil {
			log.Fatalf("publish %s: %v", body, err)
		}
	}
	_ = sender.Close(ctx)
	fmt.Printf("[send] Published %d events (pre-settled)\n", total)

	// Wait for the subscribers to drain their windows.
	done.Wait()

	// --- Assert the consumer-group semantics ---------------------------------

	// g2 (a distinct group) receives EVERY event.
	if len(g2.got) != total {
		log.Fatalf("group g2 (independent) expected all %d events, got %d", total, len(g2.got))
	}
	fmt.Printf("[recv] g2 (group g2, independent): %d/%d events — FULL stream\n", len(g2.got), total)

	// g1a + g1b TOGETHER receive every event, with NO body delivered to both.
	combined := make(map[string]struct{}, total)
	for body := range g1a.got {
		combined[body] = struct{}{}
	}
	dups := 0
	for body := range g1b.got {
		if _, both := g1a.got[body]; both {
			dups++
		}
		combined[body] = struct{}{}
	}
	if dups != 0 {
		log.Fatalf("group g1 load-balancing broken: %d event(s) delivered to BOTH g1a and g1b", dups)
	}
	if len(combined) != total {
		log.Fatalf("group g1 members together expected all %d events, got %d", total, len(combined))
	}
	if len(g1a.got) == 0 || len(g1b.got) == 0 {
		log.Fatalf("group g1 not load-balanced: g1a=%d g1b=%d (one member got nothing)", len(g1a.got), len(g1b.got))
	}
	fmt.Printf("[recv] g1a (group g1): %d events; g1b (group g1): %d events\n", len(g1a.got), len(g1b.got))
	fmt.Printf("[recv] g1a+g1b together: %d/%d events, 0 duplicates — group SPLIT the stream\n", len(combined), total)

	fmt.Println("\nDone.")
}

// Expected output (the channel carries a per-run timestamp suffix; the g1a/g1b
// split varies run to run; the totals are fixed):
//
// Broker:  amqp://localhost:5672
// Address: events/amqp10.examples.consumergroup.<ts>  (KubeMQ pattern=events, channel=amqp10.examples.consumergroup.<ts>)
//
// [recv] 3 subscribers attached: g1a+g1b (group g1), g2 (group g2)
// [send] Published 30 events (pre-settled)
// [recv] g2 (group g2, independent): 30/30 events — FULL stream
// [recv] g1a (group g1): 16 events; g1b (group g1): 14 events
// [recv] g1a+g1b together: 30/30 events, 0 duplicates — group SPLIT the stream
//
// Done.
