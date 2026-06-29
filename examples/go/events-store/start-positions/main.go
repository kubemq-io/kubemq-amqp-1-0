// Example: events-store/start-positions (master-table variant #8)
//
// The x-opt-kubemq-start link property over KubeMQ **Events Store** using the
// native github.com/Azure/go-amqp client (NO KubeMQ SDK).
//
// Events Store persists the stream, so a (non-durable) subscriber can choose
// WHERE in the history to start consuming via the x-opt-kubemq-start receiver
// link property. The full grammar (parsed by the connector's parseEventsStoreStart):
//
//	(absent) / "new-only"  -> deliver only events published AFTER attach
//	"first"                -> replay the ENTIRE history from the beginning
//	"last"                 -> start at the last stored event
//	"sequence:<n>"         -> start at store sequence n (1-BASED; sequence 1 = the
//	                          first stored event — the connector passes n straight
//	                          to NATS streaming's StartAtSequence)
//	"time:<RFC3339|secs>"  -> start at a wall-clock instant (RFC3339 or unix-seconds)
//	"time-delta:<secs>"    -> start <secs> seconds ago (relative to now)
//
// IMPORTANT — time encoding: the client sends a `time:` value as RFC3339 OR as
// unix SECONDS; the connector parses BOTH to the same instant and the broker
// stores the cursor as unix NANOSECONDS. `time-delta:` is seconds verbatim. A
// malformed value (e.g. "sequence:abc", "time:not-a-time", "whenever") is
// rejected at ATTACH with amqp:invalid-field. There is NO native "last N by
// count" form — to read the tail, compute a sequence or a time window.
//
// This program seeds 6 events, then demonstrates four start positions on fresh
// (non-durable) receivers against the SAME persisted stream:
//
//	first              -> all 6 (full replay)
//	sequence:4         -> from the 4th stored event onward (1-based ⇒ es-003,004,005)
//	time-delta:3600    -> all 6 (all were published within the last hour)
//	new-only           -> none of the existing 6; only events published after attach
//
// Grounded in connector tests TestEventsStoreDurableReplay (the start:first leg)
// and TestParseEventsStoreStart (connectors/amqp10/link_pubsub_test.go), and the
// grammar in connectors/amqp10/link.go (parseEventsStoreStart).
//
// Run:
//
//	export KUBEMQ_AMQP_URL=amqp://localhost:5672
//	go run ./events-store/start-positions
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/Azure/go-amqp"
)

// A fresh channel per run keeps the sequence numbers deterministic (this demo
// reads by absolute sequence, which is per-channel and monotonic from 0).
var channel = fmt.Sprintf("amqp10.examples.startpos.%d", time.Now().UnixNano())

const (
	startProp      = "x-opt-kubemq-start"
	seedCount      = 6
	standingCredit = 100
)

func amqpURL() string {
	if v := os.Getenv("KUBEMQ_AMQP_URL"); v != "" {
		return v
	}
	return "amqp://localhost:5672"
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	addr := "events-store/" + channel
	fmt.Printf("Broker:  %s\n", amqpURL())
	fmt.Printf("Address: %s  (KubeMQ pattern=events-store, channel=%s)\n\n", addr, channel)

	conn, err := amqp.Dial(ctx, amqpURL(), nil)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// =========================================================================
	// 0. SEED — publish 6 events into the persisted events-store stream. They are
	//    stored at sequences 0..5 (per-channel, monotonic).
	// =========================================================================
	prodSess, err := conn.NewSession(ctx, nil)
	if err != nil {
		log.Fatalf("producer session: %v", err)
	}
	sender, err := prodSess.NewSender(ctx, addr, nil)
	if err != nil {
		log.Fatalf("new sender: %v", err)
	}
	for i := 0; i < seedCount; i++ {
		body := fmt.Sprintf("es-%03d", i)
		sendCtx, sendCancel := context.WithTimeout(ctx, 15*time.Second)
		if err := sender.Send(sendCtx, amqp.NewMessage([]byte(body)), nil); err != nil {
			sendCancel()
			log.Fatalf("seed publish %s: %v", body, err)
		}
		sendCancel()
	}
	_ = sender.Close(ctx)
	fmt.Printf("[seed] Published %d events (stored at 1-based sequences 1..%d)\n\n", seedCount, seedCount)

	// =========================================================================
	// 1. start=first → FULL REPLAY (all 6 events from the beginning).
	// =========================================================================
	got := readFrom(ctx, conn, addr, "first", seedCount, 15*time.Second)
	expectExactly(got, []string{"es-000", "es-001", "es-002", "es-003", "es-004", "es-005"}, "first")
	fmt.Printf("[start=first]          replayed full history: %v\n", got)

	// =========================================================================
	// 2. start=sequence:4 → from the 4th stored event onward. Sequences are
	//    1-BASED (the connector passes the value straight to NATS streaming's
	//    StartAtSequence; sequence 1 = the first event), so the 4th stored event
	//    is es-003, delivering es-003, es-004, es-005.
	// =========================================================================
	got = readFrom(ctx, conn, addr, "sequence:4", seedCount, 15*time.Second)
	expectExactly(got, []string{"es-003", "es-004", "es-005"}, "sequence:4")
	fmt.Printf("[start=sequence:4]     from the 4th stored event (1-based): %v\n", got)

	// =========================================================================
	// 3. start=time-delta:3600 → everything from the last hour (all 6, since the
	//    seed was published seconds ago). time-delta is SECONDS verbatim.
	// =========================================================================
	got = readFrom(ctx, conn, addr, "time-delta:3600", seedCount, 15*time.Second)
	expectExactly(got, []string{"es-000", "es-001", "es-002", "es-003", "es-004", "es-005"}, "time-delta:3600")
	fmt.Printf("[start=time-delta:3600] last hour (all 6): %v\n", got)

	// (You can also start at an absolute instant, e.g.
	//   Properties{startProp: "time:" + time.Now().Add(-time.Hour).Format(time.RFC3339)}
	// or with unix-seconds: "time:1623578400". Both forms resolve to the same
	// instant; the broker stores the cursor as nanoseconds.)

	// =========================================================================
	// 4. start=new-only → NONE of the 6 existing events; only what is published
	//    AFTER this attach. We attach, then publish one more event and prove only
	//    that one is delivered.
	// =========================================================================
	demoNewOnly(ctx, conn, addr)

	// =========================================================================
	// 5. GOTCHA — a malformed start value is rejected at ATTACH with
	//    amqp:invalid-field.
	// =========================================================================
	fmt.Println()
	demoMalformed(ctx, conn, addr, "sequence:abc")
	demoMalformed(ctx, conn, addr, "whenever")

	fmt.Println("\nDone.")
}

// readFrom opens a fresh (non-durable) receiver at the given start position and
// drains up to max events within window, returning their bodies in order.
func readFrom(ctx context.Context, conn *amqp.Conn, addr, start string, max int, window time.Duration) []string {
	sess, err := conn.NewSession(ctx, nil)
	if err != nil {
		log.Fatalf("[start=%s] session: %v", start, err)
	}
	rcv, err := sess.NewReceiver(ctx, addr, &amqp.ReceiverOptions{
		Credit:     standingCredit,
		Properties: map[string]any{startProp: start},
	})
	if err != nil {
		log.Fatalf("[start=%s] attach receiver: %v", start, err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = rcv.Close(closeCtx)
		closeCancel()
	}()

	out := make([]string, 0, max)
	deadline := time.Now().Add(window)
	for len(out) < max && time.Now().Before(deadline) {
		rcvCtx, rcvCancel := context.WithTimeout(ctx, time.Until(deadline))
		msg, err := rcv.Receive(rcvCtx, nil)
		rcvCancel()
		if err != nil {
			break
		}
		_ = rcv.AcceptMessage(ctx, msg)
		out = append(out, string(msg.GetData()))
	}
	return out
}

// demoNewOnly attaches a new-only receiver, then publishes one event and proves
// ONLY the post-attach event is delivered (the 6 existing events are skipped).
func demoNewOnly(ctx context.Context, conn *amqp.Conn, addr string) {
	sess, err := conn.NewSession(ctx, nil)
	if err != nil {
		log.Fatalf("[start=new-only] session: %v", err)
	}
	rcv, err := sess.NewReceiver(ctx, addr, &amqp.ReceiverOptions{
		Credit:     standingCredit,
		Properties: map[string]any{startProp: "new-only"},
	})
	if err != nil {
		log.Fatalf("[start=new-only] attach receiver: %v", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = rcv.Close(closeCtx)
		closeCancel()
	}()
	time.Sleep(750 * time.Millisecond) // let the new-only cursor settle before publishing

	// Publish one fresh event AFTER the new-only attach.
	prodSess, err := conn.NewSession(ctx, nil)
	if err != nil {
		log.Fatalf("[start=new-only] producer session: %v", err)
	}
	sender, err := prodSess.NewSender(ctx, addr, nil)
	if err != nil {
		log.Fatalf("[start=new-only] sender: %v", err)
	}
	const fresh = "es-new-after-attach"
	sendCtx, sendCancel := context.WithTimeout(ctx, 15*time.Second)
	if err := sender.Send(sendCtx, amqp.NewMessage([]byte(fresh)), nil); err != nil {
		sendCancel()
		log.Fatalf("[start=new-only] publish: %v", err)
	}
	sendCancel()
	_ = sender.Close(ctx)

	// Only the post-attach event must arrive.
	rcvCtx, rcvCancel := context.WithTimeout(ctx, 15*time.Second)
	msg, err := rcv.Receive(rcvCtx, nil)
	rcvCancel()
	if err != nil {
		log.Fatalf("[start=new-only] expected the post-attach event, got: %v", err)
	}
	_ = rcv.AcceptMessage(ctx, msg)
	if got := string(msg.GetData()); got != fresh {
		log.Fatalf("[start=new-only] expected %q (post-attach), but got %q (an existing event leaked)", fresh, got)
	}

	// Nothing else (the 6 existing events must NOT be delivered).
	emptyCtx, emptyCancel := context.WithTimeout(ctx, 2*time.Second)
	extra, err := rcv.Receive(emptyCtx, nil)
	emptyCancel()
	if err == nil {
		log.Fatalf("[start=new-only] an existing event %q leaked (new-only must skip history)", string(extra.GetData()))
	}
	fmt.Printf("[start=new-only]       skipped all %d existing events; delivered only the post-attach event: [%s]\n", seedCount, fresh)
}

// demoMalformed proves a bad start value is rejected at ATTACH with
// amqp:invalid-field (the receiver never attaches).
func demoMalformed(ctx context.Context, conn *amqp.Conn, addr, badStart string) {
	sess, err := conn.NewSession(ctx, nil)
	if err != nil {
		log.Fatalf("[malformed] session: %v", err)
	}
	_, err = sess.NewReceiver(ctx, addr, &amqp.ReceiverOptions{
		Credit:     standingCredit,
		Properties: map[string]any{startProp: badStart},
	})
	if err == nil {
		log.Fatalf("[malformed] expected %q to be rejected, but the attach succeeded", badStart)
	}
	fmt.Printf("[gotcha] start=%q correctly REJECTED at ATTACH: %v\n", badStart, err)
}

// expectExactly fails fatally unless got contains exactly the want set.
func expectExactly(got, want []string, label string) {
	if len(got) != len(want) {
		log.Fatalf("[start=%s] expected %d events %v, got %d: %v", label, len(want), want, len(got), got)
	}
	set := make(map[string]struct{}, len(got))
	for _, b := range got {
		set[b] = struct{}{}
	}
	for _, w := range want {
		if _, ok := set[w]; !ok {
			log.Fatalf("[start=%s] missing expected event %q (got %v)", label, w, got)
		}
	}
}

// Expected output (the channel suffix is a timestamp, so it varies per run):
//
// Broker:  amqp://localhost:5672
// Address: events-store/amqp10.examples.startpos.<ts>  (KubeMQ pattern=events-store, channel=amqp10.examples.startpos.<ts>)
//
// [seed] Published 6 events (stored at 1-based sequences 1..6)
//
// [start=first]          replayed full history: [es-000 es-001 es-002 es-003 es-004 es-005]
// [start=sequence:4]     from the 4th stored event (1-based): [es-003 es-004 es-005]
// [start=time-delta:3600] last hour (all 6): [es-000 es-001 es-002 es-003 es-004 es-005]
// [start=new-only]       skipped all 6 existing events; delivered only the post-attach event: [es-new-after-attach]
//
// [gotcha] start="sequence:abc" correctly REJECTED at ATTACH: received detach frame with unknown link handle 0
// [gotcha] start="whenever" correctly REJECTED at ATTACH: received detach frame with unknown link handle 0
//
// Done.
//
// The connector DETACHes the bad attach with amqp:invalid-field (description
// "invalid start sequence: abc" / "unknown start position: whenever"). go-amqp
// v1.7.0 reports a detach that races link registration as the generic message
// above; either way NewReceiver returns an error and the receiver never attaches.
