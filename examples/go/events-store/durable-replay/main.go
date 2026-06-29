// Example: events-store/durable-replay (master-table variant #7)
//
// Durable subscriptions with resume over KubeMQ **Events Store** using the native
// github.com/Azure/go-amqp client (NO KubeMQ SDK).
//
// Unlike Events (fire-and-forget, no replay), Events Store PERSISTS the stream
// and lets a DURABLE subscriber resume where it left off. A durable subscription
// is identified by the pair:
//
//	(connection container-id, link name)
//
// To make a subscriber durable and resumable:
//
//   - dial with a STABLE container-id  → amqp.ConnOptions{ContainerID: "..."}
//   - attach with a STABLE link name   → amqp.ReceiverOptions{Name: "..."}
//   - request a non-expiring source    → amqp.ReceiverOptions{SourceExpiryPolicy: amqp.ExpiryPolicyNever}
//   - set the start position once       → Properties{"x-opt-kubemq-start": "new-only"}
//
// On a clean disconnect the connector preserves the durable position. Re-attaching
// with the SAME (container-id, link name) RESUMES the subscription and delivers
// every event published while the subscriber was away — no loss, no replay of
// already-consumed events.
//
// Flow:
//  1. Dial with container-id "amqp10-examples-durable-container"; attach durable
//     receiver "durable-sub" (start new-only). Publish 3 events; receive all 3.
//  2. Disconnect (close the connection).
//  3. Publish 5 MORE events while the durable subscriber is away.
//  4. Re-dial with the SAME container-id; re-attach "durable-sub". The
//     subscription RESUMES and delivers the 5 missed events.
//
// Grounded in connector test TestEventsStoreDurableReplay
// (connectors/amqp10/integration_pubsub_test.go).
//
// Run:
//
//	export KUBEMQ_AMQP_URL=amqp://localhost:5672
//	go run ./events-store/durable-replay
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/Azure/go-amqp"
)

const channel = "amqp10.examples.durable"

// The durable identity = (containerID, linkName). Both MUST be stable across
// reconnects for the subscription to resume.
const (
	containerID = "amqp10-examples-durable-container"
	linkName    = "durable-sub"
)

const standingCredit = 100

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
	fmt.Printf("Broker:        %s\n", amqpURL())
	fmt.Printf("Address:       %s  (KubeMQ pattern=events-store, channel=%s)\n", addr, channel)
	fmt.Printf("Durable id:    container-id=%q  link-name=%q\n\n", containerID, linkName)

	// =========================================================================
	// 0. PRODUCER — a separate, plain connection that publishes to the
	//    events-store stream throughout the demo (it does not need a stable id).
	// =========================================================================
	prodConn, err := amqp.Dial(ctx, amqpURL(), nil)
	if err != nil {
		log.Fatalf("dial producer: %v", err)
	}
	defer func() { _ = prodConn.Close() }()
	prodSess, err := prodConn.NewSession(ctx, nil)
	if err != nil {
		log.Fatalf("producer session: %v", err)
	}
	sender, err := prodSess.NewSender(ctx, addr, nil)
	if err != nil {
		log.Fatalf("new sender: %v", err)
	}
	defer func() { _ = sender.Close(context.Background()) }()

	// =========================================================================
	// 1. DURABLE SUBSCRIBE (first attach). Stable container-id + link name +
	//    non-expiring source make this subscription durable. start=new-only means
	//    "deliver events from now on" (this attach establishes the cursor).
	// =========================================================================
	durRcv, durConn := attachDurable(ctx, "first attach")
	publish(ctx, sender, 0, 3) // 3 events while the durable subscriber is live

	first := drain(ctx, durRcv, 3, 30*time.Second)
	if len(first) != 3 {
		log.Fatalf("durable subscriber expected the first 3 events, got %d: %v", len(first), first)
	}
	fmt.Printf("[recv] First attach received %d events: %v\n\n", len(first), first)

	// =========================================================================
	// 2. DISCONNECT. A clean Close detaches the durable link; the connector
	//    preserves the durable cursor for this (container-id, link name).
	// =========================================================================
	if err := durConn.Close(); err != nil {
		log.Fatalf("durable disconnect: %v", err)
	}
	fmt.Println("[conn] Durable subscriber DISCONNECTED (cursor preserved by the connector)")
	time.Sleep(1 * time.Second) // let the detach + unsubscribe settle

	// =========================================================================
	// 3. PUBLISH WHILE AWAY. 5 more events arrive at the persisted stream while
	//    the durable subscriber is offline.
	// =========================================================================
	publish(ctx, sender, 3, 8)
	fmt.Println("[send] Published 5 more events WHILE the durable subscriber was away")

	// =========================================================================
	// 4. RE-ATTACH with the SAME durable identity. The subscription RESUMES and
	//    delivers exactly the 5 events published while away (not the first 3
	//    again, and nothing lost).
	// =========================================================================
	durRcv2, durConn2 := attachDurable(ctx, "re-attach")
	defer func() { _ = durConn2.Close() }()

	resumed := drain(ctx, durRcv2, 5, 30*time.Second)
	resumedSet := make(map[string]struct{}, len(resumed))
	for _, b := range resumed {
		resumedSet[b] = struct{}{}
	}
	for i := 3; i < 8; i++ {
		body := fmt.Sprintf("es-%03d", i)
		if _, ok := resumedSet[body]; !ok {
			log.Fatalf("durable resume missing event %s (got %v)", body, resumed)
		}
	}
	fmt.Printf("[recv] Re-attach RESUMED and received the %d events published while away: %v\n", len(resumedSet), resumed)
	fmt.Println("[recv] No loss, no re-delivery of the already-consumed first 3 — the durable cursor resumed exactly")

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = durRcv2.Close(closeCtx)
	closeCancel()

	fmt.Println("\nDone.")
}

// attachDurable dials with the stable container-id and attaches the durable
// receiver (stable link name + non-expiring source + start position). The
// returned *amqp.Conn must be closed to disconnect the subscriber.
func attachDurable(ctx context.Context, phase string) (*amqp.Receiver, *amqp.Conn) {
	conn, err := amqp.Dial(ctx, amqpURL(), &amqp.ConnOptions{ContainerID: containerID})
	if err != nil {
		log.Fatalf("[%s] dial durable (container-id=%s): %v", phase, containerID, err)
	}
	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		log.Fatalf("[%s] durable session: %v", phase, err)
	}
	rcv, err := session.NewReceiver(ctx, "events-store/"+channel, &amqp.ReceiverOptions{
		Credit:             standingCredit,
		SourceExpiryPolicy: amqp.ExpiryPolicyNever,                           // never expire the durable source
		Name:               linkName,                                         // stable link name = half the durable identity
		Properties:         map[string]any{"x-opt-kubemq-start": "new-only"}, // start cursor (honoured on first attach)
	})
	if err != nil {
		log.Fatalf("[%s] attach durable receiver (name=%s): %v", phase, linkName, err)
	}
	fmt.Printf("[recv] Durable receiver attached (%s): container-id=%q name=%q expiry=never\n", phase, containerID, linkName)
	// Let the connector's subscription pump go live before producing.
	time.Sleep(750 * time.Millisecond)
	return rcv, conn
}

// publish sends events es-<lo>..es-<hi-1> on the producer sender (unsettled —
// events-store persists each accepted transfer).
func publish(ctx context.Context, sender *amqp.Sender, lo, hi int) {
	for i := lo; i < hi; i++ {
		body := fmt.Sprintf("es-%03d", i)
		sendCtx, sendCancel := context.WithTimeout(ctx, 15*time.Second)
		err := sender.Send(sendCtx, amqp.NewMessage([]byte(body)), nil)
		sendCancel()
		if err != nil {
			log.Fatalf("publish %s: %v", body, err)
		}
	}
}

// drain receives up to max events within window, returning their bodies.
func drain(ctx context.Context, rcv *amqp.Receiver, max int, window time.Duration) []string {
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

// Expected output:
//
// Broker:        amqp://localhost:5672
// Address:       events-store/amqp10.examples.durable  (KubeMQ pattern=events-store, channel=amqp10.examples.durable)
// Durable id:    container-id="amqp10-examples-durable-container"  link-name="durable-sub"
//
// [recv] Durable receiver attached (first attach): container-id="amqp10-examples-durable-container" name="durable-sub" expiry=never
// [recv] First attach received 3 events: [es-000 es-001 es-002]
//
// [conn] Durable subscriber DISCONNECTED (cursor preserved by the connector)
// [send] Published 5 more events WHILE the durable subscriber was away
// [recv] Durable receiver attached (re-attach): container-id="amqp10-examples-durable-container" name="durable-sub" expiry=never
// [recv] Re-attach RESUMED and received the 5 events published while away: [es-003 es-004 es-005 es-006 es-007]
// [recv] No loss, no re-delivery of the already-consumed first 3 — the durable cursor resumed exactly
//
// Done.
//
// NOTE: durable subscriptions are NODE-LOCAL (see README gotcha). In a cluster
// the durable cursor lives on the node that owned the original attach; reconnect
// to the SAME node (or run a single-node dev broker, as here) to resume.
