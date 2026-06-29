// Example: advanced/anonymous-terminus (master-table variant #12)
//
// An ANONYMOUS sender (a link attached with a NULL target — NewSender(ctx, "",
// nil)) carries no fixed channel. Instead, EACH message selects its own
// destination via its `properties.to` field, and the KubeMQ connector routes it
// per-message to the right pattern/channel. One link, many destinations. Driven
// with the native github.com/Azure/go-amqp client (NO KubeMQ SDK).
//
// Flow:
//   - ATTACH an anonymous sender: NewSender(ctx, "", nil) → null target.
//   - Send #1: Message{Properties:{To: "queues/<ch>"}} routes to a queue.
//   - Send #2: Message{Properties:{To: "events/<ch>"}} routes to an events topic
//     (a subscriber is attached BEFORE the send — events are fire-and-forget).
//   - The queue message is then consumed back to prove it landed correctly.
//   - (Demonstrated as expected errors) a BAD `to` (unknown prefix) and a MISSING
//     `to` are both rejected by the connector: the Send returns an error carrying
//     amqp:precondition-failed.
//
// Per-message authorization: each anonymous send is authorized for WRITE on the
// resolved (pattern, channel) via the connector's Casbin policy — there is no
// per-link grant for an anonymous terminus.
//
// Grounded in connector test TestAnonymousTerminusRouting
// (connectors/amqp10/integration_test.go).
//
// Run:
//
//	export KUBEMQ_AMQP_URL=amqp://localhost:5672
//	go run ./advanced/anonymous-terminus
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/Azure/go-amqp"
)

// Explicit <pattern>/<channel> destinations selected per-message via properties.to.
const (
	queueChannel  = "amqp10.examples.anon.q"
	eventsChannel = "amqp10.examples.anon.e"
)

func amqpURL() string {
	if v := os.Getenv("KUBEMQ_AMQP_URL"); v != "" {
		return v
	}
	return "amqp://localhost:5672"
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	queueTo := "queues/" + queueChannel
	eventsTo := "events/" + eventsChannel
	fmt.Printf("Broker: %s\n", amqpURL())
	fmt.Printf("Anonymous sender (null target) — routes per-message via properties.to\n")
	fmt.Printf("  msg #1 to: %s\n", queueTo)
	fmt.Printf("  msg #2 to: %s\n\n", eventsTo)

	conn, err := amqp.Dial(ctx, amqpURL(), nil)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		log.Fatalf("new session: %v", err)
	}

	// =========================================================================
	// 1. ATTACH an anonymous sender. The empty target ("") attaches a link with
	//    a NULL target — there is no bound channel. Every message routes by its
	//    own properties.to.
	// =========================================================================
	anon, err := session.NewSender(ctx, "", nil)
	if err != nil {
		log.Fatalf("new anonymous sender: %v", err)
	}
	fmt.Printf("[attach] Anonymous sender attached (null target)\n")

	// A consumer for the EVENTS channel must be subscribed BEFORE we publish to
	// it — events are fire-and-forget (no replay). The queue message, by
	// contrast, is durable, so we consume it after sending.
	eventRcv, err := session.NewReceiver(ctx, eventsTo, &amqp.ReceiverOptions{Credit: 5})
	if err != nil {
		log.Fatalf("new events receiver: %v", err)
	}
	// Give the fresh subscription a moment to register before the publish.
	time.Sleep(500 * time.Millisecond)

	// =========================================================================
	// 2. Send #1 — route to a QUEUE via properties.to. The connector resolves
	//    "queues/<ch>", authorizes WRITE for this connection, and stores it.
	// =========================================================================
	qMsg := amqp.NewMessage([]byte("to-queue"))
	qMsg.Properties = &amqp.MessageProperties{To: &queueTo}
	if err := sendWith(ctx, anon, qMsg); err != nil {
		log.Fatalf("send to queue: %v", err)
	}
	fmt.Printf("[send] msg #1 routed to %s (accepted)\n", queueTo)

	// =========================================================================
	// 3. Send #2 — route to an EVENTS topic via properties.to. Same anonymous
	//    link, a DIFFERENT pattern. The subscriber attached above receives it.
	// =========================================================================
	eMsg := amqp.NewMessage([]byte("to-events"))
	eMsg.Properties = &amqp.MessageProperties{To: &eventsTo}
	if err := sendWith(ctx, anon, eMsg); err != nil {
		log.Fatalf("send to events: %v", err)
	}
	fmt.Printf("[send] msg #2 routed to %s (accepted)\n", eventsTo)

	// =========================================================================
	// 4. Negative cases (expected errors) — the connector rejects a bad/missing
	//    `to` with amqp:precondition-failed, surfaced to the client as a Send
	//    error. The anonymous link stays usable afterwards.
	// =========================================================================
	badTo := "bogus/prefix/x"
	badMsg := amqp.NewMessage([]byte("nowhere"))
	badMsg.Properties = &amqp.MessageProperties{To: &badTo}
	if err := sendWith(ctx, anon, badMsg); err != nil {
		fmt.Printf("[send] msg with bad `to`=%q rejected as expected: %v\n", badTo, err)
	} else {
		log.Fatal("expected a bad `to` to be rejected, but the send succeeded")
	}

	orphan := amqp.NewMessage([]byte("orphan")) // NO Properties.To at all
	if err := sendWith(ctx, anon, orphan); err != nil {
		fmt.Printf("[send] msg with NO `to` rejected as expected: %v\n", err)
	} else {
		log.Fatal("expected a missing `to` to be rejected, but the send succeeded")
	}
	_ = anon.Close(ctx)

	// =========================================================================
	// 5. Verify routing — consume the queue message back, and receive the event.
	// =========================================================================
	qRcv, err := session.NewReceiver(ctx, queueTo, &amqp.ReceiverOptions{Credit: 1})
	if err != nil {
		log.Fatalf("new queue receiver: %v", err)
	}
	rctx, rcancel := context.WithTimeout(ctx, 30*time.Second)
	qGot, err := qRcv.Receive(rctx, nil)
	rcancel()
	if err != nil {
		log.Fatalf("receive queue message: %v", err)
	}
	if err := qRcv.AcceptMessage(ctx, qGot); err != nil {
		log.Fatalf("accept queue message: %v", err)
	}
	fmt.Printf("[recv] queue %s delivered: %q\n", queueTo, string(qGot.GetData()))

	ectx, ecancel := context.WithTimeout(ctx, 30*time.Second)
	eGot, err := eventRcv.Receive(ectx, nil)
	ecancel()
	if err != nil {
		log.Fatalf("receive event message: %v", err)
	}
	fmt.Printf("[recv] events %s delivered: %q\n", eventsTo, string(eGot.GetData()))

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = qRcv.Close(closeCtx)
	_ = eventRcv.Close(closeCtx)
	closeCancel()

	fmt.Println("\nDone.")
}

// sendWith issues one Send with a per-call deadline. A connector rejection
// (precondition-failed) returns as an error; a context timeout is also possible
// if the broker never responds.
func sendWith(ctx context.Context, sender *amqp.Sender, msg *amqp.Message) error {
	sendCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	err := sender.Send(sendCtx, msg, nil)
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("send timed out (no disposition): %w", err)
	}
	return err
}

// Expected output:
//
// Broker: amqp://localhost:5672
// Anonymous sender (null target) — routes per-message via properties.to
//   msg #1 to: queues/amqp10.examples.anon.q
//   msg #2 to: events/amqp10.examples.anon.e
//
// [attach] Anonymous sender attached (null target)
// [send] msg #1 routed to queues/amqp10.examples.anon.q (accepted)
// [send] msg #2 routed to events/amqp10.examples.anon.e (accepted)
// [send] msg with bad `to`="bogus/prefix/x" rejected as expected: *Error{Condition: amqp:precondition-failed, ...}
// [send] msg with NO `to` rejected as expected: *Error{Condition: amqp:precondition-failed, ...}
// [recv] queue queues/amqp10.examples.anon.q delivered: "to-queue"
// [recv] events events/amqp10.examples.anon.e delivered: "to-events"
//
// Done.
