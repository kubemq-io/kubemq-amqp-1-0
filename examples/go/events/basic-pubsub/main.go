// Example: events/basic-pubsub (master-table variant #4)
//
// Fan-out, at-most-once pub/sub over KubeMQ **Events** with the native
// github.com/Azure/go-amqp client (NO KubeMQ SDK).
//
// Events are a fire-hose: deliveries are pre-settled (no DISPOSITION feedback),
// there is NO replay, and a message that arrives at a subscriber with zero
// credit is SILENTLY DROPPED (counted by the server metric
// kubemq_amqp10_events_dropped_no_credit_total). Two rules follow:
//
//   - SUBSCRIBE BEFORE PUBLISH. The attach reply only confirms the link, not
//     that the connector's subscription pump is live. A publish that races the
//     subscription is lost (no replay). This example sleeps ~750ms after attach
//     before producing.
//   - GRANT STANDING CREDIT. The receiver attaches with a large standing credit
//     (and go-amqp auto-replenishes as messages settle) so the subscriber is
//     never at 0 credit when an event arrives.
//
// The sender publishes pre-settled to events/<ch> (fire-and-forget); the
// receiver drains every event on the happy path.
//
// Grounded in connector test TestEventsPubSubGroupFanout (the lone-subscriber
// fan-out leg) (connectors/amqp10/integration_pubsub_test.go).
//
// Run:
//
//	export KUBEMQ_AMQP_URL=amqp://localhost:5672
//	go run ./events/basic-pubsub
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/Azure/go-amqp"
)

const channel = "amqp10.examples.pubsub"

const total = 20

// standingCredit is granted up front so the subscriber is never at 0 credit when
// an event arrives. go-amqp auto-replenishes as deliveries settle.
const standingCredit = 100

func amqpURL() string {
	if v := os.Getenv("KUBEMQ_AMQP_URL"); v != "" {
		return v
	}
	return "amqp://localhost:5672"
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	addr := "events/" + channel
	fmt.Printf("Broker:  %s\n", amqpURL())
	fmt.Printf("Address: %s  (KubeMQ pattern=events, channel=%s)\n\n", addr, channel)

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
	// 1. SUBSCRIBE FIRST. Attach the receiver with standing credit BEFORE any
	//    publish. Events have no replay — a publish that beats the subscription
	//    is lost forever.
	// =========================================================================
	receiver, err := session.NewReceiver(ctx, addr, &amqp.ReceiverOptions{Credit: standingCredit})
	if err != nil {
		log.Fatalf("new receiver: %v", err)
	}
	fmt.Printf("[recv] Subscribed to %s with standing credit %d\n", addr, standingCredit)

	// The attach reply confirms the link, not that the connector's subscription
	// pump has run its SubscribeEvents yet. Wait for the pump to go live before
	// publishing, or the first events race the subscription and are dropped.
	time.Sleep(750 * time.Millisecond)
	fmt.Println("[recv] Subscription pump settled (waited 750ms before publishing)")

	// =========================================================================
	// 2. PUBLISH pre-settled. The sender marks every TRANSFER as settled
	//    (fire-and-forget) — events are at-most-once, so there is no DISPOSITION
	//    to await and no produce confirmation.
	// =========================================================================
	sender, err := session.NewSender(ctx, addr, &amqp.SenderOptions{
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
	fmt.Printf("[send] Published %d events (pre-settled, fire-and-forget)\n", total)

	// =========================================================================
	// 3. RECEIVE. With standing credit the subscriber drains every event. Accept
	//    is a no-op on pre-settled pub/sub deliveries but is harmless.
	// =========================================================================
	seen := make(map[string]struct{}, total)
	for len(seen) < total {
		rcvCtx, rcvCancel := context.WithTimeout(ctx, 30*time.Second)
		msg, err := receiver.Receive(rcvCtx, nil)
		rcvCancel()
		if err != nil {
			log.Fatalf("receive (%d/%d): %v", len(seen), total, err)
		}
		_ = receiver.AcceptMessage(ctx, msg) // no-op for pre-settled fan-out
		seen[string(msg.GetData())] = struct{}{}
	}
	fmt.Printf("[recv] Received all %d events (continuous credit ⇒ no 0-credit drop)\n", len(seen))

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = receiver.Close(closeCtx)
	closeCancel()

	fmt.Println("\nDone.")
}

// Expected output:
//
// Broker:  amqp://localhost:5672
// Address: events/amqp10.examples.pubsub  (KubeMQ pattern=events, channel=amqp10.examples.pubsub)
//
// [recv] Subscribed to events/amqp10.examples.pubsub with standing credit 100
// [recv] Subscription pump settled (waited 750ms before publishing)
// [send] Published 20 events (pre-settled, fire-and-forget)
// [recv] Received all 20 events (continuous credit ⇒ no 0-credit drop)
//
// Done.
//
// (Events are at-most-once with no replay: if the subscriber were at 0 credit
// when an event arrived, that event would be SILENTLY DROPPED and counted on the
// server metric kubemq_amqp10_events_dropped_no_credit_total — never surfaced as
// a client error. Standing credit + subscribe-before-publish avoid both losses.)
