// Example: queues/settlement-modes (master-table variant #3)
//
// The two producer reliability tiers, side by side, against the KubeMQ AMQP 1.0
// connector using the native github.com/Azure/go-amqp client (NO KubeMQ SDK):
//
//   - PRE-SETTLED sender (SenderSettleModeSettled): at-MOST-once. Each TRANSFER
//     is marked settled by the client, so Send returns WITHOUT waiting for a
//     server DISPOSITION. Fast and fire-and-forget — if the broker drops the
//     transfer (oversize, no capacity), the producer never learns. There is no
//     redelivery and no delivery confirmation.
//   - UNSETTLED sender (default): at-LEAST-once. Each Send blocks until the
//     connector returns an `accepted` DISPOSITION, confirming the broker stored
//     the message. This is the variant #1 contract.
//
// On the consume side this example requests ReceiverSettleModeFirst (the only
// receiver settle-mode the connector supports): the server settles the delivery
// on the first transfer. rcv-settle-mode=second is rejected by the connector
// with a DETACH carrying amqp:not-implemented (gotcha #7 — see README).
//
// Both senders' messages drain to the same consumer; the program proves no loss
// on this happy path while explaining the reliability difference.
//
// Grounded in connector test TestQueuePreSettled
// (connectors/amqp10/integration_test.go).
//
// Run:
//
//	export KUBEMQ_AMQP_URL=amqp://localhost:5672
//	go run ./queues/settlement-modes
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/Azure/go-amqp"
)

const channel = "amqp10.examples.settlement"

// We produce this many messages on each sender (pre-settled, then unsettled).
const perSender = 10

func amqpURL() string {
	if v := os.Getenv("KUBEMQ_AMQP_URL"); v != "" {
		return v
	}
	return "amqp://localhost:5672"
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	addr := "queues/" + channel
	fmt.Printf("Broker:  %s\n", amqpURL())
	fmt.Printf("Address: %s\n\n", addr)

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
	// 1. PRE-SETTLED sender (at-most-once). SenderSettleModeSettled marks every
	//    TRANSFER as already settled, so Send does NOT wait for a server
	//    DISPOSITION — it returns as soon as the frame is written. Fast, but no
	//    delivery confirmation and no redelivery.
	// =========================================================================
	settledSender, err := session.NewSender(ctx, addr, &amqp.SenderOptions{
		SettlementMode: amqp.SenderSettleModeSettled.Ptr(),
	})
	if err != nil {
		log.Fatalf("new pre-settled sender: %v", err)
	}
	for i := 0; i < perSender; i++ {
		body := fmt.Sprintf("presettled-%02d", i)
		sendCtx, sendCancel := context.WithTimeout(ctx, 15*time.Second)
		err := settledSender.Send(sendCtx, amqp.NewMessage([]byte(body)), nil)
		sendCancel()
		if err != nil {
			log.Fatalf("pre-settled send %s: %v", body, err)
		}
	}
	_ = settledSender.Close(ctx)
	fmt.Printf("[send] Pre-settled (at-most-once): produced %d messages — NO DISPOSITION awaited\n", perSender)

	// =========================================================================
	// 2. UNSETTLED sender (at-least-once — the default). Each Send blocks until
	//    the connector returns an `accepted` DISPOSITION confirming the broker
	//    stored the message. This is the variant #1 reliability contract.
	// =========================================================================
	unsettledSender, err := session.NewSender(ctx, addr, nil)
	if err != nil {
		log.Fatalf("new unsettled sender: %v", err)
	}
	for i := 0; i < perSender; i++ {
		body := fmt.Sprintf("unsettled-%02d", i)
		sendCtx, sendCancel := context.WithTimeout(ctx, 15*time.Second)
		err := unsettledSender.Send(sendCtx, amqp.NewMessage([]byte(body)), nil)
		sendCancel()
		if err != nil {
			log.Fatalf("unsettled send %s: %v", body, err)
		}
	}
	_ = unsettledSender.Close(ctx)
	fmt.Printf("[send] Unsettled (at-least-once): produced %d messages — each accepted DISPOSITION\n", perSender)

	// =========================================================================
	// 3. Consume with ReceiverSettleModeFirst. This is the ONLY receiver
	//    settle-mode the connector supports — the server settles on the first
	//    transfer. (rcv-settle-mode=second ⇒ DETACH amqp:not-implemented; see
	//    the README gotcha.) Accept each message to drain the queue.
	// =========================================================================
	receiver, err := session.NewReceiver(ctx, addr, &amqp.ReceiverOptions{
		Credit:         20,
		SettlementMode: amqp.ReceiverSettleModeFirst.Ptr(),
	})
	if err != nil {
		log.Fatalf("new receiver: %v", err)
	}

	total := 2 * perSender
	var presettledSeen, unsettledSeen int
	seen := make(map[string]struct{}, total)
	for len(seen) < total {
		rcvCtx, rcvCancel := context.WithTimeout(ctx, 30*time.Second)
		msg, err := receiver.Receive(rcvCtx, nil)
		rcvCancel()
		if err != nil {
			log.Fatalf("receive (%d/%d): %v", len(seen), total, err)
		}
		body := string(msg.GetData())
		if err := receiver.AcceptMessage(ctx, msg); err != nil {
			log.Fatalf("accept %s: %v", body, err)
		}
		if _, dup := seen[body]; !dup {
			seen[body] = struct{}{}
			switch {
			case len(body) >= 10 && body[:10] == "presettled":
				presettledSeen++
			default:
				unsettledSeen++
			}
		}
	}
	fmt.Printf("[recv] Drained %d total — %d pre-settled + %d unsettled (rcv-settle-mode=first)\n",
		len(seen), presettledSeen, unsettledSeen)

	// =========================================================================
	// 4. Assert the queue is empty — a further Receive must time out.
	// =========================================================================
	emptyCtx, emptyCancel := context.WithTimeout(ctx, 2*time.Second)
	_, err = receiver.Receive(emptyCtx, nil)
	emptyCancel()
	if err == nil {
		log.Fatal("expected an empty queue, but received another message")
	}
	fmt.Println("[recv] Queue drained to empty (no further messages)")

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = receiver.Close(closeCtx)
	closeCancel()

	fmt.Println("\nDone.")
}

// Expected output:
//
// Broker:  amqp://localhost:5672
// Address: queues/amqp10.examples.settlement
//
// [send] Pre-settled (at-most-once): produced 10 messages — NO DISPOSITION awaited
// [send] Unsettled (at-least-once): produced 10 messages — each accepted DISPOSITION
// [recv] Drained 20 total — 10 pre-settled + 10 unsettled (rcv-settle-mode=first)
// [recv] Queue drained to empty (no further messages)
//
// Done.
//
// (On a healthy broker pre-settled messages also drain — the difference is the
// PRODUCER guarantee, not the happy-path result: a pre-settled Send returns
// before any broker confirmation, so a drop on the way in is invisible to the
// producer. Unsettled sends block until the broker confirms storage.)
