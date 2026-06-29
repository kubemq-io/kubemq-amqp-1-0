// Example: queues/ack-release-redelivery (master-table variant #2)
//
// The three queue settlement outcomes, side by side, against the KubeMQ AMQP 1.0
// connector using the native github.com/Azure/go-amqp client (NO KubeMQ SDK):
//
//   - release (ReleaseMessage) ⇒ NAckRange: the message is requeued to the tail
//     and REDELIVERED with a grown delivery-count (Header.DeliveryCount >= 1) and
//     FirstAcquirer=false. Each release also increments the broker receive-count
//     toward MaxReceiveQueue (see the gotcha below).
//   - reject  (RejectMessage)  ⇒ AckRange/discard: the message is removed and NOT
//     redelivered to this receiver (poison handling is a broker MaxReceiveQueue
//     policy — there is no connector DLX).
//   - accept  (AcceptMessage)  ⇒ AckRange: the message is removed (success).
//
// Grounded in connector tests TestQueueReleasedRedelivery and
// TestQueueRejectedDiscard (connectors/amqp10/integration_test.go).
//
// Run:
//
//	export KUBEMQ_AMQP_URL=amqp://localhost:5672
//	go run ./queues/ack-release-redelivery
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/Azure/go-amqp"
)

// A per-run-unique suffix keeps the redelivery / delivery-count assertions
// deterministic: this example releases a message (which requeues it) and reads
// its grown delivery-count, so a leftover copy from a previous run or a
// concurrent runner on a shared channel would skew the counts. A fresh channel
// per run avoids that without any cross-run cleanup (mirrors the JS sibling).
var channel = fmt.Sprintf("amqp10.examples.ack.%d", time.Now().UnixNano())

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

	// Produce three distinct messages: one we release, one we reject, one we accept.
	sender, err := session.NewSender(ctx, addr, nil)
	if err != nil {
		log.Fatalf("new sender: %v", err)
	}
	for _, body := range []string{"release-me", "reject-me", "accept-me"} {
		sendCtx, sendCancel := context.WithTimeout(ctx, 15*time.Second)
		err := sender.Send(sendCtx, amqp.NewMessage([]byte(body)), nil)
		sendCancel()
		if err != nil {
			log.Fatalf("send %s: %v", body, err)
		}
	}
	_ = sender.Close(ctx)
	fmt.Println("[send] Produced: release-me, reject-me, accept-me")

	receiver, err := session.NewReceiver(ctx, addr, &amqp.ReceiverOptions{Credit: 10})
	if err != nil {
		log.Fatalf("new receiver: %v", err)
	}

	// Track which terminal outcome we still owe each body. A released message is
	// redelivered, so "release-me" appears twice (released, then accepted).
	remaining := map[string]struct{}{"release-me": {}, "reject-me": {}, "accept-me": {}}
	releasedOnce := false

	for len(remaining) > 0 {
		rcvCtx, rcvCancel := context.WithTimeout(ctx, 30*time.Second)
		msg, err := receiver.Receive(rcvCtx, nil)
		rcvCancel()
		if err != nil {
			log.Fatalf("receive: %v", err)
		}
		body := string(msg.GetData())
		dc, first := deliveryInfo(msg)

		switch {
		case body == "release-me" && !releasedOnce:
			// First sight: RELEASE it back to the queue tail (NAckRange).
			if err := receiver.ReleaseMessage(ctx, msg); err != nil {
				log.Fatalf("release: %v", err)
			}
			releasedOnce = true
			fmt.Printf("[recv] %-12s delivery-count=%d first-acquirer=%v  -> RELEASED (requeued)\n", body, dc, first)

		case body == "release-me":
			// Redelivery: grown delivery-count, no longer first-acquirer. Accept it now.
			if dc < 1 || first {
				log.Fatalf("expected redelivered copy to have delivery-count>=1 and first-acquirer=false, got dc=%d first=%v", dc, first)
			}
			if err := receiver.AcceptMessage(ctx, msg); err != nil {
				log.Fatalf("accept redelivered: %v", err)
			}
			fmt.Printf("[recv] %-12s delivery-count=%d first-acquirer=%v  -> REDELIVERED, then ACCEPTED\n", body, dc, first)
			delete(remaining, body)

		case body == "reject-me":
			// REJECT it (AckRange/discard). It will NOT be redelivered here.
			rejectErr := &amqp.Error{Condition: amqp.ErrCondInternalError, Description: "example rejection"}
			if err := receiver.RejectMessage(ctx, msg, rejectErr); err != nil {
				log.Fatalf("reject: %v", err)
			}
			fmt.Printf("[recv] %-12s delivery-count=%d first-acquirer=%v  -> REJECTED (discarded, no requeue)\n", body, dc, first)
			delete(remaining, body)

		default: // "accept-me"
			if err := receiver.AcceptMessage(ctx, msg); err != nil {
				log.Fatalf("accept: %v", err)
			}
			fmt.Printf("[recv] %-12s delivery-count=%d first-acquirer=%v  -> ACCEPTED (removed)\n", body, dc, first)
			delete(remaining, body)
		}
	}

	// The rejected body must NOT come back to this receiver.
	emptyCtx, emptyCancel := context.WithTimeout(ctx, 2*time.Second)
	if _, err := receiver.Receive(emptyCtx, nil); err == nil {
		log.Fatal("rejected message was unexpectedly redelivered")
	}
	emptyCancel()
	fmt.Println("[recv] Rejected message was not redelivered (discarded)")

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = receiver.Close(closeCtx)
	closeCancel()

	fmt.Println("\nDone.")
}

// deliveryInfo extracts the AMQP header.delivery-count and first-acquirer flag.
// The connector maps the KubeMQ broker receive-count onto these header fields:
// delivery-count = ReceiveCount-1, first-acquirer = (ReceiveCount==1).
func deliveryInfo(msg *amqp.Message) (deliveryCount uint32, firstAcquirer bool) {
	if msg.Header != nil {
		return msg.Header.DeliveryCount, msg.Header.FirstAcquirer
	}
	return 0, true
}

// Expected output (the channel carries a per-run timestamp suffix):
//
// Broker:  amqp://localhost:5672
// Address: queues/amqp10.examples.ack.<ts>
//
// [send] Produced: release-me, reject-me, accept-me
// [recv] release-me   delivery-count=0 first-acquirer=true  -> RELEASED (requeued)
// [recv] reject-me    delivery-count=0 first-acquirer=true  -> REJECTED (discarded, no requeue)
// [recv] accept-me    delivery-count=0 first-acquirer=true  -> ACCEPTED (removed)
// [recv] release-me   delivery-count=1 first-acquirer=false -> REDELIVERED, then ACCEPTED
// [recv] Rejected message was not redelivered (discarded)
//
// Done.
//
// (Delivery order between the original and the redelivered copy can vary; the
// redelivered "release-me" always carries delivery-count>=1 / first-acquirer=false.)
