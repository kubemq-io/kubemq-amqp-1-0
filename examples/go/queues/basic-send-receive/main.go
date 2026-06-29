// Example: queues/basic-send-receive (master-table variant #1)
//
// At-least-once produce + credit-based consume against the KubeMQ AMQP 1.0
// connector using the native github.com/Azure/go-amqp client (NO KubeMQ SDK).
//
// Flow:
//   - Sender → "queues/<ch>" (unsettled): each Send waits for the server's
//     receiver DISPOSITION (accepted) before returning.
//   - Receiver ← "queues/<ch>" with Credit:10: Receive + AcceptMessage each ⇒
//     the connector emits an AckRange and removes the message from the queue.
//   - After draining, the queue is empty (a further Receive times out).
//
// Grounded in connector test TestQueueProduceConsumeAtLeastOnce
// (connectors/amqp10/integration_test.go).
//
// Run:
//
//	export KUBEMQ_AMQP_URL=amqp://localhost:5672
//	go run ./queues/basic-send-receive
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/Azure/go-amqp"
)

// channel is the KubeMQ queue channel; the link address is "queues/" + channel
// (explicit prefix — never rely on DefaultPattern).
const channel = "amqp10.examples.basic"

const total = 10

func amqpURL() string {
	if v := os.Getenv("KUBEMQ_AMQP_URL"); v != "" {
		return v
	}
	return "amqp://localhost:5672"
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	addr := "queues/" + channel
	fmt.Printf("Broker:  %s\n", amqpURL())
	fmt.Printf("Address: %s  (KubeMQ pattern=queues, channel=%s)\n\n", addr, channel)

	// OPEN: connect (SASL ANONYMOUS by default — no userinfo in the URL). A
	// non-empty container-id is sent automatically by go-amqp.
	conn, err := amqp.Dial(ctx, amqpURL(), nil)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// BEGIN: one session carries the producer + consumer links below.
	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		log.Fatalf("new session: %v", err)
	}

	// =========================================================================
	// 1. Produce — ATTACH a sender (server-receiver link). The server grants
	//    credit on attach; each Send is unsettled and blocks until the server
	//    DISPOSITION (accepted) confirms the broker stored the message.
	// =========================================================================
	sender, err := session.NewSender(ctx, addr, nil)
	if err != nil {
		log.Fatalf("new sender: %v", err)
	}
	for i := 0; i < total; i++ {
		body := fmt.Sprintf("msg-%03d", i)
		sendCtx, sendCancel := context.WithTimeout(ctx, 15*time.Second)
		err := sender.Send(sendCtx, amqp.NewMessage([]byte(body)), nil)
		sendCancel()
		if err != nil {
			log.Fatalf("send %s: %v", body, err)
		}
	}
	_ = sender.Close(ctx)
	fmt.Printf("[send] Produced %d messages to %s (accepted DISPOSITION each)\n", total, addr)

	// =========================================================================
	// 2. Consume — ATTACH a receiver (server-sender link). The CLIENT grants
	//    credit (Credit:10). Receive each message and AcceptMessage it ⇒ the
	//    connector AckRanges it (removed from the queue). go-amqp auto-issues
	//    fresh credit as messages settle.
	// =========================================================================
	receiver, err := session.NewReceiver(ctx, addr, &amqp.ReceiverOptions{Credit: 10})
	if err != nil {
		log.Fatalf("new receiver: %v", err)
	}

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
		seen[body] = struct{}{}
	}
	fmt.Printf("[recv] Consumed and accepted %d messages (no loss)\n", len(seen))

	// =========================================================================
	// 3. Assert the queue is empty — a further Receive must time out.
	// =========================================================================
	emptyCtx, emptyCancel := context.WithTimeout(ctx, 2*time.Second)
	_, err = receiver.Receive(emptyCtx, nil)
	emptyCancel()
	if err == nil {
		log.Fatal("expected an empty queue, but received another message")
	}
	fmt.Println("[recv] Queue drained to empty (no further messages)")

	// DETACH / CLOSE: clean up the receiver, then the connection (deferred).
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = receiver.Close(closeCtx)
	closeCancel()

	fmt.Println("\nDone.")
}

// Expected output:
//
// Broker:  amqp://localhost:5672
// Address: queues/amqp10.examples.basic  (KubeMQ pattern=queues, channel=amqp10.examples.basic)
//
// [send] Produced 10 messages to queues/amqp10.examples.basic (accepted DISPOSITION each)
// [recv] Consumed and accepted 10 messages (no loss)
// [recv] Queue drained to empty (no further messages)
//
// Done.
