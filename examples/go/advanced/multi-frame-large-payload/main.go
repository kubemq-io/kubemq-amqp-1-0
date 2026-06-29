// Example: advanced/multi-frame-large-payload (master-table variant #11)
//
// A single AMQP 1.0 message whose body is larger than the connection's
// max-frame-size is fragmented across multiple TRANSFER frames (More:true …
// More:false) by the sender and reassembled bit-exact by the receiver — all
// transparently, with NO application-level chunking. This example drives that
// path against the KubeMQ AMQP 1.0 connector using the native
// github.com/Azure/go-amqp client (NO KubeMQ SDK).
//
// Flow:
//   - Dial with ConnOptions{MaxFrameSize:4096} on BOTH the producer and consumer
//     connections — a deliberately tiny 4 KiB frame so a ~1 MB body forces heavy
//     fragmentation in both directions.
//   - Sender → "queues/<ch>" (unsettled): one Send carries a ~1 MB Data body.
//     go-amqp splits it across many transfer frames; the connector reassembles
//     it and stores a single message.
//   - Receiver ← "queues/<ch>" Credit:1: one Receive yields the full body. The
//     example verifies the received length AND a CRC32 of the bytes match the
//     original — proving a bit-exact round-trip across the fragment boundary.
//
// Grounded in connector test TestQueueMultiFrameLargePayload
// (connectors/amqp10/integration_test.go).
//
// Run:
//
//	export KUBEMQ_AMQP_URL=amqp://localhost:5672
//	go run ./advanced/multi-frame-large-payload
package main

import (
	"context"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"time"

	amqp "github.com/Azure/go-amqp"
)

// channel is the KubeMQ queue channel; the link address is "queues/" + channel
// (explicit prefix — never rely on DefaultPattern).
const channel = "amqp10.examples.multiframe"

const (
	// payloadSize is ~1 MB — comfortably larger than maxFrameSize so the body
	// must span many transfer frames (More:true … More:false).
	payloadSize = 1 * 1024 * 1024
	// maxFrameSize is a deliberately tiny 4 KiB so the ~1 MB body fragments
	// across ~256 frames in each direction.
	maxFrameSize = 4096
)

func amqpURL() string {
	if v := os.Getenv("KUBEMQ_AMQP_URL"); v != "" {
		return v
	}
	return "amqp://localhost:5672"
}

// bodyBytes concatenates every Data body section into one byte slice. A Data
// body arrives as msg.Data ([][]byte) — one entry per Data section — so reading
// only msg.GetData() (which returns just Data[0]) would miss a multi-section
// body. This mirrors the Rust sibling's body_bytes() helper so the body
// extraction is identical across languages.
func bodyBytes(msg *amqp.Message) []byte {
	if len(msg.Data) == 0 {
		return nil
	}
	if len(msg.Data) == 1 {
		return msg.Data[0]
	}
	out := make([]byte, 0, len(msg.Data)*len(msg.Data[0]))
	for _, d := range msg.Data {
		out = append(out, d...)
	}
	return out
}

// drainStale removes any pre-existing messages on the shared canonical channel
// before the round-trip. The channel "queues/<channel>" is shared across runs
// and languages; an interrupted prior run can leave a stale (even zero-length)
// message at the head of the queue. Without draining, this example's Credit:1
// receiver would pull that stale message first — yielding len=0 — instead of
// the 1 MiB body it just sent. Draining makes the example verify its OWN
// message, matching the single-message-in-flight assumption every sibling
// language relies on.
func drainStale(ctx context.Context, addr string) {
	conn, err := amqp.Dial(ctx, amqpURL(), &amqp.ConnOptions{MaxFrameSize: maxFrameSize})
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		return
	}
	receiver, err := session.NewReceiver(ctx, addr, &amqp.ReceiverOptions{Credit: 50})
	if err != nil {
		return
	}
	drained := 0
	for {
		drainCtx, drainCancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		msg, err := receiver.Receive(drainCtx, nil)
		drainCancel()
		if err != nil {
			break // no more messages within the window
		}
		_ = receiver.AcceptMessage(ctx, msg)
		drained++
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = receiver.Close(closeCtx)
	closeCancel()
	if drained > 0 {
		fmt.Printf("[drain] Removed %d stale message(s) from %s before the round-trip\n", drained, addr)
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	addr := "queues/" + channel
	fmt.Printf("Broker:        %s\n", amqpURL())
	fmt.Printf("Address:       %s  (KubeMQ pattern=queues, channel=%s)\n", addr, channel)
	fmt.Printf("MaxFrameSize:  %d bytes\n", maxFrameSize)
	fmt.Printf("Payload:       %d bytes (~%d KiB)\n\n", payloadSize, payloadSize/1024)

	// Build a deterministic, non-trivial payload and remember its CRC + length so
	// we can prove a bit-exact round-trip after reassembly.
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i % 251) // 251 is prime → no short repeating period
	}
	wantLen := len(payload)
	wantCRC := crc32.ChecksumIEEE(payload)
	fmt.Printf("[prep] Built payload: len=%d crc32=0x%08x\n", wantLen, wantCRC)

	// Drain any stale messages left on the shared canonical channel by an earlier
	// run so the round-trip below verifies the message THIS run sends, not a
	// leftover (possibly zero-length) one ahead of it in the queue.
	drainStale(ctx, addr)

	// =========================================================================
	// 1. PRODUCER connection — OPEN with a tiny MaxFrameSize. The connector
	//    advertises its own max-frame-size in the OPEN reply; go-amqp uses the
	//    smaller of the two when fragmenting transfers.
	// =========================================================================
	prodConn, err := amqp.Dial(ctx, amqpURL(), &amqp.ConnOptions{MaxFrameSize: maxFrameSize})
	if err != nil {
		log.Fatalf("producer dial: %v", err)
	}
	defer func() { _ = prodConn.Close() }()

	prodSession, err := prodConn.NewSession(ctx, nil)
	if err != nil {
		log.Fatalf("producer session: %v", err)
	}

	// ATTACH a sender and send the whole body in ONE Send. go-amqp transparently
	// splits it across many transfer frames (More:true … final More:false). The
	// connector reassembles them into a single stored message.
	sender, err := prodSession.NewSender(ctx, addr, nil)
	if err != nil {
		log.Fatalf("new sender: %v", err)
	}
	sendCtx, sendCancel := context.WithTimeout(ctx, 60*time.Second)
	err = sender.Send(sendCtx, amqp.NewMessage(payload), nil)
	sendCancel()
	if err != nil {
		log.Fatalf("multi-frame send: %v", err)
	}
	_ = sender.Close(ctx)
	fmt.Printf("[send] Sent the %d-byte body in ONE Send (fragmented across ~%d frames, accepted)\n",
		wantLen, (wantLen/maxFrameSize)+1)

	// =========================================================================
	// 2. CONSUMER connection — same tiny MaxFrameSize so reassembly is exercised
	//    on the receive path too. One Receive yields the FULL reassembled body.
	// =========================================================================
	consConn, err := amqp.Dial(ctx, amqpURL(), &amqp.ConnOptions{MaxFrameSize: maxFrameSize})
	if err != nil {
		log.Fatalf("consumer dial: %v", err)
	}
	defer func() { _ = consConn.Close() }()

	consSession, err := consConn.NewSession(ctx, nil)
	if err != nil {
		log.Fatalf("consumer session: %v", err)
	}

	receiver, err := consSession.NewReceiver(ctx, addr, &amqp.ReceiverOptions{Credit: 1})
	if err != nil {
		log.Fatalf("new receiver: %v", err)
	}

	rcvCtx, rcvCancel := context.WithTimeout(ctx, 60*time.Second)
	msg, err := receiver.Receive(rcvCtx, nil)
	rcvCancel()
	if err != nil {
		log.Fatalf("multi-frame receive: %v", err)
	}
	if err := receiver.AcceptMessage(ctx, msg); err != nil {
		log.Fatalf("accept: %v", err)
	}

	// Reassemble the full body by concatenating every Data section (a multi-frame
	// Data body lands in msg.Data ([][]byte); GetData() would return only Data[0]).
	got := bodyBytes(msg)
	gotLen := len(got)
	gotCRC := crc32.ChecksumIEEE(got)
	fmt.Printf("[recv] Reassembled body: len=%d crc32=0x%08x\n", gotLen, gotCRC)

	// =========================================================================
	// 3. Verify the round-trip is bit-exact: length AND CRC32 must match.
	// =========================================================================
	if gotLen != wantLen {
		log.Fatalf("length mismatch: sent %d, received %d", wantLen, gotLen)
	}
	if gotCRC != wantCRC {
		log.Fatalf("CRC mismatch: sent 0x%08x, received 0x%08x", wantCRC, gotCRC)
	}
	fmt.Printf("[verify] Length and CRC32 match — multi-frame body round-tripped bit-exact\n")

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = receiver.Close(closeCtx)
	closeCancel()

	fmt.Println("\nDone.")
}

// Expected output:
//
// Broker:        amqp://localhost:5672
// Address:       queues/amqp10.examples.multiframe  (KubeMQ pattern=queues, channel=amqp10.examples.multiframe)
// MaxFrameSize:  4096 bytes
// Payload:       1048576 bytes (~1024 KiB)
//
// [prep] Built payload: len=1048576 crc32=0x........
// [send] Sent the 1048576-byte body in ONE Send (fragmented across ~257 frames, accepted)
// [recv] Reassembled body: len=1048576 crc32=0x........
// [verify] Length and CRC32 match — multi-frame body round-tripped bit-exact
//
// Done.
