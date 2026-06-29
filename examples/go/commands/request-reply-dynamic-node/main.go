// Example: commands/request-reply-dynamic-node (master-table variant #9)
//
// Native AMQP 1.0 request/reply over KubeMQ **Commands** (RPC) with the native
// github.com/Azure/go-amqp client — NO kubemq-go, NO gRPC. The whole round-trip
// stays in-protocol over a single broker connection per role.
//
// The mechanism (spec §2.4/§6.5; connector connectors/amqp10/rpc.go + dynamic.go):
//
//   - REQUESTER opens a DYNAMIC reply node: NewReceiver(ctx, "", {DynamicAddress:
//     true}) — the server creates a transient node and echoes its address back,
//     read with replyRcv.Address() (a "_amqp10.tmp.<connID>.<uuid>" token). The
//     requester sends the command to commands/<ch> carrying Properties.ReplyTo =
//     that node + Properties.CorrelationID. The connector verifies the reply-to
//     names a node THIS connection owns (snooping guard: a reply-to that does not
//     resolve to a connection-owned node is refused with amqp:not-allowed) and
//     routes the request to SendCommand. The broker Response is delivered
//     out-of-band onto the dynamic node; the requester correlates it by
//     correlation-id (the connector falls back to message-id when absent).
//
//   - RESPONDER receives requests on commands/<ch> (a server-sender link pumped
//     under credit) and replies via an ANONYMOUS sender — NewSender(ctx, "", nil)
//     (null target) — setting Properties.To = the request's ReplyTo (the connector
//     stamps that as "/responses/<RequestID>") + the echoed CorrelationID. A
//     command reply ALSO carries ApplicationProperties:
//     x-opt-kubemq-executed (bool) + x-opt-kubemq-error (string).
//
// Commands vs Queries (the #9 vs #10 contrast): a command that FAILS still
// produces a reply (executed=false + error text) so the requester is NEVER left
// waiting. This example demonstrates BOTH: a successful command (executed=true)
// and a failed command (executed=false) — both round-trip, neither hangs.
//
// Grounded in connector tests TestRPCRequesterResponderViaAMQP10 (commands leg)
// and TestRPCInteropAMQP10RequesterGRPCResponder (the executed/error app-props)
// (connectors/amqp10/integration_test.go).
//
// Run:
//
//	export KUBEMQ_AMQP_URL=amqp://localhost:5672
//	go run ./commands/request-reply-dynamic-node
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	amqp "github.com/Azure/go-amqp"
)

// channel is the KubeMQ commands channel; the link address is "commands/" + channel
// (explicit prefix — never rely on a default pattern).
const channel = "amqp10.examples.commands"

func amqpURL() string {
	if v := os.Getenv("KUBEMQ_AMQP_URL"); v != "" {
		return v
	}
	return "amqp://localhost:5672"
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	addr := "commands/" + channel
	fmt.Printf("Broker:  %s\n", amqpURL())
	fmt.Printf("Address: %s  (KubeMQ pattern=commands, channel=%s)\n\n", addr, channel)

	// Two connections: one per role. The responder runs in a goroutine so this
	// single program is runnable standalone against the broker; honoring the
	// "one *Session / *Sender per goroutine" rule, the responder and the requester
	// use SEPARATE sessions on SEPARATE connections.
	responderReady := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		runResponder(ctx, addr, responderReady)
	}()

	// Wait for the responder's subscription to go live before sending requests, so
	// the first command does not race the responder's attach.
	select {
	case <-responderReady:
	case <-ctx.Done():
		log.Fatalf("responder did not become ready: %v", ctx.Err())
	}

	runRequester(ctx, addr)

	// Cancelling the root context stops the responder goroutine.
	cancel()
	wg.Wait()
	fmt.Println("\nDone.")
}

// =============================================================================
// RESPONDER — receives commands on commands/<ch>, replies via an anonymous sender.
// =============================================================================

func runResponder(ctx context.Context, addr string, ready chan<- struct{}) {
	conn, err := amqp.Dial(ctx, amqpURL(), nil)
	if err != nil {
		log.Fatalf("[responder] dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		log.Fatalf("[responder] new session: %v", err)
	}

	// ATTACH a receiver on commands/<ch> (a server-sender link — the client
	// consumes requests). The client grants credit so the connector pumps requests.
	attachCtx, attachCancel := context.WithTimeout(ctx, 10*time.Second)
	rcv, err := session.NewReceiver(attachCtx, addr, &amqp.ReceiverOptions{Credit: 10})
	attachCancel()
	if err != nil {
		log.Fatalf("[responder] new receiver on %s: %v", addr, err)
	}

	// ATTACH an ANONYMOUS sender (null target). Each reply sets Properties.To to
	// the request's reply-to (the connector resolves it as /responses/<RequestID>),
	// so the reply routes back to the requester's dynamic node.
	sndCtx, sndCancel := context.WithTimeout(ctx, 10*time.Second)
	snd, err := session.NewSender(sndCtx, "", nil)
	sndCancel()
	if err != nil {
		log.Fatalf("[responder] anonymous sender attach: %v", err)
	}

	fmt.Printf("[responder] Listening on %s (anonymous reply sender ready)\n", addr)
	close(ready)

	for {
		req, err := rcv.Receive(ctx, nil)
		if err != nil {
			// Context cancelled (program done) or link torn down: exit cleanly.
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("[responder] receive: %v", err)
			return
		}
		// Settle the inbound request (accept). The reply itself travels out-of-band.
		_ = rcv.AcceptMessage(context.Background(), req)

		if req.Properties == nil || req.Properties.ReplyTo == nil {
			fmt.Println("[responder] request with no reply-to; cannot reply")
			continue
		}
		body := string(req.GetData())
		fmt.Printf("[responder] Received command %q (correlation-id=%v)\n", body, corrID(req))

		// Business logic: a command body of "fail" is rejected (executed=false), any
		// other body succeeds (executed=true). BOTH paths send a reply — a command
		// failure must NOT leave the requester waiting (unlike a query, variant #10).
		//
		// NOTE: a COMMAND response carries the EXECUTED/ERROR outcome, not data. The
		// broker's command-response path round-trips executed + error (and the echoed
		// correlation-id) but NOT a reply body — the reply body below is sent for
		// completeness but the requester observes an empty command body. Use a QUERY
		// (variant #10) when you need to return a value.
		ok := body != "fail"
		errText := ""
		if !ok {
			errText = "command rejected by handler"
		}

		replyTo := *req.Properties.ReplyTo
		reply := amqp.NewMessage([]byte("ack:" + body))
		reply.Properties = &amqp.MessageProperties{To: &replyTo}
		// Echo the correlation-id (fall back to message-id, the connector convention)
		// so the requester can match the reply to its request.
		if req.Properties.CorrelationID != nil {
			reply.Properties.CorrelationID = req.Properties.CorrelationID
		} else {
			reply.Properties.CorrelationID = req.Properties.MessageID
		}
		// A COMMAND reply carries the execution outcome as application-properties.
		reply.ApplicationProperties = map[string]any{
			"x-opt-kubemq-executed": ok,
			"x-opt-kubemq-error":    errText,
		}

		sendCtx, sendCancel := context.WithTimeout(ctx, 10*time.Second)
		err = snd.Send(sendCtx, reply, nil)
		sendCancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[responder] send reply: %v", err)
			continue
		}
		fmt.Printf("[responder] Replied to %q (executed=%v, error=%q)\n", body, ok, errText)
	}
}

// =============================================================================
// REQUESTER — dynamic reply node + sender on commands/<ch>; correlates replies.
// =============================================================================

func runRequester(ctx context.Context, addr string) {
	conn, err := amqp.Dial(ctx, amqpURL(), nil)
	if err != nil {
		log.Fatalf("[requester] dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		log.Fatalf("[requester] new session: %v", err)
	}

	// ATTACH a DYNAMIC reply node: an empty source + DynamicAddress:true asks the
	// server to create a transient node and echo its address. The requester reads
	// that server-assigned address and uses it as the reply-to on every request.
	attachCtx, attachCancel := context.WithTimeout(ctx, 10*time.Second)
	replyRcv, err := session.NewReceiver(attachCtx, "", &amqp.ReceiverOptions{
		DynamicAddress: true,
		Credit:         5,
	})
	attachCancel()
	if err != nil {
		log.Fatalf("[requester] dynamic reply-node attach: %v", err)
	}
	replyNode := replyRcv.Address()
	if replyNode == "" {
		log.Fatal("[requester] server did not assign a dynamic reply-node address")
	}
	fmt.Printf("[requester] Dynamic reply node: %s\n", replyNode)

	// ATTACH a sender on commands/<ch> (a server-receiver link — the client
	// produces requests). The server grants credit on attach.
	sndCtx, sndCancel := context.WithTimeout(ctx, 10*time.Second)
	snd, err := session.NewSender(sndCtx, addr, nil)
	sndCancel()
	if err != nil {
		log.Fatalf("[requester] sender attach on %s: %v", addr, err)
	}

	// 1. A SUCCESSFUL command: round-trips with executed=true.
	doRequest(ctx, snd, replyRcv, replyNode, "reboot-node-7", "corr-cmd-1")

	// 2. A FAILED command ("fail"): the responder replies executed=false + an error
	//    text — the requester is NOT left waiting (the key Commands contrast vs
	//    Queries, where a failure delivers nothing and the requester times out).
	doRequest(ctx, snd, replyRcv, replyNode, "fail", "corr-cmd-2")
}

// doRequest sends one command naming the dynamic reply node + a correlation-id,
// then awaits the correlated reply on the dynamic node and prints the executed/
// error outcome.
func doRequest(ctx context.Context, snd *amqp.Sender, replyRcv *amqp.Receiver, replyNode, body, corr string) {
	req := amqp.NewMessage([]byte(body))
	req.Properties = &amqp.MessageProperties{
		ReplyTo:       &replyNode, // MUST name a node this connection owns (snooping guard)
		CorrelationID: corr,
	}
	sendCtx, sendCancel := context.WithTimeout(ctx, 15*time.Second)
	err := snd.Send(sendCtx, req, nil)
	sendCancel()
	if err != nil {
		log.Fatalf("[requester] send command %q: %v", body, err)
	}
	fmt.Printf("[requester] Sent command %q (reply-to=dynamic node, correlation-id=%s)\n", body, corr)

	// Await the correlated reply on the dynamic node. A command always replies
	// (success OR failure), so this never times out on the happy path.
	rcvCtx, rcvCancel := context.WithTimeout(ctx, 30*time.Second)
	reply, err := replyRcv.Receive(rcvCtx, nil)
	rcvCancel()
	if err != nil {
		log.Fatalf("[requester] await reply for %q: %v", body, err)
	}
	_ = replyRcv.AcceptMessage(context.Background(), reply)

	gotCorr := corrID(reply)
	if fmt.Sprintf("%v", gotCorr) != corr {
		log.Fatalf("[requester] correlation-id mismatch: want %q got %v", corr, gotCorr)
	}
	executed, errText := commandOutcome(reply)
	fmt.Printf("[requester] Reply for %q (correlation-id=%v): executed=%v error=%q body=%q\n",
		body, gotCorr, executed, errText, string(reply.GetData()))
}

// corrID returns the message's correlation-id (the connector echoes the request's
// correlation-id, falling back to its message-id).
func corrID(m *amqp.Message) any {
	if m.Properties == nil {
		return nil
	}
	return m.Properties.CorrelationID
}

// commandOutcome reads the command-reply application-properties: x-opt-kubemq-
// executed (bool) and x-opt-kubemq-error (string). Absent => false / "".
func commandOutcome(m *amqp.Message) (bool, string) {
	if m.ApplicationProperties == nil {
		return false, ""
	}
	executed, _ := m.ApplicationProperties["x-opt-kubemq-executed"].(bool)
	errText, _ := m.ApplicationProperties["x-opt-kubemq-error"].(string)
	return executed, errText
}

// Expected output:
//
// Broker:  amqp://localhost:5672
// Address: commands/amqp10.examples.commands  (KubeMQ pattern=commands, channel=amqp10.examples.commands)
//
// [responder] Listening on commands/amqp10.examples.commands (anonymous reply sender ready)
// [requester] Dynamic reply node: _amqp10.tmp.<connID>.<uuid>
// [requester] Sent command "reboot-node-7" (reply-to=dynamic node, correlation-id=corr-cmd-1)
// [responder] Received command "reboot-node-7" (correlation-id=<RequestID>)
// [responder] Replied to "reboot-node-7" (executed=true, error="")
// [requester] Reply for "reboot-node-7" (correlation-id=corr-cmd-1): executed=true error="" body=""
// [requester] Sent command "fail" (reply-to=dynamic node, correlation-id=corr-cmd-2)
// [responder] Received command "fail" (correlation-id=<RequestID>)
// [responder] Replied to "fail" (executed=false, error="command rejected by handler")
// [requester] Reply for "fail" (correlation-id=corr-cmd-2): executed=false error="command rejected by handler" body=""
//
// Done.
//
// (The responder sees the connector-stamped RequestID as the delivered request's
// correlation-id, while the requester's reply correlation-id is its ORIGINAL
// corr-cmd-N — the connector echoes the requester's correlation-id back on the
// reply. A COMMAND response carries the executed/error outcome, NOT a body — the
// requester observes an empty command reply body; use a QUERY to return a value.)
//
// (A failed command still delivers a reply — executed=false + error text — so the
// requester is NEVER left waiting. Contrast queries/request-reply, where a failed
// query delivers nothing and the requester simply times out.)
