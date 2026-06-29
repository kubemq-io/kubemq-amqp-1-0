// Example: queries/request-reply (master-table variant #10)
//
// Native AMQP 1.0 request/reply over KubeMQ **Queries** (RPC) with the native
// github.com/Azure/go-amqp client — NO kubemq-go, NO gRPC. The whole round-trip
// stays in-protocol over a single broker connection per role.
//
// The reply path is IDENTICAL to commands (variant #9): the requester opens a
// DYNAMIC reply node (NewReceiver(ctx, "", {DynamicAddress:true}) → Address()),
// sends to queries/<ch> with Properties.ReplyTo = that node + a CorrelationID; the
// responder receives on queries/<ch> and replies via an ANONYMOUS sender
// (NewSender(ctx, "", nil)) with Properties.To = the request's reply-to + the
// echoed CorrelationID.
//
// The CONTRAST with commands (the whole point of variant #10):
//
//   - A query reply carries ONLY the body + metadata — NO x-opt-kubemq-executed /
//     x-opt-kubemq-error application-properties. A query is a "fetch a value"
//     call; there is no executed/error envelope.
//   - A FAILED query delivers NOTHING. The connector's runRequest delivers no
//     reply when a query fails or times out (MQTT-bridge parity), so the requester
//     simply TIMES OUT. (A failed command, by contrast, always replies
//     executed=false so its requester is never left waiting.)
//
// This example demonstrates BOTH: a successful query (reply round-trips, body
// intact) and a query the responder ignores (no reply ⇒ the requester times out
// on a short demo deadline; in production the connector default is ~30s).
//
// Grounded in connector test TestRPCRequesterResponderViaAMQP10 (queries leg)
// (connectors/amqp10/integration_test.go).
//
// Run:
//
//	export KUBEMQ_AMQP_URL=amqp://localhost:5672
//	go run ./queries/request-reply
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

// channel is the KubeMQ queries channel; the link address is "queries/" + channel
// (explicit prefix — never rely on a default pattern).
const channel = "amqp10.examples.queries"

// demoTimeout is the short per-request deadline used here so the "no reply" leg
// surfaces a timeout quickly. The connector's own default RPC timeout is ~30s; in
// production set the request header.ttl to choose the per-request budget.
const demoTimeout = 5 * time.Second

func amqpURL() string {
	if v := os.Getenv("KUBEMQ_AMQP_URL"); v != "" {
		return v
	}
	return "amqp://localhost:5672"
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	addr := "queries/" + channel
	fmt.Printf("Broker:  %s\n", amqpURL())
	fmt.Printf("Address: %s  (KubeMQ pattern=queries, channel=%s)\n\n", addr, channel)

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

	select {
	case <-responderReady:
	case <-ctx.Done():
		log.Fatalf("responder did not become ready: %v", ctx.Err())
	}

	runRequester(ctx, addr)

	cancel()
	wg.Wait()
	fmt.Println("\nDone.")
}

// =============================================================================
// RESPONDER — receives queries on queries/<ch>, replies via an anonymous sender.
// A query whose body is "ignore" gets NO reply (so its requester times out).
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

	// ATTACH a receiver on queries/<ch> (server-sender link — the client consumes
	// requests). The client grants credit so the connector pumps requests.
	attachCtx, attachCancel := context.WithTimeout(ctx, 10*time.Second)
	rcv, err := session.NewReceiver(attachCtx, addr, &amqp.ReceiverOptions{Credit: 10})
	attachCancel()
	if err != nil {
		log.Fatalf("[responder] new receiver on %s: %v", addr, err)
	}

	// ATTACH an ANONYMOUS sender (null target). Each reply sets Properties.To to
	// the request's reply-to so it routes back to the requester's dynamic node.
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
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("[responder] receive: %v", err)
			return
		}
		_ = rcv.AcceptMessage(context.Background(), req)

		if req.Properties == nil || req.Properties.ReplyTo == nil {
			fmt.Println("[responder] request with no reply-to; cannot reply")
			continue
		}
		body := string(req.GetData())
		fmt.Printf("[responder] Received query %q (correlation-id=%v)\n", body, corrID(req))

		// Business logic: a query body of "ignore" is dropped on the floor — the
		// responder sends NOTHING. The requester will time out. (A real responder
		// would only fail to reply on a crash / unreachable backend; "ignore" makes
		// the contrast deterministic for the demo.)
		if body == "ignore" {
			fmt.Printf("[responder] Ignoring %q — NO reply sent (requester will time out)\n", body)
			continue
		}

		// A QUERY reply carries ONLY the body + metadata — NO executed/error
		// application-properties (the Commands-vs-Queries contrast).
		replyTo := *req.Properties.ReplyTo
		reply := amqp.NewMessage([]byte("result:" + body))
		reply.Properties = &amqp.MessageProperties{To: &replyTo}
		if req.Properties.CorrelationID != nil {
			reply.Properties.CorrelationID = req.Properties.CorrelationID
		} else {
			reply.Properties.CorrelationID = req.Properties.MessageID
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
		fmt.Printf("[responder] Replied to %q (body + metadata only, no executed/error props)\n", body)
	}
}

// =============================================================================
// REQUESTER — dynamic reply node + sender on queries/<ch>; correlates replies.
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

	// ATTACH a DYNAMIC reply node (empty source + DynamicAddress:true). The server
	// creates a transient node and echoes its address (read with Address()).
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

	// ATTACH a sender on queries/<ch> (server-receiver link — the client produces
	// requests). The server grants credit on attach.
	sndCtx, sndCancel := context.WithTimeout(ctx, 10*time.Second)
	snd, err := session.NewSender(sndCtx, addr, nil)
	sndCancel()
	if err != nil {
		log.Fatalf("[requester] sender attach on %s: %v", addr, err)
	}

	// 1. A SUCCESSFUL query: round-trips, body intact, no executed/error props.
	doQuery(ctx, snd, replyRcv, replyNode, "get-temp-sensor-3", "corr-qry-1")

	// 2. A query the responder ignores: NOTHING is delivered, so the requester
	//    TIMES OUT. This is the core Queries contrast — a failed/unanswered query
	//    has no error envelope; the absence of a reply IS the failure signal.
	doQueryExpectTimeout(ctx, snd, replyRcv, replyNode, "ignore", "corr-qry-2")
}

// doQuery sends one query naming the dynamic reply node + a correlation-id, then
// awaits the correlated reply on the dynamic node and prints the result body.
func doQuery(ctx context.Context, snd *amqp.Sender, replyRcv *amqp.Receiver, replyNode, body, corr string) {
	if err := sendQuery(ctx, snd, replyNode, body, corr); err != nil {
		log.Fatalf("[requester] send query %q: %v", body, err)
	}

	rcvCtx, rcvCancel := context.WithTimeout(ctx, demoTimeout)
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
	fmt.Printf("[requester] Reply for %q (correlation-id=%v): body=%q\n", body, gotCorr, string(reply.GetData()))
}

// doQueryExpectTimeout sends a query the responder will ignore and shows the
// requester timing out (no reply is the failure signal for queries).
func doQueryExpectTimeout(ctx context.Context, snd *amqp.Sender, replyRcv *amqp.Receiver, replyNode, body, corr string) {
	if err := sendQuery(ctx, snd, replyNode, body, corr); err != nil {
		log.Fatalf("[requester] send query %q: %v", body, err)
	}

	rcvCtx, rcvCancel := context.WithTimeout(ctx, demoTimeout)
	_, err := replyRcv.Receive(rcvCtx, nil)
	rcvCancel()
	if err == nil {
		log.Fatalf("[requester] expected NO reply for %q, but one arrived", body)
	}
	// A deadline-exceeded here is the EXPECTED outcome for an unanswered query.
	fmt.Printf("[requester] No reply for %q within %s — query timed out (expected; failed queries deliver nothing)\n",
		body, demoTimeout)
}

// sendQuery sends one query message naming the dynamic reply node + correlation-id.
func sendQuery(ctx context.Context, snd *amqp.Sender, replyNode, body, corr string) error {
	req := amqp.NewMessage([]byte(body))
	req.Properties = &amqp.MessageProperties{
		ReplyTo:       &replyNode, // MUST name a node this connection owns (snooping guard)
		CorrelationID: corr,
	}
	sendCtx, sendCancel := context.WithTimeout(ctx, 15*time.Second)
	defer sendCancel()
	if err := snd.Send(sendCtx, req, nil); err != nil {
		return err
	}
	fmt.Printf("[requester] Sent query %q (reply-to=dynamic node, correlation-id=%s)\n", body, corr)
	return nil
}

// corrID returns the message's correlation-id (the connector echoes the request's
// correlation-id, falling back to its message-id).
func corrID(m *amqp.Message) any {
	if m.Properties == nil {
		return nil
	}
	return m.Properties.CorrelationID
}

// Expected output:
//
// Broker:  amqp://localhost:5672
// Address: queries/amqp10.examples.queries  (KubeMQ pattern=queries, channel=amqp10.examples.queries)
//
// [responder] Listening on queries/amqp10.examples.queries (anonymous reply sender ready)
// [requester] Dynamic reply node: _amqp10.tmp.<connID>.<uuid>
// [requester] Sent query "get-temp-sensor-3" (reply-to=dynamic node, correlation-id=corr-qry-1)
// [responder] Received query "get-temp-sensor-3" (correlation-id=corr-qry-1)
// [responder] Replied to "get-temp-sensor-3" (body + metadata only, no executed/error props)
// [requester] Reply for "get-temp-sensor-3" (correlation-id=corr-qry-1): body="result:get-temp-sensor-3"
// [requester] Sent query "ignore" (reply-to=dynamic node, correlation-id=corr-qry-2)
// [responder] Received query "ignore" (correlation-id=corr-qry-2)
// [responder] Ignoring "ignore" — NO reply sent (requester will time out)
// [requester] No reply for "ignore" within 5s — query timed out (expected; failed queries deliver nothing)
//
// Done.
//
// (Unlike a command — which always replies executed=false on failure so the
// requester is never left waiting — a query that fails/goes unanswered delivers
// NOTHING. The requester's timeout IS the failure signal. The connector's own
// default per-request timeout is ~30s; set the request header.ttl to choose it.)
