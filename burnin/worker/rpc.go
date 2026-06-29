package worker

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	amqp "github.com/Azure/go-amqp"

	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/config"
	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/payload"
	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/transport"
)

// AMQP command-reply application-property keys (spec §2.4 / connector rpc.go). A
// COMMAND reply carries the execution outcome; a QUERY reply does not.
const (
	propExecuted = "x-opt-kubemq-executed"
	propError    = "x-opt-kubemq-error"
)

// rpcTimeoutFor resolves the per-request reply budget: amqp10.rpc_timeout_ms
// first (the value mapped to the request header.ttl), then rpc.timeout_ms, with a
// 5s fallback.
func rpcTimeoutFor(cfg *config.Config) time.Duration {
	if cfg.Amqp10.RPCTimeoutMs > 0 {
		return time.Duration(cfg.Amqp10.RPCTimeoutMs) * time.Millisecond
	}
	if cfg.RPC.TimeoutMs > 0 {
		return time.Duration(cfg.RPC.TimeoutMs) * time.Millisecond
	}
	return 5 * time.Second
}

// rpcResponder runs the RESPONDER side of a native AMQP RPC channel: a receiver
// on <pattern>/<channel> (a server-sender link the client pumps under credit) and
// an ANONYMOUS sender (null target). Each request is replied to via the anonymous
// sender with Properties.To = the request's reply-to (the connector resolves it to
// /responses/<RequestID>) + the echoed correlation-id (msg-id fallback). One
// session per goroutine — responder and requester live on separate connections.
//
// withOutcome selects the command vs query reply shape: commands attach the
// executed/error application-properties; queries reply with body + metadata only,
// and (to exercise the query timeout leg) drop the reply for a deterministic
// fraction of requests.
type rpcResponder struct {
	w           *BaseWorker
	address     string
	withOutcome bool // true => command reply (executed/error props); false => query
	dropEveryN  uint64
	count       atomic.Uint64
}

func (r *rpcResponder) run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		conn, err := transport.Dial(ctx, r.w.dialCfg)
		if err != nil {
			r.w.recordError("connect_failure")
			if !sleepCtx(ctx, time.Second) {
				return
			}
			continue
		}
		r.w.registerConsumerConn(conn)

		sess, err := transport.NewSession(ctx, conn)
		if err != nil {
			r.w.recordError("attach_failure")
			r.cleanup(conn)
			continue
		}
		rcv, err := transport.NewReceiver(ctx, sess, r.address, r.w.creditOrDefault())
		if err != nil {
			r.w.recordError("consume_failure")
			r.cleanup(conn)
			if !sleepCtx(ctx, time.Second) {
				return
			}
			continue
		}
		// Anonymous reply sender (null target); each reply names Properties.To.
		snd, err := transport.NewSender(ctx, sess, "", "")
		if err != nil {
			r.w.recordError("attach_failure")
			closeReceiver(rcv)
			r.cleanup(conn)
			if !sleepCtx(ctx, time.Second) {
				return
			}
			continue
		}
		r.w.signalReady()

		r.serve(ctx, rcv, snd)
		closeSender(snd)
		closeReceiver(rcv)
		r.cleanup(conn)
		if ctx.Err() == nil {
			r.w.recordReconnection()
		}
	}
}

func (r *rpcResponder) serve(ctx context.Context, rcv *amqp.Receiver, snd *amqp.Sender) {
	for {
		if ctx.Err() != nil {
			return
		}
		req, err := rcv.Receive(ctx, nil)
		if err != nil {
			return
		}
		// Settle the inbound request; the reply travels out-of-band.
		_ = rcv.AcceptMessage(ctx, req)

		if req.Properties == nil || req.Properties.ReplyTo == nil {
			continue
		}

		// Query failure leg: drop the reply for a deterministic fraction so the
		// requester observes the query-timeout contract (failed query => no reply).
		if !r.withOutcome && r.dropEveryN > 0 {
			if n := r.count.Add(1); n%r.dropEveryN == 0 {
				continue
			}
		}

		reply := r.buildReply(req)
		sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = snd.Send(sendCtx, reply, nil)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			r.w.recordError("send_failure")
			return // re-attach the anonymous sender on the next loop
		}
	}
}

// buildReply echoes the request body + self-accounting envelope back to the
// requester's reply-to, carrying the echoed correlation-id (msg-id fallback). A
// command reply additionally stamps executed/error.
func (r *rpcResponder) buildReply(req *amqp.Message) *amqp.Message {
	replyTo := *req.Properties.ReplyTo
	// Round-trip the body bit-exact so the requester can re-verify the CRC.
	reply := amqp.NewMessage(req.GetData())
	reply.Properties = &amqp.MessageProperties{To: &replyTo}

	// Echo correlation-id; fall back to the request's message-id (connector
	// convention, TestRpcCorrelationIDFallbackToMessageID).
	if req.Properties.CorrelationID != nil {
		reply.Properties.CorrelationID = req.Properties.CorrelationID
	} else {
		reply.Properties.CorrelationID = req.Properties.MessageID
	}

	// Carry the self-accounting envelope through so requester-side fidelity holds.
	if req.ApplicationProperties != nil {
		reply.ApplicationProperties = map[string]any{
			payload.PropWorkerID:    req.ApplicationProperties[payload.PropWorkerID],
			payload.PropSequence:    req.ApplicationProperties[payload.PropSequence],
			payload.PropContentHash: req.ApplicationProperties[payload.PropContentHash],
			payload.PropTimestampNS: req.ApplicationProperties[payload.PropTimestampNS],
		}
	} else {
		reply.ApplicationProperties = map[string]any{}
	}

	if r.withOutcome {
		// Command outcome: executed=true on success. (The burn-in responder always
		// succeeds — the failure leg is exercised separately by the requester via a
		// missing responder / timeout, keeping the gate meaningful.)
		reply.ApplicationProperties[propExecuted] = true
		reply.ApplicationProperties[propError] = ""
	}
	return reply
}

func (r *rpcResponder) cleanup(conn *amqp.Conn) {
	r.w.unregisterConsumerConn(conn)
	closeConn(conn)
}

// rpcRequester runs the REQUESTER side: a DYNAMIC reply node (the server mints a
// transient node and echoes its address) plus a sender on <pattern>/<channel>.
// Each request names the dynamic node as reply-to + a unique correlation-id, then
// awaits the correlated reply on the dynamic node. The reply is correlated by
// correlation-id and accounted (success / timeout / error).
//
// verifyOutcome selects the command vs query expectation: commands require
// x-opt-kubemq-executed == true (executed=false => error); queries accept any
// correlated reply (body + metadata only).
type rpcRequester struct {
	w             *BaseWorker
	address       string
	timeout       time.Duration
	verifyOutcome bool
	seq           atomic.Uint64
	corr          atomic.Uint64
}

func (rq *rpcRequester) run(ctx context.Context) {
	var (
		conn     *amqp.Conn
		sess     *amqp.Session
		snd      *amqp.Sender
		replyRcv *amqp.Receiver
		replyTo  string
	)
	teardown := func() {
		closeReceiver(replyRcv)
		closeSender(snd)
		closeConn(conn)
		conn, sess, snd, replyRcv, replyTo = nil, nil, nil, nil, ""
	}
	defer teardown()

	for {
		if ctx.Err() != nil {
			return
		}
		if conn == nil {
			c, s, sn, rr, rt, err := rq.connect(ctx)
			if err != nil {
				rq.w.recordError("connect_failure")
				if !sleepCtx(ctx, time.Second) {
					return
				}
				continue
			}
			conn, sess, snd, replyRcv, replyTo = c, s, sn, rr, rt
			_ = sess
		}
		if err := rq.w.waitForRate(ctx); err != nil {
			return
		}
		if !rq.doOne(ctx, snd, replyRcv, replyTo) {
			// Transport-level failure on the request/reply links: rebuild them.
			if ctx.Err() != nil {
				return
			}
			teardown()
		}
	}
}

// connect builds the requester's dynamic reply node + request sender on one
// connection (one session per goroutine).
func (rq *rpcRequester) connect(ctx context.Context) (*amqp.Conn, *amqp.Session, *amqp.Sender, *amqp.Receiver, string, error) {
	conn, err := transport.Dial(ctx, rq.w.dialCfg)
	if err != nil {
		return nil, nil, nil, nil, "", err
	}
	sess, err := transport.NewSession(ctx, conn)
	if err != nil {
		closeConn(conn)
		return nil, nil, nil, nil, "", err
	}
	// Dynamic reply node: the server mints a transient node and echoes its address.
	attachCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	replyRcv, err := transport.NewReceiverWithOptions(attachCtx, sess, "", &amqp.ReceiverOptions{
		DynamicAddress: true,
		Credit:         rq.w.creditOrDefault(),
	})
	cancel()
	if err != nil {
		closeConn(conn)
		return nil, nil, nil, nil, "", err
	}
	replyTo := replyRcv.Address()
	if replyTo == "" {
		closeReceiver(replyRcv)
		closeConn(conn)
		return nil, nil, nil, nil, "", errors.New("server did not assign a dynamic reply node")
	}
	// Request sender on <pattern>/<channel>.
	snd, err := transport.NewSender(ctx, sess, rq.address, "")
	if err != nil {
		closeReceiver(replyRcv)
		closeConn(conn)
		return nil, nil, nil, nil, "", err
	}
	return conn, sess, snd, replyRcv, replyTo, nil
}

// doOne performs one RPC round-trip. It returns false ONLY on a transport-level
// failure that requires rebuilding the links; an RPC timeout/error (accounted) is
// a normal outcome and returns true.
func (rq *rpcRequester) doOne(ctx context.Context, snd *amqp.Sender, replyRcv *amqp.Receiver, replyTo string) bool {
	seq := rq.seq.Add(1)
	corr := rq.w.channelName + "-" + strconv.FormatUint(rq.corr.Add(1), 10)
	size := rq.w.selectMessageSize()
	body, crcHex := payload.Build(size)

	req := buildMessage(rq.w.channelName, seq, body, crcHex)
	req.Properties = &amqp.MessageProperties{
		ReplyTo:       &replyTo, // MUST name a node this connection owns (snooping guard)
		CorrelationID: corr,
	}

	start := time.Now()
	sendCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	err := snd.Send(sendCtx, req, nil)
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return false
		}
		rq.w.recordError("send_failure")
		rq.w.recordRPCError()
		return false
	}
	rq.w.recordSent(len(body))

	// Await the correlated reply on the dynamic node.
	rcvCtx, rcvCancel := context.WithTimeout(ctx, rq.timeout)
	reply, err := replyRcv.Receive(rcvCtx, nil)
	rcvCancel()
	if err != nil {
		if ctx.Err() != nil {
			return false
		}
		// No reply within the budget: a query timeout (expected failure leg) or a
		// command that should have replied. Either way, account it as a timeout. A
		// link/session error means the links must be rebuilt.
		if errors.Is(err, context.DeadlineExceeded) {
			rq.w.recordRPCTimeout()
			// Rebuild the reply links (return false → teardown) rather than reusing
			// them: a reply the responder sends AFTER our timeout would otherwise stay
			// buffered on the dynamic reply node and be mis-consumed by the next
			// request (correlation desync), cascading one late reply into many RPC
			// errors. Discarding the link drops the stale reply.
			return false
		}
		rq.w.recordRPCError()
		return false
	}
	_ = replyRcv.AcceptMessage(ctx, reply)

	rq.accountReply(reply, corr, start)
	return true
}

// accountReply verifies correlation, outcome (commands), and self-accounting
// fidelity (CRC + sequence) of a received reply, recording success/error.
func (rq *rpcRequester) accountReply(reply *amqp.Message, wantCorr string, start time.Time) {
	gotCorr := correlationString(reply)
	if gotCorr != wantCorr {
		// Correlation mismatch is a fidelity failure (a reply matched to the wrong
		// request) — count it as an RPC error.
		rq.w.recordRPCError()
		return
	}

	if rq.verifyOutcome {
		executed, _ := commandExecuted(reply)
		if !executed {
			rq.w.recordRPCError()
			return
		}
	}

	// Self-accounting fidelity: the responder round-trips the body bit-exact, so
	// the CRC must still verify and the sequence must be tracked.
	body := reply.GetData()
	producerID, seq, crcHex, _, ok := extractMeta(reply)
	if ok {
		if crcHex != "" && !payload.VerifyCRC(body, crcHex) {
			rq.w.recordCorrupted()
		}
		rq.w.recordTracked(producerID, seq)
	}
	rq.w.recordReceived(len(body))
	rq.w.recordRPCSuccess(time.Since(start))
}

// correlationString renders a reply's correlation-id as a string (the connector
// echoes the requester's correlation-id, which is set as a string).
func correlationString(m *amqp.Message) string {
	if m.Properties == nil || m.Properties.CorrelationID == nil {
		return ""
	}
	switch v := m.Properties.CorrelationID.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return ""
	}
}

// commandExecuted reads the command-reply x-opt-kubemq-executed / -error props.
func commandExecuted(m *amqp.Message) (bool, string) {
	if m.ApplicationProperties == nil {
		return false, ""
	}
	executed, _ := m.ApplicationProperties[propExecuted].(bool)
	errText, _ := m.ApplicationProperties[propError].(string)
	return executed, errText
}
