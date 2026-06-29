package worker

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	amqp "github.com/Azure/go-amqp"

	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/config"
	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/payload"
	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/transport"
)

// defaultCreditBatch is the fallback explicit credit granted per cycle on the
// manual-credit receiver when amqp10.credit is unset. Each cycle:
// IssueCredit(batch) → receive up to batch → DrainCredit (must complete, no hang)
// → verify NO over-delivery → repeat (the held remainder resumes on the next
// IssueCredit). The batch is sized to the configured standing credit so the
// consumer keeps pace with the producer (no unbounded queue backlog / latency
// blow-up) while still exercising the explicit credit/drain/resume cycle.
const defaultCreditBatch = 50

// firstMsgWait bounds how long the first Receive of a cycle blocks for the next
// message (so an idle queue does not stall the credit cycle). Once the cycle has
// drained at least one message, a shorter follow-on wait lets it finish promptly
// when the queue momentarily empties — keeping delivered-message latency low while
// still draining the held backlog under fresh credit.
const (
	firstMsgWait = 2 * time.Second
	nextMsgWait  = 100 * time.Millisecond
)

// noOverDeliveryWindow is the short confirmation window after a drain: with credit
// drained to 0 nothing should be pushed. A delivery here is over-delivery past
// granted credit — a fidelity failure.
const noOverDeliveryWindow = 100 * time.Millisecond

// drainEvery is how often the steady-state manual-credit consume is interrupted to
// run the explicit DRAIN / RESUME invariant pass. Draining churns at-least-once
// redeliveries on the queue, so running it periodically (not every batch) keeps
// the duplication rate low while still proving the credit/drain/resume contract.
const drainEvery = 5 * time.Second

// CreditFlowWorker (worker 6) exercises manual AMQP 1.0 flow control over the
// KubeMQ queues pattern: a manual-credit receiver (open with Credit:-1, then
// explicit IssueCredit / DrainCredit). It asserts the three credit invariants
// (spec §2.3; TestQueueCreditDrain):
//
//   - EXACT-credit delivery: after IssueCredit(N) at most N messages are pushed —
//     no over-delivery (an (N+1)-th delivery before fresh credit is a fidelity
//     failure).
//   - DRAIN completes without hang: DrainCredit returns; credit is then 0 and no
//     further message is pushed even though the queue still holds messages.
//   - HELD remainder resumes: a fresh IssueCredit(N) resumes delivery of the
//     messages held back while credit was 0.
//
// A producer feeds queues/<channel> faster than each credit batch drains, so the
// queue always holds a remainder for the next cycle.
//
// Grounds connector test TestQueueCreditDrain.
type CreditFlowWorker struct {
	*BaseWorker
	address     string
	creditBatch uint32
	seq         atomic.Uint64
}

// NewCreditFlowWorker creates a credit-flow worker for the given channel index.
// credit_flow maps to the KubeMQ queues pattern (the connector PatternFor).
func NewCreditFlowWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	channelName := transport.Channel(config.WorkerCreditFlow, idx)
	address := transport.Node("queues", config.WorkerCreditFlow, idx)
	batch := uint32(defaultCreditBatch)
	if cfg.Amqp10.Credit > 0 {
		batch = uint32(cfg.Amqp10.Credit)
	}
	if batch > 1024 {
		batch = 1024 // MaxUnsettledPerLink ceiling
	}
	return &CreditFlowWorker{
		BaseWorker:  NewBaseWorker(config.WorkerCreditFlow, channelName, idx, cfg, logger),
		address:     address,
		creditBatch: batch,
	}
}

// Start brings up the manual-credit receiver loop.
func (w *CreditFlowWorker) Start(ctx context.Context) error {
	w.consumerCtx, w.consumerCancel = context.WithCancel(ctx)
	w.consumerWG.Add(1)
	go func() {
		defer w.consumerWG.Done()
		w.consumeLoop(w.consumerCtx)
	}()
	return nil
}

func (w *CreditFlowWorker) consumeLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		conn, err := transport.Dial(ctx, w.dialCfg)
		if err != nil {
			w.recordError("connect_failure")
			if !sleepCtx(ctx, time.Second) {
				return
			}
			continue
		}
		w.registerConsumerConn(conn)

		sess, err := transport.NewSession(ctx, conn)
		if err != nil {
			w.recordError("attach_failure")
			w.cleanupConn(conn)
			continue
		}
		// Manual credit (Credit:-1) — required to use IssueCredit / DrainCredit.
		rcv, err := transport.NewReceiver(ctx, sess, w.address, -1)
		if err != nil {
			w.recordError("consume_failure")
			w.cleanupConn(conn)
			if !sleepCtx(ctx, time.Second) {
				return
			}
			continue
		}
		w.signalReady()

		w.creditCycle(ctx, rcv)
		closeReceiver(rcv)
		w.cleanupConn(conn)
		if ctx.Err() == nil {
			w.recordReconnection()
		}
	}
}

// creditCycle drives manual flow control until ctx is cancelled or the link
// fails. The STEADY state is a manual-credit consume: grant creditBatch, consume
// + accept-settle each delivery (exact-credit — the receiver never pushes more
// than the outstanding credit), and replenish as the batch is consumed. Every
// drainEvery a DRAIN/RESUME invariant pass runs: DrainCredit to 0 (must complete,
// no hang), confirm no over-delivery with credit at 0, then a fresh IssueCredit
// resumes the held remainder. Draining only periodically keeps the at-least-once
// queue from churning redeliveries every batch (the drain re-queues any in-flight
// server-side Get), while still exercising the full credit/drain/resume contract.
func (w *CreditFlowWorker) creditCycle(ctx context.Context, rcv *amqp.Receiver) {
	nextDrain := time.Now().Add(drainEvery)
	for {
		if ctx.Err() != nil {
			return
		}

		// Grant a fresh batch of credit (exact-credit: at most creditBatch in flight).
		if err := transport.IssueCredit(rcv, w.creditBatch); err != nil {
			w.recordError("consume_failure")
			return
		}

		// Consume the batch under that credit, accept-settling each delivery.
		var received uint32
		for received < w.creditBatch {
			if ctx.Err() != nil {
				return
			}
			wait := firstMsgWait
			if received > 0 {
				wait = nextMsgWait
			}
			recvCtx, cancel := context.WithTimeout(ctx, wait)
			msg, err := rcv.Receive(recvCtx, nil)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				break // queue momentarily empty — finish this batch
			}
			w.handleDelivery(msg)
			if err := rcv.AcceptMessage(ctx, msg); err != nil {
				w.recordError("accept_failure")
				return
			}
			received++
		}

		// Periodically exercise the explicit DRAIN / RESUME invariants.
		if time.Now().After(nextDrain) {
			if !w.drainAndVerify(ctx, rcv) {
				return
			}
			nextDrain = time.Now().Add(drainEvery)
		}
	}
}

// drainAndVerify runs one drain/no-over-delivery/resume verification pass:
//
//  1. DrainCredit MUST complete (no hang); credit is 0 afterwards.
//  2. Absorb any deliveries that landed during the drain (legitimate, credited).
//  3. With credit at 0, confirm NO further message arrives — over-delivery past
//     granted credit is a fidelity failure.
//
// The held remainder then resumes on the caller's next IssueCredit. Returns false
// on shutdown or a link error so the consume loop tears down and reconnects.
func (w *CreditFlowWorker) drainAndVerify(ctx context.Context, rcv *amqp.Receiver) bool {
	drainCtx, drainCancel := context.WithTimeout(ctx, 15*time.Second)
	err := transport.DrainCredit(drainCtx, rcv)
	drainCancel()
	if err != nil {
		if ctx.Err() != nil {
			return false
		}
		// A drain torn down by a link/session error (e.g. a forced disconnect closed
		// the connection) is NOT a credit-flow fault — reconnect without flagging.
		if isLinkOrSessionError(err) {
			return false
		}
		// A genuine hang (deadline exceeded on a live link) IS the credit-flow gate
		// failure — drain must complete without hanging.
		w.recordError("drain_failure")
		w.recordFidelityGap(1)
		return false
	}

	// Deliveries can keep arriving DURING a drain (AMQP spec); they were granted
	// under the prior credit, so consume + accept-settle them (not over-delivery).
	w.drainPrefetch(ctx, rcv)

	// Credit is now 0 on a live link (the drain above completed): nothing more
	// should be pushed even though the queue still holds messages. Wait a beat, then
	// re-check the prefetch (non-blocking). A delivery here is over-delivery past
	// granted credit — a fidelity failure.
	if !sleepCtx(ctx, noOverDeliveryWindow) {
		return false
	}
	if over := w.drainPrefetch(ctx, rcv); over > 0 {
		w.recordFidelityGap(uint64(over))
	}
	return true
}

// drainPrefetch consumes and accept-settles every message currently sitting in
// go-amqp's prefetch cache (non-blocking). It returns the number drained. It is
// used both to absorb deliveries that landed during a drain (legitimate, credited)
// and to detect any delivery that slipped in with zero credit (over-delivery).
func (w *CreditFlowWorker) drainPrefetch(ctx context.Context, rcv *amqp.Receiver) uint32 {
	var n uint32
	for {
		msg := rcv.Prefetched()
		if msg == nil {
			return n
		}
		w.handleDelivery(msg)
		if err := rcv.AcceptMessage(ctx, msg); err != nil {
			w.recordError("accept_failure")
			return n
		}
		n++
	}
}

func (w *CreditFlowWorker) handleDelivery(msg *amqp.Message) {
	body := msg.GetData()
	producerID, seq, crcHex, sentAt, ok := extractMeta(msg)
	if !ok {
		w.recordReceived(len(body))
		return
	}
	if crcHex != "" && !payload.VerifyCRC(body, crcHex) {
		w.recordCorrupted()
	}
	if !sentAt.IsZero() {
		w.recordLatency(time.Since(sentAt))
	}
	w.recordTracked(producerID, seq)
	w.recordReceived(len(body))
}

// StartProducers publishes to queues/<channel> (unsettled) faster than each credit
// batch drains, so a remainder is always held for the next cycle.
func (w *CreditFlowWorker) StartProducers() {
	w.producerCtx, w.producerCancel = context.WithCancel(context.Background())
	w.producerWG.Add(1)
	go func() {
		defer w.producerWG.Done()
		w.produceLoop(w.producerCtx)
	}()
}

func (w *CreditFlowWorker) produceLoop(ctx context.Context) {
	producerID := w.channelName
	var conn *amqp.Conn
	var sender *amqp.Sender
	defer func() {
		closeSender(sender)
		closeConn(conn)
	}()

	for {
		if ctx.Err() != nil {
			return
		}
		if sender == nil {
			c, s, err := w.connectSender(ctx)
			if err != nil {
				w.recordError("connect_failure")
				if !sleepCtx(ctx, time.Second) {
					return
				}
				continue
			}
			conn, sender = c, s
		}
		if err := w.waitForRate(ctx); err != nil {
			return
		}

		seq := w.seq.Add(1)
		size := w.selectMessageSize()
		body, crcHex := payload.Build(size)
		msg := buildMessage(producerID, seq, body, crcHex)

		start := time.Now()
		sendCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		err := sender.Send(sendCtx, msg, nil)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.recordError("send_failure")
			closeSender(sender)
			closeConn(conn)
			sender, conn = nil, nil
			continue
		}
		metricObserveSend(w.name, time.Since(start))
		w.recordSent(len(body))
	}
}

func (w *CreditFlowWorker) connectSender(ctx context.Context) (*amqp.Conn, *amqp.Sender, error) {
	conn, err := transport.Dial(ctx, w.dialCfg)
	if err != nil {
		return nil, nil, err
	}
	sess, err := transport.NewSession(ctx, conn)
	if err != nil {
		closeConn(conn)
		return nil, nil, err
	}
	// Unsettled (at-least-once) so each Send blocks on the server DISPOSITION.
	sender, err := transport.NewSender(ctx, sess, w.address, "unsettled")
	if err != nil {
		closeConn(conn)
		return nil, nil, err
	}
	return conn, sender, nil
}

func (w *CreditFlowWorker) cleanupConn(conn *amqp.Conn) {
	w.unregisterConsumerConn(conn)
	closeConn(conn)
}
