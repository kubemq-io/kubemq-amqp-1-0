package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	amqp "github.com/Azure/go-amqp"

	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/config"
	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/payload"
	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/transport"
)

// durableLinkName is the stable link name the events-store durable subscription
// re-attaches under. Combined with the stable per-channel container-id (the
// dialCfg ClientID = ClientIDPrefix-<channel>), it forms the durable identity
// (container-id, link-name) the connector keys replay/resume on — so a forced
// reconnect re-attaches the SAME durable terminus and resumes from the persisted
// cursor instead of starting fresh (spec F10; TestEventsStoreDurableReplay).
const durableLinkName = "burnin-durable-sub"

// EventsStoreWorker (worker 3) drives the KubeMQ events-store pattern over AMQP
// 1.0: durable replay/resume across reconnect plus stalled-credit observability.
//
//   - Producer: publishes to events-store/<channel> unsettled (each Send blocks
//     on the server DISPOSITION, so a counted Sent means the message is durably
//     stored).
//   - Consumer: a DURABLE receiver — stable container-id (the dialCfg ClientID is
//     fixed per channel) + stable link Name + ExpiryPolicy=Never + the
//     x-opt-kubemq-start link property. On a forced disconnect it re-attaches the
//     SAME durable identity and resumes from the persisted cursor (durable replay
//     /resume). Credit is sized to the connector's events-store ring buffer
//     (<= MaxUnsettledPerLink=1024) and accept-settled eagerly so the connector
//     auto-replenishes and never overflows the buffer — keeping the stalled-credit
//     gate (events_store_dropped_stalled_total == 0) green.
//
// Note (gotcha #2): if the consumer let credit stall, the connector would DETACH
// resource-limit and count ReportAmqp10EventsStoreDroppedStalled. Eager
// replenishment is the whole point of this worker's consume loop.
//
// Grounds connector tests TestEventsStoreDurableReplay, TestEventsStoreStalledCredit,
// TestDurableSubNameConflict.
type EventsStoreWorker struct {
	*BaseWorker
	address  string
	startPos string
	durable  bool
	storeTrk *subTracker
	seq      atomic.Uint64
}

// NewEventsStoreWorker creates an events-store worker for the given channel index.
func NewEventsStoreWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	channelName := transport.Channel(config.WorkerEventsStore, idx)
	// The connector address prefix is the HYPHENATED "events-store/" — the worker
	// name (and channel segment) uses the underscore form, but the link node must
	// use the connector pattern or the ATTACH is refused.
	address := transport.Node("events-store", config.WorkerEventsStore, idx)
	return &EventsStoreWorker{
		BaseWorker: NewBaseWorker(config.WorkerEventsStore, channelName, idx, cfg, logger),
		address:    address,
		startPos:   cfg.Amqp10.StartPosition,
		durable:    cfg.Amqp10.Durable,
		storeTrk:   newSubTracker(),
	}
}

// Start brings up the durable consumer (durable identity, eager credit).
func (w *EventsStoreWorker) Start(ctx context.Context) error {
	w.consumerCtx, w.consumerCancel = context.WithCancel(ctx)
	n := w.workerCfg.ConsumersPerChannel
	if n < 1 {
		n = 1
	}
	// A durable subscription is single-claim (a 2nd live attach of the same
	// container-id+link-name is refused amqp:not-allowed, TestDurableSubNameConflict).
	// Run exactly one durable consumer regardless of consumers_per_channel.
	_ = n
	w.consumerWG.Add(1)
	go func() {
		defer w.consumerWG.Done()
		w.consumeLoop(w.consumerCtx)
	}()
	return nil
}

func (w *EventsStoreWorker) consumeLoop(ctx context.Context) {
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
		rcv, err := w.attachDurable(ctx, sess)
		if err != nil {
			w.recordError("consume_failure")
			w.cleanupConn(conn)
			if !sleepCtx(ctx, time.Second) {
				return
			}
			continue
		}
		w.signalReady()

		w.drainDurable(ctx, rcv)
		closeReceiver(rcv)
		w.cleanupConn(conn)
		if ctx.Err() == nil {
			// A consume-side teardown on a healthy broker is the durable resume path:
			// the next loop re-attaches the SAME durable identity and continues.
			w.recordReconnection()
		}
	}
}

// attachDurable attaches the durable events-store consume link: stable link name,
// ExpiryPolicy=Never, and the x-opt-kubemq-start link property (start position).
// The receiver grants standing credit sized to the connector's events-store ring
// buffer; go-amqp auto-replenishes credit as deliveries settle (accepted), so the
// buffer never overflows and the stalled-credit gate stays 0.
func (w *EventsStoreWorker) attachDurable(ctx context.Context, sess *amqp.Session) (*amqp.Receiver, error) {
	opts := &amqp.ReceiverOptions{
		Name:   durableLinkName,
		Credit: w.creditClamped(),
		Properties: map[string]any{
			"x-opt-kubemq-start": w.effectiveStartPos(),
		},
	}
	if w.durable {
		// Durable terminus: expiry "never" is exactly what the connector reads as a
		// durable events-store subscription (link.go isDurableTerminus).
		opts.ExpiryPolicy = amqp.ExpiryPolicyNever
		opts.Durability = amqp.DurabilityUnsettledState
	}
	attachCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return transport.NewReceiverWithOptions(attachCtx, sess, w.address, opts)
}

// creditClamped returns the standing link credit, clamped to the connector's
// MaxUnsettledPerLink ceiling (1024) so the ATTACH is accepted.
func (w *EventsStoreWorker) creditClamped() int32 {
	c := w.credit
	if c < 1 {
		c = 1
	}
	if c > 1024 {
		c = 1024
	}
	return c
}

// effectiveStartPos returns the configured start position, defaulting to new-only.
func (w *EventsStoreWorker) effectiveStartPos() string {
	if w.startPos == "" {
		return "new-only"
	}
	return w.startPos
}

func (w *EventsStoreWorker) drainDurable(ctx context.Context, rcv *amqp.Receiver) {
	for {
		if ctx.Err() != nil {
			return
		}
		msg, err := rcv.Receive(ctx, nil)
		if err != nil {
			// A resource-limit DETACH here would be the connector's stalled-credit
			// signal — but eager accept-settle replenishment means it should not
			// occur. Surface it on the gate metric if it ever does.
			if isStalledCreditError(err) {
				w.recordEventsStoreDroppedStalled()
			}
			return
		}
		w.handleStoreDelivery(msg)
		// Accept-settle advances the durable cursor AND auto-replenishes credit
		// (go-amqp issues credit as deliveries settle) — the eager-replenish path
		// that keeps the events-store ring buffer from overflowing.
		if err := rcv.AcceptMessage(ctx, msg); err != nil {
			w.recordError("accept_failure")
			return
		}
	}
}

func (w *EventsStoreWorker) handleStoreDelivery(msg *amqp.Message) {
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
	// Track per (producer, seq) for loss/dup fidelity. Durable replay redelivers
	// on resume; the subTracker dedups so a resume does not inflate the count.
	if dup := w.storeTrk.record(seq); dup {
		w.recordDuplicated()
	}
	w.recordTracked(producerID, seq)
	w.recordReceived(len(body))
}

// StartProducers publishes durable events to events-store/<channel> unsettled.
func (w *EventsStoreWorker) StartProducers() {
	w.producerCtx, w.producerCancel = context.WithCancel(context.Background())
	w.producerWG.Add(1)
	go func() {
		defer w.producerWG.Done()
		w.produceLoop(w.producerCtx)
	}()
}

func (w *EventsStoreWorker) produceLoop(ctx context.Context) {
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
		// Unsettled Send blocks on the server DISPOSITION; a counted Sent means the
		// events-store durably persisted the message.
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

func (w *EventsStoreWorker) connectSender(ctx context.Context) (*amqp.Conn, *amqp.Sender, error) {
	conn, err := transport.Dial(ctx, w.dialCfg)
	if err != nil {
		return nil, nil, err
	}
	sess, err := transport.NewSession(ctx, conn)
	if err != nil {
		closeConn(conn)
		return nil, nil, err
	}
	// Durable store: unsettled (at-least-once) so each accepted transfer is persisted.
	sender, err := transport.NewSender(ctx, sess, w.address, "unsettled")
	if err != nil {
		closeConn(conn)
		return nil, nil, err
	}
	return conn, sender, nil
}

func (w *EventsStoreWorker) cleanupConn(conn *amqp.Conn) {
	w.unregisterConsumerConn(conn)
	closeConn(conn)
}

// ExtraLost reports confirmed-lost across the durable subscription so the verdict
// can gate events-store delivery fidelity (with at-least-once durability this
// MUST be 0 — no message persisted should be lost across replay/resume).
func (w *EventsStoreWorker) ExtraLost() uint64 {
	return w.storeTrk.missing()
}

// isLinkOrSessionError reports whether an error is an AMQP link or session
// teardown (e.g. a forced disconnect closed the connection) rather than an
// in-protocol failure — used to distinguish a torn-down drain from a genuine hang.
func isLinkOrSessionError(err error) bool {
	var le *amqp.LinkError
	if errors.As(err, &le) {
		return true
	}
	var se *amqp.SessionError
	if errors.As(err, &se) {
		return true
	}
	var ce *amqp.ConnError
	return errors.As(err, &ce)
}

// isStalledCreditError reports whether an AMQP error is the connector's
// stalled-credit resource-limit DETACH (events-store credit overflow, gotcha #2).
func isStalledCreditError(err error) bool {
	var le *amqp.LinkError
	if errors.As(err, &le) {
		if le.RemoteErr != nil && le.RemoteErr.Condition == amqp.ErrCondResourceLimitExceeded {
			return true
		}
	}
	var se *amqp.SessionError
	if errors.As(err, &se) {
		if se.RemoteErr != nil && se.RemoteErr.Condition == amqp.ErrCondResourceLimitExceeded {
			return true
		}
	}
	return false
}
