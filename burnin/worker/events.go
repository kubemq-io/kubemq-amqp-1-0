package worker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/Azure/go-amqp"

	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/config"
	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/metrics"
	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/payload"
	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/transport"
)

// EventsWorker (worker 2) drives the KubeMQ events pattern over AMQP 1.0:
// at-most-once pub/sub fan-out to events/<channel>. It asserts fan-out fidelity
// (every continuously-credited subscriber sees every message), continuous-credit
// hygiene (the connector drops events only when NO credit is outstanding — on a
// healthy broker with standing credit, events_dropped_no_credit_total MUST be 0),
// and consumer-group load balancing across the subscriber links.
//
// Events are fire-and-forget: the sender uses settled (at-most-once) settlement;
// each subscriber grants standing link credit (never drains to 0 during measure),
// and deliveries are auto-settled (no per-message accept round-trip).
//
// Grounds connector test TestEventsPubSubGroupFanout.
type EventsWorker struct {
	*BaseWorker
	address     string
	seq         atomic.Uint64
	subTrackers []*subTracker
}

// subTrackerWindow bounds the per-subscriber duplicate-detection set. Fan-out
// (re)deliveries arrive near-term, so retaining only a recent window of
// sequences is sufficient to detect duplicates while keeping memory bounded over
// an unbounded soak (the previous unbounded `seen` map grew one entry per
// message — ~28MB/h — which failed the memory-stability gate).
const subTrackerWindow = 20000

// subTracker tracks fan-out fidelity (loss/dup) for one subscriber independently.
// `seen` is bounded to a recent window; uniqueCount/lastSeq accumulate so
// missing() stays accurate over the full run.
type subTracker struct {
	mu          sync.Mutex
	lastSeq     uint64
	uniqueCount uint64
	seen        map[uint64]struct{}
	pruneLow    uint64
}

func newSubTracker() *subTracker { return &subTracker{seen: make(map[uint64]struct{})} }

func (s *subTracker) record(seq uint64) (dup bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.seen[seq]; ok {
		return true
	}
	s.seen[seq] = struct{}{}
	s.uniqueCount++
	if seq > s.lastSeq {
		s.lastSeq = seq
	}
	// Bound memory: drop sequences older than the retention window.
	if s.lastSeq > subTrackerWindow {
		newLow := s.lastSeq - subTrackerWindow
		for k := s.pruneLow; k < newLow; k++ {
			delete(s.seen, k)
		}
		s.pruneLow = newLow
	}
	return false
}

// missing returns best-effort confirmed-lost = highestSeq - uniqueSeen.
func (s *subTracker) missing() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastSeq <= s.uniqueCount {
		return 0
	}
	return s.lastSeq - s.uniqueCount
}

// NewEventsWorker creates an events worker for the given channel index.
func NewEventsWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	channelName := transport.Channel(config.WorkerEvents, idx)
	address := transport.Node("events", config.WorkerEvents, idx)
	subs := cfg.Workers.Events.Subscribers
	if subs < 1 {
		subs = 2
	}
	w := &EventsWorker{
		BaseWorker: NewBaseWorker(config.WorkerEvents, channelName, idx, cfg, logger),
		address:    address,
	}
	for i := 0; i < subs; i++ {
		w.subTrackers = append(w.subTrackers, newSubTracker())
	}
	return w
}

// Start brings up one continuously-credited subscriber per fan-out member.
func (w *EventsWorker) Start(ctx context.Context) error {
	w.consumerCtx, w.consumerCancel = context.WithCancel(ctx)
	for i := range w.subTrackers {
		w.consumerWG.Add(1)
		go func(subIdx int) {
			defer w.consumerWG.Done()
			w.subscribeLoop(w.consumerCtx, subIdx)
		}(i)
	}
	return nil
}

func (w *EventsWorker) subscribeLoop(ctx context.Context, subIdx int) {
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
		// Standing link credit keeps the fire-hose subscription continuously
		// credited so the connector never has to drop at 0 credit (gotcha #1).
		rcv, err := transport.NewReceiver(ctx, sess, w.address, w.credit)
		if err != nil {
			w.recordError("consume_failure")
			w.cleanupConn(conn)
			if !sleepCtx(ctx, time.Second) {
				return
			}
			continue
		}
		w.signalReady()

		w.drainSub(ctx, subIdx, rcv)
		w.cleanupConn(conn)
		if ctx.Err() == nil {
			w.recordReconnection()
		}
	}
}

func (w *EventsWorker) drainSub(ctx context.Context, subIdx int, rcv *amqp.Receiver) {
	st := w.subTrackers[subIdx]
	for {
		if ctx.Err() != nil {
			return
		}
		msg, err := rcv.Receive(ctx, nil)
		if err != nil {
			return
		}
		body := msg.GetData()
		_, seq, crcHex, sentAt, valid := extractMeta(msg)
		if valid {
			if crcHex != "" && !payload.VerifyCRC(body, crcHex) {
				w.recordCorrupted()
			}
			if !sentAt.IsZero() {
				w.recordLatency(time.Since(sentAt))
			}
			if dup := st.record(seq); dup {
				w.recordDuplicated()
			}
		}
		w.recordReceived(len(body))
		// at-most-once: settle each delivery as accepted (go-amqp keeps the link
		// continuously credited as deliveries settle, preserving credit hygiene).
		if err := rcv.AcceptMessage(ctx, msg); err != nil {
			return
		}
	}
}

// StartProducers publishes fire-and-forget events to events/<channel>.
func (w *EventsWorker) StartProducers() {
	w.producerCtx, w.producerCancel = context.WithCancel(context.Background())
	w.producerWG.Add(1)
	go func() {
		defer w.producerWG.Done()
		w.produceLoop(w.producerCtx)
	}()
}

func (w *EventsWorker) produceLoop(ctx context.Context) {
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
		body, crcHex := payload.Build(w.selectMessageSize())
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

func (w *EventsWorker) connectSender(ctx context.Context) (*amqp.Conn, *amqp.Sender, error) {
	conn, err := transport.Dial(ctx, w.dialCfg)
	if err != nil {
		return nil, nil, err
	}
	sess, err := transport.NewSession(ctx, conn)
	if err != nil {
		closeConn(conn)
		return nil, nil, err
	}
	// Events are at-most-once: settled sender (fire-and-forget).
	sender, err := transport.NewSender(ctx, sess, w.address, "settled")
	if err != nil {
		closeConn(conn)
		return nil, nil, err
	}
	return conn, sender, nil
}

func (w *EventsWorker) cleanupConn(conn *amqp.Conn) {
	w.unregisterConsumerConn(conn)
	closeConn(conn)
}

// ExtraLost reports the worst-case fan-out loss across subscribers, surfaced via
// the engine snapshot so the verdict can gate broadcast fidelity. Note: a true
// connector 0-credit drop would surface as EventsDropped() (gated separately ==
// 0); this ExtraLost is the harness-side fan-out gap detector.
func (w *EventsWorker) ExtraLost() uint64 {
	var maxLost uint64
	for _, st := range w.subTrackers {
		if m := st.missing(); m > maxLost {
			maxLost = m
		}
	}
	return maxLost
}

// assert the connector-side metric is reachable from this worker for the engine
// gate (events_dropped_no_credit_total == 0). The base worker increments it via
// recordEventsDropped when a drop is observed; this is a compile-time anchor.
var _ = metrics.IncEventsDroppedNoCredit
