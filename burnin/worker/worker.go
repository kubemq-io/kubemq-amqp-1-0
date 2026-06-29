// Package worker implements the six AMQP 1.0 burn-in workers (spec §9.2): queues,
// events, events_store, commands, queries, credit_flow. Each worker drives the
// KubeMQ embedded AMQP 1.0 connector directly via github.com/Azure/go-amqp (NO
// kubemq-go, NO proto, NO gRPC) and records loss/dup/latency/throughput via the
// shared BaseWorker.
//
// All six workers are real: queues + events (produce/consume + fan-out),
// events_store (durable replay/resume + stalled-credit gate), commands + queries
// (native AMQP RPC — dynamic reply node + correlation-id), and credit_flow
// (manual-credit IssueCredit/DrainCredit, exact-credit delivery).
package worker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/Azure/go-amqp"
	"golang.org/x/time/rate"

	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/config"
	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/metrics"
	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/payload"
	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/tracker"
	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/transport"
)

// Worker is the lifecycle contract every burn-in worker satisfies. The engine
// drives Start (consumers/responders) → wait ready → StartProducers (measurement
// window) → StopProducers → drain → StopConsumers.
type Worker interface {
	Name() string
	ChannelName() string
	ChannelIndex() int

	// Start brings up consumers/responders and signals ConsumerReady once they
	// are receiving.
	Start(ctx context.Context) error
	StartProducers()
	StopProducers()
	StopConsumers()
	DisconnectConsumers()
	ConsumerReady() <-chan struct{}

	Tracker() *tracker.Tracker
	LatencyAccumulator() *metrics.LatencyAccumulator
	RPCLatencyAccumulator() *metrics.LatencyAccumulator
	PeakRate() *metrics.PeakRateTracker
	RateWindow() *metrics.SlidingRateWindow

	SentCount() uint64
	ReceivedCount() uint64
	ErrorCount() uint64
	CorruptedCount() uint64
	ReconnectionCount() uint64
	DowntimeSeconds() float64
	DuplicatedCount() uint64
	FidelityFailures() uint64
	EventsDropped() uint64

	RPCSuccess() uint64
	RPCTimeout() uint64
	RPCError() uint64

	AdvanceRateWindows()
	ResetAfterWarmup()
}

// BaseWorker holds the shared state and helpers for all workers.
type BaseWorker struct {
	name         string
	channelName  string
	channelIndex int
	cfg          *config.Config
	workerCfg    *config.WorkerConfig
	logger       *slog.Logger

	dialCfg     transport.DialConfig
	settleMode  string
	credit      int32
	sizeDistrib *payload.SizeDistribution

	trk         *tracker.Tracker
	latAccum    *metrics.LatencyAccumulator
	rpcLatAccum *metrics.LatencyAccumulator
	peakRate    *metrics.PeakRateTracker
	rateWindow  *metrics.SlidingRateWindow

	limiter *rate.Limiter

	producerCtx    context.Context
	producerCancel context.CancelFunc
	consumerCtx    context.Context
	consumerCancel context.CancelFunc
	producerWG     sync.WaitGroup
	consumerWG     sync.WaitGroup
	consumerReady  chan struct{}
	readyOnce      sync.Once

	sent          atomic.Uint64
	received      atomic.Uint64
	errors        atomic.Uint64
	corrupted     atomic.Uint64
	reconnections atomic.Uint64
	downtime      atomic.Uint64 // nanoseconds
	duplicated    atomic.Uint64
	fidelityFails atomic.Uint64
	eventsDropped atomic.Uint64

	rpcSuccessCount atomic.Uint64
	rpcTimeoutCount atomic.Uint64
	rpcErrorCount   atomic.Uint64

	// consumer connections registered for forced-disconnect injection.
	consumerConns   []*amqp.Conn
	consumerConnsMu sync.Mutex
}

// NewBaseWorker constructs the shared worker scaffolding.
func NewBaseWorker(name, channelName string, channelIndex int, cfg *config.Config, logger *slog.Logger) *BaseWorker {
	workerCfg := cfg.GetWorkerConfig(name)

	targetRate := float64(workerCfg.Rate)
	burst := int(targetRate)
	if burst < 1 {
		burst = 1
	}

	var sizeDistrib *payload.SizeDistribution
	if cfg.Message.SizeMode == "distribution" {
		sizeDistrib, _ = payload.ParseDistribution(cfg.Message.SizeDistribution)
	}

	return &BaseWorker{
		name:         name,
		channelName:  channelName,
		channelIndex: channelIndex,
		cfg:          cfg,
		workerCfg:    workerCfg,
		logger:       logger.With("worker", name, "channel", channelName),

		dialCfg: transport.DialConfig{
			Address:     cfg.Broker.Address,
			ClientID:    cfg.Broker.ClientIDPrefix + "-" + channelName,
			TLS:         cfg.Amqp10.TLS,
			Auth:        cfg.Amqp10.Auth,
			IdleTimeout: cfg.ReconnectMaxInterval,
		},
		settleMode:  cfg.WorkerSettleMode(name),
		credit:      cfg.Amqp10.Credit,
		sizeDistrib: sizeDistrib,

		trk:         tracker.New(cfg.Message.ReorderWindow),
		latAccum:    metrics.NewLatencyAccumulator(),
		rpcLatAccum: metrics.NewLatencyAccumulator(),
		peakRate:    metrics.NewPeakRateTracker(),
		rateWindow:  metrics.NewSlidingRateWindow(),

		limiter:       rate.NewLimiter(rate.Limit(targetRate), burst),
		consumerReady: make(chan struct{}),
	}
}

// --- Accessors ---

func (b *BaseWorker) Name() string                                       { return b.name }
func (b *BaseWorker) ChannelName() string                                { return b.channelName }
func (b *BaseWorker) ChannelIndex() int                                  { return b.channelIndex }
func (b *BaseWorker) ConsumerReady() <-chan struct{}                     { return b.consumerReady }
func (b *BaseWorker) Tracker() *tracker.Tracker                          { return b.trk }
func (b *BaseWorker) LatencyAccumulator() *metrics.LatencyAccumulator    { return b.latAccum }
func (b *BaseWorker) RPCLatencyAccumulator() *metrics.LatencyAccumulator { return b.rpcLatAccum }
func (b *BaseWorker) PeakRate() *metrics.PeakRateTracker                 { return b.peakRate }
func (b *BaseWorker) RateWindow() *metrics.SlidingRateWindow             { return b.rateWindow }

func (b *BaseWorker) SentCount() uint64         { return b.sent.Load() }
func (b *BaseWorker) ReceivedCount() uint64     { return b.received.Load() }
func (b *BaseWorker) ErrorCount() uint64        { return b.errors.Load() }
func (b *BaseWorker) CorruptedCount() uint64    { return b.corrupted.Load() }
func (b *BaseWorker) ReconnectionCount() uint64 { return b.reconnections.Load() }
func (b *BaseWorker) DuplicatedCount() uint64   { return b.duplicated.Load() }
func (b *BaseWorker) FidelityFailures() uint64  { return b.fidelityFails.Load() }
func (b *BaseWorker) EventsDropped() uint64     { return b.eventsDropped.Load() }
func (b *BaseWorker) RPCSuccess() uint64        { return b.rpcSuccessCount.Load() }
func (b *BaseWorker) RPCTimeout() uint64        { return b.rpcTimeoutCount.Load() }
func (b *BaseWorker) RPCError() uint64          { return b.rpcErrorCount.Load() }

func (b *BaseWorker) DowntimeSeconds() float64 {
	return float64(b.downtime.Load()) / float64(time.Second)
}

// --- Counter helpers (used by concrete workers) ---

func (b *BaseWorker) recordSent(bytes int) {
	b.sent.Add(1)
	b.rateWindow.Record()
	b.peakRate.Record()
	metrics.IncSent(b.name, b.channelName)
	metrics.RecordBytesSent(b.name, bytes)
}

func (b *BaseWorker) recordReceived(bytes int) {
	b.received.Add(1)
	metrics.IncReceived(b.name, b.channelName)
	metrics.RecordBytesReceived(b.name, bytes)
}

func (b *BaseWorker) recordError(errType string) {
	b.errors.Add(1)
	metrics.IncError(b.name, errType)
}

// recordCorrupted records a CRC mismatch — also a fidelity failure (kind=crc).
func (b *BaseWorker) recordCorrupted() {
	b.corrupted.Add(1)
	b.fidelityFails.Add(1)
	metrics.IncCorrupted(b.name)
	metrics.IncFidelityFailure(b.name, "crc")
}

// recordDuplicated records a duplicate. Over-tolerance duplicates are folded into
// fidelity at verdict time, not per-message.
func (b *BaseWorker) recordDuplicated() {
	b.duplicated.Add(1)
	metrics.IncDuplicated(b.name)
}

// recordFidelityGap records an unexplained sequence gap as a fidelity failure.
func (b *BaseWorker) recordFidelityGap(delta uint64) {
	b.fidelityFails.Add(delta)
	for i := uint64(0); i < delta; i++ {
		metrics.IncFidelityFailure(b.name, "gap")
	}
}

func (b *BaseWorker) recordReconnection() {
	b.reconnections.Add(1)
	metrics.IncReconnection(b.name)
}

// recordEventsStoreDroppedStalled records a connector stalled-credit drop on an
// events-store link (resource-limit DETACH). The events_store worker gate asserts
// this stays 0 when credit is replenished eagerly.
func (b *BaseWorker) recordEventsStoreDroppedStalled() {
	b.eventsDropped.Add(1)
	metrics.IncEventsStoreDroppedStalled(b.name)
}

// --- RPC accounting (commands / queries) ---

// recordRPCSuccess records a successful RPC round-trip plus its latency.
func (b *BaseWorker) recordRPCSuccess(d time.Duration) {
	b.rpcSuccessCount.Add(1)
	b.rpcLatAccum.Record(d)
	metrics.ObserveRPCDuration(b.name, d)
	metrics.IncRPCResponse(b.name, "success")
}

// recordRPCTimeout records an RPC that timed out awaiting its reply.
func (b *BaseWorker) recordRPCTimeout() {
	b.rpcTimeoutCount.Add(1)
	metrics.IncRPCResponse(b.name, "timeout")
	metrics.IncError(b.name, "rpc_timeout")
}

// recordRPCError records an RPC that failed for a non-timeout reason (send
// failure, executed=false, correlation mismatch, transport error).
func (b *BaseWorker) recordRPCError() {
	b.rpcErrorCount.Add(1)
	metrics.IncRPCResponse(b.name, "error")
	metrics.IncError(b.name, "rpc_error")
}

func (b *BaseWorker) recordLatency(d time.Duration) {
	b.latAccum.Record(d)
	metrics.ObserveLatency(b.name, d)
}

// recordTracked feeds a received (worker-id, seq) into the tracker and records
// duplicates / out-of-order in metrics.
func (b *BaseWorker) recordTracked(producerID string, seq uint64) {
	dup, oo := b.trk.Record(producerID, seq)
	if dup {
		b.recordDuplicated()
	}
	if oo {
		metrics.IncOutOfOrder(b.name)
	}
}

// --- Rate control ---

func (b *BaseWorker) waitForRate(ctx context.Context) error {
	return b.limiter.Wait(ctx)
}

// creditOrDefault returns a positive standing link credit, clamped to the
// connector's MaxUnsettledPerLink ceiling (1024). Used by the RPC workers, whose
// receivers always run in auto-credit mode (never manual -1).
func (b *BaseWorker) creditOrDefault() int32 {
	c := b.credit
	if c < 1 {
		c = 10
	}
	if c > 1024 {
		c = 1024
	}
	return c
}

// --- Message size ---

func (b *BaseWorker) selectMessageSize() int {
	if b.cfg.Message.SizeMode == "distribution" && b.sizeDistrib != nil {
		return b.sizeDistrib.SelectSize()
	}
	return b.cfg.Message.SizeBytes
}

// --- Rate windows ---

func (b *BaseWorker) AdvanceRateWindows() {
	b.rateWindow.Advance()
	b.peakRate.Advance()
}

// --- Warmup reset ---

func (b *BaseWorker) ResetAfterWarmup() {
	b.trk.Reset()
	b.latAccum.Reset()
	b.rpcLatAccum.Reset()
	b.peakRate = metrics.NewPeakRateTracker()
	b.rateWindow.Reset()

	b.sent.Store(0)
	b.received.Store(0)
	b.errors.Store(0)
	b.corrupted.Store(0)
	b.reconnections.Store(0)
	b.downtime.Store(0)
	b.duplicated.Store(0)
	b.fidelityFails.Store(0)
	b.eventsDropped.Store(0)
	b.rpcSuccessCount.Store(0)
	b.rpcTimeoutCount.Store(0)
	b.rpcErrorCount.Store(0)
}

// --- Ready signalling ---

func (b *BaseWorker) signalReady() {
	b.readyOnce.Do(func() { close(b.consumerReady) })
}

// --- Lifecycle helpers shared by concrete workers ---

func (b *BaseWorker) StartProducers() {
	// Default no-op; concrete producer workers override.
}

func (b *BaseWorker) StopProducers() {
	if b.producerCancel != nil {
		b.producerCancel()
	}
	b.producerWG.Wait()
}

func (b *BaseWorker) StopConsumers() {
	if b.consumerCancel != nil {
		b.consumerCancel()
	}
	b.consumerWG.Wait()
}

// registerConsumerConn tracks a live consumer connection so forced disconnect
// can close it (the consumer loop then auto-reconnects).
func (b *BaseWorker) registerConsumerConn(c *amqp.Conn) {
	b.consumerConnsMu.Lock()
	b.consumerConns = append(b.consumerConns, c)
	b.consumerConnsMu.Unlock()
}

func (b *BaseWorker) unregisterConsumerConn(c *amqp.Conn) {
	b.consumerConnsMu.Lock()
	for i, cc := range b.consumerConns {
		if cc == c {
			b.consumerConns = append(b.consumerConns[:i], b.consumerConns[i+1:]...)
			break
		}
	}
	b.consumerConnsMu.Unlock()
}

// DisconnectConsumers force-closes all live consumer connections (forced
// disconnect injector). Consumers re-dial on the next loop iteration.
func (b *BaseWorker) DisconnectConsumers() {
	b.consumerConnsMu.Lock()
	conns := make([]*amqp.Conn, len(b.consumerConns))
	copy(conns, b.consumerConns)
	b.consumerConnsMu.Unlock()
	for _, c := range conns {
		if c != nil {
			_ = c.Close()
		}
	}
}
