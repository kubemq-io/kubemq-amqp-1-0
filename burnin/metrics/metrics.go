// Package metrics defines the Prometheus burnin_* metric surface for the AMQP 1.0
// burn-in (sdk label "amqp10"), plus the in-memory latency / rate accumulators
// used to compute verdict percentiles. Recast from the kubemq-amqp-rabbitmq base:
// the 0-9-1 confirms_nacked / returns counters are replaced by the two AMQP 1.0
// drop-metric gates — events_dropped_no_credit_total (fire-hose 0-credit drop,
// gate == 0) and events_store_dropped_stalled_total (stalled-credit drop, gate
// == 0) — and a fidelity-failure counter (CRC mismatch / unexplained gap /
// over-tolerance dup) gated by max_amqp10_fidelity_failures.
package metrics

import (
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const sdkLabel = "amqp10"

var (
	latencyBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}
	rpcBuckets     = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

	// ── Counters ──

	MessagesSentTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_sent_total",
		Help: "Total messages published",
	}, []string{"sdk", "worker", "producerid"})

	MessagesReceivedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_received_total",
		Help: "Total messages delivered to consumers",
	}, []string{"sdk", "worker", "consumer_id"})

	MessagesLostTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_lost_total",
		Help: "Confirmed lost messages",
	}, []string{"sdk", "worker"})

	MessagesDuplicatedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_duplicated_total",
		Help: "Messages detected as duplicated",
	}, []string{"sdk", "worker"})

	MessagesCorruptedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_corrupted_total",
		Help: "Messages with CRC32 hash mismatch",
	}, []string{"sdk", "worker"})

	MessagesOutOfOrderTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_messages_out_of_order_total",
		Help: "Messages received out of sequence order",
	}, []string{"sdk", "worker"})

	ErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_errors_total",
		Help: "Errors by type",
	}, []string{"sdk", "worker", "error_type"})

	ReconnectionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_reconnections_total",
		Help: "Number of reconnection events",
	}, []string{"sdk", "worker"})

	BytesSentTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_bytes_sent_total",
		Help: "Total bytes published",
	}, []string{"sdk", "worker"})

	BytesReceivedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_bytes_received_total",
		Help: "Total bytes delivered",
	}, []string{"sdk", "worker"})

	RPCResponsesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_rpc_responses_total",
		Help: "RPC responses by status",
	}, []string{"sdk", "worker", "status"})

	DowntimeSecondsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_downtime_seconds_total",
		Help: "Cumulative time spent reconnecting",
	}, []string{"sdk", "worker"})

	ForcedDisconnectsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_forced_disconnects_total",
		Help: "Number of forced disconnect events",
	}, []string{"sdk"})

	// ── AMQP 1.0-specific counters (spec §9.4) ──

	// EventsDroppedNoCreditTotal counts events the connector dropped because no
	// consumer credit was outstanding (fire-hose). On a healthy broker with
	// continuous-credit hygiene this MUST stay 0 (events worker gate).
	EventsDroppedNoCreditTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_events_dropped_no_credit_total",
		Help: "Events dropped because no consumer credit was outstanding (gate == 0)",
	}, []string{"sdk", "worker"})

	// EventsStoreDroppedStalledTotal counts events-store deliveries dropped due to
	// stalled credit (link unsettled window exhausted). With eager replenishment
	// this MUST stay 0 (events_store worker gate, Phase 2).
	EventsStoreDroppedStalledTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_events_store_dropped_stalled_total",
		Help: "Events-store deliveries dropped due to stalled credit (gate == 0)",
	}, []string{"sdk", "worker"})

	// FidelityFailuresTotal counts self-accounting fidelity violations: CRC
	// mismatch, unexplained sequence gap, or over-tolerance duplicate. Gated by
	// thresholds.max_amqp10_fidelity_failures.
	FidelityFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "burnin_amqp10_fidelity_failures_total",
		Help: "Self-accounting fidelity violations (CRC mismatch / gap / over-tolerance dup)",
	}, []string{"sdk", "worker", "kind"})

	// ── Histograms ──

	MessageLatencySeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "burnin_message_latency_seconds",
		Help:    "End-to-end message latency",
		Buckets: latencyBuckets,
	}, []string{"sdk", "worker"})

	SendDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "burnin_send_duration_seconds",
		Help:    "AMQP 1.0 sender Send round-trip time (unsettled DISPOSITION)",
		Buckets: latencyBuckets,
	}, []string{"sdk", "worker"})

	RPCDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "burnin_rpc_duration_seconds",
		Help:    "Native AMQP RPC round-trip duration",
		Buckets: rpcBuckets,
	}, []string{"sdk", "worker"})

	// ── Gauges ──

	ActiveConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_active_connections",
		Help: "Currently active AMQP 1.0 connections",
	}, []string{"sdk", "worker"})

	UptimeSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_uptime_seconds",
		Help: "Burn-in app uptime",
	}, []string{"sdk"})

	TargetRate = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_target_rate",
		Help: "Configured target rate (msgs/sec)",
	}, []string{"sdk", "worker"})

	ActualRate = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_actual_rate",
		Help: "Current achieved rate (msgs/sec)",
	}, []string{"sdk", "worker"})

	ConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_consumer_lag_messages",
		Help: "Sent minus received (producer-consumer lag)",
	}, []string{"sdk", "worker"})

	WarmupActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burnin_warmup_active",
		Help: "1 during warmup, 0 after",
	}, []string{"sdk"})
)

// SDK returns the metric SDK label value ("amqp10").
func SDK() string { return sdkLabel }

// InitMetrics pre-initializes all metrics to 0 with well-known label values so
// dashboards don't fire absent() alerts.
func InitMetrics(workers []string) {
	errorTypes := []string{
		"send_failure", "consume_failure", "accept_failure", "rpc_timeout",
		"rpc_error", "attach_failure", "connect_failure", "drain_failure",
	}
	for _, w := range workers {
		MessagesSentTotal.WithLabelValues(sdkLabel, w, "p-"+w+"-000").Add(0)
		MessagesReceivedTotal.WithLabelValues(sdkLabel, w, "c-"+w+"-000").Add(0)
		MessagesLostTotal.WithLabelValues(sdkLabel, w).Add(0)
		MessagesDuplicatedTotal.WithLabelValues(sdkLabel, w).Add(0)
		MessagesCorruptedTotal.WithLabelValues(sdkLabel, w).Add(0)
		MessagesOutOfOrderTotal.WithLabelValues(sdkLabel, w).Add(0)
		ReconnectionsTotal.WithLabelValues(sdkLabel, w).Add(0)
		BytesSentTotal.WithLabelValues(sdkLabel, w).Add(0)
		BytesReceivedTotal.WithLabelValues(sdkLabel, w).Add(0)
		DowntimeSecondsTotal.WithLabelValues(sdkLabel, w).Add(0)
		EventsDroppedNoCreditTotal.WithLabelValues(sdkLabel, w).Add(0)
		EventsStoreDroppedStalledTotal.WithLabelValues(sdkLabel, w).Add(0)
		for _, et := range errorTypes {
			ErrorsTotal.WithLabelValues(sdkLabel, w, et).Add(0)
		}
		for _, kind := range []string{"crc", "gap", "dup"} {
			FidelityFailuresTotal.WithLabelValues(sdkLabel, w, kind).Add(0)
		}
		RPCResponsesTotal.WithLabelValues(sdkLabel, w, "success").Add(0)
		RPCResponsesTotal.WithLabelValues(sdkLabel, w, "timeout").Add(0)
		RPCResponsesTotal.WithLabelValues(sdkLabel, w, "error").Add(0)
		ActiveConnections.WithLabelValues(sdkLabel, w).Set(0)
		TargetRate.WithLabelValues(sdkLabel, w).Set(0)
		ActualRate.WithLabelValues(sdkLabel, w).Set(0)
		ConsumerLag.WithLabelValues(sdkLabel, w).Set(0)
	}
	ForcedDisconnectsTotal.WithLabelValues(sdkLabel).Add(0)
	UptimeSeconds.WithLabelValues(sdkLabel).Set(0)
	WarmupActive.WithLabelValues(sdkLabel).Set(0)
}

// ── Counter helpers ──

func IncSent(worker, producerID string) {
	MessagesSentTotal.WithLabelValues(sdkLabel, worker, producerID).Inc()
}

func IncReceived(worker, consumerID string) {
	MessagesReceivedTotal.WithLabelValues(sdkLabel, worker, consumerID).Inc()
}

func AddLost(worker string, delta uint64) {
	MessagesLostTotal.WithLabelValues(sdkLabel, worker).Add(float64(delta))
}
func IncDuplicated(worker string) { MessagesDuplicatedTotal.WithLabelValues(sdkLabel, worker).Inc() }
func IncCorrupted(worker string)  { MessagesCorruptedTotal.WithLabelValues(sdkLabel, worker).Inc() }
func IncOutOfOrder(worker string) { MessagesOutOfOrderTotal.WithLabelValues(sdkLabel, worker).Inc() }
func IncError(worker, errorType string) {
	ErrorsTotal.WithLabelValues(sdkLabel, worker, errorType).Inc()
}
func IncReconnection(worker string) { ReconnectionsTotal.WithLabelValues(sdkLabel, worker).Inc() }
func IncEventsDroppedNoCredit(worker string) {
	EventsDroppedNoCreditTotal.WithLabelValues(sdkLabel, worker).Inc()
}

func IncEventsStoreDroppedStalled(worker string) {
	EventsStoreDroppedStalledTotal.WithLabelValues(sdkLabel, worker).Inc()
}

func IncFidelityFailure(worker, kind string) {
	FidelityFailuresTotal.WithLabelValues(sdkLabel, worker, kind).Inc()
}

func ObserveLatency(worker string, d time.Duration) {
	MessageLatencySeconds.WithLabelValues(sdkLabel, worker).Observe(d.Seconds())
}

func ObserveSendDuration(worker string, d time.Duration) {
	SendDurationSeconds.WithLabelValues(sdkLabel, worker).Observe(d.Seconds())
}

func ObserveRPCDuration(worker string, d time.Duration) {
	RPCDurationSeconds.WithLabelValues(sdkLabel, worker).Observe(d.Seconds())
}

func IncRPCResponse(worker, status string) {
	RPCResponsesTotal.WithLabelValues(sdkLabel, worker, status).Inc()
}

func AddDowntime(worker string, seconds float64) {
	DowntimeSecondsTotal.WithLabelValues(sdkLabel, worker).Add(seconds)
}
func IncForcedDisconnect() { ForcedDisconnectsTotal.WithLabelValues(sdkLabel).Inc() }
func RecordBytesSent(worker string, n int) {
	BytesSentTotal.WithLabelValues(sdkLabel, worker).Add(float64(n))
}

func RecordBytesReceived(worker string, n int) {
	BytesReceivedTotal.WithLabelValues(sdkLabel, worker).Add(float64(n))
}

// ── Gauge helpers ──

func SetActiveConnections(worker string, n float64) {
	ActiveConnections.WithLabelValues(sdkLabel, worker).Set(n)
}
func SetTargetRate(worker string, r float64) { TargetRate.WithLabelValues(sdkLabel, worker).Set(r) }
func SetActualRate(worker string, r float64) { ActualRate.WithLabelValues(sdkLabel, worker).Set(r) }
func SetConsumerLag(worker string, lag float64) {
	ConsumerLag.WithLabelValues(sdkLabel, worker).Set(lag)
}

// ── In-memory accumulators ──

// LatencyAccumulator records latency values for percentile computation at
// verdict time using an HdrHistogram (1µs–60s, 3 sig figs).
type LatencyAccumulator struct {
	mu   sync.Mutex
	hist *hdrhistogram.Histogram
}

func NewLatencyAccumulator() *LatencyAccumulator {
	return &LatencyAccumulator{hist: hdrhistogram.New(1, 60_000_000, 3)}
}

func (a *LatencyAccumulator) Record(d time.Duration) {
	a.mu.Lock()
	_ = a.hist.RecordValue(d.Microseconds())
	a.mu.Unlock()
}

// Percentiles returns P50, P95, P99, P99.9 in milliseconds.
func (a *LatencyAccumulator) Percentiles() (p50, p95, p99, p999 float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	p50 = float64(a.hist.ValueAtQuantile(50)) / 1000.0
	p95 = float64(a.hist.ValueAtQuantile(95)) / 1000.0
	p99 = float64(a.hist.ValueAtQuantile(99)) / 1000.0
	p999 = float64(a.hist.ValueAtQuantile(99.9)) / 1000.0
	return
}

func (a *LatencyAccumulator) Reset() {
	a.mu.Lock()
	a.hist.Reset()
	a.mu.Unlock()
}

func (a *LatencyAccumulator) Count() int64 {
	a.mu.Lock()
	c := a.hist.TotalCount()
	a.mu.Unlock()
	return c
}

const slidingWindowSize = 30

// SlidingRateWindow tracks message rate over a 30-second sliding window.
type SlidingRateWindow struct {
	mu      sync.Mutex
	buckets [slidingWindowSize]int64
	idx     int
	total   int64
	ticks   int
}

func NewSlidingRateWindow() *SlidingRateWindow { return &SlidingRateWindow{} }

func (w *SlidingRateWindow) Record() {
	w.mu.Lock()
	w.buckets[w.idx]++
	w.total++
	w.mu.Unlock()
}

func (w *SlidingRateWindow) Advance() {
	w.mu.Lock()
	w.idx = (w.idx + 1) % slidingWindowSize
	w.total -= w.buckets[w.idx]
	w.buckets[w.idx] = 0
	w.ticks++
	w.mu.Unlock()
}

func (w *SlidingRateWindow) Rate() float64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	window := w.ticks
	if window > slidingWindowSize {
		window = slidingWindowSize
	}
	if window == 0 {
		return 0
	}
	return float64(w.total) / float64(window)
}

func (w *SlidingRateWindow) Reset() {
	w.mu.Lock()
	for i := range w.buckets {
		w.buckets[i] = 0
	}
	w.total = 0
	w.idx = 0
	w.ticks = 0
	w.mu.Unlock()
}

const peakWindowSize = 10

// PeakRateTracker tracks peak throughput over a 10-second sliding window.
type PeakRateTracker struct {
	mu      sync.Mutex
	buckets []int64
	idx     int
	peak    float64
}

func NewPeakRateTracker() *PeakRateTracker {
	return &PeakRateTracker{buckets: make([]int64, peakWindowSize)}
}

func (p *PeakRateTracker) Record() {
	p.mu.Lock()
	p.buckets[p.idx]++
	p.mu.Unlock()
}

func (p *PeakRateTracker) Advance() {
	p.mu.Lock()
	defer p.mu.Unlock()
	var total int64
	for _, b := range p.buckets {
		total += b
	}
	avg := float64(total) / float64(peakWindowSize)
	if avg > p.peak {
		p.peak = avg
	}
	p.idx = (p.idx + 1) % peakWindowSize
	p.buckets[p.idx] = 0
}

func (p *PeakRateTracker) Peak() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peak
}
