package engine

import (
	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/metrics"
	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/worker"
)

// metricsLatency is a thin interface over the latency accumulator used to
// extract percentiles for the worker with the most samples.
type metricsLatency struct {
	acc *metrics.LatencyAccumulator
}

func wrapLatency(acc *metrics.LatencyAccumulator) *metricsLatency {
	return &metricsLatency{acc: acc}
}

func (m *metricsLatency) Percentiles() (p50, p95, p99, p999 float64) {
	return m.acc.Percentiles()
}

// extraLostReporter is implemented by workers that compute worker-specific lost
// counts not captured by the per-producer sequence tracker (events fan-out gaps).
type extraLostReporter interface {
	// ExtraLost returns additional confirmed-lost / fidelity-violation count.
	ExtraLost() uint64
}

// extraLost sums any worker-specific extra-lost contributions across a group.
func extraLost(workers []worker.Worker) uint64 {
	var total uint64
	for _, w := range workers {
		if r, ok := w.(extraLostReporter); ok {
			total += r.ExtraLost()
		}
	}
	return total
}
