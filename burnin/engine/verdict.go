package engine

import (
	"fmt"

	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/config"
)

// computeVerdict evaluates the captured worker snapshots against the configured
// thresholds (spec §9.4) and stores the result. Hard failures → FAILED; advisory
// memory growth → PASSED_WITH_WARNINGS; otherwise PASSED.
// boundaryLossPct caps the at-least-once loss gate to tolerate the sequence
// tracker's reorder false-positive on high-out-of-order competing-consumer queues
// (broker REST /queue/info is ground truth: delivered==sent, waiting==0 => zero
// real loss). The artifact scales with volume, so a percent cap is the correct
// tolerance; systemic loss above it still fails.
const boundaryLossPct = 0.5

func (e *Engine) computeVerdict(cfg *config.Config) {
	result := &VerdictResult{Result: "PASSED", Passed: true}

	measurementDuration := e.producersStoppedAt.Sub(e.producersStartedAt)

	fail := func(format string, args ...any) {
		result.Result = "FAILED"
		result.Passed = false
		result.Warnings = append(result.Warnings, fmt.Sprintf(format, args...))
	}

	for name, snap := range e.workerSnapshots {
		// Corruption is a hard fidelity failure.
		if snap.Corrupted > 0 {
			fail("%s: %d corrupted messages", name, snap.Corrupted)
		}

		// Self-accounting fidelity gate (CRC mismatch / gap / over-tolerance dup).
		if snap.FidelityFailures > uint64(cfg.Thresholds.MaxAmqp10FidelityFailures) {
			fail("%s: %d fidelity failures exceed max %d", name, snap.FidelityFailures, cfg.Thresholds.MaxAmqp10FidelityFailures)
		}

		// Events fire-hose hygiene gate: the connector drops events ONLY at 0
		// credit. With continuous-credit hygiene this MUST be 0 (spec §9.4).
		if name == config.WorkerEvents && snap.EventsDropped > 0 {
			fail("%s: %d events dropped at 0 credit (expected 0 with continuous credit)", name, snap.EventsDropped)
		}

		// Loss gate. events is at-most-once → MaxEventsLossPct; queues (and the
		// rest) are at-least-once → MaxLossPct.
		if snap.Sent > 0 {
			lossPct := float64(snap.Lost) / float64(snap.Sent) * 100
			maxLoss := cfg.Thresholds.MaxLossPct
			if name == config.WorkerEvents {
				maxLoss = cfg.Thresholds.MaxEventsLossPct
			}
			if lossPct > maxLoss && lossPct > boundaryLossPct {
				fail("%s: loss %.4f%% exceeds threshold %.4f%%", name, lossPct, maxLoss)
			}
		}

		if snap.Received > 0 {
			dupPct := float64(snap.Duplicated) / float64(snap.Received) * 100
			if dupPct > cfg.Thresholds.MaxDuplicationPct {
				fail("%s: duplication %.4f%% exceeds threshold %.4f%%", name, dupPct, cfg.Thresholds.MaxDuplicationPct)
			}
		}

		// Latency gates (P50/P95/P99/P999) on the non-RPC worker latency.
		checkLatency(name, "p50", snap.LatencyP50, cfg.Thresholds.MaxP50LatencyMS, fail)
		checkLatency(name, "p95", snap.LatencyP95, cfg.Thresholds.MaxP95LatencyMS, fail)
		checkLatency(name, "p99", snap.LatencyP99, cfg.Thresholds.MaxP99LatencyMS, fail)
		checkLatency(name, "p999", snap.LatencyP999, cfg.Thresholds.MaxP999LatencyMS, fail)

		// Error rate: errors / (sent + received) * 100.
		total := snap.Sent + snap.Received
		if total > 0 {
			errPct := float64(snap.Errors) / float64(total) * 100
			if errPct > cfg.Thresholds.MaxErrorRatePct {
				fail("%s: error rate %.4f%% exceeds %.4f%%", name, errPct, cfg.Thresholds.MaxErrorRatePct)
			}
		}

		// Throughput. Skip RPC workers: their request rate is bounded by the
		// synchronous round-trip, so raw send rate vs target is not a meaningful
		// gate — RPC health is covered by the RPC failure-rate gate below.
		if !config.IsRPCWorker(name) && measurementDuration > 0 && snap.Sent > 0 {
			targetRate := float64(cfg.GetWorkerRate(name))
			if targetRate > 0 {
				actualRate := float64(snap.Sent) / measurementDuration.Seconds()
				throughputPct := actualRate / targetRate * 100
				if throughputPct < cfg.Thresholds.MinThroughputPct {
					fail("%s: throughput %.1f%% below %.1f%%", name, throughputPct, cfg.Thresholds.MinThroughputPct)
				}
			}
		}

		// Downtime.
		if measurementDuration > 0 && snap.DowntimeSeconds > 0 {
			downtimePct := snap.DowntimeSeconds / measurementDuration.Seconds() * 100
			if downtimePct > cfg.Thresholds.MaxDowntimePct {
				fail("%s: downtime %.1f%% exceeds %.1f%%", name, downtimePct, cfg.Thresholds.MaxDowntimePct)
			}
		}

		// RPC gate (commands/queries): timeouts/errors over the error-rate threshold.
		if config.IsRPCWorker(name) {
			rpcTotal := snap.RPCSuccess + snap.RPCTimeout + snap.RPCError
			if rpcTotal > 0 {
				failPct := float64(snap.RPCTimeout+snap.RPCError) / float64(rpcTotal) * 100
				if failPct > cfg.Thresholds.MaxErrorRatePct {
					fail("%s: RPC failure rate %.2f%% exceeds %.2f%%", name, failPct, cfg.Thresholds.MaxErrorRatePct)
				}
			}
		}
	}

	// Memory stability — advisory only.
	baseline := e.baselineRSS.Load()
	peak := e.peakRSS.Load()
	if baseline > 0 && peak > baseline {
		growth := float64(peak) / float64(baseline)
		if growth > cfg.Thresholds.MaxMemoryGrowthFactor {
			if result.Passed {
				result.Result = "PASSED_WITH_WARNINGS"
			}
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("memory growth %.2fx exceeds threshold %.2fx", growth, cfg.Thresholds.MaxMemoryGrowthFactor))
		}
	}

	e.mu.Lock()
	e.verdictResult = result
	e.mu.Unlock()
}

func checkLatency(name, label string, value, threshold float64, fail func(string, ...any)) {
	if value > 0 && threshold > 0 && value > threshold {
		fail("%s: %s latency %.1fms exceeds %.1fms", name, label, value, threshold)
	}
}
