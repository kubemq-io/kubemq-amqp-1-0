// Package disconnect injects forced connection churn: on a fixed interval it
// closes every target's consumer connections, waits, then lets them
// auto-reconnect. Used to exercise at-least-once redelivery and connection
// hygiene (spec §9.1 forced_disconnect / recovery).
package disconnect

import (
	"context"
	"log/slog"
	"time"

	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/metrics"
)

// Target is anything whose consumer connections can be force-closed (every
// worker satisfies this).
type Target interface {
	DisconnectConsumers()
}

// Manager drives the forced-disconnect cycle.
type Manager struct {
	interval time.Duration
	duration time.Duration
	targets  []Target
	logger   *slog.Logger
}

// New creates a forced-disconnect manager.
func New(interval, duration time.Duration, targets []Target, logger *slog.Logger) *Manager {
	return &Manager{interval: interval, duration: duration, targets: targets, logger: logger}
}

// Run blocks until ctx is cancelled, injecting disconnects on each tick.
func (m *Manager) Run(ctx context.Context) {
	if m.interval == 0 {
		return
	}
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics.IncForcedDisconnect()
			m.logger.Info("forced disconnect: closing consumer connections")
			for _, t := range m.targets {
				t.DisconnectConsumers()
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(m.duration):
			}
			m.logger.Info("forced disconnect: consumers will auto-reconnect")
		}
	}
}
