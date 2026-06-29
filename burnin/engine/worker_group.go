// Package engine drives the burn-in run lifecycle (warmup → measure → drain →
// verdict), grouping the per-channel workers of each of the six AMQP 1.0 worker
// types.
package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/config"
	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/worker"
)

// WorkerGroup holds all channel instances of one worker type.
type WorkerGroup struct {
	name    string
	workers []worker.Worker
}

// NewWorkerGroup builds the channel instances for a worker type. All six worker
// types (queues, events, events_store, commands, queries, credit_flow) have real
// implementations.
func NewWorkerGroup(name string, cfg *config.Config, logger *slog.Logger) *WorkerGroup {
	numChannels := cfg.GetWorkerChannels(name)
	workers := make([]worker.Worker, 0, numChannels)
	for i := 1; i <= numChannels; i++ {
		switch name {
		case config.WorkerQueues:
			workers = append(workers, worker.NewQueuesWorker(cfg, i, logger))
		case config.WorkerEvents:
			workers = append(workers, worker.NewEventsWorker(cfg, i, logger))
		case config.WorkerEventsStore:
			workers = append(workers, worker.NewEventsStoreWorker(cfg, i, logger))
		case config.WorkerCommands:
			workers = append(workers, worker.NewCommandsWorker(cfg, i, logger))
		case config.WorkerQueries:
			workers = append(workers, worker.NewQueriesWorker(cfg, i, logger))
		case config.WorkerCreditFlow:
			workers = append(workers, worker.NewCreditFlowWorker(cfg, i, logger))
		}
	}
	return &WorkerGroup{name: name, workers: workers}
}

// StartConsumers starts the consumer/responder side of every worker.
func (g *WorkerGroup) StartConsumers(ctx context.Context) error {
	for _, w := range g.workers {
		if err := w.Start(ctx); err != nil {
			return fmt.Errorf("start consumer for %s/%s: %w", g.name, w.ChannelName(), err)
		}
	}
	return nil
}

// WaitForConsumerReady blocks until every worker signals ready or times out.
func (g *WorkerGroup) WaitForConsumerReady(timeout time.Duration) error {
	for _, w := range g.workers {
		select {
		case <-w.ConsumerReady():
		case <-time.After(timeout):
			return fmt.Errorf("consumer ready timeout for %s/%s", g.name, w.ChannelName())
		}
	}
	return nil
}

// StartProducers starts the producer/requester side of every worker.
func (g *WorkerGroup) StartProducers() {
	for _, w := range g.workers {
		w.StartProducers()
	}
}

// StopProducers stops the producer side of every worker.
func (g *WorkerGroup) StopProducers() {
	for _, w := range g.workers {
		w.StopProducers()
	}
}

// StopConsumers stops the consumer side of every worker.
func (g *WorkerGroup) StopConsumers() {
	for _, w := range g.workers {
		w.StopConsumers()
	}
}

// DisconnectConsumers force-closes consumer connections for every worker.
func (g *WorkerGroup) DisconnectConsumers() {
	for _, w := range g.workers {
		w.DisconnectConsumers()
	}
}

// Workers returns the worker slice.
func (g *WorkerGroup) Workers() []worker.Worker { return g.workers }

// Name returns the worker type name.
func (g *WorkerGroup) Name() string { return g.name }
