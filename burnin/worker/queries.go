package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/config"
	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/transport"
)

// queryDropEveryN selects the deterministic fraction of query requests the
// responder leaves unanswered, exercising the query-timeout failure leg: a failed
// /unanswered query delivers NOTHING, so the requester simply times out (the
// Queries-vs-Commands contrast — a command always replies executed=false).
//
// DISABLED (0) so the responder answers EVERY query (like commands) — the
// intentional 1-in-200 timeouts were read as server failures during broker
// profiling. Set back to e.g. 200 to re-enable the timeout-leg fault injection.
const queryDropEveryN = 0

// QueriesWorker (worker 5) drives the KubeMQ queries pattern over AMQP 1.0 as a
// native RPC round-trip — NO kubemq-go, NO gRPC. The reply path is identical to
// commands (dynamic reply node + correlation-id, msg-id fallback), with two
// deliberate contrasts:
//
//   - A QUERY reply carries ONLY body + metadata — NO x-opt-kubemq-executed /
//     x-opt-kubemq-error props (a query fetches a value, it has no exec envelope).
//   - A FAILED / unanswered query delivers NOTHING, so the requester TIMES OUT
//     (the absence of a reply IS the failure signal). The responder leaves a small
//     deterministic fraction of requests unanswered to exercise this leg.
//
// The requester respects RpcMaxPending implicitly: each requester goroutine awaits
// its reply synchronously before issuing the next request, so the in-flight count
// per requester is exactly 1 (bounded by senders_per_channel).
//
// Grounds connector test TestRPCRequesterResponderViaAMQP10 (queries leg).
type QueriesWorker struct {
	*BaseWorker
	address string
}

// NewQueriesWorker creates a queries RPC worker for the given channel index.
func NewQueriesWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	channelName := transport.Channel(config.WorkerQueries, idx)
	address := transport.Node("queries", config.WorkerQueries, idx)
	return &QueriesWorker{
		BaseWorker: NewBaseWorker(config.WorkerQueries, channelName, idx, cfg, logger),
		address:    address,
	}
}

// Start brings up the responder(s) on queries/<channel>.
func (w *QueriesWorker) Start(ctx context.Context) error {
	w.consumerCtx, w.consumerCancel = context.WithCancel(ctx)
	n := w.workerCfg.RespondersPerChannel
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		resp := &rpcResponder{
			w:           w.BaseWorker,
			address:     w.address,
			withOutcome: false, // query replies are body + metadata only
			dropEveryN:  queryDropEveryN,
		}
		w.consumerWG.Add(1)
		go func(r *rpcResponder) {
			defer w.consumerWG.Done()
			r.run(w.consumerCtx)
		}(resp)
	}
	return nil
}

// StartProducers launches the requester(s) (the measurement window RPC traffic).
func (w *QueriesWorker) StartProducers() {
	w.producerCtx, w.producerCancel = context.WithCancel(context.Background())
	n := w.workerCfg.SendersPerChannel
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		req := &rpcRequester{
			w:             w.BaseWorker,
			address:       w.address,
			timeout:       w.rpcTimeout(),
			verifyOutcome: false, // queries: any correlated reply is success
		}
		w.producerWG.Add(1)
		go func(r *rpcRequester) {
			defer w.producerWG.Done()
			r.run(w.producerCtx)
		}(req)
	}
}

// rpcTimeout resolves the per-request reply budget (amqp10.rpc_timeout_ms, then
// rpc.timeout_ms, default 5s).
func (w *QueriesWorker) rpcTimeout() time.Duration {
	return rpcTimeoutFor(w.cfg)
}
