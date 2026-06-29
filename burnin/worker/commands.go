package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/config"
	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/transport"
)

// CommandsWorker (worker 4) drives the KubeMQ commands pattern over AMQP 1.0 as a
// native RPC round-trip — NO kubemq-go, NO gRPC. It stands up:
//
//   - RESPONDER goroutine(s): a receiver on commands/<channel> (server-sender link
//     pumped under credit) + an ANONYMOUS sender that replies to the request's
//     reply-to, stamping x-opt-kubemq-executed / x-opt-kubemq-error on every reply
//     (the command execution outcome).
//   - REQUESTER goroutine(s): a DYNAMIC reply node + a sender on commands/<channel>;
//     each request names the dynamic node + a unique correlation-id, awaits the
//     correlated reply, and verifies x-opt-kubemq-executed == true (a failure ⇒
//     executed=false ⇒ accounted as an RPC error). The correlation falls back to
//     message-id when correlation-id is absent.
//
// Commands always reply (success OR failure) so the requester is never left
// waiting — the contrast with queries (worker 5), where a failed query delivers
// nothing and the requester times out.
//
// Grounds connector tests TestRPCRequesterResponderViaAMQP10 (commands leg) and
// TestRpcCorrelationIDFallbackToMessageID.
type CommandsWorker struct {
	*BaseWorker
	address string
}

// NewCommandsWorker creates a commands RPC worker for the given channel index.
func NewCommandsWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	channelName := transport.Channel(config.WorkerCommands, idx)
	address := transport.Node("commands", config.WorkerCommands, idx)
	return &CommandsWorker{
		BaseWorker: NewBaseWorker(config.WorkerCommands, channelName, idx, cfg, logger),
		address:    address,
	}
}

// Start brings up the responder(s) on commands/<channel>.
func (w *CommandsWorker) Start(ctx context.Context) error {
	w.consumerCtx, w.consumerCancel = context.WithCancel(ctx)
	n := w.workerCfg.RespondersPerChannel
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		resp := &rpcResponder{
			w:           w.BaseWorker,
			address:     w.address,
			withOutcome: true, // command replies carry executed/error props
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
func (w *CommandsWorker) StartProducers() {
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
			verifyOutcome: true, // require x-opt-kubemq-executed == true
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
func (w *CommandsWorker) rpcTimeout() time.Duration {
	return rpcTimeoutFor(w.cfg)
}
