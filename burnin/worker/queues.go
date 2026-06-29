package worker

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	amqp "github.com/Azure/go-amqp"

	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/config"
	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/payload"
	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/transport"
)

// QueuesWorker (worker 1) drives the KubeMQ queues pattern over AMQP 1.0:
// a sender link to queues/<channel> (unsettled — each Send blocks on the server
// DISPOSITION) and N competing receiver links (each grants standing link credit
// and AcceptMessage-settles every delivery so the connector AckRanges it). It
// verifies at-least-once delivery: no loss, bounded duplicates, redelivery of
// unsettled messages on forced disconnect.
//
// Grounds connector tests TestQueueProduceConsumeAtLeastOnce,
// TestQueueReleasedRedelivery, TestDisconnectUnsettledRedelivered,
// TestGracefulShutdown.
type QueuesWorker struct {
	*BaseWorker
	address string
	seq     atomic.Uint64
}

// NewQueuesWorker creates a queues worker for the given channel index.
func NewQueuesWorker(cfg *config.Config, idx int, logger *slog.Logger) Worker {
	channelName := transport.Channel(config.WorkerQueues, idx)
	address := transport.Node("queues", config.WorkerQueues, idx)
	return &QueuesWorker{
		BaseWorker: NewBaseWorker(config.WorkerQueues, channelName, idx, cfg, logger),
		address:    address,
	}
}

// Start brings up the competing receivers (credit-based, manual accept).
func (w *QueuesWorker) Start(ctx context.Context) error {
	w.consumerCtx, w.consumerCancel = context.WithCancel(ctx)
	n := w.workerCfg.ConsumersPerChannel
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		w.consumerWG.Add(1)
		go func(consumerIdx int) {
			defer w.consumerWG.Done()
			w.consumeLoop(w.consumerCtx, consumerIdx)
		}(i)
	}
	return nil
}

func (w *QueuesWorker) consumeLoop(ctx context.Context, consumerIdx int) {
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

		w.drainReceiver(ctx, rcv)
		w.cleanupConn(conn)
		if ctx.Err() == nil {
			w.recordReconnection()
		}
	}
}

func (w *QueuesWorker) drainReceiver(ctx context.Context, rcv *amqp.Receiver) {
	for {
		if ctx.Err() != nil {
			return
		}
		msg, err := rcv.Receive(ctx, nil)
		if err != nil {
			return // link/connection error or ctx cancel — reconnect outer loop
		}
		w.handleDelivery(msg)
		// at-least-once: accept-settle so the connector AckRanges (removes) it.
		if err := rcv.AcceptMessage(ctx, msg); err != nil {
			w.recordError("accept_failure")
			return
		}
	}
}

func (w *QueuesWorker) handleDelivery(msg *amqp.Message) {
	body := msg.GetData()
	producerID, seq, crcHex, sentAt, ok := extractMeta(msg)
	if !ok {
		// Untracked (e.g. warmup) — still count receipt.
		w.recordReceived(len(body))
		return
	}
	if crcHex != "" && !payload.VerifyCRC(body, crcHex) {
		w.recordCorrupted()
	}
	if !sentAt.IsZero() {
		w.recordLatency(time.Since(sentAt))
	}
	w.recordTracked(producerID, seq)
	w.recordReceived(len(body))
}

// StartProducers launches the sender loop (measurement window).
func (w *QueuesWorker) StartProducers() {
	w.producerCtx, w.producerCancel = context.WithCancel(context.Background())
	w.producerWG.Add(1)
	go func() {
		defer w.producerWG.Done()
		w.produceLoop(w.producerCtx)
	}()
}

func (w *QueuesWorker) produceLoop(ctx context.Context) {
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
		// unsettled Send blocks until the server DISPOSITION (accepted), so a
		// counted Sent means the broker stored the message (verdict trustworthy).
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

func (w *QueuesWorker) connectSender(ctx context.Context) (*amqp.Conn, *amqp.Sender, error) {
	conn, err := transport.Dial(ctx, w.dialCfg)
	if err != nil {
		return nil, nil, err
	}
	sess, err := transport.NewSession(ctx, conn)
	if err != nil {
		closeConn(conn)
		return nil, nil, err
	}
	sender, err := transport.NewSender(ctx, sess, w.address, w.settleMode)
	if err != nil {
		closeConn(conn)
		return nil, nil, err
	}
	return conn, sender, nil
}

func (w *QueuesWorker) cleanupConn(conn *amqp.Conn) {
	w.unregisterConsumerConn(conn)
	closeConn(conn)
}
