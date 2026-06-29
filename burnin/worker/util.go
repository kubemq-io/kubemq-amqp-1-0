package worker

import (
	"context"
	"time"

	amqp "github.com/Azure/go-amqp"

	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/metrics"
)

// closeConn closes an AMQP connection, ignoring nil/errors (best-effort cleanup).
func closeConn(conn *amqp.Conn) {
	if conn != nil {
		_ = conn.Close()
	}
}

// closeSender detaches a sender link with a short bounded context.
func closeSender(sender *amqp.Sender) {
	if sender == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = sender.Close(ctx)
}

// closeReceiver detaches a receiver link with a short bounded context.
func closeReceiver(rcv *amqp.Receiver) {
	if rcv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = rcv.Close(ctx)
}

// sleepCtx sleeps for d unless ctx is cancelled first. Returns false if the
// context was cancelled (caller should stop).
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// metricObserveSend records sender Send round-trip duration.
func metricObserveSend(workerName string, d time.Duration) {
	metrics.ObserveSendDuration(workerName, d)
}
