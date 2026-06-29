// Package transport wraps the github.com/Azure/go-amqp v1.7.0 client (AMQP 1.0)
// for the burn-in harness: Dial / NewSession / NewSender / NewReceiver plus
// credit / settle / drain helpers. It is the native, non-kubemq-go transport
// seam — there is NO KubeMQ SDK, no proto, no gRPC anywhere. The contract is
// grounded in the kubemq-amqp-1-0 examples (examples/go/queues/basic-send-receive):
// a sender link's Send blocks on the server DISPOSITION (unsettled), a receiver
// grants link credit and AcceptMessage settles each delivery.
//
// Concurrency rule (spec §9): one sender (or receiver) and its owning session per
// goroutine. go-amqp links are NOT safe for concurrent Send/Receive from multiple
// goroutines, so each producer/consumer goroutine dials its own connection.
package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	amqp "github.com/Azure/go-amqp"

	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/config"
)

// DialConfig captures everything needed to dial the AMQP 1.0 connector.
type DialConfig struct {
	Address     string
	ClientID    string
	TLS         config.TLSConfig
	Auth        config.AuthConfig
	IdleTimeout time.Duration
}

// dialTimeout bounds a single Dial attempt.
const dialTimeout = 15 * time.Second

// Dial connects to the AMQP 1.0 connector (plain or TLS depending on cfg.TLS).
// SASL ANONYMOUS by default; PLAIN (username + JWT-in-password) when Auth.Enabled.
func Dial(ctx context.Context, cfg DialConfig) (*amqp.Conn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	opts := &amqp.ConnOptions{}
	if cfg.ClientID != "" {
		opts.ContainerID = cfg.ClientID
	}
	if cfg.IdleTimeout > 0 {
		opts.IdleTimeout = cfg.IdleTimeout
	}
	if cfg.Auth.Enabled {
		opts.SASLType = amqp.SASLTypePlain(cfg.Auth.Username, cfg.Auth.Password)
	} else {
		opts.SASLType = amqp.SASLTypeAnonymous()
	}

	if cfg.TLS.Enabled {
		tlsConf, err := buildTLSConfig(cfg.TLS, cfg.Address)
		if err != nil {
			return nil, fmt.Errorf("build tls config: %w", err)
		}
		opts.TLSConfig = tlsConf
		return amqp.Dial(dialCtx, TLSURL(cfg.Address), opts)
	}
	return amqp.Dial(dialCtx, URL(cfg.Address), opts)
}

// NewSession opens a session on the connection.
func NewSession(ctx context.Context, conn *amqp.Conn) (*amqp.Session, error) {
	return conn.NewSession(ctx, nil)
}

// NewSender attaches a sender link to address (a fully-qualified <pattern>/<channel>
// node). The settle mode selects at-least-once (unsettled) vs at-most-once
// (settled). One sender per goroutine.
func NewSender(ctx context.Context, sess *amqp.Session, address, settleMode string) (*amqp.Sender, error) {
	opts := &amqp.SenderOptions{}
	if mode := senderSettleMode(settleMode); mode != nil {
		opts.SettlementMode = mode
	}
	return sess.NewSender(ctx, address, opts)
}

// NewReceiver attaches a receiver link to address with a standing link credit.
// A non-positive credit yields manual-credit mode (Credit:-1), where the caller
// must IssueCredit / DrainCredit explicitly (credit_flow worker).
func NewReceiver(ctx context.Context, sess *amqp.Session, address string, credit int32) (*amqp.Receiver, error) {
	opts := &amqp.ReceiverOptions{}
	if credit > 0 {
		opts.Credit = credit
	} else {
		opts.Credit = -1 // manual credit management
	}
	return sess.NewReceiver(ctx, address, opts)
}

// NewReceiverWithOptions attaches a receiver with caller-supplied options
// (used by events_store for durable/start-position/selector receivers in Phase 2).
func NewReceiverWithOptions(ctx context.Context, sess *amqp.Session, address string, opts *amqp.ReceiverOptions) (*amqp.Receiver, error) {
	return sess.NewReceiver(ctx, address, opts)
}

// IssueCredit grants additional link credit on a manual-credit receiver.
func IssueCredit(r *amqp.Receiver, credit uint32) error {
	return r.IssueCredit(credit)
}

// DrainCredit sends FLOW{drain=true} so the peer exhausts outstanding credit
// then stops (credit_flow worker). It returns once the drain completes.
func DrainCredit(ctx context.Context, r *amqp.Receiver) error {
	return r.DrainCredit(ctx, nil)
}

// Accept settles a delivery as accepted (the connector AckRanges / removes it).
func Accept(ctx context.Context, r *amqp.Receiver, msg *amqp.Message) error {
	return r.AcceptMessage(ctx, msg)
}

// Release settles a delivery as released (redelivery-eligible).
func Release(ctx context.Context, r *amqp.Receiver, msg *amqp.Message) error {
	return r.ReleaseMessage(ctx, msg)
}

func senderSettleMode(mode string) *amqp.SenderSettleMode {
	switch mode {
	case "settled":
		m := amqp.SenderSettleModeSettled
		return &m
	case "unsettled":
		m := amqp.SenderSettleModeUnsettled
		return &m
	default:
		return nil
	}
}

func buildTLSConfig(tc config.TLSConfig, address string) (*tls.Config, error) {
	conf := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: tc.SkipVerify, //nolint:gosec // opt-in for dev brokers
		ServerName:         tc.ServerName,
	}
	if conf.ServerName == "" {
		if host := hostOnly(address); host != "" {
			conf.ServerName = host
		}
	}
	if tc.CAFile != "" {
		caPEM, err := os.ReadFile(tc.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read ca file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("append ca certs from %s", tc.CAFile)
		}
		conf.RootCAs = pool
	}
	if tc.CertFile != "" && tc.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(tc.CertFile, tc.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		conf.Certificates = []tls.Certificate{cert}
	}
	return conf, nil
}

func hostOnly(address string) string {
	for i := 0; i < len(address); i++ {
		if address[i] == ':' {
			return address[:i]
		}
	}
	return address
}
