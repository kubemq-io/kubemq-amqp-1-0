package transport

import (
	"fmt"
	"os"
)

// BrokerEnv is the burn-in broker address env var (NOT KUBEMQ_AMQP_URL; spec §9.3).
const BrokerEnv = "KUBEMQ_BROKER_ADDRESS"

// DefaultBroker is the default host:port for the plain AMQP 1.0 listener.
const DefaultBroker = "localhost:5672"

// URL builds the plain AMQP 1.0 connection URL from the supplied broker address,
// falling back to KUBEMQ_BROKER_ADDRESS then DefaultBroker (spec §9.3:
// amqp://{address}). No userinfo — SASL ANONYMOUS by default (the connector's
// default; PLAIN+JWT is supplied via ConnOptions.SASLType, not the URL).
func URL(address string) string {
	return fmt.Sprintf("amqp://%s", resolveAddr(address))
}

// TLSURL builds the AMQPS connection URL (amqps://{address}).
func TLSURL(address string) string {
	return fmt.Sprintf("amqps://%s", resolveAddr(address))
}

func resolveAddr(address string) string {
	addr := address
	if addr == "" {
		addr = os.Getenv(BrokerEnv)
	}
	if addr == "" {
		addr = DefaultBroker
	}
	return addr
}

// Channel builds the KubeMQ channel segment for a burn-in worker channel index
// following the spec §9.3 grammar amqp10.burnin.{worker}.{idx:04d}.
func Channel(worker string, idx int) string {
	return fmt.Sprintf("amqp10.burnin.%s.%04d", worker, idx)
}

// Node builds the explicit fully-qualified link address <pattern>/<channel> for
// a worker channel (e.g. "queues/amqp10.burnin.queues.0001"). The pattern prefix
// is ALWAYS explicit — never rely on the connector DefaultPattern (gotcha).
func Node(pattern, worker string, idx int) string {
	return fmt.Sprintf("%s/%s", pattern, Channel(worker, idx))
}

// PatternFor returns the KubeMQ pattern (address prefix) for a worker.
func PatternFor(worker string) string {
	switch worker {
	case "queues", "credit_flow":
		return "queues"
	case "events":
		return "events"
	case "events_store":
		return "events_store"
	case "commands":
		return "commands"
	case "queries":
		return "queries"
	default:
		return "queues"
	}
}
