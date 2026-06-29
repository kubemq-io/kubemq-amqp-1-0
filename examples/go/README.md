# Go — KubeMQ AMQP 1.0 Examples

Runnable Go programs that drive the embedded KubeMQ **AMQP 1.0** connector with a
**native** AMQP 1.0 client — no KubeMQ SDK, no proto, no gRPC.

All examples use:

- [`github.com/Azure/go-amqp`](https://github.com/Azure/go-amqp) **v1.7.0** — the
  connector's own reference client (highest-confidence parity).

## Prerequisites

- Go 1.24+
- A running KubeMQ server with the AMQP 1.0 connector enabled (it is enabled by
  default and shares the AMQP port `5672`).

## Setup

```bash
cd examples/go
go mod download
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KUBEMQ_AMQP_URL` | `amqp://localhost:5672` | Broker address. No userinfo ⇒ SASL **ANONYMOUS** (clone-and-run on a stock auth-off dev broker). `connectivity/auth` overrides it to a SASL PLAIN `amqp://<user>:<JWT>@host:5672` form. |

The default URL has **no userinfo**, so every example connects with SASL
**ANONYMOUS** and needs zero credential setup. The documented production contract
is SASL **PLAIN** with a KubeMQ JWT in the password field — see
[`connectivity/auth`](connectivity/auth/) (Phase 2).

## Run Any Example

```bash
export KUBEMQ_AMQP_URL=amqp://localhost:5672

go run ./<group>/<variant>
# e.g.
go run ./queues/basic-send-receive
go run ./events/basic-pubsub
```

## Examples

| # | Group / Variant | Pattern | Notes |
|---|-----------------|---------|-------|
| 1 | [queues/basic-send-receive](queues/basic-send-receive/) | Queues | At-least-once produce + credit consume + `accept`; queue drains, no loss |
| 2 | [queues/ack-release-redelivery](queues/ack-release-redelivery/) | Queues | `release` requeues (grown delivery-count); `reject` discards |
| 3 | [queues/settlement-modes](queues/settlement-modes/) | Queues | Unsettled (at-least-once) vs pre-settled (at-most-once) |
| 4 | [events/basic-pubsub](events/basic-pubsub/) | Events | Fan-out at-most-once; subscribe-before-publish; continuous credit |
| 5 | [events/consumer-group](events/consumer-group/) | Events | `x-opt-kubemq-group` load balancing vs independent groups |

> Variants #6–#13 (selector, events-store, RPC, advanced, connectivity) land in Phase 2.

## Idiom Note — sessions are NOT concurrency-safe

go-amqp's `*Session`, `*Sender`, and `*Receiver` are **not** safe for concurrent
use. Use **one `*Sender` / `*Receiver` per goroutine** — never share a single
session or link across goroutines. A `*Conn` may host many sessions; open a fresh
session (and its own links) per goroutine instead of sharing one.

## Address Grammar

Every link addresses a KubeMQ pattern + channel with an **explicit prefix**
`<pattern>/<channel>` — examples NEVER rely on bare-address / `DefaultPattern`
resolution. The address is the link **target** on a sender (client writes) and
the link **source** on a receiver (client reads).

| Address | KubeMQ pattern |
|---------|----------------|
| `queues/<ch>` | Queues (move / competing-consumer) |
| `events/<ch>` | Events (fan-out, at-most-once) |
| `events-store/<ch>` | Events Store (durable, replay) |
| `commands/<ch>` | Commands (RPC) |
| `queries/<ch>` | Queries (RPC) |

Channel names must be non-empty, ≤ 255 chars, with no trailing `.`, no
whitespace, and none of `* > ; :` (else `amqp:not-found`).
