# Rust — KubeMQ AMQP 1.0 Examples

Runnable Rust programs that drive the embedded KubeMQ **AMQP 1.0** connector with a
**native** AMQP 1.0 client — no KubeMQ SDK, no proto, no gRPC.

All examples use:

- [`fe2o3-amqp`](https://crates.io/crates/fe2o3-amqp) **0.15.1** (`rustls`) +
  [`fe2o3-amqp-types`](https://crates.io/crates/fe2o3-amqp-types) **0.14.0** +
  [`serde_amqp`](https://crates.io/crates/serde_amqp) **0.14.1** — a pure-Rust
  async AMQP 1.0 implementation, driven on a [`tokio`](https://crates.io/crates/tokio) runtime.

> **Pre-1.0 pin caveat.** `fe2o3-amqp` is pre-1.0 and single-maintainer; its API
> churns across minor versions — most notably the `FilterSet` encoding (selectors)
> and the settlement-mode surface. Every coordinate above is pinned **exact**
> (`=0.15.1`, `=0.14.0`, `=0.14.1`) in `Cargo.toml` and was dial-tested against a
> live KubeMQ AMQP 1.0 connector before being committed. Bump only via
> `/check-deps` and re-dial-test selectors + settlement before re-pinning.

## Prerequisites

- Rust 1.94+ (stable) with `cargo`.
- A running KubeMQ server with the AMQP 1.0 connector enabled (it is enabled by
  default and shares the AMQP port `5672`).

## Setup

```bash
cd examples/rust
cargo build --workspace
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KUBEMQ_AMQP_URL` | `amqp://localhost:5672` | Broker address. No userinfo ⇒ SASL **ANONYMOUS** (clone-and-run on a stock auth-off dev broker). `connectivity/auth` overrides it to a SASL PLAIN `amqp://<user>:<JWT>@host:5672` form. |

The default URL has **no userinfo**, so every example connects with SASL
**ANONYMOUS** and needs zero credential setup. The documented production contract
is SASL **PLAIN** with a KubeMQ JWT in the password field — see
[`connectivity/auth`](connectivity/auth/).

## Run Any Example

```bash
export KUBEMQ_AMQP_URL=amqp://localhost:5672

cargo run -p <crate>
# e.g.
cargo run -p basic-send-receive
cargo run -p basic-pubsub
```

Each variant is a workspace member crate; the crate name is the **variant** leaf
(e.g. the `queues/basic-send-receive` directory is crate `basic-send-receive`).

## Examples

| # | Group / Variant | Crate (`-p`) | Pattern | Notes |
|---|-----------------|--------------|---------|-------|
| 1 | [queues/basic-send-receive](queues/basic-send-receive/) | `basic-send-receive` | Queues | At-least-once produce + credit consume + `accept`; queue drains, no loss |
| 2 | [queues/ack-release-redelivery](queues/ack-release-redelivery/) | `ack-release-redelivery` | Queues | `release` requeues (grown delivery-count); `reject` discards |
| 3 | [queues/settlement-modes](queues/settlement-modes/) | `settlement-modes` | Queues | Unsettled (at-least-once) vs pre-settled (at-most-once) |
| 4 | [events/basic-pubsub](events/basic-pubsub/) | `basic-pubsub` | Events | Fan-out at-most-once; subscribe-before-publish; continuous credit |
| 5 | [events/consumer-group](events/consumer-group/) | `consumer-group` | Events | `x-opt-kubemq-group` load balancing vs independent groups |
| 6 | [events/selector](events/selector/) | `selector` | Events | JMS/SQL-92 selector filter; 3-valued logic; selector-on-queues rejection |
| 7 | events-store/durable-replay | `durable-replay` | Events Store | Durable subscription + resume (expiry=never, stable identity) |
| 8 | events-store/start-positions | `start-positions` | Events Store | `x-opt-kubemq-start` grammar (first/new-only/last/sequence/time/time-delta) |
| 9 | commands/request-reply-dynamic-node | `request-reply-dynamic-node` | Commands (RPC) | Native RPC, dynamic reply node, executed/error props |
| 10 | queries/request-reply | `request-reply` | Queries (RPC) | Native RPC, reply body+metadata only |
| 11 | advanced/multi-frame-large-payload | `multi-frame-large-payload` | Queues | Reduced max-frame-size; ~1 MB body fragments + reassembles; CRC verify |
| 12 | advanced/anonymous-terminus | `anonymous-terminus` | (routes by `to`) | Anonymous sender + per-message `properties.to` |
| 13 | connectivity/auth | `auth` | (connection) | SASL PLAIN with a KubeMQ JWT in the password |

> Variants #1–#6 are in this phase; #7–#13 land alongside.

## Idiom Note — one `Sender` / `Receiver` per task

`fe2o3-amqp` is async/await on a `tokio` runtime. A `Connection` hosts many
`Session`s, and each `Session` hosts many links. A `Sender` / `Receiver` owns a
mutable link and is driven from a single task — use **one `Sender` / `Receiver`
per task** rather than sharing a link across tasks; open a fresh `Session` (and
its own links) per task that needs concurrent traffic. Always tear down cleanly:
`sender.close().await` / `receiver.close().await`, then `session.end().await`,
then `connection.close().await`.

## Settlement-mode requirement (Rust-specific)

The KubeMQ connector rejects the AMQP default sender settle-mode `mixed` at ATTACH
with `amqp:not-implemented` (`SndSettleModeNotSupported`). `fe2o3-amqp` defaults a
`Sender` to `mixed`, so **every sender in these examples sets an explicit
`sender_settle_mode`** via the `Sender::builder()`:

- `SenderSettleMode::Unsettled` — at-least-once (each `send` awaits the server
  `Accepted` outcome). The Queues / Events-Store / RPC default here.
- `SenderSettleMode::Settled` — at-most-once / fire-and-forget (pre-settled;
  `send` returns without a server outcome). Used for Events publish.

Receivers default to `ReceiverSettleMode::First`, the only receiver settle-mode
the connector supports (`second` ⇒ DETACH `amqp:not-implemented`).

## Address Grammar

Every link addresses a KubeMQ pattern + channel with an **explicit prefix**
`<pattern>/<channel>` — examples NEVER rely on bare-address / `DefaultPattern`
resolution. The address is the link **target** on a `Sender` (client writes) and
the link **source** on a `Receiver` (client reads).

| Address | KubeMQ pattern |
|---------|----------------|
| `queues/<ch>` | Queues (move / competing-consumer) |
| `events/<ch>` | Events (fan-out, at-most-once) |
| `events-store/<ch>` | Events Store (durable, replay) |
| `commands/<ch>` | Commands (RPC) |
| `queries/<ch>` | Queries (RPC) |

Channel names must be non-empty, ≤ 255 chars, with no trailing `.`, no
whitespace, and none of `* > ; :` (else `amqp:not-found`).
