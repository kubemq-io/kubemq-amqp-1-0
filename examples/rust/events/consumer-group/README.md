# Rust — Events / Consumer Group

Consumer-group load-balancing over KubeMQ **Events** with the native `fe2o3-amqp`
client. Two receivers in group `g1` split the event stream (no duplication); one
receiver in group `g2` independently gets the full stream. The group is selected
by the `x-opt-kubemq-group` receiver **link property**.

## Prerequisites

- Rust 1.94+ (stable) with `cargo`.
- `fe2o3-amqp` 0.15.1 + `fe2o3-amqp-types` 0.14.0 + `serde_amqp` 0.14.1 (pinned
  exact in the workspace `examples/rust/Cargo.toml`).
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/rust
cargo run -p consumer-group
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 cargo run -p consumer-group
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: events/amqp10.examples.consumergroup  (KubeMQ pattern=events, channel=amqp10.examples.consumergroup)

[recv] 3 subscribers attached: g1a+g1b (group g1), g2 (group g2)
[send] Published 30 events (pre-settled)
[recv] g2 (group g2, independent): 30/30 events — FULL stream
[recv] g1a (group g1): 16 events; g1b (group g1): 14 events
[recv] g1a+g1b together: 30/30 events, 0 duplicates — group SPLIT the stream

Done.
```

(The g1a/g1b split varies run to run; the totals — g2 full, g1 split with 0
duplicates — are fixed.)

## What's Happening

1. **OPEN** — connect (ANONYMOUS). One connection hosts three subscriber sessions
   plus a producer session.
2. **ATTACH (three receivers)** — each receiver is attached on its **own session**
   with `CreditMode::Auto(100)` and a `.properties(...)` link property carrying
   `x-opt-kubemq-group = g1` (×2) or `g2` (×1). The connector groups subscribers by
   that property.
3. **One task per receiver** — each `(Session, Receiver)` pair is moved into its own
   `tokio::spawn` task; links are driven from a single task, never shared.
4. **Wait, then publish** — after the pumps go live (~750 ms), a dedicated
   pre-settled sender publishes 30 events.
5. **Assert group semantics** — `g2` (a distinct group) receives **every** event;
   `g1a` + `g1b` **together** receive every event with **0** delivered to both (the
   group round-robins / splits the stream).
6. **CLOSE** — each task closes its receiver and ends its session; the producer
   session ends; then the connection closes.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events/<ch>` | `Settled` (pre-settled) | server-granted | none | none | `Data` | at-most-once fan-out |
| receiver (KubeMQ → client) | source `events/<ch>` | `First` (default) | client `CreditMode::Auto(100)` | n/a (pre-settled) | **link property** `x-opt-kubemq-group` | `Data` | same group splits; distinct group = full stream |

## Gotchas

> **Events drop silently at 0 credit.** Each group member must keep standing credit
> or it will silently drop events that arrive while it is at 0 credit (counted on
> `kubemq_amqp10_events_dropped_no_credit_total`). `CreditMode::Auto(N)` keeps the
> link fed.

- **`x-opt-kubemq-group` is a link (attach) property**, set via
  `Receiver::builder().properties(Fields)`, NOT an application property on the
  message.
- **One link per task.** Each receiver runs in its own task with its own session;
  `fe2o3-amqp` links are not shared across tasks.

## Related Examples

- [events/basic-pubsub](../basic-pubsub/) — single-subscriber fan-out
- [events/selector](../selector/) — content-based filtering with selectors
- [queues/basic-send-receive](../../queues/basic-send-receive/) — competing consumers on Queues

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
