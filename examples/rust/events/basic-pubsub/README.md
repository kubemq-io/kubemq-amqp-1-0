# Rust — Events / Basic Pub-Sub

Fan-out, at-most-once pub/sub over KubeMQ **Events** with the native `fe2o3-amqp`
client. A subscriber attaches with standing credit and waits for the subscription
pump to go live; a pre-settled publisher then fires 20 events, all of which the
subscriber drains.

## Prerequisites

- Rust 1.94+ (stable) with `cargo`.
- `fe2o3-amqp` 0.15.1 + `fe2o3-amqp-types` 0.14.0 (pinned exact in the workspace
  `examples/rust/Cargo.toml`).
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/rust
cargo run -p basic-pubsub
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 cargo run -p basic-pubsub
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: events/amqp10.examples.pubsub  (KubeMQ pattern=events, channel=amqp10.examples.pubsub)

[recv] Subscribed to events/amqp10.examples.pubsub with standing credit 100
[recv] Subscription pump settled (waited 750ms before publishing)
[send] Published 20 events (pre-settled, fire-and-forget)
[recv] Received all 20 events (continuous credit => no 0-credit drop)

Done.
```

## What's Happening

1. **OPEN / BEGIN** — connect (ANONYMOUS) and begin one session.
2. **ATTACH (receiver) FIRST** — `Receiver::builder().source("events/<ch>")` with
   `CreditMode::Auto(100)` subscribes with standing credit BEFORE any publish.
   Events have no replay; a publish that beats the subscription is lost.
3. **Wait for the pump** — the attach reply confirms the link, not that the
   connector's subscription pump is live. The program sleeps ~750 ms before
   publishing so the first events don't race the subscription.
4. **ATTACH (sender) pre-settled** — `SenderSettleMode::Settled` marks every
   TRANSFER as settled (fire-and-forget). Events are at-most-once; `send` returns
   without a server outcome.
5. **RECEIVE** — with standing credit the subscriber drains every event. `accept`
   is a no-op on pre-settled fan-out deliveries but is harmless.
6. **DETACH / CLOSE** — `receiver.close()`, `session.end()`, `connection.close()`.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events/<ch>` | `Settled` (pre-settled) | server-granted | none (fire-and-forget) | none | `Data` | at-most-once fan-out |
| receiver (KubeMQ → client) | source `events/<ch>` | `First` (default) | client `CreditMode::Auto(100)` | n/a (pre-settled) | none | `Data` | subscribe-before-publish; no replay |

## Gotchas

> **Events drop silently at 0 credit.** Events are at-most-once with no replay. If
> a subscriber is at **0 credit** when an event arrives, the connector SILENTLY
> DROPS it — there is no client error, only the server metric
> `kubemq_amqp10_events_dropped_no_credit_total`. Grant standing credit
> (`CreditMode::Auto(N)` auto-replenishes) so the link is never starved.

> **Subscribe before publish.** A publish that races the subscription pump is lost
> forever (no replay). Attach the receiver first and let the pump settle (the
> ~750 ms sleep) before producing.

## Related Examples

- [events/consumer-group](../consumer-group/) — `x-opt-kubemq-group` load balancing
- [events/selector](../selector/) — JMS/SQL-92 message selectors
- [events-store/durable-replay](../../events-store/durable-replay/) — durable, replayable subscriptions

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
