# Rust — Queues / Settlement Modes

The two producer reliability tiers side by side over KubeMQ **Queues** with the
native `fe2o3-amqp` client: a **pre-settled** sender (`SenderSettleMode::Settled`,
at-most-once, fire-and-forget) vs an **unsettled** sender
(`SenderSettleMode::Unsettled`, at-least-once, awaits an `Accepted` outcome). Both
streams drain through one `ReceiverSettleMode::First` consumer.

## Prerequisites

- Rust 1.94+ (stable) with `cargo`.
- `fe2o3-amqp` 0.15.1 + `fe2o3-amqp-types` 0.14.0 (pinned exact in the workspace
  `examples/rust/Cargo.toml`).
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/rust
cargo run -p settlement-modes
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 cargo run -p settlement-modes
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: queues/amqp10.examples.settlement

[send] Pre-settled (at-most-once): produced 10 messages — NO outcome awaited
[send] Unsettled (at-least-once): produced 10 messages — each Accepted outcome
[recv] Drained 20 total — 10 pre-settled + 10 unsettled (rcv-settle-mode=first)
[recv] Queue drained to empty (no further messages)

Done.
```

(On a healthy broker pre-settled messages also drain — the difference is the
PRODUCER guarantee, not the happy-path result.)

## What's Happening

1. **OPEN / BEGIN** — connect (ANONYMOUS) and begin one session.
2. **Pre-settled produce** — `SenderSettleMode::Settled`: every TRANSFER is marked
   settled, so `send` returns immediately with no server outcome. At-most-once.
3. **Unsettled produce** — `SenderSettleMode::Unsettled`: each `send` blocks until
   the connector returns an `Accepted` outcome (broker stored it). At-least-once.
4. **Consume** — `ReceiverSettleMode::First` is the only receiver settle-mode the
   connector supports; the server settles on the first transfer. `CreditMode::Auto(20)`
   grants standing credit; `accept` each delivery to drain.
5. **Drain check** — a final `recv` with a short `tokio::time::timeout` confirms
   the queue is empty.
6. **DETACH / CLOSE** — `receiver.close()`, `session.end()`, `connection.close()`.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender — pre-settled | target `queues/<ch>` | `Settled` | server-granted | none (settled by client) | none | `Data` | at-most-once, fire-and-forget |
| sender — unsettled | target `queues/<ch>` | `Unsettled` | server-granted | `Accepted` outcome per send | none | `Data` | at-least-once |
| receiver | source `queues/<ch>` | `First` (explicit) | client `CreditMode::Auto(20)` | `accept`⇒AckRange | none | `Data` | only `first` supported |

## Gotchas

> **`rcv-settle-mode=second` is unsupported.** Requesting
> `ReceiverSettleMode::Second` on a receiver makes the connector DETACH the link
> with `amqp:not-implemented`. Only `ReceiverSettleMode::First` (settle on first
> transfer) works — it is the default and what this example sets explicitly.

- **Pre-settled produce hides drops.** A `Settled` send returns before any broker
  confirmation, so a transfer dropped on the way in (oversize / no capacity) is
  invisible to the producer. Use `Unsettled` when you need delivery confirmation.
- **Sender settle-mode must be explicit** — `fe2o3-amqp` defaults to `mixed`, which
  the connector rejects at ATTACH.

## Related Examples

- [queues/basic-send-receive](../basic-send-receive/) — the unsettled at-least-once contract
- [queues/ack-release-redelivery](../ack-release-redelivery/) — accept / release / reject outcomes
- [events/basic-pubsub](../../events/basic-pubsub/) — pre-settled fan-out (Events)

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
