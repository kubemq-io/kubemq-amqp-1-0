# Rust — Queues / Ack-Release-Redelivery

The three queue settlement outcomes side by side over KubeMQ **Queues** with the
native `fe2o3-amqp` client: `accept` removes a message, `release` requeues it for
redelivery (grown delivery-count, `first_acquirer=false`), and `reject` discards
it with no requeue to this receiver.

## Prerequisites

- Rust 1.94+ (stable) with `cargo`.
- `fe2o3-amqp` 0.15.1 + `fe2o3-amqp-types` 0.14.0 (pinned exact in the workspace
  `examples/rust/Cargo.toml`).
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/rust
cargo run -p ack-release-redelivery
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 cargo run -p ack-release-redelivery
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: queues/amqp10.examples.ack

[send] Produced: release-me, reject-me, accept-me
[recv] release-me   delivery-count=0 first-acquirer=true  -> RELEASED (requeued)
[recv] reject-me    delivery-count=0 first-acquirer=true  -> REJECTED (discarded, no requeue)
[recv] accept-me    delivery-count=0 first-acquirer=true  -> ACCEPTED (removed)
[recv] release-me   delivery-count=1 first-acquirer=false -> REDELIVERED, then ACCEPTED
[recv] Rejected message was not redelivered (discarded)

Done.
```

(Delivery order between the original and the redelivered copy can vary; the
redelivered `release-me` always carries delivery-count ≥ 1 / first-acquirer=false.)

## What's Happening

1. **OPEN / BEGIN** — connect (ANONYMOUS) and begin one session.
2. **ATTACH (sender)** — produce three distinct messages with
   `SenderSettleMode::Unsettled` (each `send` awaits an `Accepted` outcome).
3. **ATTACH (receiver)** — `CreditMode::Auto(10)` grants standing credit.
4. **release** — on first sight of `release-me`, `receiver.release(&delivery)`
   sends a `Released` DISPOSITION ⇒ the connector NAckRanges it and requeues it to
   the tail. The redelivered copy arrives with `delivery_count >= 1` and
   `first_acquirer=false`; the program asserts this, then `accept`s it.
5. **reject** — `receiver.reject(&delivery, Error::new(AmqpError::InternalError, …))`
   sends a `Rejected` DISPOSITION ⇒ AckRange/discard. The message is removed and is
   NOT redelivered to this receiver (poison handling is the broker's
   `MaxReceiveQueue` policy — there is no connector DLX).
6. **accept** — `receiver.accept(&delivery)` removes the message (success).
7. **Redelivery check** — the rejected body never returns; a final `recv` with a
   short `tokio::time::timeout` confirms nothing more arrives.
8. **DETACH / CLOSE** — `receiver.close()`, `session.end()`, `connection.close()`.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `queues/<ch>` | `Unsettled` (explicit) | server-granted | `Accepted` outcome per send | none | `Data` | at-least-once produce |
| receiver (KubeMQ → client) | source `queues/<ch>` | `First` (default) | client `CreditMode::Auto(10)` | `accept`⇒AckRange · `release`⇒NAckRange/requeue · `reject`⇒AckRange/discard | none | `Data` | redelivery grows delivery-count |

## Gotchas

> **`release` / `modify` increment the receive-count.** Each NAck (release or
> modify) requeues the message AND bumps the broker receive-count. A message that
> keeps getting released climbs toward the broker's `MaxReceiveQueue` poison cap,
> after which the broker drops it — there is **no connector DLX**. The redelivered
> copy is observable on the AMQP header as `delivery_count >= 1` /
> `first_acquirer = false`.

- **`reject` does not requeue.** Unlike `release`, `reject` discards the message
  from this receiver — it is not redelivered here.
- **Sender settle-mode must be explicit** (`fe2o3-amqp` defaults to `mixed`, which
  the connector rejects). This example uses `SenderSettleMode::Unsettled`.

## Related Examples

- [queues/basic-send-receive](../basic-send-receive/) — accept-only happy path
- [queues/settlement-modes](../settlement-modes/) — unsettled vs pre-settled producer

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
