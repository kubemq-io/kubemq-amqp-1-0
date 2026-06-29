# Rust — Queues / Basic Send-Receive

At-least-once produce + credit-based consume over KubeMQ **Queues** with the
native `fe2o3-amqp` client. A sender publishes 10 messages to `queues/<ch>`; a
receiver grants link credit, consumes each, and `accept`s it, so the queue drains
to empty with no loss.

## Prerequisites

- Rust 1.94+ (stable) with `cargo`.
- `fe2o3-amqp` 0.15.1 + `fe2o3-amqp-types` 0.14.0 (pinned exact in the workspace
  `examples/rust/Cargo.toml`).
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/rust
cargo run -p basic-send-receive
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 cargo run -p basic-send-receive
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: queues/amqp10.examples.basic  (KubeMQ pattern=queues, channel=amqp10.examples.basic)

[send] Produced 10 messages to queues/amqp10.examples.basic (Accepted outcome each)
[recv] Consumed and accepted 10 messages (no loss)
[recv] Queue drained to empty (no further messages)

Done.
```

## What's Happening

1. **OPEN** — `Connection::open("amqp10-examples-basic", url)` connects over AMQP
   1.0. The default URL has no userinfo, so the SASL layer negotiates
   **ANONYMOUS**. The first argument is a **non-empty container-id** (a connector
   requirement — an empty container-id is closed with `amqp:invalid-field`).
2. **BEGIN** — `Session::begin` opens one session for both links.
3. **ATTACH (sender)** — `Sender::builder().target("queues/<ch>")` attaches a link
   the server sees as a *receiver* (the client produces). We pin
   `SenderSettleMode::Unsettled` because the connector rejects the AMQP default
   `mixed` at ATTACH.
4. **TRANSFER / DISPOSITION (produce)** — each `send` is **unsettled**: it returns
   only after the connector replies with an `Accepted` outcome, confirming the
   broker stored the message (at-least-once). The program asserts each outcome
   `is_accepted()`.
5. **ATTACH (receiver)** — `Receiver::builder().source("queues/<ch>")` with
   `CreditMode::Auto(10)` attaches a link the server sees as a *sender* (the client
   consumes). The **client** grants 10 credits via a FLOW and auto-replenishes as
   deliveries settle; without credit nothing is delivered.
6. **TRANSFER / DISPOSITION (consume)** — `recv` returns each message; `accept`
   sends a receiver DISPOSITION `accepted` ⇒ the connector resolves it to an
   **AckRange** and removes it from the queue.
7. **Drain check** — a final `recv` wrapped in a short `tokio::time::timeout`
   times out, proving the queue is empty.
8. **DETACH / CLOSE** — `receiver.close()`, then `session.end()`, then
   `connection.close()`.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `queues/<ch>` | `Unsettled` (explicit) | server-granted | server returns `Accepted` outcome per send | none | `Data` | at-least-once produce |
| receiver (KubeMQ → client) | source `queues/<ch>` | `First` (default) | client `CreditMode::Auto(10)` | `accept` ⇒ AckRange (removed) | none | `Data` | competing-consumer, move-only |

## Gotchas

- **Sender settle-mode must be explicit.** `fe2o3-amqp` defaults a sender to
  `mixed`, which the connector rejects at ATTACH (`amqp:not-implemented`). Always
  set `SenderSettleMode::Unsettled` (at-least-once) or `::Settled` (at-most-once).
- **No peek / browse / FIFO verb.** Queue consume is destructive and
  credit-driven only — there is no peek or browse over AMQP 1.0.
- **`release` / `modify` increment the receive-count.** This example always
  `accept`s. To see requeue-on-NAck (and the receive-count growth that pushes
  toward the broker's `MaxReceiveQueue` poison cap), see
  [queues/ack-release-redelivery](../ack-release-redelivery/).
- **Body must be `Data` or `AmqpValue`.** An `AmqpSequence` body is rejected with
  `amqp:not-implemented`. This example sends `Data` (a `String`/`&str` serializes
  to a `Data` section).
- **No AMQP transactions.** Use settlement (accept / release / reject) for
  reliability, not a transacted session.

## Related Examples

- [queues/ack-release-redelivery](../ack-release-redelivery/) — `accept` vs `release` vs `reject`
- [queues/settlement-modes](../settlement-modes/) — unsettled vs pre-settled
- [events/basic-pubsub](../../events/basic-pubsub/) — fan-out at-most-once (Events)

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
