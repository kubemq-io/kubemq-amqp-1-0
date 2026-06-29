# Rust — Advanced / Anonymous Terminus

An anonymous sender (a link attached with a NULL target) carries no fixed channel.
Each message selects its own destination via its `properties.to` field, and the KubeMQ
connector routes it per-message. One link, many destinations. This example routes one
message to a queue and one to an events topic over the same anonymous link, then shows
a bad `to` and a missing `to` rejected with `amqp:precondition-failed`.

## Prerequisites

- Rust 1.94+ (stable) with `cargo`.
- `fe2o3-amqp` 0.15.1 + `fe2o3-amqp-types` 0.14.0 (pinned exact in the workspace
  `examples/rust/Cargo.toml`).
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/rust
cargo run -p anonymous-terminus
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 cargo run -p anonymous-terminus
```

## Expected Output

```
Broker: amqp://localhost:5672
Anonymous sender (null target) — routes per-message via properties.to
  msg #1 to: queues/amqp10.examples.anon.q
  msg #2 to: events/amqp10.examples.anon.e

[attach] Anonymous sender attached (null target)
[send] msg #1 routed to queues/amqp10.examples.anon.q (accepted)
[send] msg #2 routed to events/amqp10.examples.anon.e (accepted)
[send] msg with bad `to`="bogus/prefix/x" rejected as expected: Rejected(Rejected { error: Some(Error { condition: AmqpError(PreconditionFailed), description: Some("unknown address prefix"), info: None }) })
[send] msg with NO `to` rejected as expected: Rejected(Rejected { error: Some(Error { condition: AmqpError(PreconditionFailed), description: Some("anonymous terminus message has no `to`"), info: None }) })
[recv] queue queues/amqp10.examples.anon.q delivered: "to-queue"
[recv] events events/amqp10.examples.anon.e delivered: "to-events"

Done.
```

## What's Happening

1. **OPEN / BEGIN** — connect (ANONYMOUS) and begin one session.
2. **ATTACH (anonymous sender)** — `Sender::builder().target(Target::builder().build())`
   attaches a link with a NULL target — no bound channel. Every message routes by its
   own `properties.to`.
3. **ATTACH (events subscriber)** — a receiver on `events/<ch>` is attached BEFORE the
   events publish, because Events are fire-and-forget (no replay).
4. **TRANSFER (route to queue)** — message #1 carries `properties.to = "queues/<ch>"`;
   the connector resolves it, authorizes WRITE for this connection (per-message Casbin
   policy — there is no per-link grant for an anonymous terminus), and stores it.
5. **TRANSFER (route to events)** — message #2 carries `properties.to = "events/<ch>"`;
   the same anonymous link routes to a DIFFERENT pattern. The subscriber receives it.
6. **Negative cases** — a bad `to` (`bogus/prefix/x`) and a missing `to` are both
   rejected with `amqp:precondition-failed`. fe2o3-amqp surfaces the rejection as a
   `Rejected` outcome on the `send`; the anonymous link stays usable afterwards.
7. **VERIFY** — the queue message is consumed back and the event is received, proving
   per-message routing landed correctly.
8. **DETACH / CLOSE** — the anonymous sender, both receivers, the session, and the
   connection are torn down cleanly.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| anonymous sender (client → KubeMQ) | null target | `Unsettled` (explicit) | server-granted | `Accepted` / `Rejected(precondition-failed)` per message | `Properties{to}` per message | `Data` | routes per-message; per-message Casbin WRITE |
| queue receiver (KubeMQ → client) | source `queues/<ch>` | `First` (default) | `CreditMode::Auto(1)` | `accept` ⇒ AckRange (removed) | none | `Data` | verifies queue routing |
| events receiver (KubeMQ → client) | source `events/<ch>` | `First` (default) | `CreditMode::Auto(5)` | pre-settled fan-out | none | `Data` | subscribe-before-publish |

## Related Examples

- [commands/request-reply-dynamic-node](../../commands/request-reply-dynamic-node/) — uses an anonymous sender for the RPC reply path (per-message `to` = `/responses/<RequestID>`)
- [queues/basic-send-receive](../../queues/basic-send-receive/) — a fixed-target queue sender
- [events/basic-pubsub](../../events/basic-pubsub/) — a fixed-target events sender

## Gotchas

> **A bad or missing `to` is rejected with `amqp:precondition-failed`.** An unknown
> address prefix (`bogus/prefix/x`) and a message with NO `to` at all both fail. The
> connector surfaces the failure as a `Rejected` outcome on the `send` — the link is
> NOT torn down, so the anonymous sender stays usable for the next message.

- **Authorization is per-message, not per-link.** Each anonymous send is authorized
  for WRITE on the resolved (pattern, channel) — there is no per-link grant for an
  anonymous terminus.
- **Anonymous senders still need an explicit settle-mode.** `fe2o3-amqp` defaults to
  `mixed`, which the connector rejects at ATTACH (`amqp:not-implemented`); this example
  pins `SenderSettleMode::Unsettled`.
- **Events are fire-and-forget.** Subscribe to the events destination BEFORE routing a
  message to it, or it is lost (no replay).

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT see
`connectivity/auth` + `guides/authentication.md`.
