# C# — Queues / Basic Send-Receive

At-least-once produce + credit-based consume over KubeMQ **Queues** with the
native `AMQPNetLite.Core` client. A sender publishes 10 messages to
`queues/<ch>`; a receiver grants link credit, consumes each, and `Accept`s it, so
the queue drains to empty with no loss.

## Prerequisites

- .NET SDK **8.0**
- `AMQPNetLite.Core` **2.5.3** (pinned in `examples/csharp/Directory.Packages.props`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd queues/basic-send-receive
dotnet run
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 dotnet run
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: queues/amqp10.examples.basic  (KubeMQ pattern=queues, channel=amqp10.examples.basic)

[send] Produced 10 messages to queues/amqp10.examples.basic (accepted DISPOSITION each)
[recv] Consumed and accepted 10 messages (no loss)
[recv] Queue drained to empty (no further messages)

Done.
```

## What's Happening

1. **OPEN** — `Connection.Factory.CreateAsync(new Address(url))` connects over
   AMQP 1.0. The default URL has no userinfo, so the SASL layer negotiates
   **ANONYMOUS**. AMQPNetLite sends a non-empty `container-id` automatically (a
   connector requirement — an empty container-id is closed with
   `amqp:invalid-field`).
2. **BEGIN** — `new Session(connection)` opens one session for both links.
3. **ATTACH (sender)** — `new SenderLink(session, name, "queues/<ch>")` attaches
   a link the server sees as a *receiver* (the client produces). The server
   immediately grants link credit, so each `Send` proceeds.
4. **TRANSFER / DISPOSITION (produce)** — each `Send(msg, timeout)` is
   **unsettled**: it blocks until the connector returns a receiver DISPOSITION
   `accepted`, confirming the broker stored the message (at-least-once).
5. **ATTACH (receiver)** — `new ReceiverLink(session, name, "queues/<ch>")` plus
   `SetCredit(10)` attaches a link the server sees as a *sender* (the client
   consumes). The **client** grants credit (10) via a FLOW; without credit
   nothing is delivered.
6. **TRANSFER / DISPOSITION (consume)** — `Receive` returns each message;
   `Accept` sends a receiver DISPOSITION `accepted` => the connector resolves it
   to an **AckRange** and removes it from the queue. `SetCredit(autoRestore:
   true)` re-issues credit as messages settle.
7. **Drain check** — a final `Receive` with a short timeout returns `null`,
   proving the queue is empty.
8. **DETACH / CLOSE** — the links detach and the connection closes.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `queues/<ch>` | unsettled (default) | server-granted | server emits `accepted` DISPOSITION per send | none | `Data` | at-least-once produce |
| receiver (KubeMQ → client) | source `queues/<ch>` | `first` (default) | client-granted `SetCredit(10)` | `Accept` ⇒ AckRange (removed) | none | `Data` | competing-consumer, move-only |

## Gotcha

> **Do not detach the producer before draining the consumer on the same
> connection.** With this connector + AMQPNetLite, closing the sender link while a
> sibling receiver on the same connection is still consuming can stall delivery
> to that receiver. This example keeps the sender link open through the consume
> phase and closes all links at the very end. (This is an AMQPNetLite/connector
> interaction, not an AMQP 1.0 requirement.)

Other notes:

- **No peek / browse / FIFO verb.** Queue consume is destructive and
  credit-driven only.
- **Body must be `Data` or `AmqpValue`.** An `AmqpSequence` body is rejected with
  `amqp:not-implemented`. This example sends `Data`.

## Related Examples

- [queues/ack-release-redelivery](../ack-release-redelivery/) — `Accept` vs `Release` vs `Reject`
- [queues/settlement-modes](../settlement-modes/) — unsettled vs pre-settled
- [events/basic-pubsub](../../events/basic-pubsub/) — fan-out at-most-once (Events)

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
