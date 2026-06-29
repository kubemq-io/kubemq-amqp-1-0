# Go — Queues / Basic Send-Receive

At-least-once produce + credit-based consume over KubeMQ **Queues** with the
native `github.com/Azure/go-amqp` client. A sender publishes 10 messages to
`queues/<ch>`; a receiver grants link credit, consumes each, and `accept`s it,
so the queue drains to empty with no loss.

## Prerequisites

- Go 1.24+
- `github.com/Azure/go-amqp` v1.7.0 (pinned in `examples/go/go.mod`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/go
go run ./queues/basic-send-receive
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 go run ./queues/basic-send-receive
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

1. **OPEN** — `amqp.Dial` connects over AMQP 1.0. The default URL has no
   userinfo, so the SASL layer negotiates **ANONYMOUS**. go-amqp sends a
   non-empty `container-id` automatically (a connector requirement — an empty
   container-id is closed with `amqp:invalid-field`).
2. **BEGIN** — `NewSession` opens one session for both links.
3. **ATTACH (sender)** — `NewSender("queues/<ch>")` attaches a link the server
   sees as a *receiver* (the client produces). The server immediately grants
   link credit, so each `Send` proceeds.
4. **TRANSFER / DISPOSITION (produce)** — each `Send` is **unsettled**: it
   blocks until the connector returns a receiver DISPOSITION `accepted`,
   confirming the broker stored the message (at-least-once).
5. **ATTACH (receiver)** — `NewReceiver("queues/<ch>", {Credit:10})` attaches a
   link the server sees as a *sender* (the client consumes). The **client**
   grants credit (10) via a FLOW; without credit nothing is delivered.
6. **TRANSFER / DISPOSITION (consume)** — `Receive` returns each message;
   `AcceptMessage` sends a receiver DISPOSITION `accepted` ⇒ the connector
   resolves it to an **AckRange** and removes it from the queue. go-amqp
   re-issues credit automatically as messages settle.
7. **Drain check** — a final `Receive` with a short deadline times out,
   proving the queue is empty.
8. **DETACH / CLOSE** — the receiver link detaches and the connection closes.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `queues/<ch>` | unsettled (default) | server-granted | server emits `accepted` DISPOSITION per send | none | `Data` | at-least-once produce |
| receiver (KubeMQ → client) | source `queues/<ch>` | `first` (default) | client-granted `Credit:10` | `AcceptMessage` ⇒ AckRange (removed) | none | `Data` | competing-consumer, move-only |

## Gotchas

- **No peek / browse / FIFO verb.** Queue consume is destructive and
  credit-driven only — there is no peek or browse over AMQP 1.0.
- **`released` / `modified` increment the receive-count.** This example always
  `accept`s. To see requeue-on-NAck (and the receive-count growth that pushes
  toward the broker's `MaxReceiveQueue` poison cap), see
  [queues/ack-release-redelivery](../ack-release-redelivery/).
- **Body must be `Data` or `AmqpValue`.** An `AmqpSequence` body is rejected
  with `amqp:not-implemented`. This example sends `Data`.
- **No AMQP transactions.** Use settlement (accept / release / reject) for
  reliability, not a transacted session.

## Related Examples

- [queues/ack-release-redelivery](../ack-release-redelivery/) — `accept` vs `release` vs `reject`
- [queues/settlement-modes](../settlement-modes/) — unsettled vs pre-settled
- [events/basic-pubsub](../../events/basic-pubsub/) — fan-out at-most-once (Events)

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
