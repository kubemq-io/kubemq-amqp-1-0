# JavaScript — Queues / Basic Send-Receive

At-least-once produce + credit-based consume over KubeMQ **Queues** with the
native `rhea` / `rhea-promise` client. An `AwaitableSender` publishes 10 messages
to `queues/<ch>`; a `Receiver` grants link credit, consumes each, and
`accept`s it, so the queue drains to empty with no loss.

## Prerequisites

- Node.js 20+ (developed against Node 26).
- `rhea` 3.0.4 + `rhea-promise` 3.0.3 (pinned in `examples/javascript/package.json`);
  run via `tsx`.
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

Install once from `examples/javascript`:

```bash
npm install
```

## How to Run

```bash
cd examples/javascript
npx tsx queues/basic-send-receive/index.ts
```

Override the broker URL (SASL **ANONYMOUS** by default — no credentials in the
URL):

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 npx tsx queues/basic-send-receive/index.ts
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

1. **OPEN** — `new Connection(...)` + `connection.open()` connects over AMQP 1.0.
   The default URL has no userinfo, so the SASL layer negotiates **ANONYMOUS**.
   rhea sends a non-empty `container-id` automatically (a connector requirement —
   an empty container-id is closed with `amqp:invalid-field`); we set an explicit
   one for clarity.
2. **BEGIN** — `createAwaitableSender` / `createReceiver` create their own session
   under the hood (one connection, two links).
3. **ATTACH (sender)** — `createAwaitableSender({target:{address:"queues/<ch>"}})`
   attaches a link the server sees as a *receiver* (the client produces). The
   server grants link credit on attach.
4. **TRANSFER / DISPOSITION (produce)** — each `send()` is **unsettled**: the
   promise resolves only after the connector returns a receiver DISPOSITION
   `accepted`, confirming the broker stored the message (at-least-once).
5. **ATTACH (receiver)** — `createReceiver({source:{address:"queues/<ch>"},
   credit_window:0, autoaccept:false, autosettle:false})` attaches a link the
   server sees as a *sender* (the client consumes). We grant credit **manually**
   with `addCredit(...)`.
6. **TRANSFER / DISPOSITION (consume)** — the receiver emits a `message` event per
   delivery; `delivery.accept()` sends a receiver DISPOSITION `accepted` ⇒ the
   connector resolves it to an **AckRange** and removes it from the queue. The
   message handler is registered **before** `addCredit` so no early delivery is
   missed.
7. **Drain check** — a final short-deadline receive times out, proving the queue
   is empty.
8. **DETACH / CLOSE** — the receiver link detaches and the connection closes.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `queues/<ch>` | unsettled (default) | server-granted | server emits `accepted` DISPOSITION per send | none | `Data` | at-least-once produce |
| receiver (KubeMQ → client) | source `queues/<ch>` | `first` (default) | client-granted via `addCredit` | `delivery.accept()` ⇒ AckRange (removed) | none | `Data` | competing-consumer, move-only |

## Related Examples

- [queues/ack-release-redelivery](../ack-release-redelivery/) — `accept` vs `release` vs `reject`
- [queues/settlement-modes](../settlement-modes/) — unsettled vs pre-settled
- [events/basic-pubsub](../../events/basic-pubsub/) — fan-out at-most-once (Events)

## Gotcha

> **Register the message handler before granting credit.** With manual credit
> (`credit_window:0` + `addCredit`), a delivery that arrives before a `message`
> listener is attached is lost. This example attaches the handler first, then
> calls `addCredit`.
>
> **No peek / browse / FIFO verb.** Queue consume is destructive and
> credit-driven only — there is no peek or browse over AMQP 1.0. `released` /
> `modified` increment the receive-count (see
> [queues/ack-release-redelivery](../ack-release-redelivery/)); the body must be
> `Data` or `AmqpValue` (an `AmqpSequence` body is rejected with
> `amqp:not-implemented`); there are no AMQP transactions.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
