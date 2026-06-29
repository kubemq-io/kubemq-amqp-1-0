# JavaScript — Advanced / Anonymous Terminus

One AMQP 1.0 sender with a **null target** (`createSender({target:{}})`) that routes
**per-message** by each message's `to`. A single anonymous link fans messages out to
different KubeMQ patterns/channels — a queue, then an events topic — without
re-attaching. Uses the native `rhea` / `rhea-promise` client (no KubeMQ SDK).

The example sends one message to `queues/<ch>`, one to `events/<ch>`, then shows
that a **bad** `to` (unknown prefix) and a **missing** `to` are both rejected with
`amqp:precondition-failed`.

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
npx tsx advanced/anonymous-terminus/index.ts
```

Override the broker URL (SASL **ANONYMOUS** by default):

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 npx tsx advanced/anonymous-terminus/index.ts
```

## Expected Output

```
Broker: amqp://localhost:5672
Anonymous sender (null target) — routes per-message via `to`
  msg #1 to: queues/amqp10.examples.anon.q
  msg #2 to: events/amqp10.examples.anon.e

[attach] Anonymous sender attached (null target)
[send] msg #1 routed to queues/amqp10.examples.anon.q (accepted)
[send] msg #2 routed to events/amqp10.examples.anon.e (accepted)
[send] msg with bad `to`="bogus/prefix/x" rejected as expected: amqp:precondition-failed (unknown address prefix)
[send] msg with NO `to` rejected as expected: amqp:precondition-failed (anonymous terminus message has no `to`)
[recv] queue queues/amqp10.examples.anon.q delivered: "to-queue"
[recv] events events/amqp10.examples.anon.e delivered: "to-events"

Done.
```

## What's Happening

1. **OPEN / BEGIN** — `connection.open()` (SASL ANONYMOUS by default).
2. **ATTACH (anonymous sender)** — `createAwaitableSender({target:{}})`. The empty
   target attaches a link with a **null target**: it has no bound channel, so every
   message must carry its own destination. Sends are unsettled, so each routing
   decision returns an `accepted` or `rejected` DISPOSITION.
3. **ATTACH (events subscriber)** — `createReceiver({source:{address:"events/<ch>"}})`
   is attached **before** publishing to it, because events are fire-and-forget (no
   replay).
4. **TRANSFER #1 (to a queue)** — `send({body:"to-queue", to:"queues/<ch>"})`. The
   connector resolves the prefix, authorizes **WRITE** for this connection's
   identity on `(queues, <ch>)`, and stores the message → `accepted`.
5. **TRANSFER #2 (to an events topic)** — `send({body:"to-events", to:"events/<ch>"})`.
   Same link, a different pattern. The standing subscriber receives it.
6. **Negative cases** — a message with `to="bogus/prefix/x"` (unknown prefix) and a
   message with **no** `to` are both rejected by the connector with
   `amqp:precondition-failed`; each surfaces as a rejected `send()` promise. The link
   stays usable.
7. **Verify** — the queue message is consumed back (`"to-queue"`) and the event is
   received (`"to-events"`), proving per-message routing worked.
8. **DETACH / CLOSE** — links detach and the connection closes.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| anonymous sender (client → KubeMQ) | target **null** (`createSender({target:{}})`) | unsettled (default) | server-granted | `accepted` per valid `to`; `amqp:precondition-failed` for bad/missing `to` | message `to` selects the destination per send | `Data` | per-message WRITE authz on the resolved `(pattern, channel)` |
| events receiver (KubeMQ → client) | source `events/<ch>` | `first` (default) | client-granted (`addCredit(1)`) | message delivered (fire-and-forget) | none | `Data` | subscribe-before-publish |
| queue receiver (KubeMQ → client) | source `queues/<ch>` | `first` (default) | client-granted (`addCredit(1)`) | `accept()` ⇒ AckRange (removed) | none | `Data` | consumed after send (durable) |

## Gotchas

> **A bad or missing `to` is `amqp:precondition-failed`, not a silent drop.** On an
> anonymous terminus the destination lives in the message, not the link. If `to` is
> absent, empty, or names an unknown prefix/invalid channel, the connector rejects
> that delivery with **`amqp:precondition-failed`** and the `send()` promise rejects.
> The anonymous link itself is **not** torn down — subsequent well-formed sends still
> work.
>
> **rhea may also raise a connection-level `error`.** A connector rejection can
> surface as a raw connection-level `error` event in addition to rejecting the send;
> an unhandled `error` event would crash Node. This example attaches a no-op
> `Connection` `error` listener to swallow it, and asserts the rejection on the
> `send()` promise itself.

- **Per-message authorization.** There is no per-link channel grant for an anonymous
  sender, so the connector authorizes **WRITE** on the resolved `(pattern, channel)`
  for **every** message individually. A `to` your identity cannot write to is
  rejected (authorization denial) even if the prefix is valid.
- **Events still need a subscriber first.** Routing a message to `events/<ch>` does
  not change events semantics — attach the subscriber before you publish or the
  event is lost (gotcha: 0-credit / no-subscriber drop).

## Related Examples

- [advanced/multi-frame-large-payload](../multi-frame-large-payload/) — fragmented body round-trip
- [queues/basic-send-receive](../../queues/basic-send-receive/) — fixed-target queue produce/consume
- [events/basic-pubsub](../../events/basic-pubsub/) — fixed-target events fan-out
- [commands/request-reply-dynamic-node](../../commands/request-reply-dynamic-node/) — anonymous reply sender in native RPC

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
