# JavaScript — Queues / Settlement Modes

Producer reliability tiers side by side over KubeMQ **Queues** with the native
`rhea` / `rhea-promise` client:

- **Pre-settled sender** (`snd_settle_mode: 1`, "settled") — **at-most-once**.
  Each `send()` returns immediately without waiting for a server DISPOSITION. No
  delivery confirmation, no redelivery; a drop on the way in is invisible to the
  producer. We use the plain `Sender` (its `send()` is synchronous); an
  `AwaitableSender` would hang waiting for a disposition that a pre-settled link
  never produces.
- **Unsettled sender** (the default) — **at-least-once**. Each
  `AwaitableSender.send()` resolves only after the connector returns an
  `accepted` DISPOSITION confirming the broker stored the message (the variant #1
  contract).

The consumer requests `rcv_settle_mode: 0` ("first") — the only receiver
settle-mode the connector supports. Both senders' messages drain to the same
consumer.

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
npx tsx queues/settlement-modes/index.ts
```

Override the broker URL (SASL **ANONYMOUS** by default):

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 npx tsx queues/settlement-modes/index.ts
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: queues/amqp10.examples.settlement

[send] Pre-settled (at-most-once): produced 10 messages — NO DISPOSITION awaited
[send] Unsettled (at-least-once): produced 10 messages — each accepted DISPOSITION
[recv] Drained 20 total — 10 pre-settled + 10 unsettled (rcv-settle-mode=first)
[recv] Queue drained to empty (no further messages)

Done.
```

On a healthy broker pre-settled messages also drain — the difference is the
**producer guarantee**, not the happy-path result. A pre-settled `send()` returns
before any broker confirmation, so a drop on the way in (oversize / no capacity)
is invisible to the producer; an unsettled `send()` blocks until the broker
confirms storage.

## What's Happening

1. **OPEN / BEGIN** — `connection.open()` (SASL ANONYMOUS by default).
2. **ATTACH (pre-settled sender)** — `createSender({target:{address:"queues/<ch>"},
   snd_settle_mode:1})` attaches a link whose sender-settle-mode is `settled`. The
   client marks every TRANSFER as already settled.
3. **TRANSFER (pre-settled)** — each `send()` writes the frame and returns
   **without** waiting for a DISPOSITION. There is no `accepted`/`rejected`
   feedback loop — at-most-once. We `await` `sendable` before each send so the
   link has credit.
4. **ATTACH (unsettled sender)** — `createAwaitableSender({target:{...}})` uses the
   default. Each `send()` resolves only after the connector's `accepted`
   DISPOSITION — at-least-once.
5. **ATTACH (receiver)** — `createReceiver({source:{...}, credit_window:0,
   rcv_settle_mode:0})`. `first` means the server settles the delivery on the
   first transfer (the only mode the connector supports).
6. **TRANSFER / DISPOSITION (consume)** — the receiver emits a `message` per
   delivery; `delivery.accept()` drains every message (both tiers land in the same
   queue) and removes it (AckRange).
7. **Drain check** — a final short-deadline receive times out, proving the queue
   is empty.
8. **DETACH / CLOSE** — links detach and the connection closes.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender — pre-settled (client → KubeMQ) | target `queues/<ch>` | `settled` (`snd_settle_mode:1`) | server-granted | NONE — no DISPOSITION awaited | none | `Data` | at-most-once produce (fire-and-forget) |
| sender — unsettled (client → KubeMQ) | target `queues/<ch>` | `unsettled` (default) | server-granted | server emits `accepted` DISPOSITION per send | none | `Data` | at-least-once produce |
| receiver (KubeMQ → client) | source `queues/<ch>` | `first` (`rcv_settle_mode:0`) | client-granted via `addCredit` | `delivery.accept()` ⇒ AckRange (removed) | none | `Data` | `second` rejected (see gotcha) |

## Related Examples

- [queues/basic-send-receive](../basic-send-receive/) — at-least-once produce + accept drain
- [queues/ack-release-redelivery](../ack-release-redelivery/) — `accept` vs `release` vs `reject`
- [events/basic-pubsub](../../events/basic-pubsub/) — fan-out at-most-once (Events)

## Gotcha

> **`rcv-settle-mode=second` is not implemented.** The connector supports
> sender-settle `unsettled`/`settled` and receiver-settle **`first` only**. A
> receiver that requests `rcv_settle_mode: 1` (two-phase settlement, where the
> consumer settles only after the server settles) is refused at attach: the
> connector emits a **DETACH carrying `amqp:not-implemented`** before the link is
> ever constructed. Always attach receivers with `rcv_settle_mode: 0` (the
> default) — `second` will fail the link.
>
> **Pre-settled has no redelivery.** Because there is no DISPOSITION feedback, a
> pre-settled producer cannot be told the message was lost and cannot trigger a
> resend. Use pre-settled only for data where loss is acceptable (metrics,
> telemetry). A pre-settled drop shows up only on the server metric
> `kubemq_amqp10_transfers_in_dropped_total` — never as a client-side error.
>
> **Use the plain `Sender` (not `AwaitableSender`) for pre-settled links.**
> `AwaitableSender.send()` waits for a delivery disposition; a pre-settled link
> never emits one, so the promise would reject with an operation timeout.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
