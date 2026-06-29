# JavaScript — Queues / Ack, Release & Redelivery

The three queue settlement outcomes side by side over KubeMQ **Queues** with the
native `rhea` / `rhea-promise` client:

- **`delivery.accept()`** ⇒ AckRange — the message is removed (success).
- **`delivery.release()`** ⇒ NAckRange — the message is requeued to the tail and
  **redelivered** with a grown `delivery_count` and `first_acquirer` no longer
  true.
- **`delivery.reject(error)`** ⇒ AckRange/discard — the message is removed and
  **not** redelivered to this receiver (poison handling is a broker-side policy).

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
npx tsx queues/ack-release-redelivery/index.ts
```

Override the broker URL (SASL **ANONYMOUS** by default):

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 npx tsx queues/ack-release-redelivery/index.ts
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: queues/amqp10.examples.ack.<pid>

[send] Produced: release-me, reject-me, accept-me
[recv] release-me   delivery-count=0 first-acquirer=true  -> RELEASED (requeued)
[recv] reject-me    delivery-count=0 first-acquirer=true  -> REJECTED (discarded, no requeue)
[recv] accept-me    delivery-count=0 first-acquirer=true  -> ACCEPTED (removed)
[recv] release-me   delivery-count=1 first-acquirer=false -> REDELIVERED, then ACCEPTED
[recv] Rejected message was not redelivered (discarded)

Done.
```

(Delivery order between the original and the redelivered copy can vary; the
redelivered `release-me` always carries `delivery_count >= 1` /
`first_acquirer` no longer `true`.)

> The channel carries a per-run process-id suffix
> (`amqp10.examples.ack.<pid>`). This example releases a message — which requeues
> it — and reads its grown `delivery_count`, so a leftover copy from a previous
> interrupted run on a shared channel would skew the assertions. A fresh channel
> per run keeps the demo deterministic without any cross-run cleanup.

## What's Happening

1. **Produce** three messages on `queues/<ch>` (`release-me`, `reject-me`,
   `accept-me`), each unsettled and accepted by the connector.
2. **Consume with manual credit.** Each delivery's `message` carries an AMQP
   header. On the **first** acquisition `message.delivery_count == 0` and
   `message.first_acquirer == true` (the connector maps the broker receive-count:
   `delivery-count = ReceiveCount-1`, `first-acquirer = ReceiveCount==1`). On a
   redelivery rhea omits `first_acquirer`, so absent/`false` both mean "not first".
3. **`delivery.release()`** on `release-me` sends a `released` DISPOSITION ⇒ the
   connector NAckRanges it and requeues it to the tail.
4. **`delivery.reject({condition:"amqp:internal-error"})`** on `reject-me` sends a
   `rejected` DISPOSITION ⇒ the connector AckRanges/discards it. It is not
   redelivered to this receiver.
5. **`delivery.accept()`** on `accept-me` removes it cleanly.
6. **Redelivery.** `release-me` comes back with `delivery_count >= 1` and
   `first_acquirer` no longer true; the program asserts this, then accepts it.
7. A final short-deadline receive confirms the rejected body never returns.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| receiver (KubeMQ → client) | source `queues/<ch>` | `first` (default) | client-granted via `addCredit` | `accept` ⇒ AckRange (removed); `release` ⇒ NAckRange (requeued, grown `delivery_count`); `reject` ⇒ AckRange/discard | none | `Data` | redelivery grows `delivery_count`, clears `first_acquirer` |

## Related Examples

- [queues/basic-send-receive](../basic-send-receive/) — accept-only drain
- [queues/settlement-modes](../settlement-modes/) — unsettled vs pre-settled (at-most-once)

## Gotcha

> **`released` / `modified` increment the receive-count.** Each
> `delivery.release()` (or `delivery.modified()`) requeues the message **and**
> bumps the broker receive-count toward the broker's `MaxReceiveQueue` poison
> cap. There is **no requeue-without-increment** and **no connector dead-letter
> exchange** — a message released enough times is removed by the broker poison
> policy even though the client never `reject`ed it. Use `release` for transient
> retry only; for permanent failure prefer `reject` (which discards immediately).
>
> `reject` ≠ DLX — a rejected message is discarded by the connector; any
> dead-letter / poison routing is the broker's `MaxReceiveQueue` policy, not an
> AMQP-controllable per-link feature.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
