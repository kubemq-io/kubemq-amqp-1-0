# C# — Queues / Ack, Release & Redelivery

The three queue settlement outcomes, side by side, over KubeMQ **Queues** with
the native `AMQPNetLite.Core` client: **accept** removes a message, **release**
requeues it (redelivered with a grown delivery-count), and **reject** discards it
(no requeue).

## Prerequisites

- .NET SDK **8.0**
- `AMQPNetLite.Core` **2.5.3** (pinned in `examples/csharp/Directory.Packages.props`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd queues/ack-release-redelivery
dotnet run
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 dotnet run
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: queues/amqp10.examples.ack

[send] Produced: release-me, reject-me, accept-me
[recv] release-me   delivery-count=0 first-acquirer=True  -> RELEASED (requeued)
[recv] reject-me    delivery-count=0 first-acquirer=True  -> REJECTED (discarded, no requeue)
[recv] accept-me    delivery-count=0 first-acquirer=True  -> ACCEPTED (removed)
[recv] release-me   delivery-count=1 first-acquirer=False -> REDELIVERED, then ACCEPTED
[recv] Rejected message was not redelivered (discarded)

Done.
```

(Delivery order between the original and the redelivered copy can vary; the
redelivered `release-me` always carries `delivery-count>=1` /
`first-acquirer=False`.)

## What's Happening

1. **OPEN / BEGIN** — connect (ANONYMOUS) and open one session.
2. **ATTACH (sender)** — produce three distinct messages to `queues/<ch>`:
   `release-me`, `reject-me`, `accept-me` (unsettled; each blocks for `accepted`).
3. **ATTACH (receiver)** — grant credit with `SetCredit(10, autoRestore: true)`.
4. **TRANSFER / DISPOSITION (consume)** — for each delivery, send the matching
   disposition:
   - `Release(msg)` ⇒ the connector resolves it to a **NAckRange**: the message
     is requeued to the tail and **redelivered** with `Header.DeliveryCount >= 1`
     and `FirstAcquirer = false`.
   - `Reject(msg, error)` ⇒ **AckRange/discard**: the message is removed and NOT
     redelivered to this receiver (poison handling is the broker's
     `MaxReceiveQueue` policy — there is no connector DLX).
   - `Accept(msg)` ⇒ **AckRange**: the message is removed (success).
5. **Redelivery** — the released `release-me` comes back; this run accepts it on
   the second sight.
6. **No-redelivery check** — a final `Receive` returns `null`, proving the
   rejected message was discarded (not requeued).
7. **DETACH / CLOSE** — the links detach and the connection closes.

The connector maps the KubeMQ broker receive-count onto the AMQP header:
`delivery-count = ReceiveCount - 1`, `first-acquirer = (ReceiveCount == 1)`.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `queues/<ch>` | unsettled (default) | server-granted | `accepted` DISPOSITION per send | none | `Data` | at-least-once produce |
| receiver (KubeMQ → client) | source `queues/<ch>` | `first` (default) | client-granted `SetCredit(10)` | `Release` ⇒ NAckRange (redelivered, `DeliveryCount>=1`); `Reject` ⇒ AckRange/discard; `Accept` ⇒ AckRange | none | `Data` | redelivery grows receive-count toward `MaxReceiveQueue` |

## Gotcha

> **`release` / `modified` increment the broker receive-count.** Each requeue
> pushes the message toward the broker's `MaxReceiveQueue` poison cap. There is
> **no connector DLX** — a message that exceeds the cap is dropped by broker
> policy, not routed to a dead-letter destination. `reject` discards immediately
> and is **not** redelivered to the same receiver.

Also, as in [basic-send-receive](../basic-send-receive/), the sender link is kept
open through the consume phase (closing it first can stall delivery with this
connector + AMQPNetLite).

## Related Examples

- [queues/basic-send-receive](../basic-send-receive/) — the happy-path `accept` flow
- [queues/settlement-modes](../settlement-modes/) — unsettled vs pre-settled producers
- [events/basic-pubsub](../../events/basic-pubsub/) — at-most-once Events (no redelivery)

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
