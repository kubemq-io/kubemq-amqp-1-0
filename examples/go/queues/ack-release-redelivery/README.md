# Go — Queues / Ack, Release & Redelivery

The three queue settlement outcomes side by side over KubeMQ **Queues** with the
native `github.com/Azure/go-amqp` client:

- **`AcceptMessage`** ⇒ AckRange — the message is removed (success).
- **`ReleaseMessage`** ⇒ NAckRange — the message is requeued to the tail and
  **redelivered** with a grown `delivery-count` and `first-acquirer=false`.
- **`RejectMessage`** ⇒ AckRange/discard — the message is removed and **not**
  redelivered to this receiver (poison handling is a broker-side policy).

## Prerequisites

- Go 1.24+
- `github.com/Azure/go-amqp` v1.7.0 (pinned in `examples/go/go.mod`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/go
go run ./queues/ack-release-redelivery
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 go run ./queues/ack-release-redelivery
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
redelivered `release-me` always carries `delivery-count >= 1` /
`first-acquirer=false`.)

## What's Happening

1. **Produce** three messages on `queues/<ch>` (`release-me`, `reject-me`,
   `accept-me`), each unsettled and accepted by the connector.
2. **Consume with credit 10.** Each delivery carries an AMQP `Header`. On the
   **first** acquisition `Header.DeliveryCount == 0` and
   `Header.FirstAcquirer == true` (the connector maps the broker receive-count:
   `delivery-count = ReceiveCount-1`, `first-acquirer = ReceiveCount==1`).
3. **`ReleaseMessage(release-me)`** sends a `released` DISPOSITION ⇒ the
   connector NAckRanges it and requeues it to the tail.
4. **`RejectMessage(reject-me, &amqp.Error{Condition: amqp.ErrCondInternalError})`**
   sends a `rejected` DISPOSITION ⇒ the connector AckRanges/discards it. It is
   not redelivered to this receiver.
5. **`AcceptMessage(accept-me)`** removes it cleanly.
6. **Redelivery.** `release-me` comes back with `DeliveryCount >= 1` and
   `FirstAcquirer == false`; the program asserts this, then accepts it.
7. A final short-deadline `Receive` confirms the rejected body never returns.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| receiver (KubeMQ → client) | source `queues/<ch>` | `first` (default) | client-granted `Credit:10` | `accept` ⇒ AckRange (removed); `release` ⇒ NAckRange (requeued, grown delivery-count); `reject` ⇒ AckRange/discard | none | `Data` | redelivery grows `Header.DeliveryCount`, clears `FirstAcquirer` |

## Gotchas

> **`released` / `modified` increment the receive-count.** Each
> `ReleaseMessage` (or `ModifyMessage`) requeues the message **and** bumps the
> broker receive-count toward the broker's `MaxReceiveQueue` poison cap. There
> is **no requeue-without-increment** and **no connector dead-letter exchange** —
> a message released enough times is removed by the broker poison policy even
> though the client never `reject`ed it. Use `release` for transient retry only;
> for permanent failure prefer `reject` (which discards immediately).

- **`reject` ≠ DLX.** A rejected message is discarded by the connector; any
  dead-letter / poison routing is the broker's `MaxReceiveQueue` policy, not an
  AMQP-controllable per-link feature.
- **`modify` exists too.** `ModifyMessage` with `DeliveryFailed` /
  `UndeliverableHere` also NAckRange-requeues (no per-consumer exclusion in this
  connector); this example uses `release` as the canonical requeue path.

## Related Examples

- [queues/basic-send-receive](../basic-send-receive/) — accept-only drain
- [queues/settlement-modes](../settlement-modes/) — unsettled vs pre-settled (at-most-once)

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
