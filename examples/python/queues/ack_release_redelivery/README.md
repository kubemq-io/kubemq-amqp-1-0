# Python — Queues / Ack, Release & Redelivery

The three queue settlement outcomes, side by side, over KubeMQ **Queues** using
the native `python-qpid-proton` blocking client:

- **`accept`** ⇒ AckRange — the message is removed (success).
- **`release(delivered=True)`** ⇒ NAckRange — the message is requeued to the tail
  and **redelivered** with a grown delivery-count and `first_acquirer=False`.
- **`reject`** ⇒ AckRange/discard — the message is removed and **not** redelivered
  to this receiver (poison handling is a broker `MaxReceiveQueue` policy; there is
  no connector DLX).

## Prerequisites

- Python 3.10+
- `python-qpid-proton` 0.40.0 (pinned in `examples/python/pyproject.toml`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/python
uv sync
uv run python queues/ack_release_redelivery/main.py
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 uv run python queues/ack_release_redelivery/main.py
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

(The delivery order between the original and the redelivered copy can vary; the
redelivered `release-me` always carries `delivery-count>=1` / `first-acquirer=False`.)

## What's Happening

1. **Produce three distinct messages** — `release-me`, `reject-me`, `accept-me` on
   `queues/<ch>` (unsettled).
2. **Consume with `credit=10`** and branch on the body:
   - `release-me` first sight → `receiver.release(delivered=True)`. `delivered=True`
     maps to a MODIFIED outcome with `delivery-failed`, so the broker requeues the
     message and grows its delivery-count on redelivery.
   - `reject-me` → `receiver.reject()` (discarded; not redelivered here).
   - `accept-me` → `receiver.accept()` (removed).
3. **Observe redelivery** — `release-me` comes back with `delivery-count>=1` and
   `first_acquirer=False`; the program accepts it the second time. The connector
   maps the broker receive-count onto the header: `delivery-count = ReceiveCount-1`,
   `first-acquirer = (ReceiveCount == 1)`.
4. **Drain check** — a final `receive(timeout=2.0)` times out, proving the rejected
   message was discarded (not redelivered).

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `queues/<ch>` | unsettled (default) | server-granted | `accepted` per send | none | `Data` | — |
| receiver (KubeMQ → client) | source `queues/<ch>` | `first` (default) | client-granted `credit=10` | `accepted` / `released` (modified, delivery-failed) / `rejected` | header `delivery-count`, `first-acquirer` | `Data` | release ⇒ redelivery with grown delivery-count; reject ⇒ discard |

## Gotchas

> **Each `release` increments the broker receive-count toward `MaxReceiveQueue`.**
> A message released repeatedly will eventually hit the broker's max-receive
> threshold and be dropped by broker policy — there is **no connector dead-letter
> exchange**. Use `release` for transient retries only; for a permanent failure
> use `reject` (discard) and handle the loss in your application.

- **`first_acquirer=True` on the first delivery, `False` on redelivery.** The
  connector stamps it from the broker receive-count; the delivery-count growth on
  redelivery is the reliable redelivery signal.
- **`reject` does not requeue.** The rejected message is gone from this receiver;
  it is not redelivered and there is no DLX.

## Related Examples

- [queues/basic_send_receive](../basic_send_receive/) — the happy-path accept-only loop
- [queues/settlement_modes](../settlement_modes/) — at-most-once vs at-least-once producers
- [events/basic_pubsub](../../events/basic_pubsub/) — fan-out Events (no per-message ack/release/reject)

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
