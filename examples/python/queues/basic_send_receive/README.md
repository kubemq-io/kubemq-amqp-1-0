# Python — Queues / Basic Send-Receive

At-least-once produce + credit-based consume over KubeMQ **Queues** using the
native `python-qpid-proton` blocking client. The producer sends unsettled (each
`send` blocks for the broker's `accepted` DISPOSITION); the consumer grants credit
and `accept`s each message, draining the queue with no loss.

## Prerequisites

- Python 3.10+
- `python-qpid-proton` 0.40.0 (pinned in `examples/python/pyproject.toml`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/python
uv sync
uv run python queues/basic_send_receive/main.py
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 uv run python queues/basic_send_receive/main.py
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

1. **OPEN** — `BlockingConnection(url)` performs the AMQP `OPEN` (SASL ANONYMOUS by
   default). proton sends a non-empty container-id automatically.
2. **ATTACH sender** — `create_sender("queues/<ch>")` attaches a client-sender
   link to the explicit `queues/`-prefixed address. The server grants credit on
   attach.
3. **TRANSFER (produce)** — each `sender.send(Message(...))` is unsettled and
   blocks until the connector returns an `accepted` DISPOSITION, confirming the
   broker stored the message (at-least-once).
4. **ATTACH receiver** — `create_receiver("queues/<ch>", credit=10)` attaches a
   client-receiver link and grants 10 credits. proton replenishes credit as
   deliveries settle.
5. **TRANSFER + DISPOSITION (consume)** — `receiver.receive()` + `receiver.accept()`
   per message ⇒ the connector emits an AckRange and removes the message.
6. **Drain check** — a final `receive(timeout=2.0)` times out, proving the queue
   is empty.
7. **DETACH / CLOSE** — the receiver detaches and the connection closes.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `queues/<ch>` | unsettled (default) | server-granted | `accepted` per send (stored) | none | `Data` | each send blocks for the accepted DISPOSITION |
| receiver (KubeMQ → client) | source `queues/<ch>` | `first` (default) | client-granted `credit=10` | `accepted` ⇒ AckRange (removed) | none | `Data` | proton auto-replenishes credit as deliveries settle |

## Gotchas

> **Queues are move-once (competing-consumer).** An accepted message is removed —
> it is delivered to exactly one consumer. This is the opposite of Events
> (`events/<ch>`), which fan-out a copy to every subscriber. Use the explicit
> `queues/` prefix; never rely on a default pattern.

- **`accept` confirms consumption.** Until you accept (or release/reject) a
  message it stays unsettled and counts against the link's credit window.

## Related Examples

- [queues/ack_release_redelivery](../ack_release_redelivery/) — `accept` vs `release` (redelivery) vs `reject` (discard)
- [queues/settlement_modes](../settlement_modes/) — pre-settled (at-most-once) vs unsettled (at-least-once) producers
- [events/basic_pubsub](../../events/basic_pubsub/) — the fan-out (copy-to-all) counterpart

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
