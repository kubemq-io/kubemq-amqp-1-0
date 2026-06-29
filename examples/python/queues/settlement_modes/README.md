# Python — Queues / Settlement Modes

The two producer reliability tiers, side by side, over KubeMQ **Queues** using the
native `python-qpid-proton` blocking client:

- **Pre-settled** (`AtMostOnce` → snd-settle-mode=settled): **at-most-once**. Each
  `send` is marked settled locally and returns without waiting for a server
  DISPOSITION. Fast and fire-and-forget — a drop on the way in is invisible to the
  producer.
- **Unsettled** (default): **at-least-once**. Each `send` blocks until the broker
  returns an `accepted` DISPOSITION confirming storage.

The consumer uses the default **rcv-settle-mode=first** — the only receiver
settle-mode the connector supports.

## Prerequisites

- Python 3.10+
- `python-qpid-proton` 0.40.0 (pinned in `examples/python/pyproject.toml`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/python
uv sync
uv run python queues/settlement_modes/main.py
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 uv run python queues/settlement_modes/main.py
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: queues/amqp10.examples.settlement

[send] Pre-settled (at-most-once): produced 10 messages -- NO DISPOSITION awaited
[send] Unsettled (at-least-once): produced 10 messages -- each accepted DISPOSITION
[recv] Drained 20 total -- 10 pre-settled + 10 unsettled (rcv-settle-mode=first)
[recv] Queue drained to empty (no further messages)

Done.
```

## What's Happening

1. **Pre-settled sender** — `create_sender(addr, options=AtMostOnce())` sets
   snd-settle-mode=settled. proton settles each delivery locally, so `send` returns
   as soon as the frame is written — no server DISPOSITION is awaited (at-most-once).
2. **Unsettled sender** — `create_sender(addr)` (default) is at-least-once: each
   `send` blocks for the `accepted` DISPOSITION confirming storage.
3. **Consume with rcv-settle-mode=first** — `create_receiver(addr, credit=20)`. The
   server settles each delivery on the first transfer. Both senders' messages drain
   to the same consumer.
4. **Drain check** — a final `receive(timeout=2.0)` times out, proving the queue is
   empty. (On a healthy broker both tiers drain; the difference is the *producer*
   guarantee, not the happy-path result.)

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| pre-settled sender (client → KubeMQ) | target `queues/<ch>` | **settled** (`AtMostOnce`) | server-granted | none (delivery settled by client) | none | `Data` | `send` returns before any broker confirmation |
| unsettled sender (client → KubeMQ) | target `queues/<ch>` | unsettled (default) | server-granted | `accepted` per send | none | `Data` | `send` blocks for the accepted DISPOSITION |
| receiver (KubeMQ → client) | source `queues/<ch>` | **`first`** (only mode supported) | client-granted `credit=20` | `accepted` ⇒ AckRange | none | `Data` | rcv-settle-mode=second ⇒ DETACH `amqp:not-implemented` |

## Gotchas

> **rcv-settle-mode=second is rejected with `amqp:not-implemented`.** The connector
> supports only **rcv-settle-mode=first** (the server settles on the first
> transfer). Requesting `second` (settle-on-disposition) DETACHes the link with
> `amqp:not-implemented`. proton uses `first` by default, so the happy path here is
> unaffected — but do not force `second`.

- **Pre-settled = silent loss on the producer side.** A pre-settled `send` returns
  before the broker confirms storage. If the broker drops the transfer (oversize,
  no capacity), the producer never learns. Use unsettled for at-least-once
  durability; reserve pre-settled for high-rate, loss-tolerant streams.

## Related Examples

- [queues/basic_send_receive](../basic_send_receive/) — the unsettled at-least-once baseline
- [queues/ack_release_redelivery](../ack_release_redelivery/) — consumer-side outcomes (accept/release/reject)
- [events/basic_pubsub](../../events/basic_pubsub/) — Events are always pre-settled (at-most-once) fan-out

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
