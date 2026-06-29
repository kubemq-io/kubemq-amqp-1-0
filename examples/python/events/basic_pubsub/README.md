# Python — Events / Basic Pub-Sub

Fan-out, at-most-once pub/sub over KubeMQ **Events** using the native
`python-qpid-proton` blocking client. Events are a fire-hose: deliveries are
pre-settled (no DISPOSITION feedback), there is **no replay**, and an event that
arrives at a subscriber with **zero credit is silently dropped**. The two rules:
**subscribe before publish** and **grant standing credit**.

## Prerequisites

- Python 3.10+
- `python-qpid-proton` 0.40.0 (pinned in `examples/python/pyproject.toml`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/python
uv sync
uv run python events/basic_pubsub/main.py
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 uv run python events/basic_pubsub/main.py
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: events/amqp10.examples.pubsub  (KubeMQ pattern=events, channel=amqp10.examples.pubsub)

[recv] Subscribed to events/amqp10.examples.pubsub with standing credit 100
[recv] Subscription pump settled (waited 750ms before publishing)
[send] Published 20 events (pre-settled, fire-and-forget)
[recv] Received all 20 events (continuous credit ⇒ no 0-credit drop)

Done.
```

## What's Happening

1. **SUBSCRIBE FIRST** — `create_receiver("events/<ch>", credit=100)` attaches the
   subscriber with a large standing credit *before* any publish. Events have no
   replay, so a publish that beats the subscription is lost forever.
2. **Wait for the pump** — the attach reply confirms the link, not that the
   connector's subscription pump is live. The example sleeps ~750ms before
   publishing so the first events do not race the subscription.
3. **PUBLISH pre-settled** — `create_sender(addr, options=AtMostOnce())` marks each
   TRANSFER settled (fire-and-forget); `send` returns without a DISPOSITION.
4. **RECEIVE** — with standing credit the subscriber drains every event. Because
   the fan-out deliveries are **pre-settled**, the example calls
   `accept_if_unsettled(receiver)` (a true no-op here) rather than `receiver.accept()`
   directly — see the gotcha.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events/<ch>` | **settled** (`AtMostOnce`) | server-granted | none (fire-and-forget) | none | `Data` | no DISPOSITION; at-most-once |
| receiver (KubeMQ → client) | source `events/<ch>` | pre-settled by connector | client-granted `credit=100` | none to settle (delivery pre-settled) | none | `Data` | 0-credit ⇒ silent drop; subscribe-before-publish |

## Gotchas

> **An event delivered at 0 credit is SILENTLY DROPPED.** Events are at-most-once
> with no replay. If the subscriber has no outstanding credit when an event
> arrives, the connector drops it and increments
> `kubemq_amqp10_events_dropped_no_credit_total` — it is **never** surfaced as a
> client error. Always grant generous standing credit and let proton replenish.

- **Pre-settled deliveries can't be accepted.** Fan-out events arrive already
  settled, so proton tracks no unsettled delivery — calling `receiver.accept()`
  raises `IndexError`. The example guards with `accept_if_unsettled` (accept only
  when `receiver.fetcher.unsettled` is non-empty). On `events-store/` deliveries
  are unsettled and `accept` works normally.
- **Subscribe before publish.** There is no replay — a publish that races the
  subscription pump is lost. Use `events-store/` if you need replay/durability.

## Related Examples

- [events/consumer_group](../consumer_group/) — load-balance one stream across a group with `x-opt-kubemq-group`
- [events/selector](../selector/) — deliver only events matching a SQL-92 selector
- [events_store/durable_replay](../../events_store/durable_replay/) — persisted, replayable Events Store

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
