# Python — Events / Consumer Group

Consumer-group load-balancing over KubeMQ **Events** using the native
`python-qpid-proton` blocking client. The `x-opt-kubemq-group` receiver link
property places a subscriber in a named group: within **one** group the connector
round-robins the stream across members (no duplication); a **distinct** group is
an independent subscriber that gets the **full** stream.

This example opens two receivers in group `g1` (which together receive every event,
split) and one receiver in group `g2` (which receives every event).

## Prerequisites

- Python 3.10+
- `python-qpid-proton` 0.40.0 (pinned in `examples/python/pyproject.toml`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/python
uv sync
uv run python events/consumer_group/main.py
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 uv run python events/consumer_group/main.py
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: events/amqp10.examples.consumergroup  (KubeMQ pattern=events, channel=amqp10.examples.consumergroup)

[recv] 3 subscribers attached: g1a+g1b (group g1), g2 (group g2)
[send] Published 30 events (pre-settled)
[recv] g2 (group g2, independent): 30/30 events -- FULL stream
[recv] g1a (group g1): 16 events; g1b (group g1): 14 events
[recv] g1a+g1b together: 30/30 events, 0 duplicates -- group SPLIT the stream

Done.
```

(The g1a/g1b split varies run to run; the totals are fixed.)

## What's Happening

1. **Three subscribers, three connections** — the blocking client is
   single-threaded per connection, so each subscriber runs on its **own**
   `BlockingConnection` in its **own** thread (one session/receiver per thread).
2. **Stamp the group link property** — a custom `ReceiverOption` (`ConsumerGroup`)
   sets `receiver.properties = {symbol("x-opt-kubemq-group"): "<group>"}` on the
   ATTACH frame. The connector reads it in `applyPubSubProperties`.
3. **Subscribe before publish** — the main thread waits for all three links to
   attach, then sleeps ~750ms for the subscription pumps to go live (no replay on
   Events).
4. **PUBLISH pre-settled** — 30 events on `events/<ch>` (`AtMostOnce`).
5. **Assert the semantics** — `g2` (distinct group) receives all 30; `g1a` + `g1b`
   together receive all 30 with **zero** overlap (the group split the stream).
   Fan-out deliveries are pre-settled, so the example uses `accept_if_unsettled`.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events/<ch>` | **settled** (`AtMostOnce`) | server-granted | none (fire-and-forget) | none | `Data` | one shared producer connection |
| receiver g1a / g1b (KubeMQ → client) | source `events/<ch>` | pre-settled by connector | client-granted `credit=100` | none to settle | link prop `x-opt-kubemq-group=g1` | `Data` | members of g1 SPLIT the stream (round-robin) |
| receiver g2 (KubeMQ → client) | source `events/<ch>` | pre-settled by connector | client-granted `credit=100` | none to settle | link prop `x-opt-kubemq-group=g2` | `Data` | distinct group ⇒ independent FULL stream |

## Gotchas

> **A 0-credit member misses its share — silently.** Events are at-most-once with
> no replay (see [events/basic_pubsub](../basic_pubsub/)). Within a group, an event
> routed to a member that is at 0 credit is dropped, not re-routed to a sibling.
> Grant generous standing credit to every group member.

- **Group membership is a link property, set at attach.** `x-opt-kubemq-group` is
  honoured on the ATTACH frame; you cannot change a receiver's group after attach —
  detach and re-attach with a new value.
- **One connection per subscriber.** proton's blocking API is single-threaded per
  connection; never share a connection/session across threads.

## Related Examples

- [events/basic_pubsub](../basic_pubsub/) — single-subscriber fan-out (the 0-credit drop rule)
- [events/selector](../selector/) — content filtering with SQL-92 selectors
- [events_store/durable_replay](../../events_store/durable_replay/) — durable, replayable subscriptions

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
