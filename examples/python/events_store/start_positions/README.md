# Python — Events Store / Start Positions

The `x-opt-kubemq-start` link property over KubeMQ **Events Store** using the
native `python-qpid-proton` blocking client. Because Events Store **persists**
the stream, a (non-durable) subscriber can choose **where in the history** to
start consuming.

## Prerequisites

- Python 3.10+
- `python-qpid-proton` 0.40.0 (pinned in `examples/python/pyproject.toml`; the
  prebuilt `python-qpid-proton-wheel` installs without a C toolchain)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/python
uv sync
uv run python events_store/start_positions/main.py
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 uv run python events_store/start_positions/main.py
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: events-store/amqp10.examples.startpos.<ts>  (KubeMQ pattern=events-store, channel=amqp10.examples.startpos.<ts>)

[seed] Published 6 events (stored at 1-based sequences 1..6)

[start=first]          replayed full history: ['es-000', 'es-001', 'es-002', 'es-003', 'es-004', 'es-005']
[start=sequence:4]     from the 4th stored event (1-based): ['es-003', 'es-004', 'es-005']
[start=time-delta:3600] last hour (all 6): ['es-000', 'es-001', 'es-002', 'es-003', 'es-004', 'es-005']
[start=new-only]       skipped all 6 existing events; delivered only the post-attach event: [es-new-after-attach]

[gotcha] start='sequence:abc' correctly REJECTED at ATTACH: <amqp:invalid-field detach>
[gotcha] start='whenever' correctly REJECTED at ATTACH: <amqp:invalid-field detach>

Done.
```

## What's Happening

The `x-opt-kubemq-start` value is a **link-attach property** set via a custom
`ReceiverOption` (`receiver.properties = {symbol("x-opt-kubemq-start"): "..."}`).
The connector parses it at ATTACH (`parseEventsStoreStart`, link.go).

The full start-position grammar:

| Form | Meaning |
|---|---|
| `new-only` (or absent) | deliver only events published **after** attach |
| `first` | replay the **entire** history from the beginning |
| `last` | start at the last stored event |
| `sequence:<n>` | start at store sequence `n` (**1-based**; `sequence:1` = the first event) |
| `time:<RFC3339\|unix-secs>` | start at a wall-clock instant (either encoding) |
| `time-delta:<secs>` | start `<secs>` seconds ago, relative to now |

The program seeds 6 events, then opens fresh non-durable receivers at four start
positions against the **same** persisted stream:
- **`first`** → all 6 (`OPEN → BEGIN → ATTACH(x-opt-kubemq-start=first) → FLOW → TRANSFER×6 → DISPOSITION(accept)×6 → DETACH`).
- **`sequence:4`** → the connector passes 4 straight to NATS-streaming
  `StartAtSequence` (1-based), so delivery starts at the 4th stored event:
  `es-003, es-004, es-005`.
- **`time-delta:3600`** → everything from the last hour (all 6).
- **`new-only`** → none of the 6; only an event published *after* the attach.

Finally two malformed values (`sequence:abc`, `whenever`) are rejected at ATTACH
with `amqp:invalid-field` — the connector DETACHes and `create_receiver` raises.

> **Time encoding.** A `time:` value is sent as **RFC3339 or unix seconds**; the
> connector parses both to the same instant and the broker stores the cursor as
> unix **nanoseconds**. `time-delta:` is **seconds** verbatim. There is **no
> native "last N by count"** form — compute a sequence or a time window instead.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events-store/<ch>` | `mixed`/unsettled (default) | server-granted | `accepted` per transfer (persisted at a 1-based sequence) | none | `Data` | seeds the persisted stream |
| receiver (KubeMQ → client) | source `events-store/<ch>` | `first` (default) | client-granted `credit=100` | `accepted` advances the cursor | link prop `x-opt-kubemq-start=<form>` | `Data` | start position chosen at ATTACH; malformed → `amqp:invalid-field` |

## Gotchas

> **A malformed `x-opt-kubemq-start` is a hard ATTACH failure.** Values like
> `sequence:abc` or `whenever` are rejected with **`amqp:invalid-field`** — the
> link never attaches and `create_receiver` raises. Validate start strings before
> attaching.

- **Sequences are 1-based.** `sequence:1` is the first stored event (the value is
  passed straight to NATS-streaming's `StartAtSequence`).
- **No "last N by count".** To read the tail, compute an absolute `sequence:` or a
  `time:` / `time-delta:` window.
- **`x-opt-kubemq-start` is read on the ATTACH frame as a *link* property**, not a
  source/terminus property — set `receiver.properties`, not `source.properties`.

## Related Examples

- [events_store/durable_replay](../durable_replay/) — durable subscriptions that **resume** across reconnects (the start value sets the cursor once on first attach)
- [events/basic_pubsub](../../events/basic_pubsub/) — non-durable Events (no history, no start position)
- [events/consumer_group](../../events/consumer_group/) — `x-opt-kubemq-group` load-balancing (another link property)

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
