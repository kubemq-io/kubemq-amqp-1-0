# C# — Events-Store / Start Positions

The `x-opt-kubemq-start` receiver link property over KubeMQ **Events Store** using
the native `AMQPNetLite.Core` client. Events Store persists the stream, so a
(non-durable) subscriber can choose **where in the history** to begin. This example
seeds 6 events and reads the SAME persisted stream from four different start
positions, then shows a malformed start value rejected at ATTACH.

## Prerequisites

- .NET SDK **8.0**
- `AMQPNetLite.Core` **2.5.3** (pinned in `examples/csharp/Directory.Packages.props`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd events-store/start-positions
dotnet run
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 dotnet run
```

Runs ANONYMOUS by default (no userinfo in the URL).

## Expected Output

```
Broker:  amqp://localhost:5672
Address: events-store/amqp10.examples.startpos.<ts>  (KubeMQ pattern=events-store, channel=amqp10.examples.startpos.<ts>)

[seed] Published 6 events (stored at 1-based sequences 1..6)

[start=first]           replayed full history: [es-000 es-001 es-002 es-003 es-004 es-005]
[start=sequence:4]      from the 4th stored event (1-based): [es-003 es-004 es-005]
[start=time-delta:3600] last hour (all 6): [es-000 es-001 es-002 es-003 es-004 es-005]
[start=new-only]        skipped all 6 existing events; delivered only the post-attach event: [es-new-after-attach]

[gotcha] start="sequence:abc" correctly REJECTED at ATTACH: amqp:not-found (link detached by broker)
[gotcha] start="whenever" correctly REJECTED at ATTACH: amqp:not-found (link detached by broker)

Done.
```

## What's Happening

1. **OPEN / BEGIN** — connect, then seed 6 events to `events-store/<ch>` over a
   sender (unsettled — each accepted transfer is persisted at 1-based sequences 1..6).
2. **ATTACH (receiver) with a start property** — each read attaches a fresh
   receiver whose `Attach.Properties` map carries
   `x-opt-kubemq-start -> "<position>"`. The connector parses the value, sets the
   stream cursor, and replays from there. A brief settle delay lets the
   subscription pump go live before the replay begins.
3. **The grammar** (`parseEventsStoreStart`):
   - `(absent)` / `new-only` — only events published **after** attach
   - `first` — replay the **entire** history from the beginning
   - `last` — start at the last stored event
   - `sequence:<n>` — start at store sequence **n** (1-based; sequence 1 = first event)
   - `time:<RFC3339|unix-secs>` — start at a wall-clock instant
   - `time-delta:<secs>` — start `<secs>` seconds ago
4. **Time encoding** — a `time:` value is sent as **RFC3339 or unix seconds**; the
   connector parses both to the same instant and the broker stores the cursor as
   unix **nanoseconds**. `time-delta:` is seconds verbatim. There is **no native
   "last N by count"** form — compute a sequence or a time window for the tail.
5. **new-only proof** — after attaching new-only and a short settle, one fresh event
   is published; only that post-attach event is delivered (the 6 existing events are
   skipped).
6. **Malformed reject** — a bad value (`sequence:abc`, `whenever`) is rejected at
   ATTACH with `amqp:invalid-field`; AMQPNetLite surfaces the racing detach as an
   `amqp:not-found` handle error. Each malformed probe runs on its **own
   connection** because the detach can tear the whole connection.
7. **DETACH / CLOSE** — each receiver detaches; connections close.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events-store/<ch>` | unsettled (default) | server-granted | `accepted` per send (persisted) | none | `Data` | seeds 6 events at sequences 1..6 |
| receiver (KubeMQ → client) | source `events-store/<ch>` | `first` (default) | client-granted `SetCredit(100)` | `Accept` ⇒ advances cursor | link: `Attach.Properties{x-opt-kubemq-start: first \| sequence:4 \| time-delta:3600 \| new-only}` | `Data` | start cursor honoured at attach; malformed ⇒ `amqp:invalid-field` |

## Related Examples

- [events-store/durable-replay](../durable-replay/) — durable `(container-id, link name)` resume (uses `new-only`)
- [events/basic-pubsub](../../events/basic-pubsub/) — fire-and-forget Events (no replay, contrast)
- [events/selector](../../events/selector/) — SQL-92 selector filter (another consume-link property)

## Gotcha

> **Sequences are 1-BASED and there is no "last N by count".** `sequence:1` is the
> FIRST stored event (the connector passes the value straight to NATS streaming's
> `StartAtSequence`). To read the tail of a stream, compute an absolute sequence or
> use a `time:` / `time-delta:` window — the connector exposes no count-from-end
> form. A malformed start value (`sequence:abc`, `time:not-a-time`, `whenever`) is
> rejected at ATTACH with `amqp:invalid-field`; AMQPNetLite races the connector's
> detach against link registration and reports it as an `amqp:not-found` handle
> error, so the receiver simply never attaches.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT
see `connectivity/auth` + `guides/authentication.md`.
