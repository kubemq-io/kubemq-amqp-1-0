# JavaScript — Events Store / Start Positions

The `x-opt-kubemq-start` receiver link property over KubeMQ **Events Store** with
the native `rhea` / `rhea-promise` client. Events Store persists the stream, so a
(non-durable) subscriber can choose **where in the history to start consuming**.
This example seeds 6 events, then opens fresh receivers at four different start
positions against the same persisted stream and asserts exactly what each one
delivers.

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
npx tsx events-store/start-positions/index.ts
```

Override the broker URL (SASL **ANONYMOUS** by default):

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 npx tsx events-store/start-positions/index.ts
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: events-store/amqp10.examples.startpos.<ts>  (KubeMQ pattern=events-store, channel=amqp10.examples.startpos.<ts>)

[seed] Published 6 events (stored at 1-based sequences 1..6)

[start=first]           replayed full history: [es-000 es-001 es-002 es-003 es-004 es-005]
[start=sequence:4]      from the 4th stored event (1-based): [es-003 es-004 es-005]
[start=time-delta:3600] last hour (all 6): [es-000 es-001 es-002 es-003 es-004 es-005]
[start=new-only]        skipped all 6 existing events; delivered only the post-attach event: [es-new-after-attach]

[gotcha] start="sequence:abc" correctly REJECTED at ATTACH (amqp:invalid-field)
[gotcha] start="whenever" correctly REJECTED at ATTACH (amqp:invalid-field)

Done.
```

> The channel name carries a timestamp suffix so each run gets a clean stream with
> deterministic sequence numbers (1..6).
>
> The connector DETACHes the rejected attach with `amqp:invalid-field` and a
> description naming the bad value (`invalid start sequence: abc`,
> `unknown start position: whenever`). rhea v3 surfaces a detach that races link
> registration as a raw connection-level error; this example swallows it and
> detects the rejection via the structured `connection_error` event — either way
> the receiver never attaches, which is the contract.

## What's Happening

1. **OPEN / BEGIN** — `connection.open()` (SASL ANONYMOUS by default), then links
   create their own sessions.
2. **Seed** — publish 6 events to `events-store/<ch>` with an `AwaitableSender`;
   they persist at sequences `1..6` (per-channel, **1-based**, monotonic).
3. **`start=first`** — a fresh receiver carrying
   `properties:{"x-opt-kubemq-start":"first"}` **replays the full history** (all 6).
4. **`start=sequence:4`** — starts at the **4th** stored event (sequences are
   1-based), delivering `es-003`, `es-004`, `es-005`.
5. **`start=time-delta:3600`** — starts 3600 seconds (1 hour) ago; since the seed
   was published seconds earlier, all 6 are delivered. `time-delta` is **seconds
   verbatim**.
6. **`start=new-only`** — skips all existing history and delivers only events
   published **after** the attach; the program publishes one fresh event and proves
   only that one arrives.
7. **Gotcha** — two malformed start values are rejected at ATTACH with
   `amqp:invalid-field` (the receiver never attaches).
8. **DETACH / CLOSE** — each receiver detaches; the connection closes.

## `x-opt-kubemq-start` grammar (full)

| Value | Start position | Notes |
|---|---|---|
| (absent) / `new-only` | events published **after** attach | default — no replay |
| `first` | the **beginning** of the persisted history | full replay |
| `last` | the **last** stored event | tail |
| `sequence:<n>` | store sequence `n` (**1-based**: sequence 1 = the first stored event; passed straight to NATS streaming `StartAtSequence`) | `n < 0` or non-integer → `amqp:invalid-field` |
| `time:<RFC3339 \| unix-seconds>` | absolute wall-clock instant | **sent as RFC3339 or unix-seconds**; both resolve to the same instant — see note below |
| `time-delta:<secs>` | `<secs>` seconds **before now** | seconds **verbatim**; non-integer/negative → `amqp:invalid-field` |

> **Time encoding (load-bearing).** The client sends a `time:` value as an
> **RFC3339** string *or* as **unix SECONDS**. The connector parses BOTH to the
> same instant and the **broker stores the cursor as unix NANOSECONDS**
> (`time.Unix(0, value)`), so RFC3339 and unix-seconds for the same instant yield
> an identical stored cursor — never the raw seconds. `time-delta:` is the one form
> that stays in **seconds** (relative). Examples: `time:2021-06-13T10:00:00Z`,
> `time:1623578400`, `time-delta:90`. In JS, build the RFC3339 string with
> `new Date(...).toISOString()`.

> **No native "last N by count".** There is no `last:N` form. To read the tail by
> count, compute a `sequence:<n>` (current tail minus N) or a `time:` /
> `time-delta:` window instead.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events-store/<ch>` | unsettled (default) | server-granted | `accepted` per transfer (persisted) | none | `Data` | each accepted transfer is durably stored at a monotonic sequence |
| receiver (KubeMQ → client) | source `events-store/<ch>` | `first` (default) | client-granted standing credit, auto-replenished | `accept()` advances the read cursor | `properties{x-opt-kubemq-start: <position>}` | `Data` | start position chooses the read offset; malformed → `amqp:invalid-field` at ATTACH |

## Related Examples

- [events-store/durable-replay](../durable-replay/) — durable subscriptions that resume across reconnect (cursor persisted)
- [events/basic-pubsub](../../events/basic-pubsub/) — non-durable, at-most-once Events (no replay)
- [events/selector](../../events/selector/) — SQL-92 selectors (also supported on `events-store/`)

## Gotchas

> **A malformed `x-opt-kubemq-start` is rejected at ATTACH with
> `amqp:invalid-field`.** `sequence:abc`, `sequence:-1`, `time-delta:nope`,
> `time:not-a-time`, and any unknown token (`whenever`) DETACH the link with a
> description naming the offending value — the receiver never attaches. Validate
> the start value client-side before attaching.
>
> **rhea surfaces the rejection as a connection-level `error`.** When the
> connector DETACHes the bad attach, rhea v3 raises a raw, connection-level `error`
> event (the detach races link registration) rather than rejecting `createReceiver`
> promptly — and an unhandled `error` event would crash Node. This example attaches
> a no-op `Connection` `error` listener to swallow it, and detects the rejection
> deterministically via the structured `connection_error` event.
>
> **Time is sent as RFC3339 or seconds; the broker stores nanoseconds.** Do not
> send nanoseconds in `time:` — send RFC3339 or unix-seconds. `time-delta:` is
> seconds.
>
> **Sequences are per-channel, 1-based, and monotonic.** Sequence 1 is the first
> stored event. `sequence:<n>` is an absolute position in *that* channel's store,
> not a relative offset.
>
> **Start positions are for `events-store/` only.** Plain Events (`events/<ch>`)
> has no replay; `x-opt-kubemq-start` is parsed but ignored there (and on RPC).
>
> **0-credit ⇒ silent drop still applies on the delivery path.** Keep continuous
> standing credit so a replayed event is never dropped at 0 credit.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
