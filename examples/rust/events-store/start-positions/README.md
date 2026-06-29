# Rust — Events Store / Start Positions

The `x-opt-kubemq-start` link property over KubeMQ **Events Store** with the native
`fe2o3-amqp` client. Events Store persists the stream, so a (non-durable) subscriber
can choose WHERE in the history to begin. This example seeds 6 events and reads the
same persisted stream from four start positions: `first`, `sequence:4`,
`time-delta:3600`, and `new-only` — then shows a malformed value rejected at ATTACH.

## Prerequisites

- Rust 1.94+ (stable) with `cargo`.
- `fe2o3-amqp` 0.15.1 + `fe2o3-amqp-types` 0.14.0 (pinned exact in the workspace
  `examples/rust/Cargo.toml`).
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/rust
cargo run -p start-positions
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 cargo run -p start-positions
```

A fresh channel name (timestamped) is used per run so the absolute sequence numbers
are deterministic.

## Expected Output

```
Broker:  amqp://localhost:5672
Address: events-store/amqp10.examples.startpos.<ts>  (KubeMQ pattern=events-store, channel=amqp10.examples.startpos.<ts>)

[seed] Published 6 events (stored at 1-based sequences 1..6)

[start=first]           replayed full history: ["es-000", "es-001", "es-002", "es-003", "es-004", "es-005"]
[start=sequence:4]      from the 4th stored event (1-based): ["es-003", "es-004", "es-005"]
[start=time-delta:3600] last hour (all 6): ["es-000", "es-001", "es-002", "es-003", "es-004", "es-005"]
[start=new-only]        skipped all 6 existing events; delivered only the post-attach event: [es-new-after-attach]

[gotcha] start="sequence:abc" correctly REJECTED at ATTACH: IllegalSessionState
[gotcha] start="whenever" correctly REJECTED at ATTACH: IllegalSessionState

Done.
```

## What's Happening

1. **OPEN / BEGIN** — connect (ANONYMOUS) and begin one session.
2. **SEED** — a sender publishes 6 events (unsettled, each confirmed persisted). They
   are stored at 1-based sequences 1..6 (per-channel, monotonic).
3. **ATTACH (start=first)** — a fresh receiver carrying `x-opt-kubemq-start = "first"`
   replays the ENTIRE history (all 6).
4. **ATTACH (start=sequence:4)** — sequences are **1-based** (the connector passes the
   value straight to NATS streaming's `StartAtSequence`; sequence 1 = the first
   event), so the 4th stored event is `es-003`, delivering `es-003, es-004, es-005`.
5. **ATTACH (start=time-delta:3600)** — start one hour ago; since the seed was
   published seconds ago, all 6 arrive. `time-delta` is **seconds verbatim**.
6. **ATTACH (start=new-only)** — skip all existing events; only what is published
   AFTER this attach is delivered. The example attaches, publishes one fresh event,
   and proves only that one arrives.
7. **GOTCHA (malformed)** — a bad start value (`sequence:abc`, `whenever`) is rejected
   at ATTACH with `amqp:invalid-field`; each malformed demo uses its own short-lived
   connection because the connector tears the bad attach (and its session) down.
8. **DETACH / CLOSE** — receivers, session, and connection are torn down cleanly.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events-store/<ch>` | `Unsettled` (explicit) | server-granted | `Accepted` per send (persisted) | none | `Data` | seeds the persisted stream |
| receiver (KubeMQ → client) | source `events-store/<ch>` | `First` (default) | client `CreditMode::Auto(100)` | `accept` advances the read cursor | `x-opt-kubemq-start` = `first` / `sequence:<n>` / `time-delta:<secs>` / `new-only` | `Data` | non-durable; start position per attach |

## Start-position grammar

| Value | Meaning |
|---|---|
| (absent) / `new-only` | deliver only events published AFTER attach |
| `first` | replay the entire history from the beginning |
| `last` | start at the last stored event |
| `sequence:<n>` | start at store sequence n (**1-based**; sequence 1 = the first stored event) |
| `time:<RFC3339\|unix-secs>` | start at a wall-clock instant (RFC3339 or unix-seconds; broker stores nanos) |
| `time-delta:<secs>` | start `<secs>` seconds ago (relative to now) |

## Related Examples

- [events-store/durable-replay](../durable-replay/) — durable subscriptions that RESUME across reconnect
- [events/basic-pubsub](../../events/basic-pubsub/) — Events fan-out (no persistence, no start positions)
- [events/selector](../../events/selector/) — JMS/SQL-92 selectors over Events

## Gotchas

> **Time is sent as RFC3339 or unix seconds; the broker stores nanoseconds.** A
> `time:` value accepts either textual form; `time-delta:` is seconds verbatim. There
> is **no native "last N by count" form** — to read the tail, compute a sequence or a
> time window.

- **Malformed start → `amqp:invalid-field` at ATTACH.** `sequence:abc`,
  `time:not-a-time`, or `whenever` all fail the attach. fe2o3-amqp surfaces the
  connector tearing the link down as an attach error (the text varies with timing;
  the invariant is that the attach returns `Err`).
- **Sequences are 1-based.** `sequence:1` is the first stored event, not the second.
- **Sender settle-mode must be explicit.** The seeder pins `SenderSettleMode::Unsettled`
  (the connector rejects the AMQP default `mixed`).

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT see
`connectivity/auth` + `guides/authentication.md`.
