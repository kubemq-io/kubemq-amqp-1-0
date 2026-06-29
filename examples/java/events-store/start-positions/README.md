# Java — Events Store / Start Positions — N/A (justified)

> **This variant is `N/A` for Java.** There is **no runnable program** here — only
> this README. The folder exists so the coverage matrix is never silently
> incomplete. Every other language (Go, Python, C#, JS/TS, Rust) ships this
> variant in full.

## Why Java is N/A

The `events-store/start-positions` variant demonstrates the **`x-opt-kubemq-start`
receiver link property**, which selects where in a persisted Events Store stream a
subscriber begins reading:

| `x-opt-kubemq-start` value | Start position |
|---|---|
| (absent) / `new-only` | events published **after** attach (default — no replay) |
| `first` | the beginning of the persisted history (full replay) |
| `last` | the last stored event (tail) |
| `sequence:<n>` | store sequence `n` (1-based) |
| `time:<RFC3339 \| unix-seconds>` | an absolute instant (broker stores Unix **nanos**) |
| `time-delta:<secs>` | `<secs>` seconds before now |

`x-opt-kubemq-start` is an **arbitrary AMQP link (attach) property** the client must
set on the receiver's ATTACH `properties` map. **Apache Qpid JMS exposes no API to
set arbitrary receiver-link properties** — the JMS `MessageConsumer` /
`Topic` / `Connection` surface has no hook for custom attach symbols. The connector
reads the start position only from this property (`connectors/amqp10/link.go`,
`applyPubSubProperties` → `parseEventsStoreStart`), so there is no clean
Qpid-JMS-native way to drive it.

This is a **client-library** limitation of Qpid JMS (the deliberate deep-compat /
ActiveMQ-migration target), **not** a connector gap. Go, Python, C#, JS/TS, and
Rust all expose the ATTACH `properties` map, so they ship this variant fully.

## What Java CAN do instead

Java durable subscribers always start at the JMS default (`new-only`) and then
**resume** from their preserved cursor on re-attach. That covers the most common
need — "start fresh, never miss anything after that" — natively:

- **[events-store/durable-replay](../durable-replay/)** — the supported Java
  variant: `connection.setClientID(id)` +
  `session.createDurableConsumer(topic, subName)` establishes a durable
  subscription (`new-only`), and a re-attach with the same `(clientID, subName)`
  resumes exactly where it left off (no loss, no re-replay).

For explicit historical start positions (`first` / `sequence:<n>` /
`time:<...>`), use one of the other-language examples, or a non-JMS native AMQP
1.0 client (e.g. Qpid Proton-J directly) that lets you set the ATTACH source
`properties` map.

## Server follow-up

A connector-side JMS-friendly alias for start positions (e.g. deriving the start
from a JMS property or a `?start=` destination option) is a candidate **server
enhancement** so Qpid JMS clients gain a native surface. Until then this cell
stays `N/A (justified)`.

## Related Examples

- [events-store/durable-replay](../durable-replay/) — the supported Java durable path (`new-only` + resume)
- [events/basic-pubsub](../../events/basic-pubsub/) — non-durable, at-most-once Events (no replay at all)
- [events/selector](../../events/selector/) — JMS-native selectors (also work on `events-store/`)

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT
see `connectivity/auth` + `guides/authentication.md`.
