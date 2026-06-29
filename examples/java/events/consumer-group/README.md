# Java — Events / Consumer Group — N/A (justified)

> **This variant is `N/A` for Java.** There is **no runnable program** here — only
> this README. The folder exists so the coverage matrix is never silently
> incomplete. Every other language (Go, Python, C#, JS/TS, Rust) ships this
> variant in full.

## What the variant demonstrates

`events/consumer-group` shows **load-balancing** across the members of a consumer
group: two receivers in group `g1` **split** the event stream (each event to one
member, no duplicate within the group), while an independent receiver in group
`g2` gets the **full** stream. The connector reads the group name from the
**`x-opt-kubemq-group`** receiver link property; within a group the
stream is load-balanced, and different groups each get the full stream.

## Why Java is N/A

There are **two independent connector gaps** that, together, leave Qpid JMS with
no path to consumer groups today:

1. **No arbitrary link property.** The native idiom every other language uses is
 to set the **`x-opt-kubemq-group`** symbol on the receiver's ATTACH
 `properties` map. **Apache Qpid JMS exposes no API to set arbitrary
 receiver-link properties** — the JMS `MessageConsumer` / `Topic` / `Connection`
 surface has no hook for custom attach symbols (this is the same client-library
 limit that makes `events-store/start-positions` and `advanced/anonymous-terminus`
 N/A for Java). So Qpid JMS cannot drive the connector's `x-opt-kubemq-group`
 path directly.

2. **No `SHARED-SUBS` advertisement.** The JMS-native fallback would be a **JMS
 2.0 shared subscription** — `createSharedConsumer` / `createSharedDurableConsumer`
 — which Qpid JMS implements by negotiating the **`SHARED-SUBS`** capability at
 link attach. **The KubeMQ connector advertises no capabilities** at OPEN/ATTACH (it sets no offered/desired caps), so it never advertises `SHARED-SUBS`.
 Verified live: calling `createSharedDurableConsumer` against the connector
 throws **`Remote peer does not support shared subscriptions`** — Qpid JMS
 refuses to even emit the ATTACH.

With (1) blocking the `x-opt-kubemq-group` property and (2) blocking the JMS
shared-subscription fallback, there is **no Qpid-JMS-native way** to demonstrate
consumer groups against the connector as it stands today.

This is a **current connector limitation** for the JMS deep-compat /
ActiveMQ-migration target, **not** a Java defect. Go, Python, C#, JS/TS, and Rust
all expose the ATTACH `properties` map, so they set `x-opt-kubemq-group` directly
and ship this variant fully.

## What Java CAN do instead

A plain (ungrouped) Java subscriber is a normal fan-out subscriber — the KubeMQ
default — and is fully supported:

- **[events/basic-pubsub](../basic-pubsub/)** (#4) — `createConsumer(topic)` on
 `events/<ch>`: a single subscriber receives the full fan-out stream
 (at-most-once, subscribe-before-publish).
- **[events-store/durable-replay](../../events-store/durable-replay/)** (#7) — a
 durable subscriber that resumes its own cursor on re-attach (the supported Java
 durable path).

For load-balanced consumer groups from Java, use one of the other-language
examples, or a non-JMS native AMQP 1.0 client (e.g. Qpid Proton-J directly) that
lets you set the `x-opt-kubemq-group` property on the receiver's ATTACH
`properties` map.

## Server follow-up

The `SHARED-SUBS` route is **architecturally blocked, not a quick capability flip.**
JMS shared subscriptions assume *many concurrent links share one subscription
identity*, but KubeMQ Events-Store durables are **single-claim** (one active link
owns a durable at a time). The connector therefore cannot honestly advertise
`SHARED-SUBS` by simply setting the offered cap — the underlying durable model does
not support the multi-link-per-subscription semantics Qpid JMS would then expect.
This is why `SHARED-SUBS` is **not fixed** even in the JMS-compat connector builds.

The realistic unblock is a **JMS-friendly alias** for the group — e.g. deriving the
group from a JMS property or a `?group=` destination option — since Qpid JMS cannot
set the raw `x-opt-kubemq-group` link property and cannot drive shared subscriptions
against single-claim durables. That is a candidate **server enhancement**; until it
lands, this cell stays `N/A (justified)`.

## Related Examples

- [events/basic-pubsub](../basic-pubsub/) — single-subscriber fan-out (the supported alternative, #4)
- [events/selector](../selector/) — JMS-native SQL-92 selectors on `events/` (#6)
- [events-store/durable-replay](../../events-store/durable-replay/) — durable subscriptions with replay/resume (#7)

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT
see `connectivity/auth` + `guides/authentication.md`.
