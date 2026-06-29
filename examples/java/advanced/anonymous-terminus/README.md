# Java — Advanced / Anonymous Terminus — N/A (justified)

> **This variant is `N/A` for Java.** There is **no runnable program** here — only
> this README. The folder exists so the coverage matrix is never silently
> incomplete. Every other language (Go, Python, C#, JS/TS, Rust) ships this
> variant in full.

## What the variant demonstrates

`advanced/anonymous-terminus` shows a single sender with a **null target** (an
*anonymous terminus*) that routes **per-message** by each message's
`properties.to`: one link fans messages out to different KubeMQ
patterns/channels — a queue, then an events topic — without re-attaching. The
connector recognises the null-target ATTACH and routes each delivery on its `to`, rejecting a bad/missing `to` with `amqp:precondition-failed`.

## Why Java is N/A

This is the subtle case: **Qpid JMS *does* support anonymous producers**
`session.createProducer(null)` returns an "unidentified" producer whose
destination is supplied per send via `producer.send(destination, message)`. So at
the JMS API level the pattern looks available.

The blocker is on the wire. An AMQP 1.0 client only emits the **single
null-target anonymous-terminus ATTACH** that the connector routes on when the
peer advertises the **`ANONYMOUS-RELAY`** capability at OPEN. **The KubeMQ
connector advertises no capabilities** — it sets no
offered/desired capabilities (`ContainerID`
/ `MaxFrameSize` / `ChannelMax` / `IdleTimeout` only, no `Properties` /
`OfferedCapabilities` / `DesiredCapabilities`). Without an advertised
`ANONYMOUS-RELAY`, Qpid JMS **falls back to per-destination sender links**: it
lazily attaches (and caches) a *separate* link per distinct destination behind
the unidentified producer, rather than emitting one raw null-target link. It
therefore never exercises the connector's anonymous-terminus path, and JMS
exposes **no API to force a single raw null-target link**.

This is a **client-library + capability-negotiation** interaction (the deliberate
deep-compat / ActiveMQ-migration target of Qpid JMS), not a connector gap for the
other clients. Go, Python, C#, JS/TS, and Rust let you attach a raw null-target
link directly, so they ship this variant fully.

> Note: this is exactly why the request/reply examples
> ([commands](../../commands/request-reply-dynamic-node/) /
> [queries](../../queries/request-reply/)) reply with
> `session.createProducer(null)` addressed per-send to each request's
> `JMSReplyTo`. That works because each reply targets a **concrete** temporary
> queue (so the per-destination-link fallback is the *correct* behaviour there)
> it is not the single null-target relay this variant would need.

## What Java CAN do instead

Use a **per-pattern sender** — a producer bound to a concrete destination, which
is the ordinary, fully-supported Java idiom:

- **[queues/basic-send-receive](../../queues/basic-send-receive/)** (#1) — a
 producer bound to `queues/<ch>` (at-least-once produce + accept drain).
- **[events/basic-pubsub](../../events/basic-pubsub/)** (#4) — a producer bound to
 `events/<ch>` (fire-and-forget fan-out).

To fan out across patterns/channels from Java, attach one producer per
destination (or pass the destination per send via the unidentified producer,
accepting that Qpid JMS attaches a cached link per destination under the hood).
For the genuine single null-target relay, use one of the other-language examples,
or a non-JMS native AMQP 1.0 client (e.g. Qpid Proton-J directly) that lets you
attach a raw null-target link.

## Server follow-up

Advertising the `ANONYMOUS-RELAY` capability in the connector's OPEN reply would
let Qpid JMS (and other capability-driven clients) emit the single null-target
anonymous-terminus ATTACH the connector already routes on — a candidate **server
enhancement**. Until then this cell stays `N/A (justified)`.

## Related Examples

- [queues/basic-send-receive](../../queues/basic-send-receive/) — per-pattern queue producer (the supported alternative, #1)
- [events/basic-pubsub](../../events/basic-pubsub/) — per-pattern events producer (the supported alternative, #4)
- [commands/request-reply-dynamic-node](../../commands/request-reply-dynamic-node/) — `createProducer(null)` addressed per-send to a concrete reply queue
- [advanced/multi-frame-large-payload](../multi-frame-large-payload/) — fragmented body round-trip

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT
see `connectivity/auth` + `guides/authentication.md`.
