# Architecture

## Overview

The KubeMQ **AMQP 1.0 connector** is an embedded, wire-protocol bridge inside
. It speaks the AMQP 1.0 dialect on plain port **5672** (TCP /
SASL) and TLS port **5671**, and it is **enabled by default**. Any standard
AMQP 1.0 client connects to it with **only a connection-string change** — no
code rewrite, no library swap, no KubeMQ SDK.

Unlike the AMQP 0-9-1 (RabbitMQ) connector — which bridges onto exactly one
KubeMQ primitive (the Queue) — the AMQP 1.0 connector bridges onto **all five**
KubeMQ patterns: **Queues, Events, Events-Store, Commands, and Queries.** The
leading segment of the node address selects which pattern a link is bound to.
This single fact drives the entire mental model and the way this repo is
organized.

> AMQP 1.0 is a peer-to-peer link protocol — there are **no exchanges, no
> bindings, no routing keys, and no publisher-confirms** here. Those are AMQP
> 0-9-1 concepts. In AMQP 1.0 a *link* is attached to a *node* (an address), and
> message flow is governed by *credit* and resolved by *delivery state*
> (accepted / released / modified / rejected). If you are migrating from 0-9-1,
> read [reference/migration-from-activemq.md](reference/migration-from-activemq.md).

## The shared front door: `amqpmux`

KubeMQ ships two embedded AMQP dialects — 0-9-1 (RabbitMQ) and 1.0 — and they
**share the same listeners**. A single mux per `(port, tlsPort)` group accepts
every connection, reads the **8-byte AMQP protocol header**, and dispatches the
raw connection to the engine that speaks the matching dialect. The mux never
speaks AMQP itself; once it classifies a connection it hands the `net.Conn` plus
the consumed header to the engine, which resumes the protocol exactly where the
header left off.

```
client amqpmux (shared listener) engine
 │ TCP connect :5672 / :5671 │ │
 │ ───────────────────────────► │ │
 │ 8-byte header │ read 8 bytes, classify: │
 │ "AMQP" + (id,maj,min,rev) │ │
 │ ───────────────────────────► │ AMQP\x00\x00\x09\x01 → 0-9-1 ─────┼──► AMQP 0-9-1 engine
 │ │ AMQP\x00\x01\x00\x00 → 1.0 bare ──┤
 │ │ AMQP\x03\x01\x00\x00 → 1.0 SASL ──┼──► AMQP 1.0 engine
 │ │ AMQP\x02\x01\x00\x00 → 1.0 TLS ───┘ (TLS listener only)
 │ │
 │ (no live engine for header) │ write one preferred-version header
 │ ◄─────────────────────────── │ and close (version negotiation)
```

The 8-byte header is `"AMQP"` followed by a 4-byte
`(protocol-id, major, minor, revision)` tuple. The mux recognizes:

| Header bytes | Meaning | Listener | Dispatched to |
|--------------|---------|----------|---------------|
| `AMQP\x00\x00\x09\x01` | AMQP 0-9-1 | any | 0-9-1 engine |
| `AMQP\x00\x01\x00\x00` | AMQP 1.0 (bare) | any | 1.0 engine |
| `AMQP\x03\x01\x00\x00` | AMQP 1.0 (SASL layer) | any | 1.0 engine |
| `AMQP\x02\x01\x00\x00` | AMQP 1.0 (TLS token) | **TLS only** | 1.0 engine (after TLS termination) |

Key consequences:

- **Plain port `5672` and TLS port `5671` are shared with the 0-9-1 connector.**
 Setting `CONNECTORS_AMQP10_PORT` equal to the 0-9-1 port is *intentionally
 accepted* — the mux dedupes the bind, so the two dialects coexist on one
 listener. The TLS listener at `5671` is likewise shared.
- **Version negotiation:** if a client presents an AMQP 1.0-family header but no
 live 1.0 engine is available, the mux writes back the 0-9-1 header and closes
 (and vice-versa). A client that cannot even send a header gets nothing back
 the connection is closed silently.
- **The connector advertises no negotiated capabilities** (see
 [Capabilities](#what-the-server-advertises) below).

## Connection → Session → Link

AMQP 1.0 is a three-level container model. The connector implements the
**server side** of each finite-state machine, so the peer roles invert relative
to your client:

```
Connection (one TCP connection, one OPEN)
│ container-id REQUIRED, non-empty, sanitized [a-zA-Z0-9_-], ≤256
│ server advertises: max-frame-size, channel-max=min(client, SessionMax-1)=255,
│ idle-time-out = IdleTimeoutSeconds*1000/2 ms
│
├── Session (BEGIN; up to channel-max+1 concurrent)
│ │ incoming/outgoing windows; window breach → amqp:session:window-violation
│ │
│ ├── Link: client SENDER → server RECEIVER (you produce)
│ │ target = <pattern>/<channel>
│ │ unsettled (at-least-once) by default; settled (at-most-once) opt-in
│ │
│ └── Link: client RECEIVER → server SENDER (you consume)
│ source = <pattern>/<channel>
│ you grant link credit via FLOW; rcv-settle-mode = first (only)
```

| Level | Client performative | Connector behavior |
|-------|---------------------|--------------------|
| **Connection** | `OPEN` | `container-id` is required and non-empty (empty → `amqp:invalid-field`), sanitized to `[a-zA-Z0-9_-]`, capped at 256 chars. It becomes the ClientID when there is no SASL identity, and it is **half the durable-subscription identity — so it must be stable across reconnects** for durable subscribers. The OPEN `hostname` (vhost) is accepted but **ignored**. |
| **Session** | `BEGIN` | The server advertises `channel-max = min(client, SessionMax-1)` (255 with defaults); session window violations close with `amqp:session:window-violation`; an unattached handle or handle-in-use error is `amqp:session:errant-link`. |
| **Link** | `ATTACH` | The peer role inverts: a client **sender** is a server **receiver** (you produce to a `target`), and a client **receiver** is a server **sender** (you consume from a `source`). The address resolves to a `(pattern, channel)` pair (below). Receivers grant credit; the server never delivers without it. |

## Mapping AMQP 1.0 onto the five KubeMQ patterns

The connector binds a link to a KubeMQ pattern by the **leading segment of the
node address**. The address grammar is `[/]<pattern>/<channel>`:

| Address prefix | KubeMQ pattern | Produce (client sender → target) | Consume (client receiver ← source) |
|----------------|----------------|----------------------------------|------------------------------------|
| `queues/<ch>` | **Queues** | at-least-once enqueue; server DISPOSITIONs `accepted` per send | credit-driven destructive consume; `accept`/`release`/`modify`/`reject` |
| `events/<ch>` | **Events** | pre-settled fan-out (at-most-once) | standing-credit fan-out; **0-credit → silent drop** |
| `events-store/<ch>` | **Events-Store** | persisted append | durable replay/resume; start positions via `x-opt-kubemq-start` |
| `commands/<ch>` | **Commands (RPC)** | request + dynamic reply node; reply carries `x-opt-kubemq-executed`/`-error` | responder consumes, replies via anonymous sender to `/responses/<RequestID>` |
| `queries/<ch>` | **Queries (RPC)** | request + dynamic reply node; reply = body + metadata only | responder consumes, replies via anonymous sender to `/responses/<RequestID>` |

Resolution rules (see [reference/address-mapping.md](reference/address-mapping.md)):

- **Longest-prefix wins.** `events-store/` is matched before `events/`, so
 `events-store/orders` never collides with the `events/` arm.
- **Leading slash is optional.** At most one leading `/` is stripped before
 matching.
- **Bare addresses** (no recognized prefix) resolve by a JMS node-capability
 hint (`queue` → `queues`, `topic` → `events`) or fall back to the configured
 `DefaultPattern` (`queues` by default). Examples in this repo **always emit the
 explicit prefix** and never rely on this.
- **`/responses/<RequestID>`** is the write-only RPC reply path. A receiver
 attach on it is refused with `amqp:not-allowed`; resolution returns the opaque
 `RequestID` as the channel.
- **Channel charset** is stricter than the array layer: non-empty, ≤255 chars,
 no trailing `.`, no whitespace, no `*` `>` `;` `:`. Violation → `amqp:not-found`.

### Cross-protocol interoperability

Because every pattern is backed by a normal KubeMQ channel, a message sent over
AMQP 1.0 to `queues/orders` is consumable by a gRPC/REST queue client on the same
channel, and vice-versa. The 0-9-1 connector uses a different namespace
(`amqp.<vhost>.<queue>`), so to reach 0-9-1 queue data from AMQP 1.0 you address
`queues/amqp.<vhost>.<queue>` explicitly — the two AMQP connectors do **not**
share an address namespace.

## What the server advertises

On `OPEN` the connector sends back **only** `container-id` (`"KubeMQ"`),
`max-frame-size`, `channel-max`, and `idle-time-out`. It sets **no
offered/desired connection or link capabilities** — in particular **no
`ANONYMOUS-RELAY`** and no `queue`/`topic` node capabilities. Clients must not
depend on capability negotiation.

This is why the **anonymous terminus** (a sender with a null target that routes
per-message by `properties.to`) is driven entirely by the *null target address*,
not by an advertised `ANONYMOUS-RELAY` capability. It is also why Apache Qpid JMS
cannot drive the anonymous-terminus example (it has no API to force a raw
null-target link, and there is no capability to trigger its anonymous-producer
path) — see the Java N/A note in [../examples/README.md](../examples/README.md).

## The metadata envelope and type markers

KubeMQ messages carry a JSON `Metadata` string. The connector serializes the
full AMQP message context into a single canonical envelope keyed by **`amqp10`**:

```json
{
 "amqp10": {
 "props": { "...": "original-form AMQP properties (message_id, correlation_id, to, reply_to, subject, content_type, group_id, ttl, ...)" },
 "app": { "...": "application-properties, type-preserved" },
 "annotations": { "x-opt-...": "message-annotations" },
 "delivery_annotations": { "...": "opaque pass-through" },
 "footer": { "...": "opaque pass-through" },
 "body_section": "data"
 }
}
```

- The envelope is **always present** — even `{"amqp10":{}}` for an
 empty/property-less message — so every message carries non-empty `Metadata`.
- `body_section` discriminates the body: **`"data"`** (binary `Data` section) or
 **`"value"`** (`AmqpValue` section). An `AmqpSequence` body is **rejected** with
 `amqp:not-implemented`.
- Treat the envelope as **opaque** from a client's point of view. Set standard
 AMQP properties natively (message-id, correlation-id, content-type, ttl,
 group-id) and let the connector derive the `amqp10.*` tags; do not hand-build
 the envelope.

### Type markers

JSON has no native unsigned/64-bit/binary/timestamp types, so the codec wraps
AMQP scalar values that would otherwise lose fidelity using a `$`-prefixed
marker. The set:

| Marker | AMQP type → JSON form |
|--------|------------------------|
| `$int64` | int64 beyond ±2^53 → string |
| `$u64` | uint64 → string (JSON has no unsigned long) |
| `$ts` | timestamp → Unix **seconds** |
| `$bin` | binary → base64 |
| `$uuid` | UUID → RFC-4122 string |
| `$u8` / `$i8` / `$i16` / `$u16` / `$i32` / `$u32` | sized integers |
| `$f32` / `$f64` | 32- / 64-bit floats |

Integers within ±2^53 are emitted as plain JSON numbers; only out-of-range or
type-ambiguous values are wrapped. Egress restores the exact AMQP type the client
sent (e.g. a ulong message-id stays a ulong, not a stringified copy).

---

## Appendix A — AMQP 1.0 message section layout

An AMQP 1.0 message is an ordered set of standard sections. The connector reads
and (re)emits them as follows:

```
┌──────────────────────────┐
│ header │ durable, priority, ttl, first-acquirer, delivery-count
├──────────────────────────┤ (priority is INERT — accepted, not used for ordering)
│ delivery-annotations │ opaque pass-through → envelope.delivery_annotations
├──────────────────────────┤
│ message-annotations │ x-opt-* (incl. x-opt-kubemq-*) → envelope.annotations
├──────────────────────────┤
│ properties │ message-id, correlation-id, to, reply-to, subject,
│ │ content-type, content-encoding, group-id,
│ │ creation-time, user-id, ttl → envelope.props
├──────────────────────────┤
│ application-properties │ arbitrary typed map → envelope.app
├──────────────────────────┤
│ BODY (exactly one of): │
│ • Data (binary) │ → body_section = "data" (DEFAULT for produce)
│ • AmqpValue (typed) │ → body_section = "value"
│ • AmqpSequence │ → REJECTED amqp:not-implemented
│ (empty body is valid) │
├──────────────────────────┤
│ footer │ opaque pass-through → envelope.footer
└──────────────────────────┘
```

- **Receiver-set link properties** (on the consuming ATTACH, not the message):
 - `x-opt-kubemq-group` — consumer group for events / events-store / queues.
 - `x-opt-kubemq-start` — events-store start position (`new-only` default).
- **Inert sections** are accepted but not acted on: message `priority`, `group-id`
 ordering, and `footer` are pass-through only.
- **Multi-frame transfers** fragment the body across `TRANSFER` frames
 (`more=true` … `more=false`) when it exceeds `max-frame-size`; the connector
 reassembles bit-exact up to `MaxMessageSize` (100 MiB default — one byte over
 is `amqp:link:message-size-exceeded`).

## Appendix B — Envelope shape reference

See the [metadata envelope](#the-metadata-envelope-and-type-markers) section
above for the full `{"amqp10":{...}}` shape, the `body_section` discriminator,
and the `$`-prefixed type markers. The capability/section support matrix
(supported bodies, settle modes, rejected sections, forced limits) lives in
[reference/capabilities.md](reference/capabilities.md).

---
