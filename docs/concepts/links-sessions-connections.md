# Links, Sessions, and Connections

## Concept

AMQP 1.0 is a **layered, multiplexed** protocol — and a completely different wire protocol from
AMQP 0-9-1 despite the shared name. There are **no exchanges, no bindings, no routing keys, no
publisher confirms, and no numeric reason codes**. Instead, three nested objects carry every
message:

```
TCP / TLS connection
└── session (one per AMQP channel number; a flow-control scope)
 └── link (a single unidirectional message flow: sender → receiver)
```

A single connection carries many **sessions**; each session carries many **links**. The KubeMQ
connector implements the **server side** of all three finite-state machines (FSMs). Your client
library drives the **peer side** — you call `Dial`, open a session, and attach a link, and the
connector answers.

The single most important consequence of the link model is **role inversion**: an AMQP link is
unidirectional, so *who sends* determines *what you are doing*.

| You (the client) attach a… | Server role becomes | You are… |
|----------------------------|---------------------|----------|
| **sender** link | server-**receiver** | **producing** (publishing into KubeMQ) |
| **receiver** link | server-**sender** | **consuming** (reading out of KubeMQ) |

This inversion is the foundation for everything else: the address you put on a link is the
**target** when you produce and the **source** when you consume (see
[addresses-and-nodes.md](addresses-and-nodes.md)), and credit flows in opposite directions for the
two roles (see [flow-control-and-credit.md](flow-control-and-credit.md)).

## The Three FSMs

### Connection — OPEN → ... → CLOSE

The connection lifecycle is `negotiating → opening → open → closing → closed`. A connection that
uses SASL runs an optional SASL handshake first, then re-exchanges the AMQP protocol header, then
negotiates `OPEN`.

**`container-id` is required and must be stable.** Your client **must** send a non-empty
`container-id` in `OPEN`. An empty container-id is fatal — the server replies
`CLOSE(amqp:invalid-field, "open.container-id is required")` and the connection never opens. The
value is sanitized to `[a-zA-Z0-9_-]` and capped at 256 characters.

> The container-id is more than a label. When no SASL identity is supplied it becomes your
> **ClientID**, and it is **half of the durable-subscription identity** — so a durable subscriber
> **must** use a stable container-id across reconnects, or it will not resume. See
> [durable-subscriptions.md](durable-subscriptions.md).

`OPEN` negotiation is a downward-clamp of the client's proposal against the server's limits:

| Field | Server behavior |
|-------|-----------------|
| `max-frame-size` | `min(client, 131072)`, floored at 512 |
| `channel-max` | `min(client, SessionMax-1)` = **255** by default (channels number `0..SessionMax-1`) |
| `idle-time-out` | advertised as `IdleTimeoutSeconds * 1000 / 2` ms (default `120 s → 60000 ms`); `0` disables idle entirely |

The **server's own `OPEN` always carries `container-id="KubeMQ"`** and advertises only those four
fields — **no offered/desired capabilities and no properties** (it reads your `product`/`version`
properties for the dashboard, nothing more). Do not write a client that depends on capability
negotiation; there is none (this is the root cause of one Java example limitation — see
[../reference/capabilities.md](../reference/capabilities.md)). A **second `OPEN`** on the same
connection is a protocol violation → `CLOSE(amqp:not-allowed, "unexpected second open")`.

**Protocol header echo.** Before any performative the server echoes the matching 8-byte protocol
header (`AMQP\x03\x01\x00\x00` for the SASL layer, `AMQP\x00\x01\x00\x00` for the bare/AMQP layer,
`AMQP\x02\x01\x00\x00` on the TLS listener). A conformant client blocks until it sees the echo; all
six client libraries used in these examples do this for you. After SASL completes the client sends
a **fresh bare header** to open the AMQP layer. The two AMQP dialects (0-9-1 and 1.0) share the
same TCP port (`5672`/`5671`): a `connectors/amqpmux` listener sniffs the first 8 bytes and
dispatches by dialect, so both coexist on one port.

**Idle timeout.** If `IdleTimeoutSeconds` (default 120) elapses with no inbound traffic, the server
sends `CLOSE(amqp:resource-limit-exceeded, "idle timeout expired")` and closes the socket. In
practice high-level clients keepalive at half the peer-advertised interval, so you rarely trigger
this; reconnect with backoff if you do.

**Admission rejects.** Over `MaxConnections` (default 1000) → admission reject with
`amqp:resource-limit-exceeded`; broker not ready → reject with `amqp:not-allowed` ("broker not
ready"). A reject opens then immediately error-CLOSEs the connection (it writes
`OPEN(container-id="KubeMQ")` then `CLOSE{error}`), so your client must surface the `CLOSE` error,
not just the open.

### Session — BEGIN → END

A `BEGIN` opens a session (one per AMQP channel number). The server reply carries
`incoming-window = 2048`, `outgoing-window = 2048`, and `handle-max = MaxLinksPerSession-1` (255).
Going over `SessionMax` → `END(amqp:resource-limit-exceeded)`. A duplicate live remote channel →
`END(amqp:session:errant-link)`. An inbound `TRANSFER` on a session whose window has reached 0 →
`END(amqp:session:window-violation)` (the server replenishes the window with a session `FLOW`
before that happens in normal operation). A client `END` detaches every in-flight link on that
session, the server replies with a graceful `END{}`, and the channel number is freed for reuse.

### Link — ATTACH → DETACH

`ATTACH` builds a link. The server applies these checks **in order** and `DETACH`es with the mapped
condition on the first failure:

1. `rcv-settle-mode=second` requested → `DETACH(amqp:not-implemented)` — pin
 `rcv-settle-mode=first` (see [settlement-and-delivery-state.md](settlement-and-delivery-state.md)).
2. Duplicate link name per `(session, role)` → `DETACH(amqp:not-allowed, "duplicate link name")`
 — **no link stealing, no resumption**.
3. Over `MaxLinksPerSession` → `DETACH(amqp:resource-limit-exceeded)`.
4. Address resolution (see [addresses-and-nodes.md](addresses-and-nodes.md)) — unknown prefix or a
 bad channel → `DETACH(amqp:not-found)`.
5. Attach-time authorization (Casbin Read on a consume link, Write on a fixed-target produce link)
 → `DETACH(amqp:unauthorized-access)` on denial.

The reply `ATTACH` echoes the link name, handle, the **inverted** role, the negotiated settle
modes, your source/target, `max-message-size`, and `initial-delivery-count=0`.

**`DETACH` is always final.** The connector treats any `DETACH` (whether the `closed` flag is set
or not) as terminal — there is no suspend/resume. On detach it stops the message pumps, NAcks every
unsettled delivery (returning it to the queue tail — see
[work-queues.md](work-queues.md)), frees any durable identity or dynamic node, and replies
`DETACH(closed)`.

## Why this matters

- A producer is a **sender** link to `<pattern>/<channel>`; a consumer is a **receiver** link from
 `<pattern>/<channel>`. Never confuse the two — the role determines the credit direction.
- `container-id` is load-bearing: required, stable, and identity-defining for durable subscribers.
- There is no link resumption: a reconnect is a fresh `ATTACH`, and any unsettled work is recovered
 through redelivery, not through link-state recovery.
- Both AMQP dialects share a port. You are not on a separate listener; the mux routes you by your
 protocol header.

## Examples

| Language | Example |
|----------|---------|
| Go | [connectivity/auth](../../examples/go/connectivity/auth/) |
| Python | [connectivity/auth](../../examples/python/connectivity/auth/) |
| Java | [connectivity/auth](../../examples/java/connectivity/auth/) |
| C# | [connectivity/auth](../../examples/csharp/connectivity/auth/) |
| JavaScript / TS | [connectivity/auth](../../examples/javascript/connectivity/auth/) |
| Rust | [connectivity/auth](../../examples/rust/connectivity/auth/) |

Every example opens a connection (OPEN with a container-id), a session (BEGIN), and at least one
link (ATTACH); `connectivity/auth` additionally exercises the SASL handshake. See also
[../guides/authentication.md](../guides/authentication.md).

Grounding: , .
