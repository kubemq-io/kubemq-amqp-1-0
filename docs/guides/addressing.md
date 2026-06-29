# Addressing

The terminus address is the single most client-load-bearing fact about the KubeMQ AMQP 1.0
connector: it is how you tell the connector **which KubeMQ pattern and channel** a link talks to.
This guide is the practical playbook for the `<pattern>/<channel>` grammar — always use explicit
prefixes — plus channel validation rules, longest-prefix matching, dynamic and anonymous addresses,
the absence of vhosts, and how to reach AMQP 0-9-1 data.

For the conceptual model see [Addresses and Nodes](../concepts/addresses-and-nodes.md).

---

## 1. Where the address goes

A client expresses the pattern + channel in the **terminus address**, which lives in a different
place depending on the link role:

| You are… | The address goes in… |
|---|---|
| a **receiver** (you consume) | the link **source** address |
| a **sender** to a fixed node (you produce) | the link **target** address |
| an **anonymous sender** | `properties.to` on **each message** (the target is null) |

---

## 2. The grammar — always use explicit prefixes

```
address := [ "/" ] pattern "/" channel # leading "/" is optional: queues/x ≡ /queues/x
 | bare # no recognized prefix → JMS hint, else DefaultPattern
 | "/responses/" RequestID # RPC reply token (reply path only; server-receiver only)
 | <dynamic> # source.dynamic / target.dynamic → _amqp10.tmp.<connID>.<uuid>
pattern := "queues" | "events" | "events-store" | "commands" | "queries"
```

| Terminus address | KubeMQ pattern | Channel |
|---|---|---|
| `queues/<ch>` | queues | `<ch>` |
| `events/<ch>` | events | `<ch>` |
| `events-store/<ch>` | events-store | `<ch>` |
| `commands/<ch>` | commands | `<ch>` |
| `queries/<ch>` | queries | `<ch>` |
| `responses/<RequestID>` | responses (synthetic; RPC reply path only) | the opaque reply token |
| bare (no prefix, no `/`) | JMS node-capability hint (`queue`→queues, `topic`→events), else `DefaultPattern` | the bare string |
| null target on a sender | anonymous — routed per-message by `properties.to` | (per message) |
| anything else containing `/` | **error** → `DETACH(amqp:not-found, "unknown address prefix")` | — |

> **Rule #1: always emit the explicit `<pattern>/<channel>` prefix.** Every example in this repo
> does. The prefix makes the destination **deterministic** and self-documenting: `queues/orders`,
> `events/telemetry`, `events-store/audit`, `commands/provision`, `queries/lookup`. Do not rely on
> bare addressing in application code — see §4.

The leading slash is optional and stripped: `queues/orders` ≡ `/queues/orders`. The prefix is
**stripped, never prepended** — the resolved channel (`orders`) is what the broker sees, so it
always passes the array-layer channel validation.

---

## 3. Longest-prefix matching

The connector matches prefixes **longest-first**: `events-store/` is tested **before** `events/`.
So `events-store/audit` resolves to the **events-store** pattern with channel `audit` — it is never
mis-read as the **events** pattern with channel `store/audit`. You never need to escape or
disambiguate; just write the full prefix.

---

## 4. Bare addressing — a Qpid-JMS migration convenience only

A bare address (no recognized prefix and no `/`) is resolved **non-deterministically**:

1. If the terminus carries a JMS **node-capability hint**, it selects the pattern: `queue` → queues,
 `topic` → events.
2. Otherwise the connector's configured **`DefaultPattern`** applies (default `queues`).

This exists so a **migrating Qpid-JMS / ActiveMQ app** can point at KubeMQ by changing only the
connection string and the destination name — a JMS `Queue("orders")` carries the `queue` capability
and lands on `queues/orders` without an explicit prefix. **Treat it as a migration aid, not a
design choice:**

> **Bare addressing is non-deterministic and config-dependent.** The same bare name resolves
> differently depending on the client's capability hint and the broker's `DefaultPattern`. For any
> code you write fresh, **use the explicit prefix** so the destination is unambiguous and survives a
> `DefaultPattern` change. See [migration-from-activemq](../reference/migration-from-activemq.md).

Anything bare that still **contains a `/`** is treated as an **unknown prefix**, not a bare channel
→ `DETACH(amqp:not-found, "unknown address prefix")`.

---

## 5. Channel validation — stricter than the array layer

After the prefix is stripped, the connector validates the channel **more strictly than the underlying array layer**. A channel is rejected with
`DETACH(amqp:not-found)` if it is:

| Rejected | Example |
|---|---|
| empty | `queues/` |
| longer than **255** characters | `queues/<256+ chars>` |
| has a **trailing `.`** | `queues/orders.` |
| contains **whitespace** | `queues/my orders` |
| contains `*` or `>` wildcards | `queues/orders.*` |
| contains `;` or `:` | `queues/a:b` , `queues/a;b` |

> **Gotcha #9:** a channel name that works on the gRPC/native KubeMQ side can be **rejected over
> AMQP** because of the extra `;` `:` and trailing-`.` restrictions. Keep AMQP channel names to
> `[a-zA-Z0-9._-]`, ≤255 chars, no trailing dot.

---

## 6. Dynamic and anonymous addresses

These are special address forms the connector mints or routes specially:

### Dynamic nodes (`source.dynamic` / `target.dynamic`)

When you attach a receiver with **`DynamicAddress: true`** (a dynamic source), the connector creates
a fresh node and echoes its address in the reply ATTACH: `_amqp10.tmp.<connID>.<uuid>`. This is the
mechanism for an **RPC reply node** — a requester opens a dynamic receiver, reads back its echoed
address, and stamps it as `reply-to`. See [RPC](../concepts/rpc.md) and the
[`commands/request-reply-dynamic-node`](../../examples/go/commands/request-reply-dynamic-node/) example.

> **Dynamic nodes are node-local** (gotcha #6): a temp node created on node A is not reachable from
> node B. RPC *replies* are still cluster-safe (they travel the broker reply path), but a direct
> cross-node send to a dynamic address is not.

### Anonymous terminus (null target sender)

A sender opened with a **null target** (`NewSender("", nil)`) is an *anonymous* sender: it has no
fixed destination, and each message selects its destination via **`properties.to`** (which itself
uses the `<pattern>/<channel>` grammar). Because there is no fixed channel at attach, authorization
is deferred to a **per-message Write** check on each message's `to` (see
[authentication.md](authentication.md)). A bad `to` → `amqp:precondition-failed`; a missing `to` →
a Send error. See the
[`advanced/anonymous-terminus`](../../examples/go/advanced/anonymous-terminus/) example.

> The connector advertises **no `ANONYMOUS-RELAY` capability** — do not depend on negotiated
> capabilities for this. The anonymous-terminus routing works by the connector inspecting
> `properties.to`, not by a capability handshake.

### `/responses/` reply tokens

`/responses/<RequestID>` is a synthetic RPC reply address. It is **valid only as a server-receiver
attach** (the reply path); a **receiver** attach on `/responses/` → `DETACH(amqp:not-allowed)`. You
do not construct these yourself in normal use — the RPC layer manages them and they are
connection-scoped (not authorized via Casbin).

---

## 7. No vhost

AMQP 1.0 has **no vhost**. The `OPEN` `hostname` field (which a client carrying over a 0-9-1 / other-broker
mental model might set) is **logged then ignored** (gotcha #11). There is no vhost option to expose
and no namespace scoping by hostname.

---

## 8. Reaching AMQP 0-9-1 data

The AMQP 1.0 and AMQP 0-9-1 connectors **do not share a namespace**. AMQP 0-9-1 queues live on
KubeMQ channels named `amqp.<vhost>.<queue>`. To reach the same data from an AMQP 1.0 client, use
the explicit queues prefix over that channel name:

```
/queues/amqp.<vhost>.<queue>
```

Cross-protocol equivalence holds the other way too: an AMQP 1.0 `queues/<ch>` maps to the bare
KubeMQ channel `<ch>`, so a gRPC/native client producing to `<ch>` interoperates with an AMQP 1.0
consumer of `queues/<ch>` .

---

## 9. Quick reference

| Destination | Address to use |
|---|---|
| Queue `orders` | `queues/orders` |
| Event stream `telemetry` | `events/telemetry` |
| Durable event stream `audit` | `events-store/audit` |
| Command channel `provision` | `commands/provision` |
| Query channel `lookup` | `queries/lookup` |
| RPC reply node | dynamic receiver (`DynamicAddress: true`); never a hand-built address |
| Route per-message | anonymous sender + `properties.to = "queues/<ch>"` etc. |
| AMQP 0-9-1 queue | `/queues/amqp.<vhost>.<queue>` |
| Fresh app code | **always the explicit prefix** — never rely on bare addressing |

---

## Related

- [Addresses and Nodes (concept)](../concepts/addresses-and-nodes.md) — the resolution model
- [RPC](../concepts/rpc.md) — dynamic reply nodes and `/responses/`
- [Authentication and Authorization](authentication.md) — per-message Write on anonymous `to`
- [Selectors](../concepts/selectors.md) — and why selectors on `/queues/` → `amqp:not-implemented`
- [Address Mapping (reference)](../reference/address-mapping.md)
- [Migration from ActiveMQ](../reference/migration-from-activemq.md) — bare addressing as a JMS aid

Examples:
[Go `queues/basic-send-receive`](../../examples/go/queues/basic-send-receive/) ·
[`advanced/anonymous-terminus`](../../examples/go/advanced/anonymous-terminus/) ·
[`commands/request-reply-dynamic-node`](../../examples/go/commands/request-reply-dynamic-node/)
(and the same variants for Python, Java, C#, JS/TS, Rust)
