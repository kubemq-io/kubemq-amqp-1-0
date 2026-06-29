# Reference — Address Mapping

This is the master reference for how the embedded KubeMQ AMQP 1.0 connector maps an
AMQP 1.0 terminus **address** to a KubeMQ **(pattern, channel)** pair, which **server
link role** results from a peer's source/target choice, and the settlement, credit,
filter and link-property behaviour that each pattern carries.

Everything here is grounded in (the link finite-state
machine and the `resolveAddress` table). Where a server-side rule is stricter than the
AMQP spec, the connector rule wins — examples and clients must follow the connector.

> **One rule above all — explicit prefix.** Always address a node with its full
> `<pattern>/<channel>` form (`queues/orders`, `events/telemetry`,
> `events-store/audit`). The examples in this repo never rely on the bare-address /
> `DefaultPattern` fallback. See [Addresses & Nodes](../concepts/addresses-and-nodes.md).

---

## 1. Address grammar

```
address = [ "/" ] (prefixed-node | bare-node)
prefixed-node = pattern-prefix channel
pattern-prefix = "queues/" | "events/" | "events-store/"
 | "commands/" | "queries/" | "responses/"
bare-node = channel ; no "/" — resolved via node-cap hint or DefaultPattern
channel = 1*255 VCHAR ; connector charset rules (§4)
```

- The **leading slash is optional**. `resolveAddress` strips at most one leading `/`
 before matching, so `queues/orders` and `/queues/orders` resolve identically
 .
- Each pattern prefix **includes its trailing slash** (`"events/"`, `"events-store/"`)
 so the prefix match is unambiguous.
- A null/empty address is **not** an error in itself — on a server-receiver link it
 selects the **anonymous terminus** (§3); on a dynamic terminus it asks the server to
 **mint a node** (§3).

---

## 2. The master mapping table — five patterns + RPC reply token

The pattern is taken from the address prefix. The **server link role** is the inverse
of the peer's role: a peer **receiver** attaches against a **source** address (the
server *sends* — `roleServerSender`); a peer **sender** attaches against a **target**
address (the server *receives* — `roleServerReceiver`) (
`terminusAddress`).

| Pattern | Send to (peer→server, **target** addr) | Receive from (server→peer, **source** addr) | Server link role | Settlement & credit | Filters / link-props |
|---|---|---|---|---|---|
| **Queues** | `queues/<channel>` | `queues/<channel>` | produce = `roleServerReceiver`; consume = `roleServerSender` | at-least-once (unsettled) **or** at-most-once (pre-settled `snd-settle-mode=settled` + `rcv-settle-mode=first`); consumer grants link credit; `accept`/`release`/`modify`/`reject` dispositions; **`copy` distribution-mode rejected → `amqp:invalid-field`** (queues are move-only) | **no** selector (selector on a queue → `amqp:not-implemented`); no link-props |
| **Events** | `events/<channel>` | `events/<channel>` | produce = `roleServerReceiver`; consume = `roleServerSender` | at-most-once fan-out; **continuous credit required** — a transfer with 0 link credit is **silently dropped** (`kubemq_amqp10_events_dropped_no_credit_total`) | consume link may carry a **selector** (`apache.org:selector-filter:string`) and/or `x-opt-kubemq-group` (consumer group) |
| **Events-Store** | `events-store/<channel>` | `events-store/<channel>` | produce = `roleServerReceiver`; consume = `roleServerSender` | durable replay; durable identity from **stable container-id + link name** (`durableID`); second live attach of the same identity → `amqp:not-allowed` | consume link may carry a **selector** and **`x-opt-kubemq-start`** (start position) and `x-opt-kubemq-group` |
| **Commands** (RPC) | `commands/<channel>` | — (requester reads its **dynamic reply node**) | request = `roleServerReceiver` | native RPC; reply carries body + `x-opt-kubemq-executed`/`x-opt-kubemq-error`; failure → `executed=false` reply | `reply-to` + `correlation-id` on the request message |
| **Queries** (RPC) | `queries/<channel>` | — (requester reads its **dynamic reply node**) | request = `roleServerReceiver` | native RPC; reply carries **body + metadata only** (no executed/error); failure → no reply (requester times out) | `reply-to` + `correlation-id` on the request message |
| **Responses** (RPC reply token) | `responses/<RequestID>` (peer **sender** writing a reply) | — | `roleServerReceiver` only | connection-scoped reply token; **write-only** | none |

**Notes carried by the table:**

- The same `<pattern>/<channel>` resolves to the **same KubeMQ channel** regardless of
 link direction — only the server role (and therefore the authorization check)
 differs. See §5.
- `commands/`, `queries/`, `responses/` are RPC machinery. A requester's *reply* link
 is a **dynamic node**, not a `<pattern>/<channel>` address (§3).
- `responses/<RequestID>` is valid **only** as a server-receiver link (the peer is a
 sender writing the reply). A **receiver** attach against `responses/` is rejected with
 `amqp:not-allowed` .

---

## 3. The special-row mapping — bare, dynamic, anonymous, responses

| Row | What the client does | How the connector resolves it | Source |
|---|---|---|---|
| **Bare address** | attaches to a channel with no `pattern/` prefix (e.g. `orders`) | a JMS **node capability** hint on the terminus selects the pattern: `queue` → `queues`, `topic` → `events`; otherwise the configured `DefaultPattern` applies (degrading to `queues` if misconfigured) | `bareAddressPattern` |
| **Dynamic terminus** | attaches with `source.dynamic=true` (receiver) or `target.dynamic=true` (sender) and an **empty address** | the server mints a transient node `_amqp10.tmp.<connID>.<uuid>`, echoes it in the reply ATTACH, and backs it with an in-memory node-local mailbox; **no broker channel, no §2 resolution, no attach-time authz** (the node is connection-private by its unguessable address) | `attachDynamic` , `dynamicNodeAddress` , |
| **Anonymous terminus** | a **server-receiver** link with a **null target address**; routes per-message by `properties.to` | the link binds to no fixed channel; each transfer's `to` is resolved with the **full §2 table** (including `responses/` tokens and dynamic nodes) and authorized **per message** for Write. A missing/invalid `to` or an unreachable node → `amqp:precondition-failed`; an authz denial → `amqp:unauthorized-access` | `processAttach` anonymous branch , `anonymousRouter.routeAnon` |
| **Responses token** | a peer **sender** writes a reply to `responses/<RequestID>` | resolves to `(responses, RequestID)`; the RequestID is opaque (validated by the RPC layer against the pending-reply map, not as a broker channel — only emptiness is rejected). **Receiver attach → `amqp:not-allowed`** | `resolveAddress` , |

> **Anonymous terminus is null-target-driven, NOT capability-driven.** The connector
> advertises **no `ANONYMOUS-RELAY` capability** (see
> [Capabilities](./capabilities.md)). A client gets anonymous routing by attaching a
> sender link with a **null target**, never by negotiating a capability.

---

## 4. Longest-prefix discipline

`events-store/` and `events/` share a common stem. `resolveAddress` matches
`events-store/` **before** `events/`, so `events-store/audit` is never mis-classified as
the `events` pattern with channel `store/audit` :

```go
switch {
case strings.HasPrefix(a, prefixEventsStore): // "events-store/" — checked FIRST
case strings.HasPrefix(a, prefixQueues): // "queues/"
case strings.HasPrefix(a, prefixEvents): // "events/"
case strings.HasPrefix(a, prefixCommands): // "commands/"
case strings.HasPrefix(a, prefixQueries): // "queries/"
case strings.HasPrefix(a, prefixResponses): // "responses/"
}
```

A bare address that still contains a `/` but matches **no** recognised prefix is an
**unknown prefix → `amqp:not-found`** — a bare KubeMQ channel may use
`.` segments but never the reserved `/` separator.

---

## 5. Server role → authorization

The peer's link role and the chosen terminus side together fix the server role and the
Casbin permission required at attach (`authorizeAttach`):

| Peer attaches as | Terminus side read | Server role | Permission enforced at attach |
|---|---|---|---|
| **receiver** (peer reads) | `source` | `roleServerSender` (server sends) | **Read** on `(pattern, channel)` |
| **sender** (peer writes), fixed target | `target` | `roleServerReceiver` (server receives) | **Write** on `(pattern, channel)` |
| **sender**, null target (anonymous) | — | `roleServerReceiver` | deferred — **per-message Write** on the resolved `to` |
| **sender** to `responses/<id>` | `target` | `roleServerReceiver` | **none** (connection-scoped reply token) |

The pattern→Casbin resource map is `authzResource` : `events-store` maps
to the resource `events_store`; `commands`/`queries` map to `commands`/`queries`. See
[Authentication](../guides/authentication.md).

---

## 6. Channel validation (connector charset)

The channel component (everything after the pattern prefix) is validated by
`validateConnectorChannel` . It is **stricter** than the array-layer
validation, so a channel that passes here always passes downstream. Any violation maps to
**`amqp:not-found`** (a bad address is "not found"):

| Rule | Rejected example | Reason |
|---|---|---|
| not empty | `queues/` | `empty channel` |
| ≤ 255 bytes | 300-char channel | `channel exceeds 255 chars` |
| no trailing `.` | `queues/orders.` | `channel has trailing '.'` |
| no whitespace | `queues/my orders` | `channel contains whitespace` (space/tab/CR/LF) |
| no `*` or `>` wildcard | `queues/orders.*` | `channel contains wildcard` |
| no `;` or `:` | `queues/a:b` | `channel contains ';' or ':'` |

> **Gotcha — channel charset is stricter than you may expect.** `*`, `>`, `;`, `:`,
> whitespace and a trailing `.` are all rejected at attach with `amqp:not-found`.
> Use `.` only as a path separator inside the channel (e.g.
> `queues/region.eu.orders`). See [Addressing](../guides/addressing.md).

There is **no vhost / virtual host**. The OPEN `hostname` field is accepted but ignored
— the address space is flat and global. For AMQP 0-9-1 interop the channel convention is
`queues/amqp.<vhost>.<queue>` (a naming convention, not a real vhost).

---

## 7. Quick examples

| AMQP address attached as | Peer role | Resolves to | Server role |
|---|---|---|---|
| `queues/orders` | sender | `(queues, orders)` | receiver (Write) |
| `queues/orders` | receiver | `(queues, orders)` | sender (Read) |
| `/events/telemetry` | receiver | `(events, telemetry)` | sender (Read) |
| `events-store/audit` | receiver | `(events-store, audit)` | sender (Read) |
| `commands/dispatch` | sender | `(commands, dispatch)` | receiver (Write) |
| `responses/abc123` | sender | `(responses, abc123)` | receiver (no authz) |
| `responses/abc123` | receiver | **DETACH** `amqp:not-allowed` | — |
| `orders` (bare, node-cap `queue`) | sender | `(queues, orders)` | receiver (Write) |
| `null target` | sender | anonymous terminus, routes by `to` | receiver (per-msg Write) |
| `source.dynamic=true`, empty addr | receiver | minted `_amqp10.tmp.<connID>.<uuid>` | sender (no authz) |
| `events/x;y` | receiver | **DETACH** `amqp:not-found` (`;` in channel) | — |

---

## Related

- Concepts: [Addresses & Nodes](../concepts/addresses-and-nodes.md),
 [Links, Sessions & Connections](../concepts/links-sessions-connections.md),
 [Work Queues](../concepts/work-queues.md), [Pub/Sub](../concepts/pub-sub.md),
 [Durable Subscriptions](../concepts/durable-subscriptions.md), [RPC](../concepts/rpc.md)
- Reference: [Capabilities](./capabilities.md),
 [Error Conditions](./error-conditions.md)
- Guides: [Addressing](../guides/addressing.md)
- Examples: every variant README carries an "AMQP 1.0 specifics" address table; see
 `examples/go/queues/basic-send-receive`, `examples/go/advanced/anonymous-terminus`,
 `examples/go/commands/request-reply-dynamic-node`.

---
