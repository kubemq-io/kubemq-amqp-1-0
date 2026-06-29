# Addresses and Nodes

## Concept

In AMQP 1.0 there are no exchanges or routing keys. You choose the KubeMQ destination by setting a
**terminus address** on the link:

- the link **target** when you attach a **sender** (you produce), or
- the link **source** when you attach a **receiver** (you consume), or
- `properties.to` on each message when you send over an **anonymous** sender.

The address names a KubeMQ **pattern** and **channel** with a slash-prefix grammar.

## The Address Grammar

```
address := [ "/" ] pattern "/" channel # leading "/" optional: queues/x ≡ /queues/x
 | bare # no recognized prefix → capability hint, else DefaultPattern
 | "/responses/" RequestID # RPC reply token (reply path only; server-receiver only)
 | <dynamic> # source.dynamic / target.dynamic → _amqp10.tmp.<connID>.<uuid>
pattern := "queues" | "events" | "events-store" | "commands" | "queries"
```

| Terminus address | KubeMQ pattern | Channel |
|------------------|----------------|---------|
| `queues/<ch>` | queues | `<ch>` |
| `events/<ch>` | events | `<ch>` |
| `events-store/<ch>` | events-store | `<ch>` |
| `commands/<ch>` | commands | `<ch>` |
| `queries/<ch>` | queries | `<ch>` |
| `responses/<RequestID>` | responses (synthetic; **server-receiver only**) | opaque RPC reply token |
| bare (no `/`) | terminus node capability `queue`→queues / `topic`→events, else `DefaultPattern` (default `queues`) | the bare string |
| `_amqp10.tmp.…` | dynamic node (only reachable via per-message `to`) | — |
| null target on a sender | anonymous (synthetic) — route per-message by `properties.to` | "" |
| anything else containing `/` | → `DETACH(amqp:not-found, "unknown address prefix")` | — |

> **Use the explicit prefix.** Every example in this repo writes the full `<pattern>/<channel>`
> address (e.g. `queues/orders`, `events/telemetry`). A bare address resolves to a pattern by node
> capability or by the connector's `DefaultPattern`, which is fragile and server-config-dependent.
> Be explicit and you are never surprised.

## Longest-Prefix Discipline

The connector matches `events-store/` **before** `events/`. This is deliberate: `events-store/x`
must never be mis-read as the `events` pattern with channel `-store/x`. So the address
`events-store/replay` always resolves to the events-store pattern. The matching order is
`events-store → queues → events → commands → queries → responses`.

## Channel Validation — Stricter Than the Array Layer

Once the pattern prefix is stripped, the remaining channel is validated against rules that are
**stricter** than the native (gRPC/REST) array layer. A channel that works over gRPC can be
rejected over AMQP. A violation → `DETACH(amqp:not-found)`.

The connector rejects a channel that is:

- empty,
- longer than **255** characters,
- ends with a trailing `.`,
- contains any whitespace (` `, `\t`, `\r`, `\n`),
- contains a `*` or `>` wildcard,
- contains a `;` or `:`.

> **Gotcha #9 — channel charset is stricter over AMQP.** The `;`, `:`, and 255-char rules are
> connector-specific (the array layer rejects only empty, trailing `.`, whitespace, and wildcards).
> Validate your channel names against this charset client-side, or expect an `amqp:not-found`
> `DETACH` at attach time.

Note that the **prefix is stripped, never prepended** — the resolved channel passes to KubeMQ
exactly as written after the prefix. A channel that passes the connector's check always passes the
array layer.

## Dynamic and Anonymous Nodes

- **Dynamic node.** Attach a receiver with `source.dynamic = true` (or a sender with
 `target.dynamic = true`) and the server mints a fresh address
 `_amqp10.tmp.<connID>.<uuid>` and echoes it back. The node is **owned by your connection** and is
 not a broker channel — it is only reachable by your connection (and by the RPC reply path). This
 is exactly how you create the per-requester reply node for RPC (see [rpc.md](rpc.md)).
- **Anonymous sender.** Attach a sender with a **null target** and route every message by setting
 `properties.to` to a `<pattern>/<channel>` address per message. One link can fan a message stream
 out to queues *and* events by varying `to`. A missing or unreachable `to` →
 `amqp:precondition-failed` (or a send error for an absent `to`); each anonymous send carries a
 **per-message Write authorization** on the resolved `to`.

> **No `ANONYMOUS-RELAY` capability.** The connector drives anonymous routing purely from a null
> target address, **not** from an advertised `ANONYMOUS-RELAY` capability (the server advertises no
> capabilities at all). Clients whose anonymous-producer API depends on `ANONYMOUS-RELAY`
> negotiation (notably Qpid JMS) cannot emit the single null-target link the connector routes on
> which is why Java has no runnable `advanced/anonymous-terminus` example. See
> [../reference/capabilities.md](../reference/capabilities.md).

## The `/responses/` Token

`/responses/<RequestID>` is the RPC reply path. It is a **write-only, server-receiver-only** token:
an RPC responder writes a reply by sending to `/responses/<RequestID>` (usually via an anonymous
sender with `properties.to`). A client that attaches a **receiver** on a `/responses/` source is
refused with `amqp:not-allowed` — you never *consume* from `/responses/`. The RequestID is opaque
and validated by the RPC layer against the connection's pending-reply map, not as a broker channel.
See [rpc.md](rpc.md).

## No vhost — and 0-9-1 Interop

AMQP 1.0 has **no vhost**. The OPEN `hostname` field is logged and then ignored; there is no vhost
concept to expose.

> **Gotcha #11 — no vhost.** A client carrying over the 0-9-1 vhost mental model finds it has no
> effect. The two AMQP connectors (0-9-1 and 1.0) do **not** share a namespace.

To reach data that an AMQP **0-9-1** client wrote, address it through the queues pattern using the
0-9-1 channel convention: a 0-9-1 queue `q` on vhost `v` lives on channel `amqp.<v>.<q>`, so an
AMQP 1.0 client reaches it at **`/queues/amqp.<vhost>.<queue>`**. Cross-protocol equivalence is
asserted by the connector: `queues/<ch>` over AMQP 1.0 ⇄ the bare channel `<ch>` over gRPC, so a
gRPC client and an AMQP 1.0 client interoperate on the same channel.

## Examples

| Language | Example |
|----------|---------|
| Go | [advanced/anonymous-terminus](../../examples/go/advanced/anonymous-terminus/) |
| Python | [advanced/anonymous_terminus](../../examples/python/advanced/anonymous_terminus/) |
| Java | [advanced/anonymous-terminus](../../examples/java/advanced/anonymous-terminus/) — **N/A** (Qpid JMS has no null-target-link API; README explains and points to per-pattern senders) |
| C# | [advanced/anonymous-terminus](../../examples/csharp/advanced/anonymous-terminus/) |
| JavaScript / TS | [advanced/anonymous-terminus](../../examples/javascript/advanced/anonymous-terminus/) |
| Rust | [advanced/anonymous-terminus](../../examples/rust/advanced/anonymous-terminus/) |

See also [../reference/address-mapping.md](../reference/address-mapping.md),
[../guides/addressing.md](../guides/addressing.md).

Grounding: , .
