# RPC — Commands and Queries

## Concept

The **commands** (`commands/<ch>`) and **queries** (`queries/<ch>`) patterns are
**request/reply** over AMQP 1.0. A requester sends a request and gets a reply, matched by
correlation. RPC here is **fully native and in-protocol** — there is **no gRPC responder and no
KubeMQ SDK** anywhere in the examples or the burn-in harness. A responder is just a normal AMQP
consumer that publishes a reply.

```
requester responder
 │ attach dynamic reply node (source.dynamic) │
 │ → server mints _amqp10.tmp.<conn>.<uuid> │
 │ send req → commands/<ch>: │
 │ reply-to = <minted reply node> ─────┼─▶ consume commands/<ch>
 │ correlation-id = <id> │ do work
 │ ◀───────────────────────────────────────────┤ send reply (anonymous sender):
 │ match reply by correlation-id │ to = /responses/<RequestID>
 │ │ correlation-id = <same id>
```

## The Request Path

1. **The requester creates a dynamic reply node.** Attach a receiver with `source.dynamic = true`
 and grant it a little credit; the server mints and echoes an address
 `_amqp10.tmp.<connID>.<uuid>` that this connection owns (see
 [addresses-and-nodes.md](addresses-and-nodes.md)).
2. **Send the request** to `commands/<ch>` (or `queries/<ch>`) with `reply-to =` the minted reply
 node and a unique `correlation-id`. An optional `header.ttl` (ms) sets the per-request timeout
 (clamped to a floor and `defaultTimeout*10`); otherwise `DefaultRpcTimeoutSeconds` (30 s)
 applies.
3. **The connector accepts the request** (`accepted` DISPOSITION once routed) and fires
 `SendCommand` / `SendQuery`; the eventual reply (or timeout) is delivered out-of-band to your
 reply node.
4. **The responder** consumes `commands/<ch>` / `queries/<ch>`, does the work, and sends a reply
 (typically via an **anonymous sender** with `properties.to = /responses/<RequestID>`) carrying
 the same `correlation-id`.
5. **The requester matches** each reply to its request by `correlation-id`.

## Reply-To Must Be Connection-Owned (Snooping Guard)

> **Gotcha #10 — `reply-to` must name a node this connection owns.** A requester **cannot** point
> `reply-to` at an arbitrary or foreign node — that would let it direct a response to another
> client's node (snooping). A request with a **missing** `reply-to` → `amqp:not-allowed` ("request
> missing reply-to"); a request whose `reply-to` names a node **this connection does not own** →
> `amqp:not-allowed` ("reply-to is not a node this connection owns"). Always create a **dynamic
> reply node** per requester and use its echoed `_amqp10.tmp.*` address. The `/responses/<id>`
> token the responder writes to is connection-scoped and is **not** authorization-checked.

## Correlation-ID with Message-ID Fallback

The reply's `correlation-id` is the request's `correlation-id`, or — when the request carried none
its **`message-id`** (the Qpid JMS convention). Set one or the other on every request and match the
reply on it.

## Commands vs Queries — the Failure Contract

Both share the same dynamic-reply path, but their reply shape and failure behavior differ:

| | **Commands** | **Queries** |
|---|---|---|
| Reply body | optional | the result **body + metadata** |
| Reply app-properties | `x-opt-kubemq-executed` (bool) **always**; `x-opt-kubemq-error` (string) when non-empty | **none** (no executed/error props) |
| On success | `executed=true`, body/metadata as produced | body + metadata returned |
| **On failure** | a reply **is** delivered with **`executed=false`** (and `x-opt-kubemq-error`) — the requester is never left waiting | **nothing is delivered** — the requester **times out** (~`DefaultRpcTimeoutSeconds`, 30 s) |

In short: a **command** always answers (success or `executed=false`); a **query** answers on
success and goes silent on failure (you detect failure by timeout). Choose commands when you need a
positive failure signal, queries when a missing reply is an acceptable failure mode.

## Caps and Cluster Behavior

- **`RpcMaxPending`** (default 512) bounds in-flight requests per connection. The responder pump is
 paused at the cap; a request that cannot reserve a slot → `amqp:resource-limit-exceeded` ("rpc
 pending limit reached"). Bound your concurrency accordingly.
- **Dynamic reply nodes are node-local** (gotcha #6) — but RPC **replies travel the broker reply
 path and are cluster-safe**. The dynamic node itself lives on the requester's node; the reply
 finds its way back through the broker, so request/reply works across a cluster even though the
 node is node-local. (Direct cross-node *sends* to a dynamic node are not supported — only the RPC
 reply path is.) See [durable-subscriptions.md](durable-subscriptions.md) for the node-local
 registry.

## Examples

| Language | Example |
|----------|---------|
| Go | [commands/request-reply-dynamic-node](../../examples/go/commands/request-reply-dynamic-node/) · [queries/request-reply](../../examples/go/queries/request-reply/) |
| Python | [commands/request_reply_dynamic_node](../../examples/python/commands/request_reply_dynamic_node/) · [queries/request_reply](../../examples/python/queries/request_reply/) |
| Java | [commands/request-reply-dynamic-node](../../examples/java/commands/request-reply-dynamic-node/) · [queries/request-reply](../../examples/java/queries/request-reply/) |
| C# | [commands/request-reply-dynamic-node](../../examples/csharp/commands/request-reply-dynamic-node/) · [queries/request-reply](../../examples/csharp/queries/request-reply/) |
| JavaScript / TS | [commands/request-reply-dynamic-node](../../examples/javascript/commands/request-reply-dynamic-node/) · [queries/request-reply](../../examples/javascript/queries/request-reply/) |
| Rust | [commands/request-reply-dynamic-node](../../examples/rust/commands/request-reply-dynamic-node/) · [queries/request-reply](../../examples/rust/queries/request-reply/) |

Grounding: , .
