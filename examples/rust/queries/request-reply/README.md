# Rust — Queries / Request-Reply

Native AMQP 1.0 request/reply over KubeMQ **Queries** (RPC) with the native
`fe2o3-amqp` client — NO KubeMQ SDK, NO gRPC. The reply path is identical to commands
(dynamic reply node + anonymous responder sender), but a query reply carries ONLY the
body + metadata — there is no executed/error envelope — and a FAILED query delivers
NOTHING, so the requester simply times out.

## Prerequisites

- Rust 1.94+ (stable) with `cargo`.
- `fe2o3-amqp` 0.15.1 + `fe2o3-amqp-types` 0.14.0 (pinned exact in the workspace
  `examples/rust/Cargo.toml`).
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/rust
cargo run -p request-reply
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 cargo run -p request-reply
```

The single program runs both the responder (a tokio task) and the requester against
the broker, so it is runnable standalone.

## Expected Output

```
Broker:  amqp://localhost:5672
Address: queries/amqp10.examples.queries  (KubeMQ pattern=queries, channel=amqp10.examples.queries)

[responder] Listening on queries/amqp10.examples.queries (anonymous reply sender ready)
[requester] Dynamic reply node: _amqp10.tmp.<connID>.<uuid>
[requester] Sent query "get-temp-sensor-3" (reply-to=dynamic node, correlation-id=corr-qry-1)
[responder] Received query "get-temp-sensor-3" (correlation-id=<RequestID>)
[responder] Replied to "get-temp-sensor-3" (body + metadata only, no executed/error props)
[requester] Reply for "get-temp-sensor-3" (correlation-id=corr-qry-1): body="result:get-temp-sensor-3"
[requester] Sent query "ignore" (reply-to=dynamic node, correlation-id=corr-qry-2)
[responder] Received query "ignore" (correlation-id=<RequestID>)
[responder] Ignoring "ignore" — NO reply sent (requester will time out)
[requester] No reply for "ignore" within 5s — query timed out (expected; failed queries deliver nothing)

Done.
```

## What's Happening

1. **OPEN / BEGIN** — the requester and responder each open their own connection +
   session (one role per connection).
2. **ATTACH (dynamic reply node)** — the requester attaches a receiver whose source is
   `Source::builder().dynamic(true).build()`; the connector creates a transient node
   and echoes its address back (read from `reply_rcv.source()`).
3. **ATTACH (request sender + responder receiver)** — the requester attaches a sender
   on `queries/<ch>`; the responder attaches a receiver on `queries/<ch>` plus an
   anonymous sender (null target).
4. **TRANSFER (request)** — each query is sent with `Properties.reply_to` = the dynamic
   node + a `correlation_id`.
5. **TRANSFER (reply)** — for an answered query the responder replies on the anonymous
   sender with `Properties.to` = the request's reply-to and the echoed correlation-id.
   The reply carries **only** the body + metadata — NO executed/error props.
6. **Failure path** — the `"ignore"` query gets NO reply (the responder drops it). The
   connector delivers nothing on a failed/unanswered query, so the requester simply
   TIMES OUT — the absence of a reply IS the failure signal.
7. **Correlation** — the requester matches the answered reply by correlation-id.
8. **DETACH / CLOSE** — both roles tear down their links, sessions, and connections.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| requester reply node (KubeMQ → client) | source dynamic (`dynamic=true`, no address) | `First` (default) | `CreditMode::Auto(5)` | `accept` on reply | — | `Data` | server-assigned `_amqp10.tmp.*` address |
| requester sender (client → KubeMQ) | target `queries/<ch>` | `Unsettled` (explicit) | server-granted | `Accepted` per request | `Properties{reply_to, correlation_id}` | `Data` | reply-to must name a connection-owned node |
| responder receiver (KubeMQ → client) | source `queries/<ch>` | `First` (default) | `CreditMode::Auto(10)` | `accept` per request | — | `Data` | server-sender link under credit |
| responder reply sender (client → KubeMQ) | null target (anonymous) | `Unsettled` (explicit) | server-granted | — | `Properties{to, correlation_id}` (NO executed/error) | `Data` | routes by per-message `to` |

## Related Examples

- [commands/request-reply-dynamic-node](../../commands/request-reply-dynamic-node/) — the SAME dynamic-node path, but a command reply carries `executed`/`error` and a failed command always replies
- [advanced/anonymous-terminus](../../advanced/anonymous-terminus/) — the anonymous sender / per-message `to` mechanism the reply path uses

## Gotchas

> **A failed/unanswered query delivers NOTHING.** Unlike a command (which always
> replies `executed=false` on failure so the requester is never left waiting), a query
> that fails or goes unanswered simply causes the requester to TIME OUT. The connector's
> own default per-request timeout is ~30s; set the request `header.ttl` to choose it.

- **A query reply has no executed/error envelope.** It carries only the body +
  metadata — a query is a "fetch a value" call.
- **Reply-to must name a connection-owned node** (snooping guard → `amqp:not-allowed`).
- **Both senders set an explicit settle-mode** (`Unsettled`); `fe2o3-amqp` defaults to
  `mixed`, which the connector rejects at ATTACH.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT see
`connectivity/auth` + `guides/authentication.md`.
