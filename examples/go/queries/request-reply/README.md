# Go — Queries / Request-Reply

Native AMQP 1.0 request/reply over KubeMQ **Queries** (RPC) with the native
`github.com/Azure/go-amqp` client — **no `kubemq-go`, no gRPC**. The reply path is
identical to [commands](../../commands/request-reply-dynamic-node/): a
server-assigned **dynamic reply node**, `reply-to` + `correlation-id` on the
request, and an **anonymous sender** writing the reply to `/responses/<RequestID>`.

The **contrast** with commands is the whole point of this variant:

- A query reply carries **only the body + metadata** — there are **no**
  `x-opt-kubemq-executed` / `x-opt-kubemq-error` application-properties. A query
  fetches a value; it has no execution-outcome envelope.
- A **failed / unanswered** query delivers **nothing**. The requester simply
  **times out** — the absence of a reply *is* the failure signal. (A failed
  command, by contrast, always replies `executed=false`, so its requester is never
  left waiting.)

This one program runs **both roles** (responder in a goroutine, requester in the
main flow). It demonstrates a **successful** query (reply round-trips) **and** a
query the responder ignores (the requester times out on a short demo deadline).

## Prerequisites

- Go 1.24+
- `github.com/Azure/go-amqp` v1.7.0 (pinned in `examples/go/go.mod`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/go
go run ./queries/request-reply
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 go run ./queries/request-reply
```

Runs ANONYMOUS by default — no userinfo in the URL.

## Expected Output

```
Broker:  amqp://localhost:5672
Address: queries/amqp10.examples.queries  (KubeMQ pattern=queries, channel=amqp10.examples.queries)

[responder] Listening on queries/amqp10.examples.queries (anonymous reply sender ready)
[requester] Dynamic reply node: _amqp10.tmp.<connID>.<uuid>
[requester] Sent query "get-temp-sensor-3" (reply-to=dynamic node, correlation-id=corr-qry-1)
[responder] Received query "get-temp-sensor-3" (correlation-id=corr-qry-1)
[responder] Replied to "get-temp-sensor-3" (body + metadata only, no executed/error props)
[requester] Reply for "get-temp-sensor-3" (correlation-id=corr-qry-1): body="result:get-temp-sensor-3"
[requester] Sent query "ignore" (reply-to=dynamic node, correlation-id=corr-qry-2)
[responder] Received query "ignore" (correlation-id=corr-qry-2)
[responder] Ignoring "ignore" — NO reply sent (requester will time out)
[requester] No reply for "ignore" within 5s — query timed out (expected; failed queries deliver nothing)

Done.
```

(Interleaving of `[responder]` / `[requester]` lines varies — the two roles run
concurrently. The second leg blocks for the `demoTimeout` (5s) before printing the
timeout line.)

## What's Happening

1. **OPEN** — each role dials its own AMQP 1.0 connection (`amqp.Dial`,
   ANONYMOUS); go-amqp supplies a non-empty `container-id`.
2. **BEGIN** — each role opens its own session (responder and requester use
   **separate sessions on separate connections**).
3. **ATTACH (requester reply node)** — `NewReceiver(ctx, "", {DynamicAddress:true,
   Credit:5})` → the server creates a **dynamic node** and echoes its address
   (`replyRcv.Address()`). This is the requester's private reply mailbox.
4. **ATTACH (requester sender)** — `NewSender(ctx, "queries/<ch>", nil)` (the
   client produces requests; the server grants credit on attach).
5. **ATTACH (responder receiver + anonymous sender)** — receiver on
   `queries/<ch>` (consumes requests under credit) + anonymous sender
   `NewSender(ctx, "", nil)` for replies.
6. **TRANSFER (request)** — the requester `Send`s the query with
   `Properties{ReplyTo: &dynamicNode, CorrelationID: id}`. The connector verifies
   the reply-to is connection-owned, routes to `SendQuery`, and settles the request
   `accepted`.
7. **TRANSFER (reply) — success leg** — the responder replies via the anonymous
   sender with `Properties{To: <reply-to>, CorrelationID: <echoed>}` and **only**
   the body (no executed/error props). The connector delivers it to the dynamic
   node; the requester correlates and reads the result.
8. **No reply — timeout leg** — for the `"ignore"` query the responder sends
   nothing. The connector delivers nothing (a failed query produces no reply), so
   the requester's `Receive` deadline elapses. **The timeout is the failure
   signal.** DETACH/CLOSE follow.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| requester reply node (KubeMQ → client) | source dynamic (`DynamicAddress:true`) → `_amqp10.tmp.<connID>.<uuid>` | `first` (default) | client-granted `Credit:5` | reply delivered unsettled, out-of-band | request `Properties.ReplyTo` names this node | `Data` | reply-to MUST be connection-owned (snooping guard) |
| requester sender (client → KubeMQ) | target `queries/<ch>` | unsettled (default) | server-granted | request settled `accepted` once routed | `Properties.CorrelationID` (or `MessageID` fallback); optional `header.ttl` | `Data` | failed query ⇒ no reply ⇒ requester times out |
| responder receiver (KubeMQ → client) | source `queries/<ch>` | `first` (default) | client-granted `Credit:10` | `AcceptMessage` the request | reply-to = `/responses/<RequestID>`, correlation-id = RequestID (stamped by connector) | `Data` | pumped under credit, paused at `RpcMaxPending` |
| responder reply sender (client → KubeMQ) | target null (anonymous terminus) | unsettled | server-granted | reply routed by `Properties.To` | `Properties.To` = request reply-to; **NO** executed/error props | `Data` | body + metadata only |

## Related Examples

- [commands/request-reply-dynamic-node](../../commands/request-reply-dynamic-node/) — the same dynamic-node RPC path, but the reply carries `x-opt-kubemq-executed`/`x-opt-kubemq-error` and a **failure still delivers a reply** (the requester is never left waiting)
- [advanced/anonymous-terminus](../../advanced/anonymous-terminus/) — the null-target send path the responder's reply sender uses
- [events/basic-pubsub](../../events/basic-pubsub/) — fire-and-forget fan-out (no reply path)

## Gotcha

> **A failed query has no error envelope — it just times out.** Unlike a command
> (which always replies `executed=false` + error text on failure), the connector
> delivers **nothing** for a query that fails, times out, or is ignored by the
> responder. The requester's `Receive` deadline elapsing **is** the failure
> signal, so always send queries with a bounded `Receive` context. The connector's
> own default per-request timeout is ~30s; set the request `header.ttl` (clamped to
> `[1s, 5min]`) to choose the per-request budget. This example uses a short 5s demo
> deadline so the unanswered leg surfaces quickly. The reply-to must still name a
> connection-owned dynamic node (the same snooping guard as commands).

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
