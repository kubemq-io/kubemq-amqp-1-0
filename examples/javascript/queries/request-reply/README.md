# JavaScript — Queries / Request-Reply

Native AMQP 1.0 request/reply over KubeMQ **Queries** (RPC) with the native `rhea`
/ `rhea-promise` client — **no KubeMQ SDK, no gRPC**. The reply path is identical to
[commands](../../commands/request-reply-dynamic-node/): a server-assigned **dynamic
reply node**, `reply_to` + `correlation_id` on the request, and an **anonymous
sender** writing the reply to `/responses/<RequestID>`.

The **contrast** with commands is the whole point of this variant:

- A query reply carries **only the body + metadata** — there are **no**
  `x-opt-kubemq-executed` / `x-opt-kubemq-error` application-properties. A query
  fetches a value; it has no execution-outcome envelope.
- A **failed / unanswered** query delivers **nothing**. The requester simply **times
  out** — the absence of a reply *is* the failure signal. (A failed command, by
  contrast, always replies `executed=false`, so its requester is never left
  waiting.)

This one program runs **both roles** (a `Responder` listening, the requester in the
main flow). It demonstrates a **successful** query (reply round-trips) **and** a
query the responder ignores (the requester times out on a short demo deadline).

## Prerequisites

- Node.js 20+ (developed against Node 26).
- `rhea` 3.0.4 + `rhea-promise` 3.0.3 (pinned in `examples/javascript/package.json`);
  run via `tsx`.
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

Install once from `examples/javascript`:

```bash
npm install
```

## How to Run

```bash
cd examples/javascript
npx tsx queries/request-reply/index.ts
```

Override the broker URL (SASL **ANONYMOUS** by default — no credentials in the
URL):

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 npx tsx queries/request-reply/index.ts
```

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
concurrently. The second leg blocks for the demo timeout (5s) before printing the
timeout line.)

## What's Happening

1. **OPEN** — each role opens its own AMQP 1.0 connection (`connection.open()`,
   ANONYMOUS); rhea supplies a non-empty `container-id`.
2. **BEGIN** — each link creates its own session (responder and requester are on
   **separate connections**).
3. **ATTACH (requester reply node)** —
   `createReceiver({source:{address:"", dynamic:true}})` → the server creates a
   **dynamic node** and echoes its address (`replyReceiver.address`). This is the
   requester's private reply mailbox.
4. **ATTACH (requester sender)** —
   `createAwaitableSender({target:{address:"queries/<ch>"}})` (the client produces
   requests; the server grants credit on attach).
5. **ATTACH (responder receiver + anonymous sender)** — receiver on `queries/<ch>`
   (consumes requests under manual credit, handler before `addCredit`) + anonymous
   sender `createAwaitableSender({target:{}})` for replies.
6. **TRANSFER (request)** — the requester `send`s the query with
   `{reply_to: dynamicNode, correlation_id: id}`. The connector verifies the
   reply-to is connection-owned, routes to `SendQuery`, and settles the request
   `accepted`.
7. **TRANSFER (reply) — success leg** — the responder replies via the anonymous
   sender with `{to: <reply-to>, correlation_id: <echoed>}` and **only** the body
   (no executed/error props). The connector delivers it to the dynamic node; the
   requester correlates and reads the result.
8. **No reply — timeout leg** — for the `"ignore"` query the responder sends
   nothing. The connector delivers nothing (a failed query produces no reply), so
   the requester's reply waiter elapses after the demo timeout. **The timeout is the
   failure signal.** DETACH/CLOSE follow.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| requester reply node (KubeMQ → client) | source dynamic (`source:{address:"", dynamic:true}`) → `_amqp10.tmp.<connID>.<uuid>` | `first` (default) | client-granted (`addCredit(5)`) | reply delivered unsettled, out-of-band | request `reply_to` names this node | `Data` | reply-to MUST be connection-owned (snooping guard) |
| requester sender (client → KubeMQ) | target `queries/<ch>` | unsettled (default) | server-granted | request settled `accepted` once routed | `correlation_id` (or `message_id` fallback); optional `ttl` | `Data` | failed query ⇒ no reply ⇒ requester times out |
| responder receiver (KubeMQ → client) | source `queries/<ch>` | `first` (default) | client-granted (`addCredit(10)`) | `accept()` the request | reply-to = `/responses/<RequestID>`, correlation-id = RequestID (stamped by connector) | `Data` | pumped under credit, paused at `RpcMaxPending` |
| responder reply sender (client → KubeMQ) | target null (anonymous terminus, `target:{}`) | unsettled | server-granted | reply routed by `to` | `to` = request reply-to; **NO** executed/error props | `Data` | body + metadata only |

## Related Examples

- [commands/request-reply-dynamic-node](../../commands/request-reply-dynamic-node/) — the same dynamic-node RPC path, but the reply carries `x-opt-kubemq-executed`/`x-opt-kubemq-error` and a **failure still delivers a reply** (the requester is never left waiting)
- [advanced/anonymous-terminus](../../advanced/anonymous-terminus/) — the null-target send path the responder's reply sender uses
- [events/basic-pubsub](../../events/basic-pubsub/) — fire-and-forget fan-out (no reply path)

## Gotcha

> **A failed query has no error envelope — it just times out.** Unlike a command
> (which always replies `executed=false` + error text on failure), the connector
> delivers **nothing** for a query that fails, times out, or is ignored by the
> responder. The requester's reply waiter elapsing **is** the failure signal, so
> always send queries with a bounded reply deadline. The connector's own default
> per-request timeout is ~30s; set the request `ttl` (clamped to `[1s, 5min]`) to
> choose the per-request budget. This example uses a short 5s demo deadline so the
> unanswered leg surfaces quickly. The reply-to must still name a connection-owned
> dynamic node (the same snooping guard as commands).
>
> **Arm the reply waiter before sending.** The reply arrives out-of-band on the
> dynamic node; register the `message` handler before the request `send()` resolves
> so a fast reply is not missed. This example arms `awaitReply` before `sendQuery`.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
