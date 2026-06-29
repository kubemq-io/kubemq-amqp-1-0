# JavaScript — Commands / Request-Reply (Dynamic Reply Node)

Native AMQP 1.0 request/reply over KubeMQ **Commands** (RPC) with the native
`rhea` / `rhea-promise` client — **no KubeMQ SDK, no gRPC**. The requester opens a
server-assigned **dynamic reply node**, sends a command to `commands/<ch>` carrying
that node as `reply_to` plus a `correlation_id`; the responder receives the command
and replies via an **anonymous sender** to `/responses/<RequestID>`, stamping the
command outcome as `x-opt-kubemq-executed` / `x-opt-kubemq-error`
application-properties.

This one program runs **both roles** (a `Responder` listening, the requester in the
main flow) so it is runnable standalone against a broker. It demonstrates a
**successful** command (executed=true) **and** a **failed** command (executed=false)
— both round-trip, and the requester is never left waiting.

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
npx tsx commands/request-reply-dynamic-node/index.ts
```

Override the broker URL (SASL **ANONYMOUS** by default — no credentials in the
URL):

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 npx tsx commands/request-reply-dynamic-node/index.ts
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: commands/amqp10.examples.commands  (KubeMQ pattern=commands, channel=amqp10.examples.commands)

[responder] Listening on commands/amqp10.examples.commands (anonymous reply sender ready)
[requester] Dynamic reply node: _amqp10.tmp.<connID>.<uuid>
[requester] Sent command "reboot-node-7" (reply-to=dynamic node, correlation-id=corr-cmd-1)
[responder] Received command "reboot-node-7" (correlation-id=<RequestID>)
[responder] Replied to "reboot-node-7" (executed=true, error="")
[requester] Reply for "reboot-node-7" (correlation-id=corr-cmd-1): executed=true error="" body=""
[requester] Sent command "fail" (reply-to=dynamic node, correlation-id=corr-cmd-2)
[responder] Received command "fail" (correlation-id=<RequestID>)
[responder] Replied to "fail" (executed=false, error="command rejected by handler")
[requester] Reply for "fail" (correlation-id=corr-cmd-2): executed=false error="command rejected by handler" body=""

Done.
```

(Interleaving of `[responder]` / `[requester]` lines varies — the two roles run
concurrently.)

> **Correlation-id on the wire.** The responder sees the connector-stamped
> `RequestID` as the delivered request's correlation-id, while the requester's reply
> correlation-id is its **original** `corr-cmd-N` — the connector echoes the
> requester's correlation-id back on the reply. Correlate on the value **you** sent.

> **A command response carries the outcome, not data.** The reply round-trips
> `executed` + `error` (and the echoed correlation-id) but **not a reply body** —
> the requester observes an empty command body. Use a
> [query](../../queries/request-reply/) when you need to return a value.

## What's Happening

1. **OPEN** — each role opens its own AMQP 1.0 connection (`new Connection` +
   `connection.open()`). With no userinfo the SASL layer negotiates **ANONYMOUS**;
   rhea sends a non-empty `container-id` automatically.
2. **BEGIN** — each link creates its own session under the hood (responder and
   requester are on **separate connections**, which is what the connector's
   snooping guard requires — the dynamic reply node must be owned by the requester
   connection).
3. **ATTACH (requester reply node)** —
   `createReceiver({source:{address:"", dynamic:true}})` asks the server to create
   a **dynamic node**; the reply ATTACH echoes its address
   (`_amqp10.tmp.<connID>.<uuid>`), read with `replyReceiver.address`. This node is
   the requester's private mailbox for replies.
4. **ATTACH (requester sender)** —
   `createAwaitableSender({target:{address:"commands/<ch>"}})` attaches a link the
   server sees as a *receiver* (the client produces requests). The server grants
   credit on attach. Each `send()` is unsettled and resolves on the connector's
   `accepted`.
5. **ATTACH (responder receiver + anonymous sender)** — the responder attaches a
   receiver on `commands/<ch>` (server-sender link; the client consumes requests
   under manual credit, handler registered before `addCredit`) and an **anonymous
   sender** `createAwaitableSender({target:{}})` (null target) for replies.
6. **TRANSFER (request)** — the requester `send`s the command with
   `{reply_to: dynamicNode, correlation_id: id}`. The connector verifies the
   reply-to names a node **this connection owns** (snooping guard), routes the
   command to `SendCommand`, and settles the inbound request `accepted`.
7. **TRANSFER (reply)** — the responder receives the request, runs its handler, and
   replies via the anonymous sender with
   `{to: <reply-to>, correlation_id: <echoed>, application_properties:{x-opt-kubemq-executed, x-opt-kubemq-error}}`.
   The connector resolves `to` to the requester's dynamic node (server path
   `/responses/<RequestID>`) and delivers the reply there **unsettled,
   out-of-band**.
8. **Correlation + DETACH/CLOSE** — the requester arms its reply waiter **before**
   sending, then `accept`s the reply on its dynamic node, matches the
   `correlation_id`, reads the executed/error outcome; both connections close.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| requester reply node (KubeMQ → client) | source dynamic (`source:{address:"", dynamic:true}`) → `_amqp10.tmp.<connID>.<uuid>` | `first` (default) | client-granted (`addCredit(5)`) | reply delivered unsettled, out-of-band | request `reply_to` names this node | `Data` | reply-to MUST be connection-owned (snooping guard) |
| requester sender (client → KubeMQ) | target `commands/<ch>` | unsettled (default) | server-granted | request settled `accepted` once routed | `correlation_id` (or `message_id` fallback); optional `ttl` | `Data` | request, not the reply |
| responder receiver (KubeMQ → client) | source `commands/<ch>` | `first` (default) | client-granted (`addCredit(10)`) | `accept()` the request | reply-to = `/responses/<RequestID>`, correlation-id = RequestID (stamped by connector) | `Data` | pumped under credit, paused at `RpcMaxPending` |
| responder reply sender (client → KubeMQ) | target null (anonymous terminus, `target:{}`) | unsettled | server-granted | reply routed by `to` | `to` = request reply-to; `x-opt-kubemq-executed` (bool) + `x-opt-kubemq-error` (string) | `Data` | failure ⇒ `executed=false` reply (requester not left waiting) |

## Related Examples

- [queries/request-reply](../../queries/request-reply/) — same dynamic-node path, but the reply is **body + metadata only** (no executed/error props) and a failed query delivers **nothing** (the requester times out)
- [advanced/anonymous-terminus](../../advanced/anonymous-terminus/) — the null-target send path the responder's reply sender uses
- [queues/basic-send-receive](../../queues/basic-send-receive/) — the simplest produce/consume primitive

## Gotcha

> **RPC `reply-to` must name a connection-owned node.** The connector rejects a
> request whose `reply_to` is missing or names a node this connection does not own
> with `amqp:not-allowed` ("request missing reply-to" / "reply-to is not a node this
> connection owns") — this is the **snooping guard** that stops a client from
> directing a response to another client's node. Always use a dynamic node you
> opened on the **same connection** as the request sender, as this example does.
> Reply tokens are connection-scoped (no authz `Enforce`) and expire by TTL
> (`Request.Timeout` + 5s grace); a late reply is dropped and counted.
>
> **Arm the reply waiter before sending.** The reply arrives out-of-band on the
> dynamic node. Register the `message` handler (and grant credit) **before** the
> request `send()` resolves, or a fast reply can land before you are listening.
> This example arms `awaitReply` before `send`.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
