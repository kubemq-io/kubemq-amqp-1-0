# C# — Queries / Request-Reply (Dynamic Node)

Native AMQP 1.0 request/reply over KubeMQ **Queries** (RPC) with the native
`AMQPNetLite.Core` client — **NO kubemq SDK, NO gRPC**. The reply path is identical
to [commands](../../commands/request-reply-dynamic-node/): a requester opens a
**dynamic reply node**, sends to `queries/<ch>` with a reply-to + correlation-id, and
a responder replies through an **anonymous sender**. The **contrast**: a query reply
carries only the **body + metadata** (no executed/error props), and a **failed query
delivers nothing** — the requester simply times out.

## Prerequisites

- .NET SDK **8.0**
- `AMQPNetLite.Core` **2.5.3** (pinned in `examples/csharp/Directory.Packages.props`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd queries/request-reply
dotnet run
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 dotnet run
```

Runs ANONYMOUS by default (no userinfo in the URL).

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

## What's Happening

1. **OPEN** — two connections, one per role (responder on a background Task,
   requester on the main thread).
2. **ATTACH (dynamic reply node)** — the requester attaches a `ReceiverLink` whose
   `Attach.Source.Dynamic = true`; the server-assigned `_amqp10.tmp.<connID>.<uuid>`
   address is read from `attach.Source.Address` in the `OnAttached` callback.
3. **ATTACH (links)** — the requester attaches a `SenderLink` to `queries/<ch>`; the
   responder attaches a `ReceiverLink` on `queries/<ch>` (with credit) plus an
   **anonymous** `SenderLink` (`Attach.Target = null`) for replies.
4. **TRANSFER (request)** — each query sets `Properties.ReplyTo = <dynamic node>`
   and `Properties.CorrelationId`. The connector verifies the reply-to is owned by
   this connection (snooping guard) and routes the query.
5. **TRANSFER (reply)** — for a successful query the responder sets `Properties.To`
   to the request's reply-to + the echoed correlation-id and sends a body. **No**
   `x-opt-kubemq-executed` / `x-opt-kubemq-error` props — a query just fetches a
   value.
6. **Failure = silence** — the `ignore` query gets no reply. The connector delivers
   nothing on a failed/unanswered query, so the requester's `Receive` returns `null`
   at the deadline. That timeout **is** the failure signal.
7. **Correlation** — the requester matches replies by correlation-id (its original
   `corr-qry-N`).
8. **DETACH / CLOSE** — links detach; both connections close.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| requester dynamic reply receiver | source dynamic (`Attach.Source.Dynamic=true`) → `_amqp10.tmp.<connID>.<uuid>` | `first` (default) | client-granted `SetCredit(5)` | `Accept` on the reply | msg: `Properties.CorrelationId` echoed back | `Data` | server-assigned address read via `OnAttached`; **no reply on failure** |
| requester query sender | target `queries/<ch>` | unsettled (default) | server-granted | `accepted` per send | msg: `Properties{ReplyTo, CorrelationId}` | `Data` | reply-to must name a connection-owned node |
| responder query receiver | source `queries/<ch>` | `first` (default) | client-granted `SetCredit(10)` | `Accept` the request | none | `Data` | request pump |
| responder anonymous reply sender | target null (anonymous terminus) | unsettled (default) | server-granted | `accepted` per reply | msg: `Properties{To, CorrelationId}` — **NO executed/error app props** | `Data` | body + metadata only |

## Related Examples

- [commands/request-reply-dynamic-node](../../commands/request-reply-dynamic-node/) — same dynamic-node RPC; commands carry `executed`/`error` props and always reply (even on failure)
- [advanced/anonymous-terminus](../../advanced/anonymous-terminus/) — the anonymous sender (null target) used for replies, in isolation

## Gotcha

> **A failed or unanswered query delivers NOTHING — the timeout IS the failure
> signal.** Unlike a command (which always replies `executed=false` on failure so
> its requester is never left waiting), the connector delivers no reply when a query
> fails or goes unanswered (MQTT-bridge parity). Always set a sensible per-request
> deadline; this demo uses 5s, but the connector's own default is ~30s. Choose the
> per-request budget with the request `header.ttl`. As with commands, the reply-to
> must name a node THIS connection owns, and dynamic reply nodes are node-local.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT
see `connectivity/auth` + `guides/authentication.md`.
