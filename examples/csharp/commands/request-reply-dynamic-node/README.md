# C# — Commands / Request-Reply (Dynamic Node)

Native AMQP 1.0 request/reply over KubeMQ **Commands** (RPC) with the native
`AMQPNetLite.Core` client — **NO kubemq SDK, NO gRPC**. The whole round-trip stays
in-protocol over a single broker connection per role. A requester opens a **dynamic
reply node**, sends commands to `commands/<ch>` with a reply-to + correlation-id,
and a responder replies through an **anonymous sender**. Both a successful command
(`executed=true`) and a failed one (`executed=false`) round-trip — neither hangs.

## Prerequisites

- .NET SDK **8.0**
- `AMQPNetLite.Core` **2.5.3** (pinned in `examples/csharp/Directory.Packages.props`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd commands/request-reply-dynamic-node
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
Address: commands/amqp10.examples.commands  (KubeMQ pattern=commands, channel=amqp10.examples.commands)

[responder] Listening on commands/amqp10.examples.commands (anonymous reply sender ready)
[requester] Dynamic reply node: _amqp10.tmp.<connID>.<uuid>
[requester] Sent command "reboot-node-7" (reply-to=dynamic node, correlation-id=corr-cmd-1)
[responder] Received command "reboot-node-7" (correlation-id=<RequestID>)
[responder] Replied to "reboot-node-7" (executed=True, error="")
[requester] Reply for "reboot-node-7" (correlation-id=corr-cmd-1): executed=True error="" body=""
[requester] Sent command "fail" (reply-to=dynamic node, correlation-id=corr-cmd-2)
[responder] Received command "fail" (correlation-id=<RequestID>)
[responder] Replied to "fail" (executed=False, error="command rejected by handler")
[requester] Reply for "fail" (correlation-id=corr-cmd-2): executed=False error="command rejected by handler" body=""

Done.
```

## What's Happening

1. **OPEN** — two connections, one per role (responder on a background Task,
   requester on the main thread).
2. **ATTACH (dynamic reply node)** — the requester attaches a `ReceiverLink` whose
   `Attach.Source.Dynamic = true`. The server creates a transient node and echoes
   its address in the ATTACH reply; AMQPNetLite delivers that reply to the
   `OnAttached` callback, where we read `attach.Source.Address` (a
   `_amqp10.tmp.<connID>.<uuid>` token).
3. **ATTACH (request sender)** — the requester attaches a `SenderLink` to
   `commands/<ch>`. The responder attaches a `ReceiverLink` on `commands/<ch>` (with
   credit) plus an **anonymous** `SenderLink` (`Attach.Target = null`) for replies.
4. **TRANSFER (request)** — each command sets `Properties.ReplyTo = <dynamic node>`
   and `Properties.CorrelationId`. The connector verifies the reply-to names a node
   **this connection owns** (snooping guard — otherwise `amqp:not-allowed`) and
   routes the command via `SendCommand`.
5. **TRANSFER (reply)** — the responder sets `Properties.To` to the delivered
   request's reply-to (the connector stamps it as `/responses/<RequestID>`), echoes
   the correlation-id, and adds `ApplicationProperties` `x-opt-kubemq-executed`
   (bool) + `x-opt-kubemq-error` (string). The reply travels **out-of-band** onto
   the dynamic node.
6. **Correlation** — the requester correlates the reply by correlation-id. It gets
   back its ORIGINAL `corr-cmd-N` (the connector echoes the requester's correlation
   id), while the responder sees the connector-stamped `RequestID`.
7. **Success vs failure** — `reboot-node-7` → `executed=True`; `fail` →
   `executed=False` + error text. **Both reply**, so the requester never hangs.
8. **DETACH / CLOSE** — links detach; both connections close.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| requester dynamic reply receiver | source dynamic (`Attach.Source.Dynamic=true`) → `_amqp10.tmp.<connID>.<uuid>` | `first` (default) | client-granted `SetCredit(5)` | `Accept` on the reply | msg: `Properties.CorrelationId` echoed back | `Data` (empty for commands) | server-assigned address read via `OnAttached` |
| requester command sender | target `commands/<ch>` | unsettled (default) | server-granted | `accepted` per send | msg: `Properties{ReplyTo, CorrelationId}` | `Data` | reply-to must name a connection-owned node |
| responder command receiver | source `commands/<ch>` | `first` (default) | client-granted `SetCredit(10)` | `Accept` the request | delivered `Properties.ReplyTo = /responses/<RequestID>` | `Data` | request pump |
| responder anonymous reply sender | target null (anonymous terminus) | unsettled (default) | server-granted | `accepted` per reply | msg: `Properties{To, CorrelationId}` + app props `x-opt-kubemq-executed`(bool), `x-opt-kubemq-error`(string) | `Data` | per-message routing by `To` |

## Related Examples

- [queries/request-reply](../../queries/request-reply/) — same dynamic-node RPC; reply = body + metadata only (no executed/error; failures deliver nothing)
- [advanced/anonymous-terminus](../../advanced/anonymous-terminus/) — the anonymous sender (null target) used for replies, in isolation

## Gotcha

> **The reply-to must name a node THIS connection owns.** The connector enforces a
> snooping guard: a `reply-to` that does not resolve to a node owned by the sending
> connection is refused with `amqp:not-allowed`. Always use the address echoed back
> from your own `DynamicAddress` attach (read via `OnAttached`), not a hand-built
> token. Dynamic reply nodes are also **node-local** — in a cluster the transient
> node lives on the node that owns the requester's connection.
>
> **A COMMAND reply carries the executed/error outcome, not a body.** The requester
> observes an empty command body even though the responder set one. Use a **query**
> (variant #10) when you need to return a value.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT
see `connectivity/auth` + `guides/authentication.md`.
