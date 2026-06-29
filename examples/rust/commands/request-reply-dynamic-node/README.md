# Rust — Commands / Request-Reply (Dynamic Node)

Native AMQP 1.0 request/reply over KubeMQ **Commands** (RPC) with the native
`fe2o3-amqp` client — NO KubeMQ SDK, NO gRPC. The requester opens a dynamic reply
node, sends commands to `commands/<ch>` with a reply-to + correlation-id, and the
responder replies via an anonymous sender. A command always replies — success
(`executed=true`) OR failure (`executed=false` + error text) — so the requester is
never left waiting.

## Prerequisites

- Rust 1.94+ (stable) with `cargo`.
- `fe2o3-amqp` 0.15.1 + `fe2o3-amqp-types` 0.14.0 (pinned exact in the workspace
  `examples/rust/Cargo.toml`).
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/rust
cargo run -p request-reply-dynamic-node
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 cargo run -p request-reply-dynamic-node
```

The single program runs both the responder (a tokio task) and the requester against
the broker, so it is runnable standalone.

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

## What's Happening

1. **OPEN / BEGIN** — the requester and responder each open their own connection +
   session (one role per connection).
2. **ATTACH (dynamic reply node)** — the requester attaches a receiver whose source is
   `Source::builder().dynamic(true).build()` (no address). The connector creates a
   transient node and echoes its address back in the attached source; the requester
   reads it from `reply_rcv.source()` (a `_amqp10.tmp.<connID>.<uuid>` token).
3. **ATTACH (request sender + responder receiver)** — the requester attaches a sender
   on `commands/<ch>`; the responder attaches a receiver on `commands/<ch>` plus an
   **anonymous sender** (null target via `Target::builder().build()`).
4. **TRANSFER (request)** — each command is sent with `Properties.reply_to` = the
   dynamic node + a `correlation_id`. The connector verifies the reply-to names a node
   THIS connection owns (snooping guard → `amqp:not-allowed` otherwise) and routes the
   request to `SendCommand`.
5. **TRANSFER (reply)** — the responder replies on the anonymous sender with
   `Properties.to` = the request's reply-to and the echoed correlation-id, plus
   `x-opt-kubemq-executed` (bool) + `x-opt-kubemq-error` (string).
6. **Correlation** — the requester awaits the reply on the dynamic node and matches it
   by correlation-id (the connector echoes the requester's original id).
7. **Commands vs Queries** — the `"fail"` command still produces a reply
   (`executed=false`), so the requester is never left waiting. (A query — variant #10
   — delivers nothing on failure.)
8. **DETACH / CLOSE** — both roles tear down their links, sessions, and connections.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| requester reply node (KubeMQ → client) | source dynamic (`dynamic=true`, no address) | `First` (default) | `CreditMode::Auto(5)` | `accept` on reply | — | `Data` | server-assigned `_amqp10.tmp.*` address |
| requester sender (client → KubeMQ) | target `commands/<ch>` | `Unsettled` (explicit) | server-granted | `Accepted` per request | `Properties{reply_to, correlation_id}` | `Data` | reply-to must name a connection-owned node |
| responder receiver (KubeMQ → client) | source `commands/<ch>` | `First` (default) | `CreditMode::Auto(10)` | `accept` per request | — | `Data` | server-sender link under credit |
| responder reply sender (client → KubeMQ) | null target (anonymous) | `Unsettled` (explicit) | server-granted | — | `Properties{to, correlation_id}` + `x-opt-kubemq-executed`/`-error` | `Data` | routes by per-message `to` (`/responses/<RequestID>`) |

## Related Examples

- [queries/request-reply](../../queries/request-reply/) — the SAME dynamic-node path; reply is body+metadata only (no executed/error), and a failed query delivers nothing
- [advanced/anonymous-terminus](../../advanced/anonymous-terminus/) — the anonymous sender / per-message `to` mechanism the reply path uses
- [queues/basic-send-receive](../../queues/basic-send-receive/) — at-least-once produce/consume

## Gotchas

> **A COMMAND response carries the executed/error outcome, NOT a body.** The broker
> round-trips `executed` + `error` (and the echoed correlation-id) but not a reply
> body — the requester observes an empty command body. Use a **QUERY** (variant #10)
> when you need to return a value.

- **Reply-to must name a connection-owned node.** A reply-to that does not resolve to
  a node THIS connection owns is refused with `amqp:not-allowed` (snooping guard).
- **Both senders set an explicit settle-mode.** The request sender and the anonymous
  reply sender both pin `SenderSettleMode::Unsettled`; `fe2o3-amqp` defaults to `mixed`,
  which the connector rejects at ATTACH (`amqp:not-implemented`).
- **A failed command still replies.** `executed=false` + error text means the
  requester is never left waiting — unlike a query.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT see
`connectivity/auth` + `guides/authentication.md`.
