# Python — Commands / Request-Reply (Dynamic Reply Node)

**Native** AMQP 1.0 request/reply over KubeMQ **Commands** (RPC) with the native
`python-qpid-proton` blocking client — **no kubemq SDK, no gRPC**. The whole
round-trip stays in-protocol over a single broker connection per role, using a
**dynamic reply node** + `reply-to` / `correlation-id`.

A command that **fails** still produces a reply (`executed=false` + error text),
so the requester is never left waiting — the key contrast with
[queries/request_reply](../../queries/request_reply/).

## Prerequisites

- Python 3.10+
- `python-qpid-proton` 0.40.0 (pinned in `examples/python/pyproject.toml`; the
  prebuilt `python-qpid-proton-wheel` installs without a C toolchain)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/python
uv sync
uv run python commands/request_reply_dynamic_node/main.py
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 uv run python commands/request_reply_dynamic_node/main.py
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: commands/amqp10.examples.commands  (KubeMQ pattern=commands, channel=amqp10.examples.commands)

[responder] Listening on commands/amqp10.examples.commands (anonymous reply sender ready)
[requester] Dynamic reply node: _amqp10.tmp.<connID>.<uuid>
[requester] Sent command 'reboot-node-7' (reply-to=dynamic node, correlation-id=corr-cmd-1)
[responder] Received command 'reboot-node-7' (correlation-id=<RequestID>)
[responder] Replied to 'reboot-node-7' (executed=True, error='')
[requester] Reply for 'reboot-node-7' (correlation-id=corr-cmd-1): executed=True error='' body=''
[requester] Sent command 'fail' (reply-to=dynamic node, correlation-id=corr-cmd-2)
[responder] Received command 'fail' (correlation-id=<RequestID>)
[responder] Replied to 'fail' (executed=False, error='command rejected by handler')
[requester] Reply for 'fail' (correlation-id=corr-cmd-2): executed=False error='command rejected by handler' body=''

Done.
```

## What's Happening

The blocking client is single-threaded per connection, so the **responder** runs
on its own connection in its own thread while the **requester** drives the main
thread — honouring "one session/sender per thread".

1. **Requester opens a DYNAMIC reply node** —
   `create_receiver(None, dynamic=True, credit=5)`. The server creates a transient
   node and echoes its address, read via
   `reply_rcv.link.remote_source.address` (a `_amqp10.tmp.<connID>.<uuid>` token).
2. **Requester sends the command** to `commands/<ch>` with
   `Message.reply_to = <dynamic node>` and `Message.correlation_id`. The connector
   verifies the reply-to names a node **this connection owns** (snooping guard:
   otherwise `amqp:not-allowed`) and routes the request to `SendCommand`.
3. **Responder receives** on `commands/<ch>` (a server-sender link pumped under
   credit), accepts the request, and **replies via an anonymous sender**
   (`create_sender(None)` — null target) with `Message.address = <request reply_to>`
   (the connector resolves it as `/responses/<RequestID>`) + the echoed
   `correlation_id`.
4. **Command outcome envelope** — a command reply carries application properties
   `x-opt-kubemq-executed` (bool) + `x-opt-kubemq-error` (string). The
   `fail` command replies `executed=false` so its requester is never stuck.
5. **Requester correlates** the out-of-band reply on the dynamic node by
   correlation-id and prints `executed` / `error`.

Frame-level: `OPEN → BEGIN → ATTACH(dynamic source)` (reply node) +
`ATTACH(commands/<ch>)` (request) → `TRANSFER` (request) → `TRANSFER` (reply onto
the dynamic node) → `DISPOSITION(accept)` → `DETACH/CLOSE`.

> **A COMMAND response carries the executed/error outcome, NOT a body.** The
> requester observes an **empty** command reply body even though the responder
> sets one — use a **query** ([queries/request_reply](../../queries/request_reply/))
> when you need to return a value.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| requester reply node (KubeMQ → client) | dynamic source (server-assigned `_amqp10.tmp.*`) | `first` (default) | client-granted `credit=5` | `accepted` on the reply | `correlation-id` | `Data` | `dynamic=True`; address read from `remote_source.address` |
| requester sender (client → KubeMQ) | target `commands/<ch>` | unsettled (default) | server-granted | `accepted` | `reply-to`, `correlation-id` | `Data` | reply-to must name a connection-owned node |
| responder receiver (KubeMQ → client) | source `commands/<ch>` | `first` (default) | client-granted `credit=10` | `accepted` per request | request `reply-to`/`correlation-id` | `Data` | server-stamps reply-to as `/responses/<RequestID>` |
| responder reply sender (client → KubeMQ) | **anonymous** (null target) | unsettled (default) | server-granted | `accepted` | `to` (=reply-to), `correlation-id`, `x-opt-kubemq-executed`, `x-opt-kubemq-error` | `Data` | per-message routing via `Message.address` |

## Gotchas

> **Dynamic reply nodes are NODE-LOCAL** (like durable subscriptions). The
> transient node lives on the node that created it; in a cluster, keep the
> request/reply on a single connection (as here). Note: the RPC reply *delivery*
> itself is cluster-safe — only the dynamic node's lifetime is node-local.

- **Reply-to snooping guard.** The `reply-to` MUST name a node **this connection
  owns**, or the connector refuses the request with `amqp:not-allowed`.
- **Correlation-id fallback.** If the request carries no `correlation_id`, the
  connector falls back to the **message-id**; the responder mirrors that.
- **Commands ≠ queries.** A failed command always replies (`executed=false`); a
  failed query delivers **nothing** (the requester times out).

## Related Examples

- [queries/request_reply](../../queries/request_reply/) — same dynamic-node path; reply is **body + metadata only** (no executed/error), and a failure delivers nothing
- [advanced/anonymous_terminus](../../advanced/anonymous_terminus/) — the null-target sender pattern used for the reply leg
- [queues/basic_send_receive](../../queues/basic_send_receive/) — the simpler at-least-once queue round-trip

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
