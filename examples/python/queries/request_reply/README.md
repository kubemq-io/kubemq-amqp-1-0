# Python — Queries / Request-Reply

**Native** AMQP 1.0 request/reply over KubeMQ **Queries** (RPC) with the native
`python-qpid-proton` blocking client — **no kubemq SDK, no gRPC**. The reply path
is the **same dynamic-reply-node** mechanism as
[commands/request_reply_dynamic_node](../../commands/request_reply_dynamic_node/),
but the semantics differ in two ways that define a query:

- A query reply carries **only the body + metadata** — **no** `x-opt-kubemq-executed`
  / `x-opt-kubemq-error` envelope. A query is a "fetch a value" call.
- A **failed/unanswered query delivers nothing** — the requester simply **times
  out**. The absence of a reply *is* the failure signal.

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
uv run python queries/request_reply/main.py
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 uv run python queries/request_reply/main.py
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: queries/amqp10.examples.queries  (KubeMQ pattern=queries, channel=amqp10.examples.queries)

[responder] Listening on queries/amqp10.examples.queries (anonymous reply sender ready)
[requester] Dynamic reply node: _amqp10.tmp.<connID>.<uuid>
[requester] Sent query 'get-temp-sensor-3' (reply-to=dynamic node, correlation-id=corr-qry-1)
[responder] Received query 'get-temp-sensor-3' (correlation-id=corr-qry-1)
[responder] Replied to 'get-temp-sensor-3' (body + metadata only, no executed/error props)
[requester] Reply for 'get-temp-sensor-3' (correlation-id=corr-qry-1): body='result:get-temp-sensor-3'
[requester] Sent query 'ignore' (reply-to=dynamic node, correlation-id=corr-qry-2)
[responder] Received query 'ignore' (correlation-id=corr-qry-2)
[responder] Ignoring 'ignore' -- NO reply sent (requester will time out)
[requester] No reply for 'ignore' within 5.0s -- query timed out (expected; failed queries deliver nothing)

Done.
```

## What's Happening

The blocking client is single-threaded per connection, so the **responder** runs
on its own connection in its own thread while the **requester** drives the main
thread.

1. **Requester opens a DYNAMIC reply node** —
   `create_receiver(None, dynamic=True, credit=5)`; the server-assigned address is
   read via `reply_rcv.link.remote_source.address`.
2. **Requester sends the query** to `queries/<ch>` with
   `Message.reply_to = <dynamic node>` + a `correlation_id`.
3. **Responder receives** on `queries/<ch>`, accepts, and **replies via an
   anonymous sender** (`create_sender(None)`) with `Message.address = <reply_to>`
   + the echoed `correlation_id` — and **nothing else** (no executed/error props).
4. **The contrast** — for the `ignore` query the responder sends nothing, so the
   requester's `receive(timeout=...)` raises and the requester reports a timeout.
   That is the only failure signal a query has.

Frame-level: identical to commands — `ATTACH(dynamic source)` + `ATTACH(queries/<ch>)`
→ `TRANSFER` (request) → `TRANSFER` (reply) → `DISPOSITION(accept)`. The failed
leg simply produces no reply `TRANSFER`.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| requester reply node (KubeMQ → client) | dynamic source (server-assigned `_amqp10.tmp.*`) | `first` (default) | client-granted `credit=5` | `accepted` on the reply | `correlation-id` | `Data` | `dynamic=True`; address read from `remote_source.address` |
| requester sender (client → KubeMQ) | target `queries/<ch>` | unsettled (default) | server-granted | `accepted` | `reply-to`, `correlation-id` | `Data` | reply-to must name a connection-owned node |
| responder receiver (KubeMQ → client) | source `queries/<ch>` | `first` (default) | client-granted `credit=10` | `accepted` per request | request `reply-to`/`correlation-id` | `Data` | — |
| responder reply sender (client → KubeMQ) | **anonymous** (null target) | unsettled (default) | server-granted | `accepted` | `to` (=reply-to), `correlation-id` — **no executed/error** | `Data` | reply = body + metadata only |

## Gotchas

> **A failed query delivers NOTHING.** Unlike a command (which always replies
> `executed=false` on failure), a query that fails or goes unanswered produces no
> reply at all — the requester **times out**. Always set a sensible per-request
> timeout (the connector default is ~30s; choose it via the request message ttl).

- **No executed/error envelope.** A query reply is purely `body + metadata`. If
  you need an explicit success/failure flag in the reply, use a **command**.
- **Reply-to snooping guard.** The `reply-to` MUST name a node **this connection
  owns**, or the connector refuses the request with `amqp:not-allowed`.

## Related Examples

- [commands/request_reply_dynamic_node](../../commands/request_reply_dynamic_node/) — same dynamic-node path, but with the `executed`/`error` envelope; a failed command still replies
- [advanced/anonymous_terminus](../../advanced/anonymous_terminus/) — the null-target sender used for the reply leg
- [events/basic_pubsub](../../events/basic_pubsub/) — fire-and-forget Events (no reply path)

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
