# Java — Queries / Request-Reply

JMS request/reply over KubeMQ **Queries** (RPC) with **Apache Qpid JMS**
(`javax.jms`) — **no KubeMQ SDK, no gRPC**. The reply path is identical to
[commands](../../commands/request-reply-dynamic-node/): a JMS **temporary queue**
as the dynamic reply node, `JMSReplyTo` + `JMSCorrelationID` on the request, and a
reply producer addressed to the request's `JMSReplyTo` (the connector's
`/responses/<RequestID>`).

The **contrast** with commands is the whole point of this variant:

- A query reply carries **only the body + metadata** — there are **no**
  `x-opt-kubemq-executed` / `x-opt-kubemq-error` application-properties. A query
  fetches a value; it has no execution-outcome envelope (so this example does not
  need `jms.validatePropertyNames=false` at all).
- A **failed / unanswered** query delivers **nothing**. The requester simply
  **times out** — the absence of a reply *is* the failure signal. (A failed
  command, by contrast, always replies `executed=false`, so its requester is never
  left waiting.)

This one program runs **both roles** (responder on a thread, requester on the
main flow). It demonstrates a **successful** query (reply round-trips) **and** a
query the responder ignores (the requester times out on a short demo deadline).

## Prerequisites

- Java 21+ and Maven 3.8+
- `org.apache.qpid:qpid-jms-client` **1.16.0** (the latest `javax.jms` line —
  pinned in the parent `examples/java/pom.xml`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/java
mvn -pl queries/request-reply exec:java
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 mvn -pl queries/request-reply exec:java
```

Runs ANONYMOUS by default — no userinfo in the URL.

## Expected Output

```
Broker:  amqp://localhost:5672
Address: queries/amqp10.examples.queries  (KubeMQ pattern=queries, channel=amqp10.examples.queries)

[responder] Listening on queries/amqp10.examples.queries (reply producer ready)
[requester] Dynamic reply node (temp queue): <server-assigned temp queue name>
[requester] Sent query "get-temp-sensor-3" (reply-to=temp queue, correlation-id=corr-qry-1)
[responder] Received query "get-temp-sensor-3" (correlation-id=corr-qry-1)
[responder] Replied to "get-temp-sensor-3" (body + metadata only, no executed/error props)
[requester] Reply for "get-temp-sensor-3" (correlation-id=corr-qry-1): body="result:get-temp-sensor-3"
[requester] Sent query "ignore" (reply-to=temp queue, correlation-id=corr-qry-2)
[responder] Received query "ignore" (correlation-id=corr-qry-2)
[responder] Ignoring "ignore" — NO reply sent (requester will time out)
[requester] No reply for "ignore" within 5000ms — query timed out (expected; failed queries deliver nothing)

Done.
```

(Interleaving of `[responder]` / `[requester]` lines varies — the two roles run
concurrently. The second leg blocks for the 5s demo timeout before printing the
timeout line.)

## What's Happening

1. **OPEN / BEGIN** — `JmsConnectionFactory("amqp://...")` + `createConnection()`
   open each role's connection (SASL ANONYMOUS by default); `createSession`
   begins a session.
2. **ATTACH (requester reply node)** — `session.createTemporaryQueue()` attaches a
   receiver on a server-assigned, connection-owned **transient node**. Its
   consumer is the requester's private reply mailbox.
3. **ATTACH (requester sender)** — `createProducer(queries)` attaches a
   server-receiver link on `queries/<ch>` (the client produces requests).
4. **ATTACH (responder consumer + reply producer)** — the responder attaches a
   consumer on `queries/<ch>` (pumped under credit) and an **unidentified**
   producer (`createProducer(null)`) for replies.
5. **TRANSFER (request)** — the requester `send`s the query with
   `setJMSReplyTo(tempQueue)` + `setJMSCorrelationID(id)`. The connector verifies
   the reply-to is connection-owned, routes to `SendQuery`, and settles the request
   `accepted`.
6. **TRANSFER (reply) — success leg** — the responder replies with the producer
   addressed to `req.getJMSReplyTo()`, echoing `JMSCorrelationID` and **only** the
   body (no executed/error props). The connector delivers it to the temp node; the
   requester correlates and reads the result.
7. **No reply — timeout leg** — for the `"ignore"` query the responder sends
   nothing. The connector delivers nothing (a failed query produces no reply), so
   the requester's `receive(timeout)` returns `null`. **The timeout is the failure
   signal.** DETACH/CLOSE follow.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| requester reply node (KubeMQ → client) | source dynamic (JMS `createTemporaryQueue()`) | `first` (JMS default) | client-granted (Qpid JMS prefetch) | reply delivered out-of-band | request `JMSReplyTo` names this node | `Data` | reply-to MUST be connection-owned (snooping guard) |
| requester sender (client → KubeMQ) | target `queries/<ch>` | unsettled (default) | server-granted | request settled `accepted` once routed | `JMSCorrelationID` (or message-id fallback) | `Data` | failed query ⇒ no reply ⇒ requester times out |
| responder consumer (KubeMQ → client) | source `queries/<ch>` | `first` (JMS default) | client-granted (Qpid JMS prefetch) | `acknowledge`/AUTO settles the request | reply-to = `/responses/<RequestID>`, correlation-id = RequestID (stamped by connector) | `Data` | pumped under credit, paused at `RpcMaxPending` |
| responder reply producer (client → KubeMQ) | target = request's `JMSReplyTo` | unsettled | server-granted | reply routed by the reply-to address | `JMSCorrelationID` echoed; **NO** executed/error props | `Data` | body + metadata only |

## Related Examples

- [commands/request-reply-dynamic-node](../../commands/request-reply-dynamic-node/) — the same temp-queue RPC path, but the reply carries `x-opt-kubemq-executed`/`x-opt-kubemq-error` and a **failure still delivers a reply** (the requester is never left waiting)
- [events/basic-pubsub](../../events/basic-pubsub/) — fire-and-forget fan-out (no reply path)
- [queues/basic-send-receive](../../queues/basic-send-receive/) — the simplest produce/consume primitive

## Gotcha

> **A failed query has no error envelope — it just times out.** Unlike a command
> (which always replies `executed=false` + error text on failure), the connector
> delivers **nothing** for a query that fails, times out, or is ignored by the
> responder. The requester's `receive(timeout)` returning `null` **is** the
> failure signal, so always consume replies with a bounded `receive(...)` deadline
> (never a blocking `receive()`). The connector's own default per-request timeout
> is ~30s; this example uses a short 5s demo deadline so the unanswered leg
> surfaces quickly. The reply-to must still name a **connection-owned** temporary
> queue (the same snooping guard as commands — `amqp:not-allowed` otherwise).

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT
see `connectivity/auth` + `guides/authentication.md`.
