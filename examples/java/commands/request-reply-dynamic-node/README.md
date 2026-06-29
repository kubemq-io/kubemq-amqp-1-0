# Java — Commands / Request-Reply (Dynamic Reply Node)

JMS request/reply over KubeMQ **Commands** (RPC) with **Apache Qpid JMS**
(`javax.jms`) — **no KubeMQ SDK, no gRPC**. The requester opens a JMS
**temporary queue** as its dynamic reply node, sends a command to `commands/<ch>`
carrying that node as `JMSReplyTo` plus a `JMSCorrelationID`; the responder
consumes the command and replies — addressed to the request's `JMSReplyTo` (the
connector's `/responses/<RequestID>`) — stamping the command outcome as
`x-opt-kubemq-executed` / `x-opt-kubemq-error` AMQP application-properties.

This one program runs **both roles** (responder on a thread, requester on the
main flow) so it is runnable standalone against a broker. It demonstrates a
**successful** command (executed=true) **and** a **failed** command
(executed=false) — both round-trip, and the requester is never left waiting.

## Prerequisites

- Java 21+ and Maven 3.8+
- `org.apache.qpid:qpid-jms-client` **1.16.0** (the latest `javax.jms` line —
  pinned in the parent `examples/java/pom.xml`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/java
mvn -pl commands/request-reply-dynamic-node exec:java
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 mvn -pl commands/request-reply-dynamic-node exec:java
```

Runs ANONYMOUS by default — no userinfo in the URL.

## Expected Output

```
Broker:  amqp://localhost:5672
Address: commands/amqp10.examples.commands  (KubeMQ pattern=commands, channel=amqp10.examples.commands)

[responder] Listening on commands/amqp10.examples.commands (reply producer ready)
[requester] Dynamic reply node (temp queue): <server-assigned temp queue name>
[requester] Sent command "reboot-node-7" (reply-to=temp queue, correlation-id=corr-cmd-1)
[responder] Received command "reboot-node-7" (correlation-id=<RequestID>)
[responder] Replied to "reboot-node-7" (executed=true, error="")
[requester] Reply for "reboot-node-7" (correlation-id=corr-cmd-1): executed=true error="" body=""
[requester] Sent command "fail" (reply-to=temp queue, correlation-id=corr-cmd-2)
[responder] Received command "fail" (correlation-id=<RequestID>)
[responder] Replied to "fail" (executed=false, error="command rejected by handler")
[requester] Reply for "fail" (correlation-id=corr-cmd-2): executed=false error="command rejected by handler" body=""

Done.
```

(Interleaving of `[responder]` / `[requester]` lines varies — the two roles run
concurrently.)

> **Correlation-id on the wire.** The responder sees the connector-stamped
> `RequestID` as the delivered request's correlation-id, while the requester's
> reply correlation-id is its **original** `corr-cmd-N` — the connector echoes the
> requester's correlation-id back on the reply. Correlate on the value **you** sent.

> **A command response carries the outcome, not data.** The reply round-trips
> `executed` + `error` (and the echoed correlation-id) but **not a reply body** —
> the requester observes an empty command body. Use a
> [query](../../queries/request-reply/) when you need to return a value.

## What's Happening

1. **OPEN / BEGIN** — `JmsConnectionFactory("amqp://...")` + `createConnection()`
   open each role's connection (SASL ANONYMOUS by default); `createSession`
   begins a session. The factory sets `validatePropertyNames=false` so the
   hyphenated outcome app-properties are settable (see Gotcha).
2. **ATTACH (requester reply node)** — `session.createTemporaryQueue()` attaches a
   receiver on a server-assigned, connection-owned **transient node** — the JMS
   dynamic reply node. Its consumer is the requester's private reply mailbox.
3. **ATTACH (requester sender)** — `createProducer(commands)` attaches a
   server-receiver link on `commands/<ch>` (the client produces requests).
4. **ATTACH (responder consumer + reply producer)** — the responder attaches a
   consumer on `commands/<ch>` (server-sender link, pumped under credit) and an
   **unidentified** producer (`createProducer(null)`) for replies.
5. **TRANSFER (request)** — the requester `send`s the command with
   `setJMSReplyTo(tempQueue)` + `setJMSCorrelationID(id)`. The connector verifies
   the reply-to names a node **this connection owns** (snooping guard), routes the
   command to `SendCommand`, and settles the inbound request `accepted`.
6. **TRANSFER (reply)** — the responder runs its handler and replies with the
   producer addressed to `req.getJMSReplyTo()`, echoing `JMSCorrelationID` and
   setting `x-opt-kubemq-executed` (boolean) + `x-opt-kubemq-error` (string). The
   connector resolves the reply-to to the requester's temp node (server path
   `/responses/<RequestID>`) and delivers the reply there **out-of-band**.
7. **Correlation + DETACH/CLOSE** — the requester's reply consumer receives the
   reply, matches `JMSCorrelationID`, reads the executed/error outcome, then both
   connections close (try-with-resources).

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| requester reply node (KubeMQ → client) | source dynamic (JMS `createTemporaryQueue()`) | `first` (JMS default) | client-granted (Qpid JMS prefetch) | reply delivered out-of-band | request `JMSReplyTo` names this node | `Data` | reply-to MUST be connection-owned (snooping guard) |
| requester sender (client → KubeMQ) | target `commands/<ch>` | unsettled (default) | server-granted | request settled `accepted` once routed | `JMSCorrelationID` (or message-id fallback) | `Data` | request, not the reply |
| responder consumer (KubeMQ → client) | source `commands/<ch>` | `first` (JMS default) | client-granted (Qpid JMS prefetch) | `acknowledge`/AUTO settles the request | reply-to = `/responses/<RequestID>`, correlation-id = RequestID (stamped by connector) | `Data` | pumped under credit, paused at `RpcMaxPending` |
| responder reply producer (client → KubeMQ) | target = request's `JMSReplyTo` | unsettled | server-granted | reply routed by the reply-to address | `JMSCorrelationID` echoed; `x-opt-kubemq-executed` (bool) + `x-opt-kubemq-error` (string) | `Data` | failure ⇒ `executed=false` reply (requester not left waiting) |

## Related Examples

- [queries/request-reply](../../queries/request-reply/) — same temp-queue reply path, but the reply is **body + metadata only** (no executed/error props) and a failed query delivers **nothing** (the requester times out)
- [queues/basic-send-receive](../../queues/basic-send-receive/) — the simplest produce/consume primitive
- [connectivity/auth](../../connectivity/auth/) — SASL PLAIN with a KubeMQ JWT

## Gotcha

> **JMS app-property names must be valid identifiers — turn off validation for the
> hyphenated outcome props.** The connector's command outcome travels as the AMQP
> application-properties `x-opt-kubemq-executed` / `x-opt-kubemq-error`. JMS
> requires property names to be valid Java identifiers, so by default Qpid JMS
> **rejects** these hyphenated names in `setStringProperty` /
> `setBooleanProperty`. The JMS-native escape hatch is the connection-factory
> option `jms.validatePropertyNames=false` (URI) or
> `factory.setValidatePropertyNames(false)` (as used here) — without it the
> executed/error envelope is unreachable from the JMS API. The reply-to must still
> name a **connection-owned** temporary queue (the snooping guard: a reply-to that
> does not resolve to a connection-owned node is refused with `amqp:not-allowed`).

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT
see `connectivity/auth` + `guides/authentication.md`.
