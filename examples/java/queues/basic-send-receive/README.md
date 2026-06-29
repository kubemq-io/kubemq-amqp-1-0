# Java — Queues / Basic Send-Receive

At-least-once produce + credit-based consume against the KubeMQ AMQP 1.0
connector using **Apache Qpid JMS** (`javax.jms`) — NO KubeMQ SDK. A JMS Queue
named `queues/<ch>` resolves to KubeMQ pattern=queues, channel=`<ch>`; the
producer is a server-receiver link and the `CLIENT_ACKNOWLEDGE` consumer is a
server-sender link whose `message.acknowledge()` settles each delivery
`accepted` (removed from the queue).

## Prerequisites

- Java 21+ and Maven 3.8+
- `org.apache.qpid:qpid-jms-client` **1.16.0** (the latest `javax.jms` line —
  pinned in the parent `examples/java/pom.xml`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/java
mvn -pl queues/basic-send-receive exec:java
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 mvn -pl queues/basic-send-receive exec:java
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: queues/amqp10.examples.basic  (KubeMQ pattern=queues, channel=amqp10.examples.basic)

[send] Produced 10 messages to queues/amqp10.examples.basic (accepted DISPOSITION each)
[recv] Consumed and accepted 10 messages (no loss)
[recv] Queue drained to empty (no further messages)

Done.
```

## What's Happening

1. **OPEN / BEGIN** — `JmsConnectionFactory("amqp://...")` + `createConnection()`
   open the connection (SASL ANONYMOUS by default); `createSession(false,
   CLIENT_ACKNOWLEDGE)` begins a session.
2. **ATTACH (sender)** — `createProducer(queue)` attaches a server-receiver link.
   The server grants credit on attach.
3. **TRANSFER / DISPOSITION (produce)** — each `producer.send` writes an unsettled
   TRANSFER and blocks until the connector returns an `accepted` DISPOSITION,
   confirming the broker stored the message (at-least-once).
4. **ATTACH (receiver) / FLOW** — `createConsumer(queue)` attaches a server-sender
   link; Qpid JMS grants the link credit (prefetch) via FLOW.
5. **TRANSFER / DISPOSITION (consume)** — each `consumer.receive()` returns one
   delivery; `message.acknowledge()` settles `accepted` ⇒ the connector AckRanges
   it and removes it from the queue.
6. **DETACH / CLOSE** — try-with-resources closes the consumer, session, and
   connection.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `queues/<ch>` | `mixed`/unsettled (default) | server-granted | `accepted` per transfer (broker stored) | none | `Data` (TextMessage) | each send blocks for the accepted DISPOSITION |
| receiver (KubeMQ → client) | source `queues/<ch>` | `first` (JMS default) | client-granted (Qpid JMS prefetch), auto-replenished | `message.acknowledge()` ⇒ `accepted` ⇒ AckRange (removed) | none | `Data` | CLIENT_ACKNOWLEDGE so each ack is an explicit settle |

## Related Examples

- [queues/ack-release-redelivery](../ack-release-redelivery/) — accept vs release/redelivery (`session.recover()`)
- [queues/settlement-modes](../settlement-modes/) — pre-settled (at-most-once) vs unsettled (at-least-once) producers
- [events/basic-pubsub](../../events/basic-pubsub/) — fire-and-forget fan-out (at-most-once, no replay)

## Gotcha

> **No AMQP transactions.** The connector has no transaction coordinator, so a JMS
> `Session.SESSION_TRANSACTED` session cannot commit (it fails cleanly). Use
> explicit settlement — `CLIENT_ACKNOWLEDGE` + `acknowledge()` (accept) or
> `recover()` (release) — for reliability, never transactions.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT
see `connectivity/auth` + `guides/authentication.md`.
