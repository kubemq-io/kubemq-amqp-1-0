# Java — Queues / Ack, Release & Redelivery

Accept vs release/redelivery on a KubeMQ Queue through **Apache Qpid JMS**
(`javax.jms`) — NO KubeMQ SDK. `message.acknowledge()` on a `CLIENT_ACKNOWLEDGE`
session settles a delivery `accepted` (removed); `session.recover()` releases
every un-acknowledged delivery so the broker requeues and REDELIVERS it with
`JMSRedelivered=true`, a grown `JMSXDeliveryCount`, and first-acquirer=false.

## Prerequisites

- Java 21+ and Maven 3.8+
- `org.apache.qpid:qpid-jms-client` **1.16.0** (`javax.jms`; parent-pinned)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default) at
  `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/java
mvn -pl queues/ack-release-redelivery exec:java
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 mvn -pl queues/ack-release-redelivery exec:java
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: queues/amqp10.examples.ack

[send] Produced: accept-me, release-me
[recv] accept-me    JMSXDeliveryCount=1 redelivered=false  -> ACCEPTED (removed)
[recv] release-me   JMSXDeliveryCount=1 redelivered=false  -> RELEASING (recover ⇒ requeue)
[recv] release-me   JMSXDeliveryCount=2 redelivered=true   -> REDELIVERED, then ACCEPTED
[recv] Queue drained to empty (released message resumed exactly once)

Done.
```

> Delivery order between `accept-me` and `release-me` on the first pass can vary;
> the redelivered `release-me` always carries `JMSRedelivered=true` and
> `JMSXDeliveryCount>=2`.

## What's Happening

1. **OPEN / BEGIN / ATTACH** — connect, begin a `CLIENT_ACKNOWLEDGE` session,
   produce `accept-me` + `release-me`, then attach a consumer.
2. **accept** — `accept-me` is acknowledged on first sight: `message.acknowledge()`
   settles `accepted` ⇒ the connector **AckRanges** it (removed).
3. **release** — `release-me` is left un-acknowledged, then `session.recover()`
   abandons it. Qpid JMS settles it `released` ⇒ the connector **NAckRanges** it:
   the message is requeued to the tail and redelivered.
4. **redelivery** — the released `release-me` comes back with `JMSRedelivered=true`
   and a grown `JMSXDeliveryCount` (the connector maps the KubeMQ receive-count
   onto the AMQP header: `delivery-count = ReceiveCount-1`,
   `first-acquirer = (ReceiveCount==1)`). It is acknowledged to drain the queue.
5. **DETACH / CLOSE** — try-with-resources tears everything down.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| receiver (KubeMQ → client) | source `queues/<ch>` | `first` (JMS default) | client-granted (prefetch) | `acknowledge()` ⇒ `accepted` ⇒ AckRange (removed); `recover()` ⇒ `released` ⇒ NAckRange (requeued + redelivered) | none | `Data` | `JMSRedelivered` / `JMSXDeliveryCount` surface the grown delivery-count |

## Related Examples

- [queues/basic-send-receive](../basic-send-receive/) — the at-least-once happy path (accept-only)
- [queues/settlement-modes](../settlement-modes/) — producer reliability tiers (pre-settled vs unsettled)

## Gotcha

> **`released` / `modified` increment the broker receive-count.** Each release
> requeues the message AND bumps its receive-count toward the broker's
> `MaxReceiveQueue` poison threshold. **There is no connector dead-letter
> exchange** — poison handling is a broker-side `MaxReceiveQueue` policy, not an
> AMQP-controllable per-link feature. A `reject` ⇒ discard (no requeue) has no
> first-class Qpid JMS verb on a plain `CLIENT_ACKNOWLEDGE` session; rely on the
> broker poison policy for messages that must be discarded.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT
see `connectivity/auth` + `guides/authentication.md`.
