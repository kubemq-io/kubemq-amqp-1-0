# Java — Events / Basic Pub-Sub

Fan-out, at-most-once pub/sub over KubeMQ Events through **Apache Qpid JMS**
(`javax.jms`) — NO KubeMQ SDK. A JMS Topic named `events/<ch>` resolves to
KubeMQ pattern=events, channel=`<ch>`. Events are a fire-hose: deliveries are
pre-settled, there is **no replay**, and an event that arrives at 0 link credit
is silently dropped — so the rules are **subscribe before publish** and **keep
standing credit**.

## Prerequisites

- Java 21+ and Maven 3.8+
- `org.apache.qpid:qpid-jms-client` **1.16.0** (`javax.jms`; parent-pinned)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default) at
  `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/java
mvn -pl events/basic-pubsub exec:java
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 mvn -pl events/basic-pubsub exec:java
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: events/amqp10.examples.pubsub  (KubeMQ pattern=events, channel=amqp10.examples.pubsub)

[recv] Subscribed to events/amqp10.examples.pubsub (Qpid JMS standing prefetch credit)
[recv] Subscription pump settled (waited 750ms before publishing)
[send] Published 20 events (fire-and-forget)
[recv] Received all 20 events (continuous credit ⇒ no 0-credit drop)

Done.
```

## What's Happening

1. **OPEN / BEGIN** — connect (ANONYMOUS) and begin an `AUTO_ACKNOWLEDGE` session.
2. **ATTACH (subscribe first)** — `createConsumer(topic)` attaches the
   server-sender link with a standing prefetch credit window **before any
   publish**. Events have no replay — a publish that beats the subscription is
   lost forever. The code sleeps ~750ms so the connector's subscription pump goes
   live before producing.
3. **TRANSFER (publish)** — a `NON_PERSISTENT` producer publishes 20 events; the
   connector sends them pre-settled (at-most-once), so there is no DISPOSITION to
   await.
4. **TRANSFER (receive)** — with standing credit the subscriber drains every
   event on the happy path.
5. **DETACH / CLOSE** — try-with-resources tears everything down.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events/<ch>` | pre-settled (connector forces `Settled:true`) | server-granted | none (fire-and-forget) | none | `Data` | NON_PERSISTENT delivery mode |
| receiver (KubeMQ → client) | source `events/<ch>` | `first` (JMS default) | client-granted standing prefetch, auto-replenished | n/a (pre-settled) | none | `Data` | subscribe-before-publish; keep continuous credit |

## Related Examples

- [events/consumer-group](../consumer-group/) — load-balanced consumer groups (**N/A for Java** — no `SHARED-SUBS` advertisement; see its README)
- [events/selector](../selector/) — SQL-92 JMS message selectors
- [events-store/durable-replay](../../events-store/durable-replay/) — durable subscriptions WITH replay (Events Store)

## Gotcha

> **Events dropped at 0 credit (silent data loss).** Events are at-most-once with
> no replay. If the subscriber is at 0 link credit when an event arrives, that
> event is **silently dropped** and counted on the server metric
> `kubemq_amqp10_events_dropped_no_credit_total` — never surfaced as a client
> error. Qpid JMS's standing prefetch + subscribe-before-publish avoid both the
> 0-credit drop and the attach↔publish race. For durable replay, use Events Store
> ([events-store/durable-replay](../../events-store/durable-replay/)).

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT
see `connectivity/auth` + `guides/authentication.md`.
