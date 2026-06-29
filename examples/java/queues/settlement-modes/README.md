# Java — Queues / Settlement Modes

The two producer reliability tiers, side by side, on a KubeMQ Queue through
**Apache Qpid JMS** (`javax.jms`) — NO KubeMQ SDK. A pre-settled producer
(`jms.presettlePolicy.presettleProducers=true`) negotiates
`snd-settle-mode=settled` for at-most-once fire-and-forget; the default producer
is unsettled (at-least-once), blocking until the connector's `accepted`
DISPOSITION.

## Prerequisites

- Java 21+ and Maven 3.8+
- `org.apache.qpid:qpid-jms-client` **1.16.0** (`javax.jms`; parent-pinned)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default) at
  `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/java
mvn -pl queues/settlement-modes exec:java
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 mvn -pl queues/settlement-modes exec:java
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: queues/amqp10.examples.settlement

[send] Pre-settled (at-most-once): produced 10 messages — NO DISPOSITION awaited
[send] Unsettled (at-least-once): produced 10 messages — each accepted DISPOSITION
[recv] Drained 20 total — 10 pre-settled + 10 unsettled (rcv-settle-mode=first)
[recv] Queue drained to empty (no further messages)

Done.
```

> On a healthy broker pre-settled messages also drain — the difference is the
> **producer guarantee**, not the happy-path result: a pre-settled send returns
> before any broker confirmation, so a drop on the way in is invisible to the
> producer.

## What's Happening

1. **Pre-settled producer** — a `JmsConnectionFactory` whose pre-settle policy
   presettles producers makes Qpid JMS negotiate `snd-settle-mode=settled`. Each
   `send` marks the TRANSFER settled and returns WITHOUT waiting for a server
   DISPOSITION (at-most-once).
2. **Unsettled producer** — the default `JmsConnectionFactory` leaves the sender
   unsettled; each `send` blocks until the connector returns an `accepted`
   DISPOSITION confirming the broker stored the message (at-least-once — the
   variant #1 contract).
3. **Consume** — a single consumer drains both producers' messages with the
   default `rcv-settle-mode=first` (the only mode the connector supports).
4. **DETACH / CLOSE** — try-with-resources tears everything down.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (pre-settled) | target `queues/<ch>` | `snd-settle-mode=settled` | server-granted | none (settled at send) | none | `Data` | `jms.presettlePolicy.presettleProducers=true` URI option |
| sender (unsettled) | target `queues/<ch>` | `mixed`/unsettled (default) | server-granted | `accepted` per transfer | none | `Data` | each send blocks for the DISPOSITION |
| receiver | source `queues/<ch>` | `rcv-settle-mode=first` (JMS default) | client-granted (prefetch) | `accepted` ⇒ AckRange | none | `Data` | `second` is unsupported (see gotcha) |

## Related Examples

- [queues/basic-send-receive](../basic-send-receive/) — the unsettled at-least-once happy path
- [events/basic-pubsub](../../events/basic-pubsub/) — Events are ALWAYS pre-settled (at-most-once, no replay)

## Gotcha

> **`rcv-settle-mode=second` is rejected.** The connector replies
> `rcv-settle-mode=first` and DETACHes any link that requests `second` with
> `amqp:not-implemented`. Qpid JMS uses `first` by default, so this is not hit in
> normal use — but do not configure a `second`-mode receiver.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT
see `connectivity/auth` + `guides/authentication.md`.
