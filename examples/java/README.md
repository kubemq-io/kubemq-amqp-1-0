# Java — KubeMQ AMQP 1.0 Examples

Native **AMQP 1.0** examples driving the embedded KubeMQ AMQP 1.0 connector with
**Apache Qpid JMS** — NO KubeMQ SDK. A Qpid-JMS / ActiveMQ app points at KubeMQ
by changing only the connection-factory URI and the JMS destination name to the
`<pattern>/<channel>` connector address.

Library: **`org.apache.qpid:qpid-jms-client` 1.16.0** — the latest **`javax.jms`**
release of Qpid JMS (NOT Jakarta). See ["Why qpid-jms 1.16.0 (javax.jms)"](#why-qpid-jms-1160-javaxjms)
below.

## Prerequisites

- **Java 21+** and **Maven 3.8+**.
- `org.apache.qpid:qpid-jms-client` **1.16.0** (`javax.jms`; pinned in the parent
  `pom.xml`, downloaded from Maven Central on first build).
- A running KubeMQ broker with the **AMQP 1.0 connector enabled** (it is on by
  default) reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## Broker URL Environment Variable

Every example reads `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`). The dev
broker accepts **SASL ANONYMOUS** by default — no credentials needed.

```bash
export KUBEMQ_AMQP_URL=amqp://localhost:5672          # plain / ANONYMOUS (default)
export KUBEMQ_AMQP_URL=amqp://user:JWT@my-host:5672   # SASL PLAIN (JWT in password)
```

## Build

Verify every runnable variant compiles from the language root:

```bash
cd examples/java
mvn -q compile
```

## Run a variant

Each runnable variant is its own Maven module with a `Main` class wired through
the exec plugin. Run by module path:

```bash
cd examples/java
mvn -pl queues/basic-send-receive exec:java
```

Override the broker URL inline:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 mvn -pl events/basic-pubsub exec:java
```

## The 13 variants

| # | Group / Variant | Pattern | Status | Module path |
|---|---|---|---|---|
| 1 | [queues/basic-send-receive](queues/basic-send-receive) | Queues | runnable | `queues/basic-send-receive` |
| 2 | [queues/ack-release-redelivery](queues/ack-release-redelivery) | Queues | runnable | `queues/ack-release-redelivery` |
| 3 | [queues/settlement-modes](queues/settlement-modes) | Queues | runnable | `queues/settlement-modes` |
| 4 | [events/basic-pubsub](events/basic-pubsub) | Events | runnable | `events/basic-pubsub` |
| 5 | [events/consumer-group](events/consumer-group) | Events | **N/A (justified)** | folder + README only |
| 6 | [events/selector](events/selector) | Events | runnable | `events/selector` |
| 7 | [events-store/durable-replay](events-store/durable-replay) | Events Store | runnable | `events-store/durable-replay` |
| 8 | [events-store/start-positions](events-store/start-positions) | Events Store | **N/A (justified)** | folder + README only |
| 9 | [commands/request-reply-dynamic-node](commands/request-reply-dynamic-node) | Commands (RPC) | runnable | `commands/request-reply-dynamic-node` |
| 10 | [queries/request-reply](queries/request-reply) | Queries (RPC) | runnable | `queries/request-reply` |
| 11 | [advanced/multi-frame-large-payload](advanced/multi-frame-large-payload) | Queues | runnable | `advanced/multi-frame-large-payload` |
| 12 | [advanced/anonymous-terminus](advanced/anonymous-terminus) | (routes by `to`) | **N/A (justified)** | folder + README only |
| 13 | [connectivity/auth](connectivity/auth) | (connection) | runnable | `connectivity/auth` |

**10 runnable + 3 justified N/A = 13 folders + READMEs.** Variants 9–13 are
delivered alongside 1–8 (the folder + README for every cell always exists).

## The 3 N/A cells (current connector limitations for the Qpid JMS target)

All three are documented in full — never silently omitted. The folder + README
always exist; only the runnable program is absent. Each is a current connector
limitation for the Qpid JMS deep-compat target (no advertised capability and/or no
JMS surface for an arbitrary link property), **not** a Java defect.

| Cell | Why N/A | Supported Java alternative |
|---|---|---|
| **#5 `events/consumer-group`** | Two connector gaps: the connector advertises **no `SHARED-SUBS` capability** (so Qpid JMS `createSharedConsumer` / `createSharedDurableConsumer` throws *"Remote peer does not support shared subscriptions"*), **and** Qpid JMS exposes no API to set the `x-opt-kubemq-group` link property — so there is no Qpid-JMS path to consumer groups today. The other languages (Go/Python/C#/JS/Rust) set `x-opt-kubemq-group` directly. | [events/basic-pubsub](events/basic-pubsub) — a single (ungrouped) fan-out subscriber, the supported Java events path. |
| **#8 `events-store/start-positions`** | The `x-opt-kubemq-start` start position is an **arbitrary AMQP link (attach) property**. Qpid JMS exposes **no API to set arbitrary receiver-link properties**, so the start-position grammar (`first`/`sequence:<n>`/`time:<...>`) has no clean JMS surface. | [events-store/durable-replay](events-store/durable-replay) — durable subscriptions start at `new-only` and **resume** from their preserved cursor (the common case). |
| **#12 `advanced/anonymous-terminus`** | The variant needs a single null-target anonymous sender with per-message `properties.to`. The connector advertises **no `ANONYMOUS-RELAY` capability**, so Qpid JMS falls back to per-destination sender links rather than the null-target ATTACH the connector routes on — and JMS producers bind to a destination at create time anyway. | Per-pattern senders: [queues/basic-send-receive](queues/basic-send-receive), [events/basic-pubsub](events/basic-pubsub), [events-store/durable-replay](events-store/durable-replay). |

## JMS → KubeMQ address mapping

The JMS destination **name** is the connector address (explicit
`<pattern>/<channel>` — never rely on bare-address / `DefaultPattern`):

| JMS call | Connector address | KubeMQ pattern |
|---|---|---|
| `session.createQueue("queues/<ch>")` | `queues/<ch>` | Queues (move / competing-consumer) |
| `session.createTopic("events/<ch>")` | `events/<ch>` | Events (fire-and-forget fan-out) |
| `session.createTopic("events-store/<ch>")` | `events-store/<ch>` | Events Store (durable replay) |
| `session.createQueue("commands/<ch>")` / `createTopic` | `commands/<ch>` | Commands (RPC) |
| `session.createQueue("queries/<ch>")` / `createTopic` | `queries/<ch>` | Queries (RPC) |
| `session.createTemporaryQueue()` | dynamic node | RPC reply mailbox |

## Java idiom notes

- **Blocking JMS, session-per-thread.** A JMS `Session` (and its consumers /
  producers) is single-threaded — never share one across threads. Use one session
  per thread or a `MessageListener` for async dispatch.
- **Acknowledgement = settlement.** `CLIENT_ACKNOWLEDGE` + `message.acknowledge()`
  settles `accepted` (AckRange); `session.recover()` releases un-acknowledged
  deliveries (NAckRange ⇒ redelivery with `JMSRedelivered=true`).
- **No transactions.** A JMS `SESSION_TRANSACTED` session cannot commit — the
  connector has no transaction coordinator. Use explicit settlement for
  reliability.
- **Consumer groups are N/A on Qpid JMS today.** Qpid JMS has no API for the
  `x-opt-kubemq-group` link property, and the connector advertises no `SHARED-SUBS`
  capability — so the JMS 2.0 shared-subscription fallback
  (`createSharedDurableConsumer`) is refused with *"Remote peer does not support
  shared subscriptions"*. There is therefore no Qpid-JMS path to consumer groups
  yet (see [events/consumer-group](events/consumer-group)). Use a plain
  ([events/basic-pubsub](events/basic-pubsub)) subscriber, or another language, for
  groups.
- **Selectors are JMS-native.** `createConsumer(topic, "color='red' AND size>2")`
  becomes the AMQP source filter `apache.org:selector-filter:string` — Events and
  Events-Store only (rejected on `queues/`).
- **Subscribe before publish on Events.** Events have no replay; attach the
  consumer before producing (the examples sleep ~750ms for the subscription pump).

## Why qpid-jms 1.16.0 (javax.jms)

This repo's deep-compat target is the **`javax.jms`** API (the ActiveMQ /
classic-JMS migration path). Apache Qpid JMS splits cleanly by major line:

- **1.x line** (1.16.0 is the latest) depends on `jakarta.jms-api:2.0.3`, whose
  **package namespace is still `javax.jms`** (Jakarta EE 8 retained `javax.*`). It
  ships the full **JMS 2.0** API — shared/durable consumers, selectors.
- **2.x line** (e.g. 2.10.0) moved to the **`jakarta.jms` PACKAGE**
  (`jakarta.jms-api:3.x`) — a different namespace, not what this repo wants.

Therefore the parent `pom.xml` pins **1.16.0** to deliver the required `javax.jms`
API. Bump-and-lock via `/check-deps` against a live broker — stay on the 1.x line
to keep `javax.jms`.

---

**Authentication:** runs **ANONYMOUS** by default on a stock dev broker. For SASL
PLAIN with a KubeMQ JWT (JWT in the password, username audit-only) see
[connectivity/auth](connectivity/auth) and `docs/guides/authentication.md`.
