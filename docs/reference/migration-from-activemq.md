# Reference — Migrating from ActiveMQ (Qpid JMS) to KubeMQ

If you have a JMS application talking AMQP 1.0 to **Apache ActiveMQ** (Classic or
Artemis) via the **Qpid JMS** client, you can point it at the embedded KubeMQ AMQP 1.0
connector by changing **only the connection string and the JMS destination names** — no
code rewrite, no new client library. This is the connector's central promise.

> **Library:** `org.apache.qpid:qpid-jms-client` **1.16.0** — the latest **`javax.jms`**
> release (full JMS 2.0 API). Do **not** use Qpid JMS **2.x**: that line moved to
> `jakarta.jms`, which is a different namespace and an incompatible drop-in. All the
> Java examples in this repo pin **1.16.0 / `javax.jms`**.
>
> **Rollback is config-only.** Because only the URI and destination names change, you
> revert by pointing the same binary back at ActiveMQ — no redeploy of code.

---

## 1. The two changes

### 1.1 ConnectionFactory URI

```diff
- ConnectionFactory cf = new JmsConnectionFactory(
- "amqp://activemq-host:5672");
+ ConnectionFactory cf = new JmsConnectionFactory(
+ "amqp://kubemq-host:5672"); // AMQP 1.0 listener (amqpmux), ANONYMOUS by default
```

- KubeMQ's AMQP 1.0 listener is shared (`amqpmux`) on port **5672**; TLS is `amqps://`
 on **5671** (doc-only in this repo — see [TLS & mTLS](../guides/tls-and-mtls.md)).
- For SASL **PLAIN** with a KubeMQ JWT, pass the JWT as the **password** (the username is
 audit-only): `cf.createConnection("<user>", "<JWT>")`. See
 [Authentication](../guides/authentication.md).
- **No vhost.** The Qpid URI's `amqp.vhost` is ignored — KubeMQ's address space is flat
 and global.

### 1.2 JMS destination = `<pattern>/<channel>`

The single most important rule: a JMS destination name becomes a KubeMQ
`<pattern>/<channel>` address. Always use the **explicit prefix**.

```diff
- Queue q = session.createQueue("ORDERS.INBOUND");
+ Queue q = session.createQueue("queues/orders.inbound");

- Topic t = session.createTopic("telemetry");
+ Topic t = session.createTopic("events/telemetry"); // fan-out, at-most-once
+ // or: createTopic("events-store/telemetry") for durable replay
```

See [Address Mapping](./address-mapping.md) for the full grammar and longest-prefix rule.

---

## 2. JMS cheat-sheet

| Your ActiveMQ pattern | KubeMQ AMQP 1.0 mapping | Notes |
|---|---|---|
| `createQueue("Q")` | `createQueue("queues/Q")` | competing consumers, at-least-once, move semantics |
| `createTopic("T")` (non-durable) | `createTopic("events/T")` | fan-out, at-most-once, **no replay** |
| durable subscriber on a topic | `createTopic("events-store/T")` + `setClientID(...)` + `createDurableConsumer(topic, subName)` | durable identity = (clientID, subName) → (container-id, link-name); **node-local** |
| `CLIENT_ACKNOWLEDGE` + `message.acknowledge` | same | at-least-once accept |
| `AUTO_ACKNOWLEDGE` | same | pre-settled-ish (at-most-once) |
| `session.recover` | same | released → `JMSRedelivered`-marked redelivery (receive-count increments) |
| message selector `createConsumer(topic, "color='red'")` | **only on `events/` or `events-store/`** | a selector on a `queues/` link → `amqp:not-implemented` |
| `JMSReplyTo` + `JMSCorrelationID` request/reply | `createTemporaryQueue` as the dynamic reply node + `JMSReplyTo`/`JMSCorrelationID` | native RPC; `commands/` carry executed/error props, `queries/` reply body+metadata only |

---

## 3. Migrant deviations (read before you cut over)

These behaviours differ from a typical ActiveMQ broker. Each is a documented connector
contract, not a bug.

| # | Deviation | Impact on a migrant |
|---|---|---|
| 1 | **No AMQP transactions.** | A `session` created `TRANSACTED` and `session.commit` will fail — there is no coordinator. Remove transactional sessions. |
| 2 | **Events are at-most-once; subscribe-before-publish.** | A non-durable `events/` topic does **not** replay history. A subscriber must be attached **before** the publisher sends, or it misses messages. For replay use `events-store/` (durable). |
| 3 | **0-credit silent drop (events).** | If your consumer stops granting credit, `events/` messages are **silently dropped** (`kubemq_amqp10_events_dropped_no_credit_total`). Keep prefetch/credit flowing. |
| 4 | **Stalled-credit window loss (events-store).** | A durable consumer that stops replenishing credit overflows the credit-0 buffer and **DETACHes with lost messages** (`kubemq_amqp10_events_store_dropped_stalled_total`). |
| 5 | **`released`/`modified` increment receive-count.** | A `session.recover` redelivery raises the delivery-count and clears `FirstAcquirer` — your poison-message logic must read receive-count, not assume first-delivery. |
| 6 | **Selectors only on `events/` & `events-store/`.** | A selector on a `queues/` consumer is refused (`amqp:not-implemented`). Move selective consumption to a topic. |
| 7 | **`AmqpSequence` body rejected.** | Send `Data` (BytesMessage) or `AmqpValue` (Text/Object) — not an `AmqpSequence` body. |
| 8 | **Durable subs and dynamic/temporary nodes are node-local.** | A durable subscriber or a `TemporaryQueue` reply node resolves on the **same broker node**. RPC replies stay cluster-safe through the broker reply path, but a raw cross-node temporary-node direct send is unsupported. |
| 9 | **Java start-position is N/A.** | Qpid JMS exposes no API for the `x-opt-kubemq-start` link property, so you cannot set an arbitrary events-store start position from JMS. Use a native durable consumer with `new-only`, or a native client (Go/.NET/Python/Rust/JS) for `first`/`last`/`sequence:`/`time:`. See the `examples/java/events-store/start-positions` README. |
| — | **`rcv-settle-mode=second`, anonymous-relay, WebSocket, hot-reload, DLX, exactly-once, link-resumption** | all unsupported — see [Capabilities](./capabilities.md). |

---

## 4. From other AMQP 1.0 brokers (Solace / Azure Service Bus)

The connector speaks **standard AMQP 1.0**, so non-ActiveMQ AMQP 1.0 clients migrate the
same way — change the endpoint, then map destinations to `<pattern>/<channel>`.

- **Solace PubSub+** — a Solace AMQP 1.0 sender/receiver targets a queue or topic by
 name. Remap the Solace destination to `queues/<name>` (persistent) or `events/<name>`
 (direct). Solace exclusive/non-exclusive durable topic endpoints map to
 `events-store/<name>` durable subscriptions. Solace selectors map onto the pub/sub
 selector (`events/`-only).
- **Azure Service Bus** — Service Bus AMQP 1.0 entities (`queues/<q>`,
 `topics/<t>/subscriptions/<s>`) remap to `queues/<channel>` and
 `events-store/<channel>` (durable). Azure SB sessions, scheduled/deferred delivery,
 dead-lettering, and transactions have **no KubeMQ equivalent** — drop those features
 (see [Capabilities §3](./capabilities.md#3-rejected--unsupported-features)). Azure SB's
 `amqps://` + SAS-token auth maps to KubeMQ SASL PLAIN with a JWT.

For any AMQP 1.0 broker, the discipline is identical: **explicit `<pattern>/<channel>`
addresses**, continuous credit for at-most-once patterns, symbolic `amqp:*` error
conditions (never numeric codes), and the deviations in §3.

---

## 5. Interop proof — the Qpid JMS conformance harness

The server ships a **Qpid JMS conformance harness** that drives the live connector with a
real `javax.jms` Qpid JMS client and asserts the JMS-level behaviour migrants depend on:
 (**9 cases** across queues, topics, durable subscribers,
request/reply, `recover` redelivery, message bodies, and header round-trip).

**Reference it, do not copy it.** It is the server-side proof that a stock Qpid JMS app
works unchanged against KubeMQ; the runnable Java examples in this repo
(`examples/java/`) are the client-side demonstration of the same flows.

---

## Related

- Reference: [Address Mapping](./address-mapping.md) (destination → `<pattern>/<channel>`),
 [Capabilities](./capabilities.md) (what is unsupported),
 [Error Conditions](./error-conditions.md) (symbolic `amqp:*`, no numeric codes),
 [Connections & Observability](./connections-endpoint.md) (the two drop counters)
- Guides: [Authentication](../guides/authentication.md) (JWT in the JMS password),
 [Reliability](../guides/reliability.md) (transactions, receive-count, DLX),
 [Flow Control](../guides/flow-control.md) (0-credit / stalled-credit footguns),
 [Addressing](../guides/addressing.md), [TLS & mTLS](../guides/tls-and-mtls.md)
- Concepts: [Work Queues](../concepts/work-queues.md), [Pub/Sub](../concepts/pub-sub.md),
 [Durable Subscriptions](../concepts/durable-subscriptions.md), [RPC](../concepts/rpc.md),
 [Selectors](../concepts/selectors.md)
- Examples: `examples/java/` (Qpid JMS 1.16.0 / `javax.jms`); the two N/A folders
 `events-store/start-positions` and `advanced/anonymous-terminus` explain the Qpid JMS
 limitations.

---
