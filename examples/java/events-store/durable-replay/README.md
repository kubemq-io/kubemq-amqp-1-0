# Java — Events Store / Durable Replay

Durable subscriptions with resume over KubeMQ Events Store through **Apache Qpid
JMS** (`javax.jms`) — NO KubeMQ SDK. A JMS durable subscriber
(`connection.setClientID(id)` + `session.createDurableConsumer(topic, subName)`)
attaches with `terminus-expiry-policy=never`, which the connector maps onto a
durable Events Store subscription. The durable identity is the pair `(JMS
clientID, subscription name)` → `(container-id, link name)` → STAN durable; a
re-attach with the same identity resumes exactly where it left off.

## Prerequisites

- Java 21+ and Maven 3.8+
- `org.apache.qpid:qpid-jms-client` **1.16.0** (`javax.jms`, full JMS 2.0 API;
  parent-pinned)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default) at
  `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/java
mvn -pl events-store/durable-replay exec:java
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 mvn -pl events-store/durable-replay exec:java
```

## Expected Output

```
Broker:     amqp://localhost:5672
Address:    events-store/amqp10.examples.durable  (KubeMQ pattern=events-store, channel=amqp10.examples.durable)
Durable id: clientID="amqp10-examples-durable-container"  sub-name="durable-sub"

[recv] Durable receiver attached (first attach): clientID="amqp10-examples-durable-container" sub="durable-sub" expiry=never
[recv] First attach received 3 events: [es-000, es-001, es-002]

[conn] Durable subscriber DISCONNECTED (cursor preserved by the connector)
[send] Published 5 more events WHILE the durable subscriber was away
[recv] Durable receiver attached (re-attach): clientID="amqp10-examples-durable-container" sub="durable-sub" expiry=never
[recv] Re-attach RESUMED and received the 5 events published while away: [es-003, es-004, es-005, es-006, es-007]
[recv] No loss, no re-delivery of the already-consumed first 3 — the durable cursor resumed exactly
[recv] Durable subscription "durable-sub" unsubscribed (removed cleanly — no orphan left behind)

Done.
```

## What's Happening

1. **First attach** — `setClientID(...)` + `createDurableConsumer(topic, subName)`
   establishes a durable subscription on `events-store/<ch>` (expiry=never).
   Publish 3 events; receive all 3.
2. **Disconnect** — `consumer.close()` detaches the link but **keeps** the durable
   subscription registered; closing the connection preserves the cursor.
3. **Publish while away** — 5 more events land in the persisted Events Store.
4. **Re-attach** — re-connect with the SAME `clientID` and re-create the durable
   consumer with the SAME `subName`. The subscription RESUMES and delivers exactly
   the 5 missed events (not the first 3 again). This is the heart of the example.
5. **Clean teardown** — `consumer.close()` then `session.unsubscribe(subName)`
   permanently removes the durable subscription (the connector deletes the
   underlying durable cleanly), leaving no orphan behind. The demo then exits 0.
   Re-running starts a fresh durable from scratch. **This requires a connector build
   with the AMQP 1.0 JMS-compat fixes** — older connectors reject the deletion
   ATTACH with `amqp:not-found` (see the gotcha below).

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events-store/<ch>` | `mixed`/unsettled (default) | server-granted | `accepted` per transfer (persisted) | none | `Data` | each accepted transfer is durably stored at a monotonic sequence |
| receiver (KubeMQ → client) | source `events-store/<ch>` (terminus-expiry-policy `never`) | `first` (JMS default) | client-granted (prefetch) | `acknowledge()` advances the read cursor | `(clientID, subName)` → durable identity | `Data` | re-attach with the same identity resumes; `session.unsubscribe(subName)` sends a null-source ATTACH that the connector deletes cleanly (requires the JMS-compat connector build — see gotcha) |

## Related Examples

- [events-store/start-positions](../start-positions/) — the `x-opt-kubemq-start` start-position grammar (**N/A for Java** — see its README)
- [events/basic-pubsub](../../events/basic-pubsub/) — non-durable, at-most-once Events (no replay)
- [events/consumer-group](../../events/consumer-group/) — consumer groups (**N/A for Java** — no `SHARED-SUBS` advertisement; see its README)

## Gotcha

> **Durable subscriptions are node-local.** The durable cursor lives on the node
> that owned the original attach. In a cluster, reconnect to the **same node** (or
> run a single-node dev broker, as here) to resume. The `clientID` is half the
> durable identity, so it **MUST be stable across reconnects** — a changed clientID
> creates a new, empty subscription instead of resuming.

> **JMS `unsubscribe()` is supported and removes the durable cleanly — but it
> requires a connector build with the AMQP 1.0 JMS-compat fixes.** A JMS
> `session.unsubscribe(subName)` deletes a durable subscription by sending a
> **null-source ATTACH** for the durable link. On a connector that includes the
> JMS-compat fixes (as this example assumes), the connector deletes the underlying
> durable cleanly and the call completes — this example calls it at the end as a
> teardown step, so it leaves **no orphan subscription** behind and re-running
> starts a fresh durable from scratch. **On older connectors that lack the fixes,
> the deletion ATTACH is rejected with `amqp:not-found`** and the `unsubscribe`
> call fails; in that case drop the `session.unsubscribe(SUB_NAME)` line and rely on
> idempotent re-attach instead (re-running the SAME `(clientID, subName)` resumes
> the same cursor without accumulating orphans). The fixes are not yet released, so
> verify your broker build before depending on durable deletion from JMS.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT
see `connectivity/auth` + `guides/authentication.md`.
