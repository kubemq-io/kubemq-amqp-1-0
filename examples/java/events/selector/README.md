# Java — Events / Selector

JMS / SQL-92 message selectors over KubeMQ Events through **Apache Qpid JMS**
(`javax.jms`) — NO KubeMQ SDK. `session.createConsumer(topic, selector)` is the
JMS-native selector surface: Qpid JMS encodes the selector on the consumer's
ATTACH source filter under the OASIS key `apache.org:selector-filter:string`, and
the connector evaluates it against each event's application properties — delivering
only the matching events (non-matching events are silently withheld).

## Prerequisites

- Java 21+ and Maven 3.8+
- `org.apache.qpid:qpid-jms-client` **1.16.0** (`javax.jms`; parent-pinned)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default) at
  `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/java
mvn -pl events/selector exec:java
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 mvn -pl events/selector exec:java
```

## Expected Output

```
Broker:   amqp://localhost:5672
Address:  events/amqp10.examples.selector  (KubeMQ pattern=events, channel=amqp10.examples.selector)
Selector: color = 'red' AND size > 2

[recv] Subscribed to events/amqp10.examples.selector with selector
[recv] Subscription pump settled (waited 750ms before publishing)
[send] match-1       color=red   size=5 → should MATCH (color=red AND size>2)
[send] miss-blue     color=blue  size=9 → should be FILTERED OUT (color!=red)
[send] miss-small    color=red   size=1 → should be FILTERED OUT (size not > 2)
[send] match-2       color=red   size=3 → should MATCH (color=red AND size>2)
[send] miss-nocolor  color=<null> size=8 → should be FILTERED OUT (color IS NULL ⇒ UNKNOWN (3-valued))
[recv] delivered: match-1
[recv] delivered: match-2
[recv] Received exactly 2 matching event(s); 3 non-matching event(s) were silently withheld
[recv] Trailing receive(3s) returned null — drain confirms no non-matching events leaked

[probe] Attempting createConsumer on queues/amqp10.examples.selector WITH a selector (expected: rejection)
[probe] Rejected promptly (no hang): selector filter not supported on this address [condition = amqp:not-implemented]
[probe] queues/ is move-only — a selector is not supported there (amqp:not-implemented)

Done.
```

## What's Happening

1. **ATTACH (subscribe first)** — `createConsumer(topic, "color='red' AND
   size>2")`. A successful attach means the connector accepted (and echoed) the
   selector filter; a parse error or unsupported pattern would DETACH the link.
2. **TRANSFER (publish)** — 5 events with `color` (nullable) + `size` application
   properties (`setStringProperty` / `setIntProperty`).
3. **Filter** — the connector evaluates the selector against each event's
   application properties and delivers only `match-1` and `match-2`. The consumer
   receives **exactly the 2 known matches** (count-based), then issues a trailing
   `receive(timeout)` that returns `null` promptly, confirming no non-matching event
   leaked (the connector's `drain=true` FLOW fix makes a timed receive a reliable
   end-of-stream signal — see the drain gotcha below).
4. **Three-valued logic** — `miss-nocolor` has no `color`, so `color = 'red'` is
   NULL ⇒ UNKNOWN (not true) ⇒ withheld, even though it has nothing to disqualify
   it.
5. **Selector-on-queues rejection** — the example then attempts
   `createConsumer(queue, selector)` on a `queues/` source and proves the connector
   rejects it **promptly** with `amqp:not-implemented` (surfaced as a `JMSException`
   on `createConsumer`), rather than hanging. The program then exits cleanly (exit
   0). See the queues gotcha below.

> Selectors apply only to `events/` and `events-store/` — a selector on a `queues/`
> source is **not** supported (queues are move-only). This example **does** exercise
> that case in code now: with the JMS-compat connector fixes, a queues+selector
> ATTACH is answered with a prompt `amqp:not-implemented` DETACH instead of being
> left unanswered — see the queues gotcha below.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events/<ch>` | pre-settled | server-granted | none | app-props `color` (string), `size` (int) | `Data` | selector resolves against these app-props |
| receiver (KubeMQ → client) | source `events/<ch>` | `first` (JMS default) | client-granted (prefetch) | n/a (pre-settled) | source filter `apache.org:selector-filter:string` | `Data` | non-matching events silently withheld (copy semantics) |

## Related Examples

- [events/basic-pubsub](../basic-pubsub/) — unfiltered fan-out
- [events/consumer-group](../consumer-group/) — consumer groups (**N/A for Java** — no `SHARED-SUBS` advertisement; see its README)
- [events-store/durable-replay](../../events-store/durable-replay/) — selectors also apply on `events-store/`

## Gotcha

> **Selectors are not supported on `queues/` — but the rejection is now clean and
> prompt** (requires a connector build with the AMQP 1.0 JMS-compat fixes). A
> selector is honoured only on `events/` and `events-store/` consume links;
> `queues/` is move-only / competing-consumer, not filterable. A queues+selector
> ATTACH now DETACHes with `amqp:not-implemented`, which Qpid JMS surfaces as a
> `JMSException` on `createConsumer` (*"selector filter not supported on this
> address"*) — **no hang**. This example exercises that path directly and asserts
> the rejection. **Note:** older connectors that lack the JMS-compat fixes left the
> ATTACH **unanswered**, so Qpid JMS would block on `createConsumer` until its
> request timeout — **infinite by default**. The fixes are not yet released, so on
> an older broker bound a `jms.requestTimeout` on the URL (e.g.
> `amqp://localhost:5672?jms.requestTimeout=8000`) to surface a *request timed out*
> `JMSException` instead of hanging.

> **`receive(timeout)` is now a reliable end-of-stream signal** (requires the
> JMS-compat connector fixes). With the fixes, the connector completes a
> `drain=true` FLOW on a pub-sub link — it echoes the drained credit response — so a
> JMS `receive(timeout)` on an exhausted consumer returns `null` promptly when the
> stream is empty. This example relies on that: after the 2 matches it issues a
> trailing `receive(3s)` that returns `null`, confirming no non-matching event
> leaked. **Note:** older connectors **ignored** `drain=true` FLOW, so a timed
> receive could block for the full timeout regardless of whether the stream was
> genuinely empty; on such a broker, consume by expected count instead of relying on
> the trailing receive. The fixes are not yet released, so verify your broker build.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT
see `connectivity/auth` + `guides/authentication.md`.
