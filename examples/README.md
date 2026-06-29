# KubeMQ AMQP 1.0 Examples

Runnable, copy-paste examples that drive the KubeMQ embedded **AMQP 1.0**
connector using **standard, native AMQP 1.0 client libraries** — there is **no
KubeMQ SDK, no proto, and no published package**. Each example speaks to the
connector exactly the way any off-the-shelf AMQP 1.0 client would.

**13 variants × 6 languages = 78 example folders** (75 runnable + 3 justified
Java `N/A` cells). Every cell — including the N/A ones — has a folder and a
README; the matrix is never silently incomplete.

> See [`../SHARED-CONVENTIONS.md`](../SHARED-CONVENTIONS.md) for the repo-wide
> conventions (addressing, env var, the per-example README template, settlement /
> credit defaults). See [`../docs/`](../docs/) for the connector reference.

## Connection

Every example reads a **single** environment variable:

```bash
# default: amqp://localhost:5672
export KUBEMQ_AMQP_URL="amqp://localhost:5672"
```

A URL with no userinfo negotiates **SASL ANONYMOUS** — the runnable default on a
stock dev broker, no credentials required.

> **Auth.** Examples run **ANONYMOUS by default**. The documented production
> contract is **SASL PLAIN** with the KubeMQ JWT carried in the **password**
> (the username is informational / audit-only). See the `connectivity/auth`
> variant and [`../docs/guides/authentication.md`](../docs/guides/authentication.md).
> TLS / mTLS is **documentation-only** — see
> [`../docs/guides/tls-and-mtls.md`](../docs/guides/tls-and-mtls.md).

## Languages, pinned libraries & run commands

| Language | Library (pinned) | Prereq | Run a variant |
|----------|------------------|--------|---------------|
| Go | `github.com/Azure/go-amqp` v1.7.0 | Go 1.24+ | `cd examples/go && go run ./<group>/<variant>` |
| Python | `python-qpid-proton` 0.40.0 (+ wheel), via **uv** | Python 3.10+, `uv` | `cd examples/python && uv sync && uv run python <group>/<variant>/main.py` |
| Java | `org.apache.qpid:qpid-jms-client` 1.16.0 (latest `javax.jms`) | Java 21+, Maven 3.8+ | `cd examples/java && mvn -pl <group>/<variant> exec:java` |
| C# / .NET | `AMQPNetLite.Core` 2.5.3 | .NET 8 | `cd examples/csharp/<group>/<variant> && dotnet run` |
| JS / TS | `rhea` 3.0.4 + `rhea-promise` 3.0.3 | Node 20+ | `cd examples/javascript && npm install && npx tsx <group>/<variant>/index.ts` |
| Rust | `fe2o3-amqp` 0.15.1 (`rustls`) | Rust 1.94+ | `cd examples/rust && cargo run -p <variant-crate>` |

> **Folder casing:** kebab-case dirs for Go / Java / C# / JS / Rust;
> **snake_case** for Python (e.g. `events_store/durable_replay`). The Rust crate
> name passed to `cargo run -p` is the **variant** name (e.g.
> `basic-send-receive`), not `<group>-<variant>`. Each library pin is locked at
> the version above; re-pin via `/check-deps` and dial-test against the live
> `amqpmux` 1.0 listener.

### Per-language idiom notes

- **Go** `Azure/go-amqp` — the connector's own reference client; **one
 `*Sender` / `*Session` per goroutine** (a session is not concurrency-safe).
- **Python** `python-qpid-proton` — sync `BlockingConnection`; install the
 prebuilt wheel so examples clone-and-run with no C toolchain.
- **Java** `qpid-jms-client` 1.16.0 — the latest **`javax.jms`** (not Jakarta)
 release; session-per-thread. No API for arbitrary link properties, and the
 connector advertises no `SHARED-SUBS` / `ANONYMOUS-RELAY` capabilities (this
 drives the 3 N/A cells: #5 consumer-group, #8 start-positions, #12
 anonymous-terminus).
- **C#** `AMQPNetLite.Core` — task-based async; selectors need manual
 `Source.FilterSet` Map/Symbol plumbing.
- **JS/TS** `rhea` + `rhea-promise` — event-driven wrapped in promises;
 `rhea` is in maintenance mode.
- **Rust** `fe2o3-amqp` 0.15.1 — async/await on tokio; pre-1.0 with a churning
 API (the `FilterSet`/source-filter surface has moved between releases), so the
 pin is **exact** (`=0.15.1`).

## The 13 variants

Grouped by KubeMQ pattern. Each variant is traceable to a connector integration
`Test*`.

| # | Group / Variant | KubeMQ pattern | Demonstrates |
|---|-----------------|----------------|--------------|
| 1 | `queues/basic-send-receive` | Queues | at-least-once produce + credit consume + `accept`; queue drains, no loss |
| 2 | `queues/ack-release-redelivery` | Queues | `accept` removes; `release`/`modify` requeue (grown delivery-count); `reject` discards |
| 3 | `queues/settlement-modes` | Queues | unsettled (at-least-once) vs pre-settled (at-most-once); `rcv-settle-mode=second` → `amqp:not-implemented` |
| 4 | `events/basic-pubsub` | Events | fan-out at-most-once; subscribe-before-publish; **grant credit continuously (0-credit ⇒ silent drop)** |
| 5 | `events/consumer-group` | Events | `x-opt-kubemq-group`: `g1` receivers split, `g2` gets the full stream (**Java N/A** — no `SHARED-SUBS` advertisement + no link-property API) |
| 6 | `events/selector` | Events | SQL92 selector (`apache.org:selector-filter:string`); selector on `queues/` → `amqp:not-implemented` |
| 7 | `events-store/durable-replay` | Events-Store | durable sub + resume; expiry `never`, stable `container-id` + link name |
| 8 | `events-store/start-positions` | Events-Store | `x-opt-kubemq-start` grammar (`first`/`new-only`/`last`/`sequence:`/`time:`/`time-delta:`); malformed → `amqp:invalid-field` |
| 9 | `commands/request-reply-dynamic-node` | Commands (RPC) | **native** RPC: dynamic reply node, `reply-to`+`correlation-id`; reply carries `x-opt-kubemq-executed`/`-error` |
| 10 | `queries/request-reply` | Queries (RPC) | same dynamic-reply path; reply = **body + metadata only** (no executed/error props); failure ⇒ requester times out |
| 11 | `advanced/multi-frame-large-payload` | Queues | body > `max-frame-size` fragments and reassembles bit-exact; over 100 MiB → `amqp:link:message-size-exceeded` |
| 12 | `advanced/anonymous-terminus` | (routes by `to`) | null-target sender selects destination per-message via `properties.to`; bad `to` → `amqp:precondition-failed` |
| 13 | `connectivity/auth` | (connection) | the one runnable auth variant: SASL **PLAIN** with a KubeMQ JWT in the password; denied attach → `amqp:unauthorized-access` |

## Coverage matrix

✅ = runnable program + README · N/A = README-only (justified client-library
limitation; folder + README always present).

| # | Variant | Go | Python | Java | C# | JS/TS | Rust |
|---|---------|----|--------|------|----|-------|------|
| 1 | queues/basic-send-receive | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 2 | queues/ack-release-redelivery | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 3 | queues/settlement-modes | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 4 | events/basic-pubsub | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 5 | events/consumer-group | ✅ | ✅ | **N/A³** | ✅ | ✅ | ✅ |
| 6 | events/selector | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 7 | events-store/durable-replay | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 8 | events-store/start-positions | ✅ | ✅ | **N/A¹** | ✅ | ✅ | ✅ |
| 9 | commands/request-reply-dynamic-node | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 10 | queries/request-reply | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 11 | advanced/multi-frame-large-payload | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 12 | advanced/anonymous-terminus | ✅ | ✅ | **N/A²** | ✅ | ✅ | ✅ |
| 13 | connectivity/auth | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

**75 runnable + 3 N/A = 78 folders.**

> **¹ Java — `events-store/start-positions` is N/A.** `x-opt-kubemq-start` is an
> arbitrary AMQP link (ATTACH) property, and **Apache Qpid JMS exposes no API to
> set arbitrary receiver-link properties**. The Java README points to
> `events-store/durable-replay` (the supported JMS durable path: `new-only` +
> resume). This is a client-library limit of the deep-compat target, not a
> connector gap.
>
> **² Java — `advanced/anonymous-terminus` is N/A.** The connector advertises
> **no `ANONYMOUS-RELAY` capability** (its `serverOpen` sets no offered/desired
> caps), and Qpid JMS has no API to force a raw null-target sender link — so JMS
> falls back to per-destination sender links instead of emitting the single
> null-target ATTACH the connector routes on. The Java README points to the
> per-pattern senders in variants #1 / #4 / #7.
>
> **³ Java — `events/consumer-group` is N/A.** Two connector gaps block it: the
> connector advertises **no `SHARED-SUBS` capability**, so Qpid JMS
> `createSharedConsumer` / `createSharedDurableConsumer` throws *"Remote peer does
> not support shared subscriptions"*; **and** Qpid JMS has no API to set the
> connector's `x-opt-kubemq-group` link property — so there is no Qpid-JMS path to
> consumer groups today. The Java README points to `events/basic-pubsub` (#4,
> ungrouped fan-out). The other languages (Go/Python/C#/JS/Rust) set
> `x-opt-kubemq-group` directly and ship this variant fully. This is a current
> connector limitation, not a Java defect.

## Universal client recipe

Every example — in every language — follows the same five rules. If you write
your own client, follow them too:

1. **Always emit the explicit `<pattern>/<channel>` prefix.** Never rely on a
 bare address or the server's `DefaultPattern` — it is non-deterministic. Use
 `queues/...`, `events/...`, `events-store/...`, `commands/...`, `queries/...`.
2. **Send a stable, non-empty `container-id`.** It is required on `OPEN` (empty →
 `amqp:invalid-field`) and is half the durable-subscription identity — keep it
 **stable across reconnects** for durable subscribers.
3. **Grant link credit before you expect delivery.** The server never delivers
 without credit. For consumers, set a standing credit window and replenish it.
4. **For Events / Events-Store: subscribe (and grant credit) BEFORE you
 publish.** Events are at-most-once with no replay; a message that arrives at a
 consumer with **zero credit is dropped and counted** — this is the #1 data-loss
 footgun. Events-Store stalls and loses its window if credit is not replenished.
5. **Resolve delivery state explicitly.** `accept` removes (AckRange);
 `release`/`modify` requeue (and increment the receive-count); `reject`
 discards. Use settlement, not transactions — there is no transaction
 coordinator. Send `Data` or `AmqpValue` bodies only; `AmqpSequence` is rejected.

For RPC (commands/queries): let the client create a **dynamic reply node** and
set `reply-to` + `correlation-id`; never hand the connector an arbitrary
reply-to (it is refused by the snooping guard). Commands replies carry
`x-opt-kubemq-executed` / `x-opt-kubemq-error`; queries replies are body +
metadata only.

---
