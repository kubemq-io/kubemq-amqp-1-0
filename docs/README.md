# KubeMQ AMQP 1.0 — Documentation

**This is the documentation index.** It maps the doc tree for the KubeMQ embedded
**AMQP 1.0 connector** — the bridge that lets standard native AMQP 1.0 clients
drive KubeMQ's Queues, Events, Events-Store, Commands, and Queries patterns by
changing only the connection string and the node address.

The connector bridges the AMQP 1.0 wire protocol onto KubeMQ's messaging
patterns. Any standard AMQP 1.0 client (Go `Azure/go-amqp`, Python
`python-qpid-proton`, Java `qpid-jms-client`, .NET `AMQPNetLite.Core`, JS `rhea`,
Rust `fe2o3-amqp`) connects with **only a connection-string change** — no KubeMQ
SDK, no proto, no published package.

## Reading order

If you are new, read in this order:

1. **[getting-started.md](getting-started.md)** — point a client at a running
 broker and send your first message in minutes.
2. **[architecture.md](architecture.md)** — the connection → session → link
 model, the shared `amqpmux` front door, and how AMQP 1.0 maps onto the five
 KubeMQ patterns (plus the wire-format appendix).
3. **[concepts/](#concepts)** — one document per protocol idea, in the order
 below (links → addresses → flow-control → settlement → the five patterns →
 selectors).
4. **[guides/](#guides)** — task-oriented how-tos (auth, TLS, reliability, flow
 control, addressing).
5. **[reference/](#reference)** — the exact grammar, capability matrix, error
 conditions, observability surface, and the ActiveMQ migration cheat-sheet.

Then pick a language under [`../examples/`](../examples/) and run a variant.

## Root

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Connection / session / link model, the shared `amqpmux` front door, how AMQP 1.0 maps onto KubeMQ's patterns, the metadata envelope, and the wire-format appendix |
| [getting-started.md](getting-started.md) | Connection assumption + first message in minutes (no docker-compose) + ANONYMOUS-default auth banner |
| [configuration.md](configuration.md) | The 14 `CONNECTORS_AMQP10_*` connector env vars + defaults + validation + enable/disable |

## Concepts

| Document | Description |
|----------|-------------|
| [concepts/links-sessions-connections.md](concepts/links-sessions-connections.md) | The connection → session → link hierarchy and how each maps onto KubeMQ |
| [concepts/addresses-and-nodes.md](concepts/addresses-and-nodes.md) | The `<pattern>/<channel>` address grammar, longest-prefix matching, dynamic / anonymous terminus |
| [concepts/flow-control-and-credit.md](concepts/flow-control-and-credit.md) | Credit-based flow control, drain, and the standing-credit discipline |
| [concepts/settlement-and-delivery-state.md](concepts/settlement-and-delivery-state.md) | Settlement modes and the accepted / released / modified / rejected delivery states |
| [concepts/work-queues.md](concepts/work-queues.md) | Competing consumers, at-least-once delivery, ack / release / reject |
| [concepts/pub-sub.md](concepts/pub-sub.md) | Events fan-out, subscribe-before-publish, consumer groups |
| [concepts/durable-subscriptions.md](concepts/durable-subscriptions.md) | Events-Store durable replay and start positions |
| [concepts/rpc.md](concepts/rpc.md) | Commands and Queries request/reply over the native AMQP 1.0 reply path |
| [concepts/selectors.md](concepts/selectors.md) | Selector filters on events, and where they are not supported |

## Guides

| Document | Description |
|----------|-------------|
| [guides/authentication.md](guides/authentication.md) | SASL ANONYMOUS (default), PLAIN + JWT, EXTERNAL, and the audit surface |
| [guides/tls-and-mtls.md](guides/tls-and-mtls.md) | `amqps://:5671`, the server `Security` block, TLS / mTLS (documentation-only) |
| [guides/reliability.md](guides/reliability.md) | At-least-once delivery, redelivery, settlement-driven durability, no transactions |
| [guides/flow-control.md](guides/flow-control.md) | Granting / replenishing credit to avoid the zero-credit and stalled-credit drops |
| [guides/addressing.md](guides/addressing.md) | Building addresses for every pattern with the explicit prefix; channel charset |

## Reference

| Document | Description |
|----------|-------------|
| [reference/address-mapping.md](reference/address-mapping.md) | Formal `<pattern>/<channel>` grammar, prefix table, longest-prefix rule, channel validation |
| [reference/capabilities.md](reference/capabilities.md) | Supported sections / bodies / settle modes, forced limits, and the non-goals |
| [reference/error-conditions.md](reference/error-conditions.md) | The 13 symbolic `amqp:*` error conditions (no numeric codes) |
| [reference/connections-endpoint.md](reference/connections-endpoint.md) | The `/api/amqp10/*` surface, the 11 Prometheus metrics, and the audit events |
| [reference/migration-from-activemq.md](reference/migration-from-activemq.md) | Connection-string swap and behavioral deviations when moving from ActiveMQ |

## Examples

Runnable code examples in six languages live under [`../examples/`](../examples/).
The [examples index](../examples/README.md) carries the per-language coverage
matrix, run commands, and the universal client recipe. Repo-wide conventions
(addressing, env vars, the README template) live in
[`../SHARED-CONVENTIONS.md`](../SHARED-CONVENTIONS.md).

## Prerequisites

All documentation and examples assume:

- A **KubeMQ server with the AMQP 1.0 connector enabled** — the connector is
 **enabled by default** and listens on AMQP port **`5672`** (plain TCP / SASL),
 served by the shared `amqpmux` front door. There is **no docker-compose** in
 this repo; bring up a broker any way you like (see
 [getting-started.md](getting-started.md)).
- The broker reachable at **`KUBEMQ_AMQP_URL`** (default `amqp://localhost:5672`).
 Every example reads this single environment variable.
- TLS examples are documentation-only and target `amqps://…@host:5671` (active
 only when the server-global `Security` block is configured) — see
 [guides/tls-and-mtls.md](guides/tls-and-mtls.md).

By default the connector accepts **SASL ANONYMOUS**, so a stock dev broker is
clone-and-run with no credentials. For SASL PLAIN with a KubeMQ JWT in the
password, see [guides/authentication.md](guides/authentication.md) and the
[`connectivity/auth`](../examples/go/connectivity/auth/) example.

## Gotchas (read before you ship)

These are the behaviors that most often surprise developers coming from other
AMQP 1.0 brokers. Each is documented in full in the linked doc and surfaced again
in the relevant example READMEs. The first two are **data-loss footguns** — treat
them as first-class.

| # | Gotcha | Where it bites | Documented in |
|---|--------|----------------|---------------|
| 1 | **Events drop silently at zero credit.** Events TRANSFERs are pre-settled (at-most-once); a message that arrives at a consumer with no link credit is **dropped and counted** (`kubemq_amqp10_events_dropped_no_credit_total`). | `events/*` | [guides/flow-control.md](guides/flow-control.md), [concepts/pub-sub.md](concepts/pub-sub.md) |
| 2 | **Events-Store stalls and loses its window if credit is not replenished.** When a durable consumer's unsettled buffer fills, the new message plus the buffered window is dropped and the link is DETACHed with `amqp:resource-limit-exceeded`. | `events-store/*` | [guides/flow-control.md](guides/flow-control.md), [concepts/durable-subscriptions.md](concepts/durable-subscriptions.md) |
| 3 | **`released` / `modified` increment the receive-count.** Requeued queue messages come back with a grown delivery-count and push toward the broker's `MaxReceiveQueue` poison cap. | `queues/ack-release-redelivery` | [concepts/settlement-and-delivery-state.md](concepts/settlement-and-delivery-state.md), [guides/reliability.md](guides/reliability.md) |
| 4 | **Selectors are rejected on `queues/`.** A selector filter is honored only on `events/` and `events-store/` links; on a queue link it returns `amqp:not-implemented`. | `events/selector` | [concepts/selectors.md](concepts/selectors.md), [guides/addressing.md](guides/addressing.md) |
| 5 | **`AmqpSequence` bodies are rejected.** Producers must send `Data` (binary) or `AmqpValue`; an `AmqpSequence` body returns `amqp:not-implemented`. Empty body is valid. | every producer | [concepts/settlement-and-delivery-state.md](concepts/settlement-and-delivery-state.md), [reference/capabilities.md](reference/capabilities.md) |
| 6 | **Durable subscriptions and dynamic reply nodes are node-local.** They do not advertise cross-node reachability; reconnect to the same node. (RPC replies themselves are cluster-safe via the broker path.) | `events-store/*`, `commands/*`, `queries/*` | [concepts/durable-subscriptions.md](concepts/durable-subscriptions.md), [concepts/rpc.md](concepts/rpc.md) |
| 7 | **`rcv-settle-mode=second` is unsupported.** Receivers must use `first`; requesting `second` is DETACHed with `amqp:not-implemented`. | `queues/settlement-modes` | [concepts/settlement-and-delivery-state.md](concepts/settlement-and-delivery-state.md), [reference/capabilities.md](reference/capabilities.md) |
| 8 | **No AMQP transactions.** There is no transaction coordinator; use settlement (accept / release / reject) for reliability. | every producer | [guides/reliability.md](guides/reliability.md), [reference/capabilities.md](reference/capabilities.md) |
| 9 | **Channel charset is stricter than the array layer.** A channel may not be empty, exceed 255 chars, end with `.`, or contain whitespace, `*`, `>`, `;`, or `:` — violation → `amqp:not-found`. | every address | [guides/addressing.md](guides/addressing.md), [reference/address-mapping.md](reference/address-mapping.md) |
| 10 | **RPC `reply-to` must be connection-owned.** The connector creates a dynamic reply node for you; it never accepts an arbitrary user reply-to (snooping guard). | `commands/*`, `queries/*` | [concepts/rpc.md](concepts/rpc.md), [concepts/addresses-and-nodes.md](concepts/addresses-and-nodes.md) |
| 11 | **No vhost.** The AMQP 1.0 OPEN `hostname` is logged then ignored — there is no vhost option. (Reach 0-9-1 data via `queues/amqp.<vhost>.<queue>`; the two connectors do not share a namespace.) | `connectivity/auth`, addressing | [concepts/addresses-and-nodes.md](concepts/addresses-and-nodes.md), [guides/addressing.md](guides/addressing.md) |
