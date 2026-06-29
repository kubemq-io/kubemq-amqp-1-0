# Reference — Connections & Observability

This reference documents the connector's observability surface: the **HTTP detail
endpoints**, the **11 Prometheus metric families**, the **SSE metrics group**, and the
**audit events** the AMQP 1.0 connector emits.

---

## 1. HTTP detail endpoints

The connector registers two read-only JSON routes on the internal API port (`8080`),
network-protected like all `/api/*` routes .
They are registered up-front and nil-check the live provider, so they respond `200` with
**empty lists** until the connector is wired (and even when AMQP 1.0 is disabled),
returning `503` only while the API service is not yet ready.

| Method & path | Returns |
|---|---|
| `GET /api/amqp10/connections` | `{ "connections": [Amqp10ConnectionDTO…], "total": <n> }` |
| `GET /api/amqp10/links` | `{ "links": [Amqp10LinkDTO…], "total": <n> }` |

### `Amqp10ConnectionDTO`

| JSON field | Type | Meaning |
|---|---|---|
| `client_id` | string | auth ClientID, or the container-id when anonymous |
| `container_id` | string | OPEN container-id (sanitized) |
| `product` | string | from OPEN properties, if the client sent them |
| `version` | string | from OPEN properties, if sent |
| `sessions` | int | live sessions on the connection |
| `links` | int | live links across all sessions |
| `connected_at` | string | RFC3339 timestamp |
| `source_ip` | string | peer IP |
| `sasl` | string | `plain` / `anonymous` / `external` / `none` |
| `tls` | bool | TLS-terminated connection |

### `Amqp10LinkDTO`

| JSON field | Type | Meaning |
|---|---|---|
| `client_id` | string | owning connection's ClientID |
| `name` | string | peer-assigned link name |
| `role` | string | `sender` / `receiver` (**server** perspective) |
| `address` | string | resolved terminus address |
| `pattern` | string | `queues` / `events` / `events-store` / `commands` / `queries` / … |
| `channel` | string | resolved KubeMQ channel |
| `credit` | int64 | current link credit (server view) |
| `unsettled` | int64 | deliveries awaiting settlement |
| `durable` | bool | durable subscription link |
| `dynamic` | bool | dynamic-node terminus link |

> AMQP 1.0 has no exchange/binding topology, so there is **no `Topology` endpoint**
> (unlike the 0-9-1 connector). The link list *is* the topology view.

---

## 2. Prometheus metrics — 11 families

The connector exposes **11** `kubemq_amqp10_*` metric families: **3 gauges + 8 counters**
. All are scraped from the standard
KubeMQ metrics endpoint.

### Gauges (3)

| Metric | Labels | Meaning |
|---|---|---|
| `kubemq_amqp10_connections` | — | current open connector connections |
| `kubemq_amqp10_sessions` | — | current open connector sessions |
| `kubemq_amqp10_links` | `role` (`sender`/`receiver`) | current attached links by server role |

### Counters (8)

| Metric | Labels | Meaning |
|---|---|---|
| `kubemq_amqp10_transfers_total` | `direction` (`in`/`out`), `pattern` | transfers by direction and KubeMQ pattern |
| `kubemq_amqp10_dispositions_total` | `outcome` (`accepted`/`released`/`modified`/`rejected`) | terminal dispositions by outcome |
| `kubemq_amqp10_rpc_requests_total` | `pattern` (`commands`/`queries`) | RPC requests routed through the connector |
| `kubemq_amqp10_errors_total` | `scope` (`conn`/`session`/`link`) | connector error conditions by scope |
| `kubemq_amqp10_transfers_in_dropped_total` | — | **inbound transfers dropped: oversize / no-consumer / pre-settled failure** |
| `kubemq_amqp10_events_dropped_no_credit_total` | — | events dropped on an outbound link with **no credit** (fire-hose semantics) |
| `kubemq_amqp10_events_store_dropped_stalled_total` | — | events-store messages dropped when the **credit-0 buffer stalled** (link DETACH) |
| `kubemq_amqp10_rpc_late_responses_total` | — | RPC responses arriving **after the requester went away** |

### The two drop counters to watch

These are the connector's **data-loss footgun signals** — on a healthy producer/consumer
they should stay at **0**:

- **`kubemq_amqp10_events_dropped_no_credit_total`** — increments every time the connector
 has an `events/` message to deliver but the consumer link has **zero credit**. Events
 are at-most-once fire-hose: with no credit the message is **silently dropped**. Grant
 standing credit continuously. (`transfer_out_pubsub.go`;
 `ReportAmqp10EventDroppedNoCredit`)
- **`kubemq_amqp10_events_store_dropped_stalled_total`** — increments when a durable
 events-store consumer stops granting credit and the credit-0 bounded buffer overflows,
 forcing a **DETACH with lost messages**. Replenish credit eagerly.

> **`kubemq_amqp10_transfers_in_dropped_total` covers three causes, not one.** Its Help
> text is *"inbound AMQP 1.0 transfers dropped (oversize, no consumer, pre-settled
> failure)"* . It counts an **oversize** inbound transfer, a
> transfer with **no consumer**, **and** a **pre-settled routing failure** — do not read
> it as pre-settled-only.

---

## 3. SSE metrics group

The live dashboard streams a snapshot of these metrics as an **`api.Amqp10MetricsGroup`**
over the metrics SSE channel. The store mirrors the same gauges and
counters as the Prometheus families above (the store clamps gauges at 0 and the Prometheus
mirror follows the clamp, so the two never diverge).

---

## 4. Audit events

> **The AMQP 1.0 connector emits exactly TWO audit events: `auth.success` and
> `auth.failure`.** It does **not** emit `client.connected` or `client.disconnected`
> those are **not** part of this connector's audit surface.

Both come from the SASL layer , through the audit seam
`var auditReport = audit.ReportControl` :

| Event | When | Fields |
|---|---|---|
| `auth.success` | SASL outcome OK | `ClientID`, `Transport: "amqp10"`, `SourceIP`, `Metadata{mechanism}` |
| `auth.failure` | SASL rejected | `ClientID`, `Transport: "amqp10"`, `SourceIP`, `Error` (sanitized reason), `Metadata{mechanism}` |

The `mechanism` metadata is `plain` / `anonymous` / `external`, and `SourceIP` is the peer
IP. The **full failure reason** lives only in the `auth.failure` audit record — the SASL
**wire** outcome carries the code alone (the no-leak rule, see
[Error Conditions](./error-conditions.md)).

To observe connection lifecycle (count, age, source IP, SASL mechanism), poll
`GET /api/amqp10/connections` (§1) or scrape `kubemq_amqp10_connections` (§2) — **not**
the audit log.

---

## Related

- Reference: [Error Conditions](./error-conditions.md) (`kubemq_amqp10_errors_total`
 scopes, no-leak rule), [Capabilities](./capabilities.md) (limits behind the caps),
 [Address Mapping §5](./address-mapping.md#5-server-role--authorization) (link role →
 the `role` label / `Amqp10LinkDTO.role`)
- Guides: [Authentication](../guides/authentication.md) (the audit surface in context),
 [Flow Control](../guides/flow-control.md) (the two drop counters as footgun signals)
- Concepts: [Pub/Sub](../concepts/pub-sub.md) (0-credit drop),
 [Durable Subscriptions](../concepts/durable-subscriptions.md) (stalled-credit drop),
 [Flow Control & Credit](../concepts/flow-control-and-credit.md)
- The `burnin/` soak harness scrapes these connector metrics and gates
 `events_dropped_no_credit` / `events_store_dropped_stalled` at **0**.

---
