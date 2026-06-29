# Reference — Capabilities & Feature Support

This reference defines exactly what the embedded KubeMQ AMQP 1.0 connector **supports**,
what it **rejects**, and — critically — what it **does not advertise**. Use it to decide
which client features are safe to rely on and which ones will be refused at attach,
transfer or connection time.

---

## 1. Capability negotiation — the connector advertises NONE

> **The connector advertises NO offered or desired connection capabilities and NO
> offered or desired link/terminus capabilities.** There is **no `ANONYMOUS-RELAY`**, no
> `queue`/`topic` node capability, no `DELAYED_DELIVERY`, no `SHARED-SUBS`, no `sole-connection-for-container` — nothing. Clients **MUST NOT** depend on AMQP capability
> negotiation with this connector.

This is verified in the connector's OPEN handler. `serverOpen` constructs the reply OPEN
performative with **only four fields** — `ContainerID`, `MaxFrameSize`, `ChannelMax`,
`IdleTimeout` :

```go
serverOpen := &frames.PerformOpen{
 ContainerID: "KubeMQ",
 MaxFrameSize: c.maxFrameSize,
 ChannelMax: c.channelMax,
 IdleTimeout: advertisedIdle,
}
```

`OfferedCapabilities` and `DesiredCapabilities` are **never set** on the OPEN reply, and
the attach-reply path likewise sets no terminus capabilities. The connector *reads* a
peer's terminus `Capabilities` only to honour a bare-address `queue`/`topic` hint
(`terminusAddress`) — it never echoes or offers capabilities back.

### What this means in practice

- **Anonymous senders work, but not via `ANONYMOUS-RELAY`.** A client gets anonymous
 routing by attaching a sender link with a **null target address**; the connector then
 routes each transfer by its `properties.to` . It is
 **null-target-driven**, not capability-driven.
- **This is the root cause of the Java / Qpid JMS `anonymous-terminus` N/A.** Qpid JMS
 decides whether to use a single anonymous producer link by checking the peer's offered
 `ANONYMOUS-RELAY` capability. Because the connector offers none, Qpid JMS falls back to
 **per-destination sender links** and exposes no API to force the raw null-target ATTACH
 the connector routes on. Native clients (Go `Azure/go-amqp`, .NET AMQPNetLite, Python
 qpid-proton, Rust fe2o3-amqp, JS rhea) can open a null-target sender directly and use
 anonymous routing. See [Address Mapping §3](./address-mapping.md#3-the-special-row-mapping--bare-dynamic-anonymous-responses).
- Do **not** branch your client logic on a negotiated capability — there will be none.
 Drive behaviour from the address (`<pattern>/<channel>`) and link-properties instead.

---

## 2. Supported features

| Feature | Support | Notes |
|---|---|---|
| Message body `Data` (binary) | ✅ | the primary body section |
| Message body `AmqpValue` | ✅ | string / map / list / scalar values |
| Empty body | ✅ | accepted |
| Settlement `accepted` / `released` / `modified` / `rejected` | ✅ | mapped to KubeMQ Ack/NAck (`settle.go`); `released`/`modified` requeue and **increment receive-count** |
| Sender settle-mode `unsettled` (at-least-once) | ✅ | the default; queues durability path |
| Sender settle-mode `settled` (pre-settled, at-most-once) | ✅ | paired with `rcv-settle-mode=first` |
| Receiver settle-mode `first` | ✅ | the **only** supported receiver settle mode |
| Link credit + drain (FLOW) | ✅ | manual or windowed credit; drain to exhaust-then-stop |
| Multi-frame transfers (fragmentation/reassembly) | ✅ | `More:true…More:false`; bit-exact reassembly up to the 100 MiB cap |
| Dynamic nodes (`dynamic=true`, null address) | ✅ | minted `_amqp10.tmp.<connID>.<uuid>`; node-local, no TTL/persistence |
| Anonymous terminus (null target, route by `to`) | ✅ | per-message Write authz; **not** `ANONYMOUS-RELAY` |
| JMS/SQL-92 selector on `events/` & `events-store/` consume links | ✅ | `apache.org:selector-filter:string` (3-valued logic) |
| Consumer groups (`x-opt-kubemq-group`) | ✅ | link property on the consume link |
| Durable subscriptions (events-store, expiry `never`) | ✅ | durable identity = stable container-id + link name; **node-local** |
| Start positions (`x-opt-kubemq-start`) | ✅ | `first` / `new-only` / `last` / `sequence:<n>` / `time:<RFC3339\|unix-secs>` / `time-delta:<secs>` |
| Native RPC (commands / queries) | ✅ | dynamic reply node + `reply-to` + `correlation-id`; **no gRPC, no KubeMQ SDK** |
| SASL `ANONYMOUS` / `PLAIN` (JWT) / `EXTERNAL` (mTLS) | ✅ | see [Authentication](../guides/authentication.md) |
| TLS / mTLS on `amqps://:5671` | ✅ (connector) | doc-only in this repo; see [TLS & mTLS](../guides/tls-and-mtls.md) |

---

## 3. Rejected / unsupported features

These are **non-goals** — they are refused deterministically, not silently ignored.
Each maps to a wire condition from the [13 error conditions](./error-conditions.md).

| Feature | Behaviour | Condition / mechanism | Source |
|---|---|---|---|
| `rcv-settle-mode=second` | DETACH at attach | `amqp:not-implemented` | |
| Selector on a `queues/` link | DETACH at attach | `amqp:not-implemented` (queues are move-only) | |
| `copy` distribution-mode on `queues/` | DETACH at attach | `amqp:invalid-field` | |
| `AmqpSequence` body section | rejected | `translate.go` (`errAMQPSequenceUnsupported`) | `translate.go` |
| AMQP **transactions** (txn coordinator) | not implemented | no coordinator node in the connector | design non-goal |
| **Exactly-once** delivery | not provided | at-least-once (queues) or at-most-once (events) only | design non-goal |
| **Link resumption** / delivery-state recovery on re-attach | not supported | re-attach is a fresh link | (`link re-attach not supported`) |
| Message **peek / browse / FIFO ordering guarantee** | not provided | competing-consumer move semantics only | design non-goal |
| **Dead-letter exchange (DLX)** | none | broker `MaxReceiveQueue` caps backlog; no DLX | design non-goal |
| Second live attach of the same durable identity | DETACH | `amqp:not-allowed` (durable subscription in use) | |
| Receiver attach on `responses/<id>` | DETACH | `amqp:not-allowed` | |
| AMQP-over-**WebSocket** | not supported | raw TCP / TLS only | non-goal |
| **vhost** / virtual host | none | OPEN `hostname` accepted but ignored; flat address space | `conn.go` OPEN |
| Config **hot-reload** | not supported | connector config is read at start | non-goal |
| Capability negotiation (offered/desired) | none advertised | drive behaviour from address + link-props | |

---

## 4. Inert / accepted-but-ignored

These are accepted on the wire without error but carry **no connector semantics**
(documented so you do not expect behaviour that is not there):

- **Message priority** — accepted, not honoured for ordering.
- **`group-id` / `group-sequence`** — accepted, not used for grouping/ordering.
- **`footer` section** — accepted, passed through where applicable, not interpreted.
- **OPEN `hostname`** — accepted, ignored (no vhost, §3).
- **Idle-timeout from the client** — honoured by the connector emitting empty frames at
 half the client's advertised interval , but it is not a feature you
 negotiate via capabilities.

---

## 5. Limits & forced caps

| Limit | Value | Enforcement / condition |
|---|---|---|
| Max channel length | 255 bytes | `validateConnectorChannel` → `amqp:not-found` |
| Max multi-frame message size | 100 MiB | oversize → `amqp:link:message-size-exceeded` (`transfer_in.go`) |
| Receiver settle modes | `first` only | `second` → `amqp:not-implemented` |
| Connection / session / link caps | from config | over-cap → `amqp:resource-limit-exceeded` |
| Durable identity components | 40 chars each + FNV suffix | `durableID` / `sanitize40` |

See [Configuration](../configuration.md) for the `CONNECTORS_AMQP10_*` knobs behind these
caps, and [Connections & Observability](./connections-endpoint.md) for the metrics that
count limit breaches.

---

## Related

- Reference: [Address Mapping](./address-mapping.md),
 [Error Conditions](./error-conditions.md),
 [Connections & Observability](./connections-endpoint.md)
- Concepts: [Settlement & Delivery State](../concepts/settlement-and-delivery-state.md),
 [Selectors](../concepts/selectors.md),
 [Durable Subscriptions](../concepts/durable-subscriptions.md)
- Guides: [Reliability](../guides/reliability.md), [Flow Control](../guides/flow-control.md)
- Examples: `examples/go/queues/settlement-modes` (rcv-settle-mode note),
 `examples/go/events/selector` (selector-on-queues note),
 `examples/go/advanced/anonymous-terminus` (null-target, no ANONYMOUS-RELAY)

---
