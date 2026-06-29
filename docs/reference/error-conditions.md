# Reference — Error Conditions

The embedded KubeMQ AMQP 1.0 connector reports every failure with an **AMQP 1.0 symbolic
error condition** carried on a `DETACH`, `END`, `CLOSE` or rejected-disposition
performative. There are exactly **13** of them, and the connector **never emits a
condition outside this set** — it is a pinned, greppable, testable vocabulary
.

> **AMQP 1.0 only — no numeric reason codes.** This connector is the AMQP **1.0**
> dialect. It does **not** use the numeric reason codes of the AMQP 0-9-1 connector
> (`kubemq-amqp-rabbitmq`). If you are migrating mental models from 0-9-1, replace
> "reply-code 312/404/406…" with the **`amqp:*` symbols** below. Numeric codes appear
> **nowhere** in this connector.

---

## The 13 conditions

The constant block is (symbols on lines 20-47).
The order below matches the source.

| # | Constant | Symbol | Meaning | Typical trigger |
|---|---|---|---|---|
| 1 | `condInternalError` | `amqp:internal-error` | unexpected server-side failure | a broker send error or other internal fault; the description carries the **sanitized broker message only** |
| 2 | `condNotFound` | `amqp:not-found` | unknown address / bad channel at attach | unrecognised address prefix, or a channel that violates the connector charset (empty, `>255`, trailing `.`, whitespace, `*`/`>`, `;`/`:`) — see [Address Mapping §6](./address-mapping.md#6-channel-validation-connector-charset) |
| 3 | `condUnauthorized` | `amqp:unauthorized-access` | authorization denial | attach denied by Casbin (Read on consume, Write on produce), or a per-message anonymous-terminus Write denial |
| 4 | `condDecodeError` | `amqp:decode-error` | malformed frame / codec failure | a corrupt or invalid AMQP frame on the wire |
| 5 | `condResourceLimit` | `amqp:resource-limit-exceeded` | capacity breach | connection / session / link / RPC cap reached, **idle timeout**, or events-store **stalled-credit** buffer overflow |
| 6 | `condNotAllowed` | `amqp:not-allowed` | protocol / FSM violation | duplicate link name, receiver attach on `responses/`, **duplicate durable subscription identity**, broker-not-ready reject, link re-attach |
| 7 | `condInvalidField` | `amqp:invalid-field` | invalid link property | a malformed **`x-opt-kubemq-start`** start position, an unparseable **selector**, or `copy` distribution-mode on a queue |
| 8 | `condNotImplemented` | `amqp:not-implemented` | well-formed but unsupported request | a **selector on a `queues/` link**, or **`rcv-settle-mode=second`** |
| 9 | `condPreconditionFailed` | `amqp:precondition-failed` | missing/invalid anonymous-terminus `to` | an anonymous sender transfer with no `to`, an unknown prefix in `to`, or a dynamic node the connection cannot reach |
| 10 | `condMessageSizeExceeded` | `amqp:link:message-size-exceeded` | oversize multi-frame transfer (link scope) | a message body over the **100 MiB** reassembly cap |
| 11 | `condWindowViolation` | `amqp:session:window-violation` | session incoming/outgoing window breach | the peer sent more transfers than the advertised incoming-window allowed (`session.go`) |
| 12 | `condErrantLink` | `amqp:session:errant-link` | unattached-handle / handle-in-use session error | a TRANSFER/DISPOSITION on an unknown handle, or an ATTACH reusing a live handle |
| 13 | `condConnectionForced` | `amqp:connection:forced` | server-initiated CLOSE | graceful shutdown or broker-down — the connection is forced closed |

The same set is re-listed as the `allConditions` array so it can be
asserted in one place across the session / link / transfer / RPC layers.

---

## Scopes

The condition prefix tells you the AMQP scope on which it is delivered:

- `amqp:*` — **link or message** scope (delivered on `DETACH` or a rejected disposition):
 conditions 1-10.
- `amqp:session:*` — **session** scope (delivered on `END`): conditions 11-12.
- `amqp:connection:*` — **connection** scope (delivered on `CLOSE`): condition 13.

---

## Message sanitization (no-leak rule)

Every wire `description` is **sanitized to at most 512 characters** and carries **only a
broker error message** — never a file path, NATS subject, stack trace or policy internal
. This matches the gRPC connector's sanitization. A SASL auth failure
takes this further: the **wire** SASL outcome conveys only the failure code, while the
full reason is kept in the **audit record** (see
[Connections & Observability](./connections-endpoint.md)) — the cleartext error never
crosses the wire .

So a client should treat the `description` as a short, human-readable hint and branch its
logic on the **symbolic condition**, not on the description string.

---

## Client handling guidance

| Condition | Recommended client response |
|---|---|
| `amqp:internal-error` | retry with backoff; the broker hit a transient fault |
| `amqp:not-found` | fix the address / channel — it does not exist or breaks the charset; do not retry unchanged |
| `amqp:unauthorized-access` | re-authenticate (refresh the JWT) or request a policy grant; do not retry unchanged |
| `amqp:decode-error` | a client/codec bug — inspect the encoded frame; do not blind-retry |
| `amqp:resource-limit-exceeded` | back off and reconnect (caps), grant credit faster (stalled events-store), or send keepalives (idle) |
| `amqp:not-allowed` | resolve the conflict — release the durable identity, use a fresh link name, attach `responses/` as a sender |
| `amqp:invalid-field` | fix the link property — correct the `x-opt-kubemq-start` grammar or the selector expression |
| `amqp:not-implemented` | the feature is a documented non-goal — use the supported alternative (selector on `events/` not `queues/`; `rcv-settle-mode=first`) |
| `amqp:precondition-failed` | set a valid `to` on the anonymous-terminus message |
| `amqp:link:message-size-exceeded` | split the payload or stay under the 100 MiB cap |
| `amqp:session:window-violation` | respect the advertised session window; grant credit before sending |
| `amqp:session:errant-link` | a handle-management bug in the client — do not reuse live handles |
| `amqp:connection:forced` | reconnect; the server is shutting down or the broker went away |

---

## Related

- Reference: [Capabilities](./capabilities.md) (which features map to
 `not-implemented` / `invalid-field` / `not-allowed`),
 [Address Mapping §6](./address-mapping.md#6-channel-validation-connector-charset)
 (`amqp:not-found` channel rules),
 [Connections & Observability](./connections-endpoint.md) (`kubemq_amqp10_errors_total`)
- Guides: [Authentication](../guides/authentication.md) (`amqp:unauthorized-access`),
 [Reliability](../guides/reliability.md) (`amqp:internal-error`,
 `amqp:link:message-size-exceeded`, `amqp:connection:forced`),
 [Flow Control](../guides/flow-control.md) (`amqp:resource-limit-exceeded`),
 [Addressing](../guides/addressing.md) (`amqp:not-found`, `amqp:invalid-field`)
- Concepts: [Settlement & Delivery State](../concepts/settlement-and-delivery-state.md),
 [Selectors](../concepts/selectors.md),
 [Durable Subscriptions](../concepts/durable-subscriptions.md)

---
