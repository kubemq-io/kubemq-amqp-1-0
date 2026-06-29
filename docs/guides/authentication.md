# Authentication and Authorization

This guide explains how a native AMQP 1.0 client proves *who it is* to the KubeMQ AMQP 1.0
connector (authentication / SASL) and *what it may do* once attached (authorization / Casbin). It
covers the three SASL mechanisms the connector offers, how the client identity (`ClientID`) is
derived, why `container-id` matters, and exactly which audit events the connector emits — and which
it does **not**.

> **Runnable default:** the examples in this repo connect **ANONYMOUS** on a stock dev broker
> (authentication is off by default), so they clone-and-run with no credentials. The one runnable
> auth variant — [`connectivity/auth`](../../examples/go/connectivity/auth/) — demonstrates SASL
> PLAIN with a KubeMQ JWT. See the per-language variants:
> [Go](../../examples/go/connectivity/auth/) ·
> [Python](../../examples/python/connectivity/auth/) ·
> [Java](../../examples/java/connectivity/auth/) ·
> [C#](../../examples/csharp/connectivity/auth/) ·
> [JavaScript / TS](../../examples/javascript/connectivity/auth/) ·
> [Rust](../../examples/rust/connectivity/auth/).

---

## 1. The three SASL mechanisms

The connector computes the SASL mechanism list **per connection** from the auth and TLS context and
offers it in a fixed order: **EXTERNAL → PLAIN → ANONYMOUS**.

| Mechanism (wire symbol) | Offered when | Credential | Identity (`ClientID`) becomes |
|---|---|---|---|
| **`PLAIN`** | **always** | RFC 4616 `authzid\x00authcid\x00passwd`; the **password is the KubeMQ JWT** | auth on → `Claims.ClientID` from the JWT; auth off → sanitized SASL username, else the `container-id` |
| **`ANONYMOUS`** | only when the auth service is **disabled** | none | sanitized `container-id` |
| **`EXTERNAL`** | only on an **mTLS** connection with a *verified* client certificate | the certificate itself (no JWT) | the cert **Subject CN**, sanitized |
| bare (no SASL, `%d0` header) | only when auth is disabled | none | sanitized `container-id` (or `amqp10-<uuid8>` if empty) |

### PLAIN — the documented contract

PLAIN is the mechanism to use when authentication is enabled. The credential layout is the standard
RFC 4616 triple `authzid \x00 authcid \x00 passwd`:

- **`passwd` (password) = the KubeMQ JWT.** It is validated via `AuthenticateWithContext`; on
 success the connector takes `ClientID` from the JWT's `Claims.ClientID`.
- **`authcid` (username) is audit-only / informational when auth is on.** It does **not** become
 the identity — the JWT does. (When auth is *off*, the username is used as a convenience identity;
 see precedence below.)
- The SASL initial-response binary is capped at **16 KiB**. A larger response is treated as a
 malformed/hostile peer and rejected. A KubeMQ JWT fits comfortably.
- **No SCRAM-SHA-256, GSSAPI, or Azure CBS.** PLAIN is the only credentialed mechanism.

```
# conceptual: most clients accept (user, password) — put the JWT in the password slot
amqp.Dial(ctx, "amqp://broker:5672",
 &amqp.ConnOptions{SASLType: amqp.SASLTypePlain("audit-username", "<KUBEMQ_JWT>")})
```

### ANONYMOUS — the auth-off default

When the broker's authentication service is disabled, the connector offers `ANONYMOUS`. This is the
mechanism the runnable examples rely on: no credential is sent and the identity falls back to the
`container-id`. **ANONYMOUS is only offered when auth is off** — if you request it against an
auth-enabled broker it is rejected as an auth failure (it was never advertised).

### EXTERNAL — mTLS, cert CN → ClientID

`EXTERNAL` is offered **only** when the TLS handshake presented a client certificate that the
listener *verified* (i.e. the listener was configured `RequireAndVerifyClientCert` — the mTLS
listener). The identity is the client certificate's **Subject CN**, sanitized to a valid
`ClientID`; no JWT is needed. An empty CN is rejected. Because EXTERNAL depends on mTLS, it is
covered as **documentation-only** alongside TLS — see [tls-and-mtls.md](tls-and-mtls.md).

---

## 2. Identity precedence (how `ClientID` is chosen)

The connector resolves the client identity in this order, depending on the negotiated mechanism and
whether auth is enabled:

1. **PLAIN + auth on** → the JWT's `Claims.ClientID`.
2. **PLAIN + auth off** → the sanitized SASL username (`authcid`); if empty, the `container-id`.
3. **EXTERNAL** → the client certificate's Subject CN (sanitized).
4. **ANONYMOUS** → the sanitized `container-id`.
5. **bare (no SASL)** → the sanitized `container-id`; if empty, a generated `amqp10-<uuid8>`.

`sanitizeContainerID` caps the value at **256** characters and maps every character outside
`[a-zA-Z0-9_-]` to `_`.

---

## 3. `container-id` is required, and must be stable for durable subscribers

Every AMQP 1.0 `OPEN` **must** carry a non-empty `container-id`. An empty value is rejected with
`CLOSE(amqp:invalid-field, "open.container-id is required")`. The value is sanitized
(`[a-zA-Z0-9_-]`, ≤256).

`container-id` matters for two reasons beyond identity:

- It becomes the `ClientID` whenever there is **no SASL identity** (ANONYMOUS / bare / auth-off
 PLAIN-with-empty-username).
- It is **half of the durable-subscription identity**
 (`sanitize40(containerID)_sanitize40(linkName)_fnv1a32hex(cid|link)`). A durable events-store
 subscriber that reconnects with a *different* `container-id` will not resume its old position — it
 becomes a different durable identity. **Set a stable `container-id` for any durable subscriber.**
 See [durable-subscriptions.md](../concepts/durable-subscriptions.md).

The AMQP `OPEN` `hostname` field (a vhost-like notion carried over from other brokers) is **logged
then ignored** — there is no vhost. See [addressing.md](addressing.md).

---

## 4. Authorization (Casbin) — enforced at attach, per resource

Once authenticated, the connector enforces a Casbin policy **at `ATTACH` time**, against the
`ClientID`, the resolved pattern (mapped to a resource name), the channel, and the link role
(`authorizeAttach`). If no authorizer is wired (auth off), nothing is enforced.

| Link the client attaches | Server role | Casbin permission enforced |
|---|---|---|
| **Receiver** from `<pattern>/<ch>` (client *consumes*) | server-sender | **Read** on `(ClientID, resource, channel)` |
| **Sender** to a fixed `<pattern>/<ch>` (client *produces*) | server-receiver | **Write** on `(ClientID, resource, channel)` |
| **Sender** with a **null (anonymous) target** | server-receiver | **deferred** — no attach-time check; each message is authorized **per-message with Write** on its `properties.to` |
| **`/responses/<RequestID>`** (RPC reply token) | server-receiver | **not enforced** — connection-scoped reply token |

- The resource name maps the pattern: `events-store` → `events_store`; `queues`/`events`/
 `commands`/`queries` map to themselves (`authzResource`).
- **Anonymous-terminus links** (a sender opened with a null target that routes per-message by
 `properties.to`) cannot be checked at attach because there is no fixed channel yet. Instead, the
 transfer layer authorizes **each message** with a **Write** check on the message's `to`, backed by
 a 1024-entry / 60s-TTL LRU cache keyed by `(clientID, resource, channel)`. See
 [addressing.md](addressing.md) and the
 [`advanced/anonymous-terminus`](../../examples/go/advanced/anonymous-terminus/) example.
- **`/responses/` reply tokens are connection-scoped and are NOT `Enforce`d** — they are minted by
 the connector for a specific RPC requester, so re-authorizing them would be meaningless.

### Denied attach → `amqp:unauthorized-access`

A denied authorization closes the link with `DETACH(amqp:unauthorized-access)` and a generic
description: `"not authorized for <channel>"`. **No policy internals leak** — the wire error never
reveals the policy rule, subject mapping, or resource path. Your client should surface the
`amqp:unauthorized-access` condition and treat it as a permission error, not retry it as transient.

---

## 5. Audit events — exactly two, and what is NOT emitted

The connector's audit surface for AMQP 1.0 lives entirely in the SASL layer
. The audit seam is `var auditReport = audit.ReportControl`
, and it emits **only two event types**:

| Audit event | Emitted on | Fields |
|---|---|---|
| **`auth.success`** | successful SASL authentication (`acceptSASL`) | `ClientID`, `Transport: "amqp10"`, `SourceIP`, `Metadata{"mechanism": <PLAIN\|ANONYMOUS\|EXTERNAL>}` |
| **`auth.failure`** | failed/rejected SASL authentication (`rejectSASL`) | `ClientID`, `Transport: "amqp10"`, `SourceIP`, `Error` (sanitized), `Metadata{"mechanism": …}` |

> **The AMQP 1.0 connector does NOT emit `client.connected` or `client.disconnected` audit
> events.** Do not build alerting, dashboards, or compliance reporting that depends on
> connection-lifecycle audit events from this connector — they are not produced. The only audit
> signal is authentication success/failure (with the SASL mechanism and source IP). If you need
> connection/link visibility, use the dashboard API and Prometheus metrics instead — see
> [../reference/connections-endpoint.md](../reference/connections-endpoint.md).

Both audit events carry the **SASL mechanism** and the **source IP**, so an `auth.failure` stream
tells you which mechanism was attempted and from where — but the wire SASL outcome sent back to the
client never carries the internal error detail (the reason stays in the audit record only).

---

## 6. Quick decision guide

| You want… | Do this |
|---|---|
| Clone-and-run on a stock dev broker | Connect ANONYMOUS (no credentials); the examples default to this |
| Authenticate with KubeMQ identity | SASL **PLAIN**, JWT in the **password** slot; username is audit-only |
| Authenticate with a client certificate | mTLS + SASL **EXTERNAL** (cert CN → ClientID) — see [tls-and-mtls.md](tls-and-mtls.md) |
| Resume a durable events-store subscription after reconnect | Set a **stable `container-id`** (it is half the durable identity) |
| Diagnose a permission failure | Look for `DETACH(amqp:unauthorized-access)`; check the Casbin Read/Write policy for that channel |
| Audit who authenticated | Consume `auth.success` / `auth.failure` (mechanism + source IP) — **not** connect/disconnect events |

---

## Related

- [Links, Sessions, and Connections](../concepts/links-sessions-connections.md) — the OPEN/ATTACH model
- [Addressing](addressing.md) — anonymous terminus and `properties.to` per-message Write
- [TLS and mTLS](tls-and-mtls.md) — EXTERNAL / cert CN identity (doc-only)
- [Durable Subscriptions](../concepts/durable-subscriptions.md) — why `container-id` must be stable
- [Error Conditions](../reference/error-conditions.md) — the 13 `amqp:*` conditions
- [Connections Endpoint](../reference/connections-endpoint.md) — dashboard/metrics visibility
