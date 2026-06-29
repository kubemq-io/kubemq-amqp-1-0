# TLS and mTLS

> **Documentation-only.** This repo ships **no runnable TLS/mTLS example**. Every runnable example
> uses plain `amqp://` against a stock dev broker. This guide documents how the KubeMQ AMQP 1.0
> connector exposes TLS (`amqps://:5671`), mutual TLS (mTLS), and SASL **EXTERNAL** so you can
> configure them in production — but there is no `connectivity/tls` example to clone-and-run. To
> exercise it you supply your own certificates and a broker configured with the top-level
> `Security` block.

---

## 1. Why doc-only?

The runnable examples are written to **clone-and-run on a stock dev broker** with no certificates,
no PKI, and authentication off. A TLS example would require the reader to generate a CA, a server
cert, and (for mTLS) a client cert, then configure the broker's `Security` block before anything
ran — friction that defeats the clone-and-run goal. TLS is therefore documented here as the
production hardening path, while the [`connectivity/auth`](../../examples/go/connectivity/auth/)
example covers the one credential mechanism that *does* run on a stock broker (SASL PLAIN with a
JWT). See [authentication.md](authentication.md).

---

## 2. Ports and schemes

The connector listens on two ports, both **shared with the AMQP 0-9-1 connector** through the
`amqpmux` listener (see §4):

| Scheme | Port | When active |
|---|---|---|
| `amqp://host:5672` | `5672` (plain / SASL) | always (unless `CONNECTORS_AMQP10_PORT=0`) |
| `amqps://host:5671` | `5671` (TLS) | **only when the top-level `Security` block is configured** |

The TLS port is **not** controlled by any `Amqp10` field other than `TlsPort`
(`CONNECTORS_AMQP10_TLS_PORT`, default `5671`, `0` disables it). The certificate material, the CA,
and the mode all come from the **top-level `Security` block** — see [configuration](../configuration.md).
There is **no AMQP-over-WebSocket**: raw TCP and TLS only.

---

## 3. TLS server-auth vs mTLS

The connector derives its `tls.Config` from the `Security` mode :

### `SecurityModeTLS` — server authentication only

- The server presents its certificate; `MinVersion` is **TLS 1.2**.
- **No client certificate is requested or verified.**
- The client authenticates separately, at the SASL layer (PLAIN with a JWT, or ANONYMOUS if auth is
 off). Use `amqps://` for the transport and SASL PLAIN for identity.

```
# conceptual: server-auth TLS + SASL PLAIN (JWT in password)
amqps://broker:5671 + SASLTypePlain("audit-user", "<KUBEMQ_JWT>")
```

### `SecurityModeMTLS` — mutual TLS

- The server presents its certificate **and** requires a client certificate:
 `ClientAuth = RequireAndVerifyClientCert`, with the `ClientCAs` pool built from `Security.Ca`.
 `MinVersion` is **TLS 1.2**.
- A verified client certificate populates the TLS `VerifiedChains`, which is the **precondition for
 SASL EXTERNAL**.

---

## 4. SASL EXTERNAL — cert CN → ClientID

`EXTERNAL` is offered **only** when the connection is mTLS **and** the client presented a
*verified* client certificate (`VerifiedChains` non-empty). When you authenticate with EXTERNAL:

- **No JWT is sent.** The certificate *is* the credential.
- The client identity (`ClientID`) becomes the client certificate's **Subject CN**, sanitized to a
 valid `ClientID` (`[a-zA-Z0-9_-]`, ≤256). An empty CN is rejected as an auth failure.
- Authorization (Casbin Read/Write at attach) then runs against that CN-derived `ClientID` exactly
 as for PLAIN — see [authentication.md](authentication.md).

> **EXTERNAL is available iff `Security.Mode == mtls`.** Plain TLS (server-auth only) does not
> populate `VerifiedChains`, so EXTERNAL is not offered there — fall back to PLAIN (JWT) or
> ANONYMOUS.

```
# conceptual: mTLS + SASL EXTERNAL (identity = client cert CN, no JWT)
amqps://broker:5671 + client cert (CN=order-service) + SASLTypeExternal("")
# resolved ClientID = "order-service"
```

The offered-mechanism order is **EXTERNAL → PLAIN → ANONYMOUS**, so on an mTLS connection a
spec-conformant client that supports EXTERNAL will pick it first.

---

## 5. How the TLS listener shares the `amqpmux` port

Both AMQP dialects (0-9-1 and 1.0) coexist on the **same** ports. The `amqpmux` listener accepts
every connection, reads the **8-byte AMQP protocol header**, and dispatches by dialect
(`engineForHeader`). The relevant 8-byte headers are:

| Header bytes | Meaning |
|---|---|
| `AMQP\x00\x00\x09\x01` | AMQP 0-9-1 |
| `AMQP\x00\x01\x00\x00` | AMQP 1.0, plain (bare/AMQP layer) |
| `AMQP\x03\x01\x00\x00` | AMQP 1.0, SASL layer |
| `AMQP\x02\x01\x00\x00` | AMQP 1.0, **TLS** token — only meaningful on the TLS listener |

For TLS, the connection is wrapped in the `tls.Config` from the `Security` block **before** the
header is interpreted; the `AMQP\x02\x01\x00\x00` token is the TLS-listener variant of the AMQP 1.0
header. The dispatching engine **must not re-read the 8 bytes** — the mux hands them to the engine.
A dialect is disabled simply by not registering its engine. The upshot for clients: point an
`amqps://` AMQP 1.0 client at `:5671`, and the same listener that serves 0-9-1 will route you to the
1.0 engine. See [Links, Sessions, and Connections](../concepts/links-sessions-connections.md) for
the connection handshake.

---

## 6. Production checklist

| Goal | Configuration |
|---|---|
| Encrypt transport, authenticate clients with JWT | `Security` mode `tls` + `amqps://:5671` + SASL PLAIN (JWT in password) |
| Authenticate clients with certificates (no JWT) | `Security` mode `mtls` + `amqps://:5671` + SASL EXTERNAL (CN → ClientID) |
| Keep plain `amqp://` for local/dev | leave `CONNECTORS_AMQP10_PORT=5672`; the examples use this |
| Disable the TLS port | `CONNECTORS_AMQP10_TLS_PORT=0` (or leave the `Security` block unset) |

> **Reminder:** the primary examples in this repo all use plain `amqp://`. There is no runnable TLS
> path — wire the `Security` block and your own certificates per your KubeMQ server configuration to
> use `amqps://`.

---

## Related

- [Authentication and Authorization](authentication.md) — SASL PLAIN / EXTERNAL, identity precedence
- [Configuration](../configuration.md) — the `Security` block and `CONNECTORS_AMQP10_TLS_PORT`
- [Links, Sessions, and Connections](../concepts/links-sessions-connections.md) — the `amqpmux` handshake
- [Error Conditions](../reference/error-conditions.md) — the 13 `amqp:*` conditions
