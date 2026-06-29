# C# — Connectivity / Auth (SASL PLAIN with a KubeMQ JWT)

The ONE runnable authentication variant. It connects to the KubeMQ AMQP 1.0
connector with **SASL PLAIN** — the username is **audit-only** and the password is a
**KubeMQ JWT** — then runs a `queues/<ch>` round-trip. On a stock dev broker (auth
OFF) it falls back to **ANONYMOUS**, so it clone-and-runs either way. Native
`AMQPNetLite.Core`; NO KubeMQ SDK.

## Prerequisites

- .NET SDK **8.0**
- `AMQPNetLite.Core` **2.5.3** (pinned in `examples/csharp/Directory.Packages.props`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

**ANONYMOUS (stock dev broker):**

```bash
cd connectivity/auth
dotnet run
```

**SASL PLAIN (auth-enabled broker):**

```bash
export KUBEMQ_AMQP_USER=my-service     # audit-only username
export KUBEMQ_AMQP_JWT=<a-kubemq-jwt>  # password = a KubeMQ JWT
dotnet run
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 dotnet run
```

## Expected Output

**ANONYMOUS (no env set):**

```
Broker:  amqp://localhost:5672
Address: queues/amqp10.examples.auth  (KubeMQ pattern=queues, channel=amqp10.examples.auth)
Auth:    ANONYMOUS  (KUBEMQ_AMQP_JWT unset — falling back to the dev-broker default)
         Set KUBEMQ_AMQP_USER + KUBEMQ_AMQP_JWT to dial SASL PLAIN with a KubeMQ JWT.

[open] Connected — SASL handshake accepted
[send] Produced 1 message to queues/amqp10.examples.auth (accepted)
[recv] Consumed and accepted 1 message: "auth-round-trip"

Done.
```

**SASL PLAIN (`KUBEMQ_AMQP_USER` + `KUBEMQ_AMQP_JWT` set):**

```
Broker:  amqp://localhost:5672
Address: queues/amqp10.examples.auth  (KubeMQ pattern=queues, channel=amqp10.examples.auth)
Auth:    SASL PLAIN  (username="my-service" audit-only; password=<KubeMQ JWT>)

[open] Connected — SASL handshake accepted
[send] Produced 1 message to queues/amqp10.examples.auth (accepted)
[recv] Consumed and accepted 1 message: "auth-round-trip"

Done.
```

## What's Happening

1. **Select the mechanism** — if `KUBEMQ_AMQP_JWT` is set, the example builds an
   `Address` carrying the username + JWT as userinfo (which makes AMQPNetLite
   negotiate **SASL PLAIN**). Otherwise it pins the **ANONYMOUS** profile on a
   `ConnectionFactory`.
2. **OPEN (SASL handshake)** — the SASL exchange happens at connect. With auth
   ENABLED, a JWT that fails validation makes the connect fail with
   `amqp:unauthorized-access` at the SASL layer (`TestAuthenticationBadCredential`).
   With auth DISABLED, any credential is accepted.
3. **ATTACH + SEND (WRITE check)** — the WRITE authorization check runs at sender
   attach / send. An identity without a WRITE grant on this channel is refused with
   `amqp:unauthorized-access`.
4. **ATTACH + RECEIVE (READ check)** — the READ authorization check runs at receiver
   attach (`TestAuthorizationReadDenied`). The example receives + `Accept`s the
   message it just sent.
5. **DETACH / CLOSE** — links detach; the connection closes.

### Identity precedence (connector contract)

- **Auth ENABLED:** the JWT in the SASL PLAIN **password** must validate; the
  identity is derived from the verified token. The SASL **username** is recorded for
  audit (`auth.success` / `auth.failure`) only.
- **Auth DISABLED (stock dev default):** the SASL PLAIN **username** becomes the
  ClientID and any password is accepted; with ANONYMOUS, a default identity is used.

> **Why the JWT goes in the password, not the username:** the connector derives the
> verified identity from the token, so the JWT must be the SASL PLAIN **password**.
> The username is audit metadata only — set it to a human-readable service label.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| connection | — | — | — | SASL PLAIN (JWT in password) or ANONYMOUS; bad JWT ⇒ `amqp:unauthorized-access` | conn: SASL mechanism via Address userinfo / `SASL.Profile` | — | username audit-only; identity from JWT |
| sender (client → KubeMQ) | target `queues/<ch>` | unsettled (default) | server-granted | `accepted` per send; denied identity ⇒ `amqp:unauthorized-access` at attach/send | none | `Data` | WRITE authorization at attach/send |
| receiver (KubeMQ → client) | source `queues/<ch>` | `first` (default) | client-granted `SetCredit(1)` | `Accept` ⇒ AckRange; denied identity ⇒ `amqp:unauthorized-access` at attach | none | `Data` | READ authorization at attach |

## Related Examples

- [queues/basic-send-receive](../../queues/basic-send-receive/) — the same round-trip without explicit auth
- [advanced/anonymous-terminus](../../advanced/anonymous-terminus/) — per-message Casbin WRITE on an anonymous link

## Gotcha

> **`AMQPNetLite.Core`'s `SaslPlainProfile` is internal — drive SASL PLAIN through
> the `Address` userinfo.** When the `Address` carries a User + Password, the client
> negotiates PLAIN; with neither, it negotiates ANONYMOUS. This example uses the
> `Address(host, port, user, password, path, scheme)` constructor so a JWT (which can
> contain `/`, `+`, `=` and `.`) is passed verbatim without URL-encoding. There is no
> runnable `connectivity/tls` variant — TLS is doc-only; use an `amqps://` URL +
> `Connection.DisableServerCertValidation` only for local testing.
>
> **Keep the producer open through the consume.** As in
> [queues/basic-send-receive](../../queues/basic-send-receive/), detaching the sender
> before the sibling receiver on the same connection has drained can stall delivery
> to that receiver (an AMQPNetLite/connector interaction). This example closes the
> sender at the very end, after the receive.

---

This IS the auth example. For the conceptual identity-precedence walkthrough see
`guides/authentication.md`; for the default ANONYMOUS round-trip see
`queues/basic-send-receive`.
