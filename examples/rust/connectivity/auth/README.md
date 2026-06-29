# Rust — Connectivity / Auth

The one runnable authentication variant. It connects to the KubeMQ AMQP 1.0 connector
with SASL **PLAIN** — the username is audit-only and the password is a KubeMQ JWT —
then runs a `queues/<ch>` round-trip. With no credentials in the environment it falls
back to **ANONYMOUS**, so it clone-and-runs on a stock dev broker. Driven with the
native `fe2o3-amqp` client (NO KubeMQ SDK).

## Prerequisites

- Rust 1.94+ (stable) with `cargo`.
- `fe2o3-amqp` 0.15.1 + `fe2o3-amqp-types` 0.14.0 (pinned exact in the workspace
  `examples/rust/Cargo.toml`).
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

ANONYMOUS (stock dev broker — no credentials):

```bash
cd examples/rust
cargo run -p auth
```

SASL PLAIN with a KubeMQ JWT (auth-enabled broker):

```bash
cd examples/rust
KUBEMQ_AMQP_USER=my-service KUBEMQ_AMQP_JWT=<a-kubemq-jwt> cargo run -p auth
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 cargo run -p auth
```

| Env var | Meaning |
|---|---|
| `KUBEMQ_AMQP_USER` | SASL PLAIN username (audit identity; defaults to a label) |
| `KUBEMQ_AMQP_JWT` | SASL PLAIN password = a KubeMQ JWT (required to use PLAIN) |

If `KUBEMQ_AMQP_JWT` is set the example dials SASL PLAIN; otherwise it dials ANONYMOUS.

## Expected Output

ANONYMOUS (no env set):

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

SASL PLAIN (`KUBEMQ_AMQP_USER` + `KUBEMQ_AMQP_JWT` set):

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

1. **Choose the mechanism** — the example reads `KUBEMQ_AMQP_USER` / `KUBEMQ_AMQP_JWT`.
   With a JWT it builds `SaslProfile::Plain { username, password: jwt }`; otherwise
   `SaslProfile::Anonymous`.
2. **OPEN** — `Connection::builder().sasl_profile(...).open(url)` runs the SASL
   handshake. With auth ENABLED, a JWT that fails validation makes the open fail
   (`amqp:unauthorized-access` at the SASL layer). With auth DISABLED, any credential
   is accepted.
3. **BEGIN** — one session carries the round-trip.
4. **ATTACH + SEND** — the WRITE authorization check runs at sender attach / send. With
   authorization ENABLED, an identity without a WRITE grant on the channel is refused
   with `amqp:unauthorized-access`. The sender pins `SenderSettleMode::Unsettled`.
5. **DISPOSITION** — the send is confirmed `Accepted` (the broker stored it).
6. **ATTACH + RECEIVE** — the READ authorization check runs at receiver attach; a
   denied identity's receiver attach is refused with `amqp:unauthorized-access`.
7. **`accept`** — the delivery is accepted (AckRange) and removed from the queue.
8. **DETACH / CLOSE** — the receiver, session, and connection are torn down cleanly.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| connection | — | SASL PLAIN (user+JWT) or ANONYMOUS | — | — | — | — | identity from the verified JWT; username audit-only |
| sender (client → KubeMQ) | target `queues/<ch>` | `Unsettled` (explicit) | server-granted | `Accepted` per send | none | `Data` | WRITE authz at attach/send |
| receiver (KubeMQ → client) | source `queues/<ch>` | `First` (default) | client `CreditMode::Auto(1)` | `accept` ⇒ AckRange (removed) | none | `Data` | READ authz at attach |

## Identity precedence

- **Auth ENABLED** — the JWT in the SASL PLAIN *password* must validate; the
  ClientID/identity is derived from the verified token. The SASL *username* is recorded
  for audit (`auth.success` / `auth.failure`) only.
- **Auth DISABLED** (stock dev-broker default) — the SASL PLAIN *username* becomes the
  ClientID and any password is accepted; with ANONYMOUS, a default identity is used.

## Related Examples

- [queues/basic-send-receive](../../queues/basic-send-receive/) — the same queue round-trip without the auth banner
- [advanced/anonymous-terminus](../../advanced/anonymous-terminus/) — per-message authorization on an anonymous link

## Gotchas

> **A denied attach is refused with `amqp:unauthorized-access`.** Authorization is
> checked at link attach (READ on a receiver, WRITE on a sender) — see the connector's
> `TestAuthorizationReadDenied`. A bad/expired JWT fails the SASL handshake at OPEN
> (`TestAuthenticationBadCredential`).

- **The SASL username is audit-only when auth is enabled.** Identity comes from the
  JWT; the username only appears in the `auth.success` / `auth.failure` audit events
  (with the mechanism + source IP).
- **There is no `connectivity/tls` runnable example.** TLS / mTLS is doc-only — see
  `guides/tls-and-mtls.md`.
- **Sender settle-mode must be explicit** (`Unsettled`); `fe2o3-amqp` defaults to
  `mixed`, which the connector rejects at ATTACH.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT this
IS that example — set `KUBEMQ_AMQP_USER` + `KUBEMQ_AMQP_JWT`. See also
`guides/authentication.md`.
