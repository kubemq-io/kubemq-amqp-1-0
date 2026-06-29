# JavaScript — Connectivity / Auth

The one runnable authentication variant: connect to the KubeMQ AMQP 1.0 connector
with **SASL PLAIN**, where the **username is audit-only** and the **password is a
KubeMQ JWT**, then run a `queues/<ch>` round-trip. Uses the native `rhea` /
`rhea-promise` client (no KubeMQ SDK).

It is written to **clone-and-run on a stock dev broker** (authentication OFF,
ANONYMOUS accepted): it reads the credentials from the environment and falls back to
ANONYMOUS when they are unset.

## Prerequisites

- Node.js 20+ (developed against Node 26).
- `rhea` 3.0.4 + `rhea-promise` 3.0.3 (pinned in `examples/javascript/package.json`);
  run via `tsx`.
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).
- *(Optional, to exercise SASL PLAIN)* an auth-enabled broker and a KubeMQ JWT.

Install once from `examples/javascript`:

```bash
npm install
```

## How to Run

ANONYMOUS (stock dev broker — nothing to set):

```bash
cd examples/javascript
npx tsx connectivity/auth/index.ts
```

SASL PLAIN with a KubeMQ JWT (auth-enabled broker):

```bash
cd examples/javascript
export KUBEMQ_AMQP_USER=my-service     # audit identity (optional; a label only)
export KUBEMQ_AMQP_JWT=<a-kubemq-jwt>   # the SASL PLAIN password
npx tsx connectivity/auth/index.ts
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 npx tsx connectivity/auth/index.ts
```

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

1. **Select the mechanism** — the example reads `KUBEMQ_AMQP_USER` /
   `KUBEMQ_AMQP_JWT`. If a JWT is present it sets `username` + `password` on the
   connection (rhea negotiates **SASL PLAIN** when both are present); otherwise it
   leaves them unset (rhea negotiates **ANONYMOUS**, the dev-broker default) and
   prints a note.
2. **OPEN (SASL handshake)** — `new Connection({username, password})` +
   `connection.open()`. The SASL exchange happens here. With auth **enabled**, the
   JWT in the PLAIN password must validate or `open()` rejects; with auth
   **disabled**, any credential is accepted.
3. **BEGIN** — the sender/receiver create their own session under the hood.
4. **ATTACH (sender) + WRITE authz** —
   `createAwaitableSender({target:{address:"queues/<ch>"}})`. With authorization
   enabled, the connection's identity must hold a **WRITE** grant on this channel;
   otherwise the attach is refused with `amqp:unauthorized-access`.
5. **TRANSFER / DISPOSITION** — one unsettled `send()` resolves on the connector's
   `accepted` DISPOSITION.
6. **ATTACH (receiver) + READ authz** —
   `createReceiver({source:{address:"queues/<ch>"}})` with manual credit. A denied
   identity's receiver attach is refused with `amqp:unauthorized-access` (the
   `TestAuthorizationReadDenied` contract).
7. **Receive / Accept** — one message is consumed and accepted.
8. **DETACH / CLOSE** — links detach and the connection closes.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| connection (SASL PLAIN) | n/a (`{username, password}` ⇒ rhea negotiates PLAIN) | n/a | n/a | bad/expired JWT ⇒ `open()` rejects (SASL `amqp:unauthorized-access`) | username audit-only; password = KubeMQ JWT | n/a | identity derived from the verified JWT when auth is enabled |
| sender (client → KubeMQ) | target `queues/<ch>` | unsettled (default) | server-granted | `accepted` DISPOSITION; denied attach ⇒ `amqp:unauthorized-access` | none | `Data` | WRITE authz checked at attach/send |
| receiver (KubeMQ → client) | source `queues/<ch>` | `first` (default) | client-granted (`addCredit(1)`) | `accept()` ⇒ AckRange (removed); denied attach ⇒ `amqp:unauthorized-access` | none | `Data` | READ authz checked at attach |

## Gotchas

> **Identity precedence depends on whether authentication is enabled.**
> - **Auth ENABLED:** the SASL PLAIN **password must be a valid KubeMQ JWT**. The
>   ClientID/identity is taken from the **verified token**; the SASL **username is
>   audit-only** (recorded with `auth.success` / `auth.failure`, alongside mechanism
>   + source IP). A bad/expired JWT fails the handshake (`amqp:unauthorized-access` —
>   `TestAuthenticationBadCredential`).
> - **Auth DISABLED (stock dev broker):** the SASL PLAIN **username becomes the
>   ClientID** and any password is accepted; ANONYMOUS uses a default identity.
>
> Authorization (the WRITE/READ channel grants) is a **separate** Casbin layer: a
> denied attach is refused with `amqp:unauthorized-access`
> (`TestAuthorizationReadDenied`) even when authentication succeeded.

- **rhea selects PLAIN when `username` + `password` are set.** Provide both on the
  connection options; omit both to negotiate ANONYMOUS. There is no separate
  mechanism flag in this example.
- **No `connectivity/tls` variant.** TLS / mTLS is **doc-only** for this repo — see
  `guides/tls-and-mtls.md`. This example uses plain `amqp://`; in production carry
  the JWT over `amqps://`.
- **The JWT is a secret.** Pass it via `KUBEMQ_AMQP_JWT` (env), never hard-code it or
  commit it.

## Related Examples

- [queues/basic-send-receive](../../queues/basic-send-receive/) — the same round-trip, ANONYMOUS
- [advanced/anonymous-terminus](../../advanced/anonymous-terminus/) — per-message WRITE authz
- See `guides/authentication.md` and `reference/error-conditions.md` for the full
  identity contract and the 13 `amqp:*` conditions.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT set `KUBEMQ_AMQP_USER` + `KUBEMQ_AMQP_JWT` (see `guides/authentication.md`).
