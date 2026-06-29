# Python — Connectivity / Authentication

The **one runnable authentication variant**. It connects to the KubeMQ AMQP 1.0
connector with **SASL PLAIN** — the username is **audit-only** and the password is
a **KubeMQ JWT** — then runs a `queues/<ch>` round-trip. Driven with the native
`python-qpid-proton` blocking client.

On a stock dev broker (authentication OFF, ANONYMOUS accepted), the example reads
the credentials from the environment and **falls back to ANONYMOUS** when they are
unset — so it clone-and-runs either way.

## Prerequisites

- Python 3.10+
- `python-qpid-proton` 0.40.0 (pinned in `examples/python/pyproject.toml`; the
  prebuilt `python-qpid-proton-wheel` installs without a C toolchain)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

**ANONYMOUS (stock dev broker):**

```bash
cd examples/python
uv sync
uv run python connectivity/auth/main.py
```

**SASL PLAIN with a KubeMQ JWT (auth-enabled broker):**

```bash
export KUBEMQ_AMQP_USER=my-service          # audit identity (optional; defaults to a label)
export KUBEMQ_AMQP_JWT=<a-kubemq-jwt>       # SASL PLAIN password = a KubeMQ JWT
uv run python connectivity/auth/main.py
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 uv run python connectivity/auth/main.py
```

## Expected Output

ANONYMOUS (no env set):

```
Broker:  amqp://localhost:5672
Address: queues/amqp10.examples.auth  (KubeMQ pattern=queues, channel=amqp10.examples.auth)
Auth:    ANONYMOUS  (KUBEMQ_AMQP_JWT unset -- falling back to the dev-broker default)
         Set KUBEMQ_AMQP_USER + KUBEMQ_AMQP_JWT to dial SASL PLAIN with a KubeMQ JWT.

[open] Connected -- SASL handshake accepted
[send] Produced 1 message to queues/amqp10.examples.auth (accepted)
[recv] Consumed and accepted 1 message: 'auth-round-trip'

Done.
```

SASL PLAIN (`KUBEMQ_AMQP_USER` + `KUBEMQ_AMQP_JWT` set):

```
Broker:  amqp://localhost:5672
Address: queues/amqp10.examples.auth  (KubeMQ pattern=queues, channel=amqp10.examples.auth)
Auth:    SASL PLAIN  (username="my-service" audit-only; password=<KubeMQ JWT>)

[open] Connected -- SASL handshake accepted
[send] Produced 1 message to queues/amqp10.examples.auth (accepted)
[recv] Consumed and accepted 1 message: 'auth-round-trip'

Done.
```

## What's Happening

1. **Choose the mechanism from the environment** — if `KUBEMQ_AMQP_JWT` is set,
   dial **SASL PLAIN** via `BlockingConnection(url, user=..., password=<JWT>,
   allowed_mechs="PLAIN", allow_insecure_mechs=True)`; otherwise dial **ANONYMOUS**
   via `allowed_mechs="ANONYMOUS"`. (`allow_insecure_mechs=True` permits PLAIN over
   a plaintext `amqp://` socket — a dev broker. Use `amqps://` + TLS in production.)
2. **OPEN — the SASL handshake** — happens during connect. With auth **enabled**, a
   JWT that fails validation makes the connect fail with `amqp:unauthorized-access`
   at the SASL layer (`TestAuthenticationBadCredential`). With auth **disabled**,
   any credential is accepted.
3. **ATTACH + SEND** — the **WRITE** authorization check runs at sender attach /
   send. An identity without a WRITE grant on the channel is refused with
   `amqp:unauthorized-access`.
4. **ATTACH + RECEIVE** — the **READ** authorization check runs at receiver attach;
   a denied identity's receiver attach is refused with `amqp:unauthorized-access`
   (`TestAuthorizationReadDenied`).
5. **DETACH / CLOSE** — the links detach and the connection closes.

**Identity precedence (connector contract):**
- Auth **enabled** → the **JWT (password)** must validate; identity is derived
  from the verified token. The **username** is recorded for audit
  (`auth.success` / `auth.failure`) only.
- Auth **disabled** → the SASL PLAIN **username** becomes the ClientID and any
  password is accepted; with **ANONYMOUS**, a default identity is used.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| connection (SASL) | — | — | — | `amqp:unauthorized-access` on a bad/expired JWT | — | — | SASL PLAIN: username audit-only, password = JWT; ANONYMOUS fallback |
| sender (client → KubeMQ) | target `queues/<ch>` | unsettled (default) | server-granted | `accepted`; WRITE denied → `amqp:unauthorized-access` | none | `Data` | authorization checked at attach/send |
| receiver (KubeMQ → client) | source `queues/<ch>` | `first` (default) | client-granted `credit=1` | `accepted`; READ denied → `amqp:unauthorized-access` | none | `Data` | authorization checked at attach |

## Gotchas

> **PLAIN over a plaintext socket needs `allow_insecure_mechs=True`.** Without it,
> proton refuses to send PLAIN credentials over an unencrypted `amqp://` transport.
> This is fine for a dev broker; in production, use `amqps://` + TLS so credentials
> (and the JWT) are encrypted in transit. **TLS is documentation-only in this repo**
> (see `guides/tls-and-mtls.md`); there is no runnable `connectivity/tls` variant.

- **The JWT goes in the SASL *password*, not the username.** The username is
  audit-only when auth is enabled.
- **Authorization is checked per link.** WRITE at sender attach/send, READ at
  receiver attach — both denied with `amqp:unauthorized-access`.
- **SCRAM / GSSAPI / CBS are not supported.** Only PLAIN and ANONYMOUS.

## Related Examples

- [queues/basic_send_receive](../../queues/basic_send_receive/) — the same queue round-trip without explicit auth (ANONYMOUS)
- [advanced/anonymous_terminus](../../advanced/anonymous_terminus/) — per-message WRITE authorization on a null-target sender
- [commands/request_reply_dynamic_node](../../commands/request_reply_dynamic_node/) — the reply-to snooping guard (`amqp:not-allowed`), another connection-scoped check

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT this is the variant — see also `guides/authentication.md`.
