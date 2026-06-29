# Configuration

The AMQP 1.0 connector is configured server-side through **14 settings** under
the `Connectors.Amqp10.*` config namespace. None of them live in this repo
they are set on the **KubeMQ server**. This page documents each setting so you
know what behavior the broker your clients connect to will exhibit.

> The single thing **clients** in this repo configure is the broker endpoint via
> the `KUBEMQ_AMQP_URL` environment variable (default `amqp://localhost:5672`).
> Everything below is broker-side server configuration.

## Enable / disable

The connector is **enabled by default**. To turn it off:

```bash
CONNECTORS_AMQP10_ENABLE=false
```

When `Enable` is `false`, **all other AMQP 1.0 validation is skipped** and the
connector does not bind a listener.

## Environment-variable form

Each setting binds a mixed-case config key `Connectors.Amqp10.<Field>`, which the
server's `convertEnvFormat` helper turns into an environment variable by
snake-casing the field, stripping the `.` separators, and upper-casing. So
`Connectors.Amqp10.TlsPort` → **`CONNECTORS_AMQP10_TLS_PORT`**. The literal `10`
stays attached to `AMQP` in every variable name.

## The 14 settings

| Field | Environment variable | Default | Type | Validation / notes |
|-------|----------------------|---------|------|--------------------|
| `Enable` | `CONNECTORS_AMQP10_ENABLE` | `true` | bool | When `false`, all other validation is skipped and no listener binds. |
| `Port` | `CONNECTORS_AMQP10_PORT` | `5672` | int | `0..65535`; `0` disables the plain listener. **Shared with the 0-9-1 connector** — equal to the 0-9-1 port is intentionally accepted (the `amqpmux` dedupes the bind). |
| `TlsPort` | `CONNECTORS_AMQP10_TLS_PORT` | `5671` | int | `0..65535`; `0` disables TLS. The TLS listener binds only when the server-global `Security` block is configured. Shared with 0-9-1 TLS. |
| `MaxFrameSize` | `CONNECTORS_AMQP10_MAX_FRAME_SIZE` | `131072` | int | `>= 512`. Advertised in `OPEN`; the effective frame size is `min(client, MaxFrameSize)`. |
| `MaxMessageSize` | `CONNECTORS_AMQP10_MAX_MESSAGE_SIZE` | `104857600` (100 MiB) | int64 | `> 0`. One byte over → `amqp:link:message-size-exceeded`. |
| `SessionMax` | `CONNECTORS_AMQP10_SESSION_MAX` | `256` | int | `1..65535`. Advertises `channel-max = min(client, SessionMax-1)` (255 with the default). |
| `MaxLinksPerSession` | `CONNECTORS_AMQP10_MAX_LINKS_PER_SESSION` | `256` | int | `>= 1`. |
| `MaxConnections` | `CONNECTORS_AMQP10_MAX_CONNECTIONS` | `1000` | int | `>= 0` (`0` = unlimited). Per-node, independent of the 0-9-1 connector's cap. |
| `IdleTimeoutSeconds` | `CONNECTORS_AMQP10_IDLE_TIMEOUT_SECONDS` | `120` | int | `>= 0` (`0` = disabled). Advertises `idle-time-out = IdleTimeoutSeconds*1000/2` ms. |
| `DefaultPattern` | `CONNECTORS_AMQP10_DEFAULT_PATTERN` | `"queues"` | string | Exactly one of `queues` \| `events` \| `events-store` \| `commands` \| `queries`. Used only to resolve a **bare** (prefix-less) address with no JMS capability hint. |
| `GetBatchSize` | `CONNECTORS_AMQP10_GET_BATCH_SIZE` | `32` | int | `1..1024`. Ceiling on the queue `Get` `MaxItems` per long-poll. |
| `MaxUnsettledPerLink` | `CONNECTORS_AMQP10_MAX_UNSETTLED_PER_LINK` | `1024` | int | `>= 1`. The unsettled / pub-sub buffer cap; drives the inbound credit the server grants. |
| `DefaultRpcTimeoutSeconds` | `CONNECTORS_AMQP10_DEFAULT_RPC_TIMEOUT_SECONDS` | `30` | int | `>= 1`. The default RPC reply timeout when a request carries no `ttl`. |
| `RpcMaxPending` | `CONNECTORS_AMQP10_RPC_MAX_PENDING` | `512` | int | `>= 1`. When full, a new request → `amqp:resource-limit-exceeded`. |

A disabled connector (`Enable: false`) is always valid regardless of the other
fields. If at least one of `Port` / `TlsPort` is not set (both `0`), validation
fails.

## `DefaultPattern` and the bare-address fallback

`DefaultPattern` only matters for **bare addresses** — a node address with no
recognized `<pattern>/` prefix and no JMS node-capability hint
(`queue` → `queues`, `topic` → `events`). It must be one of the five pattern
names; anything else fails validation. Since every example in this repo emits the
**explicit prefix**, `DefaultPattern` never affects them. Treat bare addressing
as a Qpid-JMS migration convenience only (it is non-deterministic by design)
see [guides/addressing.md](guides/addressing.md).

## Port sharing with AMQP 0-9-1

By default both the AMQP 1.0 connector and the 0-9-1 (RabbitMQ) connector listen
on `5672` (plain) and `5671` (TLS). This is **intentional and supported**: a
single `amqpmux` listener accepts every connection, reads the 8-byte protocol
header, and routes it to the correct dialect engine. The configuration `Validate`
step does **not** cross-check the two ports — equal ports are accepted and the
mux dedupes the bind. See [architecture.md](architecture.md) for the dispatch
detail.

## Hot-reload

These settings are **not hot-reloadable** — changing any of them requires a
**server restart** to take effect.

## TLS

TLS for the AMQP 1.0 connector is driven entirely by the **server-global
`Security` block** (the same one the gRPC/0-9-1 listeners use), not by an
AMQP-1.0-specific TLS field. When `Security` is configured, the shared `amqpmux`
TLS listener binds on `TlsPort` with `MinVersion` TLS 1.2; mTLS
(`SecurityModeMTLS`) adds the client CA pool and requires a verified client
certificate (mapped to SASL EXTERNAL). TLS is **documentation-only** in this repo
— see [guides/tls-and-mtls.md](guides/tls-and-mtls.md).

---
