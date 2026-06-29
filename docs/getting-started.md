# Getting Started

Get a message flowing through the KubeMQ AMQP 1.0 connector in minutes.

## Connection assumption

This repo assumes a **running KubeMQ server with the AMQP 1.0 connector
enabled** — it is **enabled by default** and served by the shared `amqpmux`
front door on plain port **5672**. There is **no docker-compose** and no
boot-the-server step in this repo; point your client at an existing broker.

Bring up a broker any way you already run KubeMQ. For a throwaway local one:

```bash
docker run -d -p 5672:5672 -p 50000:50000 kubemq/kubemq
```

> The connector is on by default. Set `CONNECTORS_AMQP10_ENABLE=false` to turn
> it off; see [configuration.md](configuration.md) for all 14 connector settings.

## The one connection variable

Every example reads a **single** environment variable for the broker endpoint:

```bash
# default: amqp://localhost:5672
export KUBEMQ_AMQP_URL="amqp://localhost:5672"
```

A default URL with no userinfo negotiates **SASL ANONYMOUS** — so a stock dev
broker is clone-and-run with no credentials. That is the runnable default for
every example in this repo.

| Scheme | Transport | Endpoint |
|--------|-----------|----------|
| `amqp://` | Plain TCP (SASL ANONYMOUS / PLAIN) | `amqp://localhost:5672` |
| `amqps://` | TLS over TCP (documentation-only — see [guides/tls-and-mtls.md](guides/tls-and-mtls.md)) | `amqps://localhost:5671` |

For SASL PLAIN with a KubeMQ JWT in the password, the form is
`amqp://<user>:<JWT>@host:5672` — see [guides/authentication.md](guides/authentication.md)
and the [`connectivity/auth`](../examples/go/connectivity/auth/) example. There
is **no vhost** in AMQP 1.0 — the OPEN `hostname` is logged then ignored.

## Addresses at a glance

A node address is `<pattern>/<channel>`. The leading **pattern** selects the
KubeMQ pattern; the rest is the KubeMQ channel. Examples in this repo **always
emit the explicit prefix**:

| Pattern | KubeMQ pattern | Example address |
|---------|----------------|-----------------|
| `queues` | Queues | `queues/amqp10.examples.basic` |
| `events` | Events | `events/amqp10.examples.pubsub` |
| `events-store` | Events-Store | `events-store/orders` |
| `commands` | Commands (RPC) | `commands/restart` |
| `queries` | Queries (RPC) | `queries/status` |

See [concepts/addresses-and-nodes.md](concepts/addresses-and-nodes.md) and
[reference/address-mapping.md](reference/address-mapping.md) for the full grammar
(longest-prefix matching, the channel charset, dynamic/anonymous terminus, and
the write-only `/responses/<RequestID>` reply path).

## Run your first example

Pick any language and run the `queues/basic-send-receive` variant — it produces
10 messages to a queue, consumes and `accept`s each, and confirms the queue drains
with no loss.

```bash
# Go
cd examples/go && go run ./queues/basic-send-receive

# Python (uv)
cd examples/python && uv sync && uv run python queues/basic_send_receive/main.py

# Java (Qpid JMS, javax.jms)
cd examples/java && mvn -q -pl queues/basic-send-receive exec:java

# C# / .NET (AMQPNetLite.Core)
cd examples/csharp/queues/basic-send-receive && dotnet run

# JavaScript / TypeScript (rhea + rhea-promise)
cd examples/javascript && npm install && npx tsx queues/basic-send-receive/index.ts

# Rust (fe2o3-amqp)
cd examples/rust && cargo run -p basic-send-receive
```

Override the broker on any of them:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 go run ./queues/basic-send-receive
```

## What success looks like

```
Broker: amqp://localhost:5672
Address: queues/amqp10.examples.basic (KubeMQ pattern=queues, channel=amqp10.examples.basic)

[send] Produced 10 messages to queues/amqp10.examples.basic (accepted DISPOSITION each)
[recv] Consumed and accepted 10 messages (no loss)
[recv] Queue drained to empty (no further messages)

Done.
```

Behind that, the AMQP 1.0 handshake ran end-to-end: **OPEN** (with an
auto-generated non-empty `container-id`) → **BEGIN** → **ATTACH** (sender, then
receiver) → **TRANSFER / FLOW / DISPOSITION** (the receiver grants credit, each
`accept` resolves to an AckRange and removes the message) → **DETACH / CLOSE**.
See the per-variant README for the frame-by-frame walkthrough.

## Where to go next

1. **[architecture.md](architecture.md)** — the connection/session/link model,
 the shared `amqpmux` front door, and how AMQP 1.0 maps onto the five patterns.
2. **[concepts/](README.md#concepts)** — one doc per protocol idea.
3. **[../examples/README.md](../examples/README.md)** — the full 13-variant ×
 6-language matrix and the universal client recipe.

> **Two data-loss footguns to internalize early:** Events drop silently at zero
> credit (always keep a consumer's credit topped up), and Events-Store stalls and
> loses its window if a durable consumer's credit is not replenished. Both are
> covered first-class in [guides/flow-control.md](guides/flow-control.md).

---

Runs **ANONYMOUS by default** on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see [`connectivity/auth`](../examples/go/connectivity/auth/) and
[guides/authentication.md](guides/authentication.md).
