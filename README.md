# KubeMQ AMQP 1.0

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Languages](https://img.shields.io/badge/languages-6%20%28Go%2C%20Python%2C%20Java%2C%20C%23%2C%20JS%2FTS%2C%20Rust%3B%20no%20Ruby%29-informational.svg)](examples/)
[![Examples](https://img.shields.io/badge/examples-78-success.svg)](examples/)
[![Protocol](https://img.shields.io/badge/protocol-AMQP%201.0%20%28OASIS%20%2F%20ISO--IEC%2019464%29-orange.svg)](docs/)
[![Direct Connect](https://img.shields.io/badge/KubeMQ-direct--connect-9cf.svg)](https://kubemq.io/)

Documentation, working examples, and a burn-in soak harness for the **KubeMQ embedded AMQP 1.0 connector** — the bridge that lets standard **native AMQP 1.0 clients** talk to KubeMQ's Queues, Events, Events-Store, Commands, and Queries patterns over the AMQP 1.0 wire protocol.

The whole point: a Qpid-JMS / AMQP 1.0 app points at KubeMQ by changing only the **connection string** and the **node address** — no code rewrite, no library swap.

The examples and burn-in use **standard third-party AMQP 1.0 libraries only** — there is **no KubeMQ SDK, no proto bindings, and no published package** in this repo. They speak to the connector exactly the way any off-the-shelf AMQP 1.0 client would.

## What's here

| Path | What it is |
|------|------------|
| [`docs/`](docs/) | Connector reference: architecture, getting-started, configuration, concept docs, pattern guides, the address grammar, capabilities, and the `amqp:*` error conditions. |
| [`examples/`](examples/) | Runnable, per-pattern examples in Go, Python, Java, C#, JavaScript/TypeScript, and Rust — each using a pinned native AMQP 1.0 client. |
| [`burnin/`](burnin/) | Standalone Go soak-test harness that exercises the connector under sustained multi-pattern load. |

## Prerequisites

A running **KubeMQ server with the AMQP 1.0 connector** — the connector is **enabled by default**. It listens on AMQP port `5672` (plain TCP / SASL) and, when the server-global TLS `Security` block is configured, on port `5671` (TLS — documentation-only in this repo).

```bash
docker run -d \
  -p 5672:5672 \
  -p 5671:5671 \
  -p 50000:50000 \
  kubemq/kubemq
```

> The connector is on by default; set `CONNECTORS_AMQP10_ENABLE=false` to disable it.

## Configuration

Every example reads a **single** environment variable for the broker endpoint:

```bash
# default: amqp://localhost:5672
export KUBEMQ_AMQP_URL=amqp://localhost:5672
```

| Scheme | Transport | Default endpoint |
|--------|-----------|------------------|
| `amqp://`  | Plain TCP (SASL ANONYMOUS / PLAIN) | `amqp://localhost:5672` |
| `amqps://` | TLS over TCP (doc-only)            | `amqps://localhost:5671` |

## Address grammar (at a glance)

A node address is `<pattern>/<channel>`, where the leading **pattern** selects the KubeMQ pattern and the remainder is the KubeMQ channel:

| Pattern | KubeMQ pattern | Example address |
|---------|----------------|-----------------|
| `queues`        | Queues             | `queues/jobs` |
| `events`        | Events             | `events/site1.temp` |
| `events-store`  | Events-Store       | `events-store/orders` |
| `commands`      | Commands (RPC)     | `commands/restart` |
| `queries`       | Queries (RPC)      | `queries/status` |

See [`docs/`](docs/) for the full grammar (longest-prefix matching, dynamic / anonymous terminus, the write-only `/responses/<RequestID>` reply path) and the connector's capabilities and `amqp:*` error conditions.

## Client libraries

The examples pin one native AMQP 1.0 client per language:

| Language | Library | Notes |
|----------|---------|-------|
| Go         | [`github.com/Azure/go-amqp`](https://github.com/Azure/go-amqp) | the connector's own reference client |
| Python     | [`python-qpid-proton`](https://qpid.apache.org/proton/) | sync `BlockingConnection`; managed with `uv` |
| Java       | [`org.apache.qpid:qpid-jms-client`](https://qpid.apache.org/components/jms/) | `javax.jms` (not Jakarta) |
| C# / .NET  | [`AMQPNetLite.Core`](https://github.com/Azure/amqpnetlite) | task-based async |
| JS / TS    | [`rhea`](https://github.com/amqp/rhea) + [`rhea-promise`](https://github.com/amqp/rhea-promise) | event-driven, promise-wrapped |
| Rust       | [`fe2o3-amqp`](https://github.com/minghuaw/fe2o3-amqp) | async/await on tokio |

## Getting started

1. Start a KubeMQ server with the AMQP 1.0 connector enabled (see [Prerequisites](#prerequisites)).
2. Read [`docs/getting-started.md`](docs/getting-started.md).
3. Pick a language under [`examples/`](examples/) and follow its README.

## License

[Apache-2.0](LICENSE)
