# Python — KubeMQ AMQP 1.0 Examples

Native [`python-qpid-proton`](https://qpid.apache.org/proton/) examples for the
embedded KubeMQ **AMQP 1.0** connector. Each example points a stock AMQP 1.0 client
at KubeMQ — **no KubeMQ SDK, no gRPC, no proto**. You drive KubeMQ by changing only
the connection string and the link address (the `<pattern>/<channel>` node name).

- **Client library:** `python-qpid-proton==0.40.0` (pinned in
  [`pyproject.toml`](pyproject.toml)).
- **Concurrency model:** synchronous **`BlockingConnection`** — one connection per
  thread. The blocking client is single-threaded per connection, so any example
  that needs two concurrent roles (RPC requester + responder, multiple consumer
  group members) runs each role on its **own connection in its own thread**.
- **Package manager:** [**uv**](https://docs.astral.sh/uv/).

## Prerequisites

- Python **3.10+**
- [uv](https://docs.astral.sh/uv/) (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- A running KubeMQ broker with the AMQP 1.0 connector (**enabled by default**),
  reachable at `KUBEMQ_AMQP_URL`.

`uv sync` builds `python-qpid-proton` from source. On most systems this is
transparent; if your machine has no C toolchain, install the prebuilt wheel package
(`uv add python-qpid-proton-wheel`) so the examples clone-and-run.

## How to Run

```bash
cd examples/python
uv sync                                            # create .venv + install deps
uv run python <group>/<variant>/main.py            # run any example

# example:
uv run python queues/basic_send_receive/main.py
```

### Broker URL

Every example reads `KUBEMQ_AMQP_URL`, defaulting to `amqp://localhost:5672`:

```bash
export KUBEMQ_AMQP_URL=amqp://localhost:5672       # the default
KUBEMQ_AMQP_URL=amqp://my-server:5672 uv run python events/basic_pubsub/main.py
```

### Authentication

Examples run **SASL ANONYMOUS** by default — the stock dev-broker mode. For SASL
PLAIN with a KubeMQ JWT (the username is audit-only, the password is the JWT), see
[`connectivity/auth`](connectivity/auth/) and `docs/guides/authentication.md`.

## Addressing

Every link address uses an **explicit `<pattern>/<channel>` prefix** —
`queues/<ch>`, `events/<ch>`, `events-store/<ch>`, `commands/<ch>`, `queries/<ch>`.
Examples never rely on a default pattern.

## The 13 Examples

| # | Example | Pattern | Demonstrates |
|---|---------|---------|--------------|
| 1 | [queues/basic_send_receive](queues/basic_send_receive/) | Queues | at-least-once produce + credit consume + accept; queue drains, no loss |
| 2 | [queues/ack_release_redelivery](queues/ack_release_redelivery/) | Queues | accept removes; release requeues (grown delivery-count); reject discards |
| 3 | [queues/settlement_modes](queues/settlement_modes/) | Queues | pre-settled (at-most-once) vs unsettled (at-least-once) producers; rcv-settle-mode=first |
| 4 | [events/basic_pubsub](events/basic_pubsub/) | Events | fan-out at-most-once; subscribe-before-publish; 0-credit ⇒ silent drop |
| 5 | [events/consumer_group](events/consumer_group/) | Events | `x-opt-kubemq-group`: members split a stream; distinct groups each get the full stream |
| 6 | [events/selector](events/selector/) | Events | SQL-92 selector (`apache.org:selector-filter:string`); 3-valued logic; rejected on `queues/` |
| 7 | [events_store/durable_replay](events_store/durable_replay/) | Events Store | durable subscription + resume after disconnect (container-id + link name) |
| 8 | [events_store/start_positions](events_store/start_positions/) | Events Store | `x-opt-kubemq-start` grammar: first / new-only / last / sequence / time / time-delta |
| 9 | [commands/request_reply_dynamic_node](commands/request_reply_dynamic_node/) | Commands (RPC) | native RPC; dynamic reply node; `executed`/`error` reply props; failure still replies |
| 10 | [queries/request_reply](queries/request_reply/) | Queries (RPC) | native RPC; reply = body + metadata only; a failed query delivers nothing (timeout) |
| 11 | [advanced/multi_frame_large_payload](advanced/multi_frame_large_payload/) | Queues | body > max-frame-size fragments + reassembles bit-exact (CRC-verified) |
| 12 | [advanced/anonymous_terminus](advanced/anonymous_terminus/) | (routes by `to`) | anonymous sender (null target) selects the destination per-message via `to` |
| 13 | [connectivity/auth](connectivity/auth/) | (connection) | SASL PLAIN with a KubeMQ JWT in the password; identity precedence; ANONYMOUS default |

Each example directory has a `README.md` (prerequisites, run command, expected
output, an AMQP-1.0-specifics table, gotchas, and related examples) and a
runnable `main.py`.

## Linting

```bash
uv run ruff format .
uv run ruff check .
```
