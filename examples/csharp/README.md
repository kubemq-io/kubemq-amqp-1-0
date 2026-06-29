# C# / .NET — KubeMQ AMQP 1.0 Examples

Native **AMQP 1.0** examples for the KubeMQ AMQP 1.0 connector. Client library:
**AMQPNetLite.Core 2.5.3** (`Amqp.Connection` / `Amqp.Session` / `Amqp.SenderLink`
/ `Amqp.ReceiverLink`), pinned via Central Package Management in
[`Directory.Packages.props`](Directory.Packages.props). Target framework
`net8.0`. **There is NO KubeMQ SDK here** — every example speaks raw AMQP 1.0.

## Prerequisites

- .NET SDK **8.0**
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).
- The default connection uses **SASL ANONYMOUS** (no userinfo in the URL). For
  SASL PLAIN with a KubeMQ JWT see `connectivity/auth` + `guides/authentication.md`.

## Build & Run

```bash
export KUBEMQ_AMQP_URL=amqp://localhost:5672
dotnet build kubemq-amqp-1-0-examples.sln
```

Each variant is its own console project. Run any of them with
`cd <group>/<variant> && dotnet run`:

```bash
cd queues/basic-send-receive
dotnet run
```

Override the broker URL per run:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 dotnet run
```

## The 13 example variants

The full master table (the single source of truth for every language) lives in
the plan / shared conventions. The variants in this directory:

| # | Group / Variant | Project dir | Demonstrates |
|---|---|---|---|
| 1 | queues / basic-send-receive | `queues/basic-send-receive` | at-least-once produce + credit consume + `accept`; queue drains |
| 2 | queues / ack-release-redelivery | `queues/ack-release-redelivery` | `accept` / `release` (redelivery, grown delivery-count) / `reject` (discard) |
| 3 | queues / settlement-modes | `queues/settlement-modes` | unsettled (at-least-once) vs pre-settled (at-most-once); `rcv-settle-mode=first` |
| 4 | events / basic-pubsub | `events/basic-pubsub` | fan-out at-most-once; subscribe-before-publish; standing credit (0-credit = silent drop) |
| 5 | events / consumer-group | `events/consumer-group` | `x-opt-kubemq-group`: group split (no dup) vs independent full stream |
| 6 | events / selector | `events/selector` | JMS/SQL-92 selector (`apache.org:selector-filter:string`); 3-valued logic; selector-on-`queues/` rejected |
| 7 | events-store / durable-replay | `events-store/durable-replay` | durable subscription + resume |
| 8 | events-store / start-positions | `events-store/start-positions` | `x-opt-kubemq-start` grammar (`first` / `new-only` / `last` / `sequence:` / `time:` / `time-delta:`) |
| 9 | commands / request-reply-dynamic-node | `commands/request-reply-dynamic-node` | native RPC: dynamic reply node; `executed` / `error` reply props |
| 10 | queries / request-reply | `queries/request-reply` | native RPC; reply body + metadata only |
| 11 | advanced / multi-frame-large-payload | `advanced/multi-frame-large-payload` | body > max-frame-size fragments + reassembles |
| 12 | advanced / anonymous-terminus | `advanced/anonymous-terminus` | anonymous sender (null target) + per-message `Properties.To` |
| 13 | connectivity / auth | `connectivity/auth` | SASL PLAIN with a KubeMQ JWT in the password |

> Variants **1–6** are provided in this batch; variants **7–13** are added by a
> sibling batch and registered in the same solution.

## Idiom note (AMQPNetLite.Core)

`AMQPNetLite.Core` is the cross-platform core of AMQP.Net Lite. Key API surface
used throughout these examples:

- `await Connection.Factory.CreateAsync(new Address(url))` to OPEN a connection.
- `new Session(connection)` to BEGIN a session.
- `new SenderLink(session, name, address)` / `new ReceiverLink(session, name, address)`
  for a plain ATTACH; the `(session, name, Attach, onAttached)` overload when you
  need to set the settlement mode, link properties, or a source filter on the
  ATTACH frame.
- `SenderLink.Send(message, timeout)` — on an **unsettled** link this blocks for
  the `accepted` DISPOSITION; on a **pre-settled** link (`Attach.SndSettleMode =
  SenderSettleMode.Settled`) it returns fire-and-forget.
- `ReceiverLink.SetCredit(n, autoRestore: true)` to grant standing credit (it
  auto-replenishes as deliveries settle).
- `ReceiverLink.Receive(timeout)` returns `null` on timeout (no exception).
- Dispositions: `receiver.Accept(msg)` / `Release(msg)` /
  `Reject(msg, new Error(...))`.
- Body as the AMQP `Data` (binary) section:
  `new Message { BodySection = new Data { Binary = bytes } }`; read it back via
  `((Data)msg.BodySection).Binary`.
- Application properties: `msg.ApplicationProperties.Map["key"] = value`.
- Selectors have no helper — plumb them by hand into `Source.FilterSet` (see
  `events/selector`).

### Connector interaction note

With this connector, **do not detach a producer link on a connection while a
sibling consumer on the same connection is still draining** — closing the sender
first can stall delivery to the consumer. The queue examples therefore keep the
sender link open through the consume phase and close all links at the end. This
is an AMQPNetLite + connector interaction, not a protocol requirement.
