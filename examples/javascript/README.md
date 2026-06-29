# KubeMQ AMQP 1.0 — JavaScript / TypeScript Examples

Runnable examples that drive the **KubeMQ AMQP 1.0 connector** with the native
[`rhea`](https://github.com/amqp/rhea) / [`rhea-promise`](https://github.com/amqp/rhea-promise)
AMQP 1.0 client — **no KubeMQ SDK**. A standard AMQP 1.0 application points at
KubeMQ by changing only the connection string and the node address
(`<pattern>/<channel>`).

## Prerequisites

- **Node.js 20+** (developed and verified against Node 26).
- **`rhea` 3.0.4 + `rhea-promise` 3.0.3** — pinned in
  [`package.json`](./package.json). Examples are written in strict TypeScript and
  run directly with [`tsx`](https://github.com/privatenumber/tsx) (no build step).
- A running **KubeMQ broker with the AMQP 1.0 connector** (enabled by default),
  reachable at `KUBEMQ_AMQP_URL`.

Install the pinned dependencies once:

```bash
cd examples/javascript
npm install
```

## Configuration

| Variable | Default | Meaning |
|----------|---------|---------|
| `KUBEMQ_AMQP_URL` | `amqp://localhost:5672` | Broker AMQP 1.0 endpoint. The default URL carries no userinfo, so the SASL layer negotiates **ANONYMOUS** (no credentials). |

Connections are **ANONYMOUS by default** on a stock dev broker. For SASL PLAIN
with a KubeMQ JWT (passed in the password), see `connectivity/auth` and
`docs/guides/authentication.md`.

## How to Run

Each variant is a self-contained `<group>/<variant>/index.ts`. Run any one with:

```bash
cd examples/javascript
npx tsx <group>/<variant>/index.ts
```

For example:

```bash
npx tsx queues/basic-send-receive/index.ts
KUBEMQ_AMQP_URL=amqp://my-server:5672 npx tsx events/basic-pubsub/index.ts
```

Type-check every example with:

```bash
npm run typecheck   # tsc --noEmit
```

## Examples

Every address uses the **explicit `<pattern>/<channel>` prefix** — examples never
rely on a bare-address / default-pattern fallback.

| # | Group / Variant | Pattern | Demonstrates |
|---|-----------------|---------|--------------|
| 1 | [`queues/basic-send-receive`](./queues/basic-send-receive/) | Queues | At-least-once produce (unsettled, accepted DISPOSITION) + credit consume + `accept`; queue drains with no loss. |
| 2 | [`queues/ack-release-redelivery`](./queues/ack-release-redelivery/) | Queues | `accept` removes; `release` requeues (grown `delivery_count`, receive-count++); `reject` discards (no requeue). |
| 3 | [`queues/settlement-modes`](./queues/settlement-modes/) | Queues | Unsettled (at-least-once) vs pre-settled (`snd_settle_mode:1`, at-most-once); `rcv-settle-mode=second` → `amqp:not-implemented`. |
| 4 | [`events/basic-pubsub`](./events/basic-pubsub/) | Events | Fan-out at-most-once; subscribe-before-publish (no replay); continuous credit (0-credit ⇒ silent drop). |
| 5 | [`events/consumer-group`](./events/consumer-group/) | Events | `x-opt-kubemq-group`: two `g1` receivers split the stream (no dup), one `g2` gets the full stream. |
| 6 | [`events/selector`](./events/selector/) | Events | JMS / SQL-92 selector (`apache.org:selector-filter:string`); 3-valued logic; selector on `queues/` → `amqp:not-implemented`. |
| 7 | `events-store/durable-replay` | Events Store | Durable subscription + resume across reconnect (expiry never, stable container-id + link name). |
| 8 | `events-store/start-positions` | Events Store | `x-opt-kubemq-start` grammar (`first` / `new-only` / `last` / `sequence:<n>` / `time:` / `time-delta:`); malformed → `amqp:invalid-field`. |
| 9 | `commands/request-reply-dynamic-node` | Commands (RPC) | Native RPC: dynamic reply node, `reply-to` + `correlation-id`; `x-opt-kubemq-executed` / `x-opt-kubemq-error` reply props. |
| 10 | `queries/request-reply` | Queries (RPC) | Same dynamic-reply path; reply = body + metadata only (no executed/error props); failure ⇒ requester times out. |
| 11 | `advanced/multi-frame-large-payload` | Queues | Body larger than the max frame size fragments and reassembles bit-exact; CRC verify; 100 MiB cap. |
| 12 | `advanced/anonymous-terminus` | (routes by `to`) | Anonymous sender (null target) selects the destination per message via `properties.to`. |
| 13 | `connectivity/auth` | (connection) | SASL **PLAIN** with a KubeMQ JWT in the password; contrasts the ANONYMOUS default; denied attach → `amqp:unauthorized-access`. |

> Variants **1–6** are documented here; variants **7–13** ship as sibling
> directories with the same `<group>/<variant>/index.ts` + `README.md` layout.

## Conventions

- **One connection, links per concern.** Each example opens a single
  `Connection`; senders and receivers get their own session under the hood. When
  multiple independent consumers run concurrently (e.g. `events/consumer-group`),
  each gets its own `Session` — a session/link is not shared across consumers.
- **Manual credit + manual settlement.** Receivers use `credit_window: 0` with
  `autoaccept: false` / `autosettle: false`, then `addCredit(...)` and
  `delivery.accept() / release() / reject()` to mirror the AMQP 1.0 flow exactly.
  Register the `message` handler **before** granting credit, or early deliveries
  are missed.
- **Awaitable vs plain sender.** Use `createAwaitableSender` for unsettled
  at-least-once produce (its `send()` resolves on the `accepted` DISPOSITION); use
  the plain `createSender` for pre-settled fire-and-forget (an `AwaitableSender`
  would hang waiting for a disposition a pre-settled link never sends).
- **Clean shutdown.** Every example closes its links and the connection, and uses
  bounded timeouts so a missing broker fails fast instead of hanging.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `docs/guides/authentication.md`.
