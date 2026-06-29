# Rust — Events Store / Durable Replay

Durable subscriptions with resume over KubeMQ **Events Store** with the native
`fe2o3-amqp` client. A durable subscriber (stable container-id + link name +
non-expiring source) consumes 3 events, disconnects, and — after 5 more events are
published while it is away — re-attaches with the SAME identity and RESUMES exactly
where it left off: no loss, no replay of the already-consumed events.

## Prerequisites

- Rust 1.94+ (stable) with `cargo`.
- `fe2o3-amqp` 0.15.1 + `fe2o3-amqp-types` 0.14.0 (pinned exact in the workspace
  `examples/rust/Cargo.toml`).
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/rust
cargo run -p durable-replay
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 cargo run -p durable-replay
```

Runs ANONYMOUS by default — no credentials required on a stock dev broker.

## Expected Output

```
Broker:     amqp://localhost:5672
Address:    events-store/amqp10.examples.durable  (KubeMQ pattern=events-store, channel=amqp10.examples.durable)
Durable id: container-id="amqp10-examples-durable-container"  link-name="durable-sub"

[recv] Durable receiver attached (first attach): container-id="amqp10-examples-durable-container" name="durable-sub" expiry=never
[recv] First attach received 3 events: ["es-000", "es-001", "es-002"]

[conn] Durable subscriber DISCONNECTED (cursor preserved by the connector)
[send] Published 5 more events WHILE the durable subscriber was away
[recv] Durable receiver attached (re-attach): container-id="amqp10-examples-durable-container" name="durable-sub" expiry=never
[recv] Re-attach RESUMED and received the 5 events published while away: ["es-003", "es-004", "es-005", "es-006", "es-007"]
[recv] No loss, no re-delivery of the already-consumed first 3 — the durable cursor resumed exactly

Done.
```

## What's Happening

1. **OPEN** — the durable subscriber opens with a **stable container-id** via
   `Connection::builder().container_id("amqp10-examples-durable-container")`. The
   container-id is half the durable identity.
2. **BEGIN** — a session carries the durable receiver link.
3. **ATTACH (durable receiver)** — `Receiver::builder().name("durable-sub")` (the
   stable link name = the other half of the durable identity) with a source whose
   `expiry_policy(TerminusExpiryPolicy::Never)` asks the connector to preserve the
   subscription across detach. The `x-opt-kubemq-start = "new-only"` link property
   sets the start cursor (honoured on the FIRST attach).
4. **FLOW / TRANSFER (first leg)** — 3 events are published (unsettled, so each is
   confirmed persisted) and delivered to the durable subscriber under standing
   credit.
5. **DETACH / CLOSE (disconnect)** — `receiver.close()` + `session.end()` +
   `connection.close()`. The connector preserves the durable cursor for this
   `(container-id, link name)`.
6. **PUBLISH while away** — 5 more events land in the persisted stream while the
   durable subscriber is offline.
7. **RE-ATTACH (resume)** — re-opening with the SAME container-id and re-attaching
   the SAME link name RESUMES the subscription; the connector delivers exactly the 5
   events published while away.
8. **DETACH / CLOSE** — both connections are torn down cleanly.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events-store/<ch>` | `Unsettled` (explicit) | server-granted | server returns `Accepted` per send (persisted) | none | `Data` | events-store persists each transfer |
| receiver (KubeMQ → client) | source `events-store/<ch>` (`expiry_policy=never`) | `First` (default) | client `CreditMode::Auto(100)` | `accept` advances the durable cursor | link `name="durable-sub"`, `x-opt-kubemq-start="new-only"` | `Data` | durable identity = (container-id, link name); resume on re-attach |

## Related Examples

- [events-store/start-positions](../start-positions/) — the `x-opt-kubemq-start` grammar (first / new-only / sequence / time / time-delta)
- [events/basic-pubsub](../../events/basic-pubsub/) — Events fan-out (no persistence, no replay)
- [events/consumer-group](../../events/consumer-group/) — `x-opt-kubemq-group` load-balancing

## Gotchas

> **Durable subscriptions are NODE-LOCAL.** In a cluster the durable cursor lives on
> the node that owned the original attach. Reconnect to the SAME node (or run a
> single-node dev broker, as here) to resume. A re-attach to a different node starts
> fresh.

- **Both halves of the identity must be stable.** Change the container-id OR the link
  name and the connector treats it as a NEW durable subscription (it will not
  resume).
- **`x-opt-kubemq-start` is honoured on the FIRST attach only.** Once the durable
  cursor exists, the start property is ignored on resume — the subscription always
  resumes from its preserved position.
- **Sender settle-mode must be explicit.** `fe2o3-amqp` defaults a sender to `mixed`,
  which the connector rejects at ATTACH (`amqp:not-implemented`). The producer here
  pins `SenderSettleMode::Unsettled`.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT see
`connectivity/auth` + `guides/authentication.md`.
