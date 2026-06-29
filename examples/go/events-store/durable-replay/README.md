# Go — Events Store / Durable Replay

Durable subscriptions with **resume** over KubeMQ **Events Store** using the
native `github.com/Azure/go-amqp` client. Unlike Events (fire-and-forget, no
replay), Events Store **persists** the stream and lets a durable subscriber pick
up exactly where it left off after a disconnect — no loss, no re-delivery of
already-consumed events.

A durable subscription is identified by the pair **(connection container-id, link
name)**. Reconnecting with the same pair resumes the subscription.

## Prerequisites

- Go 1.24+
- `github.com/Azure/go-amqp` v1.7.0 (pinned in `examples/go/go.mod`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/go
go run ./events-store/durable-replay
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 go run ./events-store/durable-replay
```

## Expected Output

```
Broker:        amqp://localhost:5672
Address:       events-store/amqp10.examples.durable  (KubeMQ pattern=events-store, channel=amqp10.examples.durable)
Durable id:    container-id="amqp10-examples-durable-container"  link-name="durable-sub"

[recv] Durable receiver attached (first attach): container-id="amqp10-examples-durable-container" name="durable-sub" expiry=never
[recv] First attach received 3 events: [es-000 es-001 es-002]

[conn] Durable subscriber DISCONNECTED (cursor preserved by the connector)
[send] Published 5 more events WHILE the durable subscriber was away
[recv] Durable receiver attached (re-attach): container-id="amqp10-examples-durable-container" name="durable-sub" expiry=never
[recv] Re-attach RESUMED and received the 5 events published while away: [es-003 es-004 es-005 es-006 es-007]
[recv] No loss, no re-delivery of the already-consumed first 3 — the durable cursor resumed exactly

Done.
```

## What's Happening

1. **OPEN with a stable container-id** —
   `amqp.Dial(ctx, url, &amqp.ConnOptions{ContainerID: "..."})`. The container-id
   is half of the durable identity, so it MUST be the same on every reconnect.
2. **ATTACH a durable receiver** —
   `NewReceiver("events-store/<ch>", {Name:"durable-sub", SourceExpiryPolicy: amqp.ExpiryPolicyNever, Properties:{"x-opt-kubemq-start":"new-only"}})`.
   - `Name` is the stable link name — the other half of the durable identity.
   - `SourceExpiryPolicy: ExpiryPolicyNever` requests a non-expiring source so
     the durable cursor survives the detach.
   - `x-opt-kubemq-start: new-only` sets the start cursor on the first attach
     (deliver events from now on).
3. **TRANSFER (publish + consume)** — 3 events are published and the durable
   subscriber receives all 3.
4. **DETACH / CLOSE (disconnect)** — a clean `conn.Close()` detaches the durable
   link. The connector preserves the durable cursor for this
   (container-id, link name).
5. **Publish while away** — 5 more events arrive at the persisted stream while
   the subscriber is offline.
6. **Re-OPEN + re-ATTACH with the SAME identity** — re-dial with the same
   container-id and re-attach the same link name. The subscription **resumes**
   and delivers exactly the 5 missed events — the already-consumed first 3 are
   not re-delivered, and nothing is lost.
7. **DETACH / CLOSE** — the receiver detaches and both connections close.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events-store/<ch>` | `mixed`/unsettled (default) | server-granted | `accepted` per transfer (persisted) | none | `Data` | each accepted transfer is durably stored |
| receiver (KubeMQ → client) | source `events-store/<ch>`, `expiry-policy=never`, link `Name="durable-sub"` | `first` (default) | client-granted `Credit:100`, auto-replenished | `accepted` advances the durable cursor | link `Name` (stable) + `Properties{x-opt-kubemq-start:new-only}` | `Data` | durable identity = (container-id, link name); re-attach resumes |

## Gotchas

> **Durable subscriptions are NODE-LOCAL.** The durable cursor for a
> (container-id, link name) lives on the **node that owned the original attach**.
> In a multi-node KubeMQ cluster you MUST reconnect to the **same node** to
> resume — reconnecting to a different node starts a fresh subscription rather
> than resuming. (On a single-node dev broker, as in this example, there is only
> one node, so resume always works.) This same node-locality applies to dynamic
> reply nodes; note that RPC replies themselves are cluster-safe.

- **The durable identity is BOTH parts.** Resume requires the **same**
  container-id **and** the **same** link `Name`. Changing either starts a new
  subscription. The connector also rejects a SECOND live attach of the same
  durable identity (a durable-sub name conflict).
- **`x-opt-kubemq-start` sets the cursor on the FIRST attach.** On re-attach the
  subscription resumes from the persisted cursor, not from the start value. To
  replay history regardless, use `start:first` on a non-durable receiver — see
  [events-store/start-positions](../start-positions/).
- **Use `events-store/`, not `events/`, for durability.** Plain Events
  (`events/<ch>`) is fire-and-forget with no replay and no durable cursor.

## Related Examples

- [events-store/start-positions](../start-positions/) — the full `x-opt-kubemq-start` grammar (first / new-only / last / sequence / time / time-delta)
- [events/basic-pubsub](../../events/basic-pubsub/) — non-durable, at-most-once Events (no replay)
- [events/selector](../../events/selector/) — SQL-92 selectors (also supported on `events-store/`)

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
