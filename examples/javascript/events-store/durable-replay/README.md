# JavaScript — Events Store / Durable Replay

Durable subscriptions with **resume** over KubeMQ **Events Store** using the
native `rhea` / `rhea-promise` client. Unlike Events (fire-and-forget, no replay),
Events Store **persists** the stream and lets a durable subscriber pick up exactly
where it left off after a disconnect — no loss, no re-delivery of already-consumed
events.

A durable subscription is identified by the pair **(connection container-id, link
name)**. Reconnecting with the same pair resumes the subscription.

## Prerequisites

- Node.js 20+ (developed against Node 26).
- `rhea` 3.0.4 + `rhea-promise` 3.0.3 (pinned in `examples/javascript/package.json`);
  run via `tsx`.
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

Install once from `examples/javascript`:

```bash
npm install
```

## How to Run

```bash
cd examples/javascript
npx tsx events-store/durable-replay/index.ts
```

Override the broker URL (SASL **ANONYMOUS** by default):

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 npx tsx events-store/durable-replay/index.ts
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

1. **OPEN with a stable container_id** — `new Connection({container_id: "..."})`.
   The container-id is half of the durable identity, so it MUST be the same on
   every reconnect.
2. **ATTACH a durable receiver** —
   `createReceiver({name:"durable-sub", source:{address:"events-store/<ch>", expiry_policy:"never"}, properties:{"x-opt-kubemq-start":"new-only"}})`.
   - `name` is the stable link name — the other half of the durable identity.
   - `source.expiry_policy:"never"` requests a non-expiring source so the durable
     cursor survives the detach.
   - `x-opt-kubemq-start:"new-only"` sets the start cursor on the first attach
     (deliver events from now on).
   - The `message` handler is registered **before** `addCredit` (manual-credit
     quirk), so no early delivery is dropped.
3. **TRANSFER (publish + consume)** — 3 events are published with an
   `AwaitableSender` (each `send()` resolves on the connector's `accepted`
   DISPOSITION); the durable subscriber receives all 3.
4. **DETACH / CLOSE (disconnect)** — a clean `connection.close()` detaches the
   durable link. The connector preserves the durable cursor for this
   (container-id, link name).
5. **Publish while away** — 5 more events arrive at the persisted stream while the
   subscriber is offline.
6. **Re-OPEN + re-ATTACH with the SAME identity** — re-open with the same
   container-id and re-attach the same link name. The subscription **resumes** and
   delivers exactly the 5 missed events — the already-consumed first 3 are not
   re-delivered, and nothing is lost.
7. **DETACH / CLOSE** — the receiver detaches and both connections close.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events-store/<ch>` | unsettled (default) | server-granted | `accepted` per transfer (persisted) | none | `Data` | each accepted transfer is durably stored |
| receiver (KubeMQ → client) | source `events-store/<ch>`, `expiry_policy="never"`, link `name="durable-sub"` | `first` (default) | client-granted standing credit, auto-replenished | `accept()` advances the durable cursor | link `name` (stable) + `properties{x-opt-kubemq-start:new-only}` | `Data` | durable identity = (container-id, link name); re-attach resumes |

## Related Examples

- [events-store/start-positions](../start-positions/) — the full `x-opt-kubemq-start` grammar (first / new-only / last / sequence / time / time-delta)
- [events/basic-pubsub](../../events/basic-pubsub/) — non-durable, at-most-once Events (no replay)
- [events/selector](../../events/selector/) — SQL-92 selectors (also supported on `events-store/`)

## Gotchas

> **Durable subscriptions are NODE-LOCAL.** The durable cursor for a
> (container-id, link name) lives on the **node that owned the original attach**.
> In a multi-node KubeMQ cluster you MUST reconnect to the **same node** to
> resume — reconnecting to a different node starts a fresh subscription rather
> than resuming. (On a single-node dev broker, as in this example, there is only
> one node, so resume always works.) This same node-locality applies to dynamic
> reply nodes; note that RPC replies themselves are cluster-safe.
>
> **The durable identity is BOTH parts.** Resume requires the **same**
> container-id **and** the **same** link `name`. Changing either starts a new
> subscription. The connector also rejects a SECOND live attach of the same
> durable identity (a durable-sub name conflict).
>
> **`x-opt-kubemq-start` sets the cursor on the FIRST attach.** On re-attach the
> subscription resumes from the persisted cursor, not from the start value. To
> replay history regardless, use `start:first` on a non-durable receiver — see
> [events-store/start-positions](../start-positions/).
>
> **Use `events-store/`, not `events/`, for durability.** Plain Events
> (`events/<ch>`) is fire-and-forget with no replay and no durable cursor.
>
> **Register the message handler before granting credit.** With manual credit
> (`credit_window:0` + `addCredit`), a delivery that arrives before a `message`
> listener is attached is lost. This example attaches the handler first.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
