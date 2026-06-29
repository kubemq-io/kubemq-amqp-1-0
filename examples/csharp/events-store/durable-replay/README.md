# C# — Events-Store / Durable Replay

Durable subscriptions with **resume** over KubeMQ **Events Store** using the native
`AMQPNetLite.Core` client. A durable subscriber identified by a stable
`(container-id, link name)` pair receives 3 live events, disconnects, and — after 5
more events are published while it is away — **re-attaches with the same identity
and resumes exactly where it left off** (the 5 missed events, no loss, no
re-delivery of the first 3).

## Prerequisites

- .NET SDK **8.0**
- `AMQPNetLite.Core` **2.5.3** (pinned in `examples/csharp/Directory.Packages.props`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd events-store/durable-replay
dotnet run
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 dotnet run
```

Runs ANONYMOUS by default (no userinfo in the URL).

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

1. **OPEN (durable identity)** — the durable subscriber connects via a
   `ConnectionFactory` whose `AMQP.ContainerId` is the **stable** container-id
   (`amqp10-examples-durable-container`). This is half the durable identity;
   AMQPNetLite would otherwise generate a random container-id per connect.
2. **BEGIN** — `new Session(connection)` opens a session for the durable receiver.
3. **ATTACH (durable receiver)** — the `Attach` carries three durability knobs:
   - `Source.ExpiryPolicy = Symbol("never")` — the connector keeps the source (and
     its cursor) alive across detach/disconnect.
   - `Attach.LinkName = "durable-sub"` — the **stable** link name, the other half
     of the durable identity.
   - `Attach.Properties["x-opt-kubemq-start"] = "new-only"` — sets the cursor on
     the FIRST attach to "deliver only events from now on".
4. **FLOW / TRANSFER (first leg)** — `SetCredit(100)` issues credit; 3 events are
   published (unsettled — events-store persists each accepted transfer) and all 3
   are received + `Accept`ed.
5. **DETACH / CLOSE (disconnect)** — `connection.CloseAsync()` cleanly detaches the
   durable link. The connector **preserves the durable cursor** for this
   `(container-id, link name)`.
6. **Publish while away** — 5 more events land on the persisted stream while the
   subscriber is offline.
7. **Re-ATTACH (same identity)** — a fresh connection with the SAME container-id +
   the SAME link name + `expiry=never` **resumes** the subscription. The connector
   delivers exactly the 5 missed events (not the first 3 again).
8. **DETACH / CLOSE** — the receiver detaches and both connections close.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events-store/<ch>` | unsettled (default) | server-granted | `accepted` DISPOSITION per send (persisted) | none | `Data` | producer connection (no stable id needed) |
| receiver (KubeMQ → client) | source `events-store/<ch>` | `first` (default) | client-granted `SetCredit(100)`, auto-restore | `Accept` ⇒ advances durable cursor | link: `Source.ExpiryPolicy="never"`, `LinkName="durable-sub"`, `Attach.Properties{x-opt-kubemq-start:"new-only"}` | `Data` | durable = `(container-id, link name)`; resume on re-attach |

## Related Examples

- [events-store/start-positions](../start-positions/) — the full `x-opt-kubemq-start` grammar (first / sequence / time)
- [events/basic-pubsub](../../events/basic-pubsub/) — fire-and-forget Events (no replay, contrast)
- [events/consumer-group](../../events/consumer-group/) — competing consumers over a shared subscription

## Gotcha

> **Durable subscriptions are NODE-LOCAL.** The durable cursor lives on the broker
> node that owned the original attach. In a cluster you must reconnect to the SAME
> node to resume — reconnecting to a different node starts a fresh cursor. This
> example targets a single-node dev broker, so resume always works. Both halves of
> the identity (`container-id` AND `link name`) must match exactly on re-attach; a
> random container-id (AMQPNetLite's default) would create a brand-new
> subscription, not resume the old one.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT
see `connectivity/auth` + `guides/authentication.md`.
