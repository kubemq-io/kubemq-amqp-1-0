# C# — Events / Basic Pub-Sub

Fan-out, at-most-once pub/sub over KubeMQ **Events** with the native
`AMQPNetLite.Core` client. A subscriber attaches with standing credit **before**
any publish; a pre-settled sender fires 20 events; the subscriber drains them all.

## Prerequisites

- .NET SDK **8.0**
- `AMQPNetLite.Core` **2.5.3** (pinned in `examples/csharp/Directory.Packages.props`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd events/basic-pubsub
dotnet run
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 dotnet run
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: events/amqp10.examples.pubsub  (KubeMQ pattern=events, channel=amqp10.examples.pubsub)

[recv] Subscribed to events/amqp10.examples.pubsub with standing credit 100
[recv] Subscription pump settled (waited 750ms before publishing)
[send] Published 20 events (pre-settled, fire-and-forget)
[recv] Received all 20 events (continuous credit => no 0-credit drop)

Done.
```

## What's Happening

1. **OPEN / BEGIN** — connect (ANONYMOUS) and open one session.
2. **ATTACH (receiver) — subscribe FIRST** — `new ReceiverLink(session, name,
   "events/<ch>")` + `SetCredit(100, autoRestore: true)`. Events have **no
   replay**, so the subscriber must be attached before the first publish.
3. **Pump settle** — the attach reply confirms the link, **not** that the
   connector's subscription pump is live. The program waits ~750ms before
   publishing so the first events do not race the subscription.
4. **ATTACH (sender)** — built from an `Attach` frame with
   `SndSettleMode = SenderSettleMode.Settled`. Every TRANSFER is pre-settled
   (fire-and-forget) — events are at-most-once, so there is no DISPOSITION to
   await and no produce confirmation.
5. **TRANSFER (publish)** — 20 events sent to `events/<ch>`.
6. **TRANSFER (receive)** — with standing credit the subscriber drains every
   event. `Accept` is a no-op on pre-settled fan-out deliveries but is harmless.
7. **DETACH / CLOSE** — the links detach and the connection closes.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events/<ch>` | `SndSettleMode = Settled` | server-granted | none (pre-settled) | none | `Data` | at-most-once fan-out, no replay |
| receiver (KubeMQ → client) | source `events/<ch>` | `first` (default) | client standing credit `SetCredit(100, autoRestore)` | `Accept` is a no-op (pre-settled) | none | `Data` | **0-credit ⇒ silent drop**; subscribe-before-publish |

## Gotcha

> **A subscriber at 0 credit silently drops events.** Events are at-most-once
> with no replay. If an event arrives when the subscriber has no link credit, the
> connector **drops it silently** and counts it on the server metric
> `kubemq_amqp10_events_dropped_no_credit_total` — it is **never** surfaced as a
> client error. Avoid both data-loss footguns by (1) **subscribing before
> publishing** and (2) **granting standing credit** that auto-replenishes
> (`SetCredit(n, autoRestore: true)`).

## Related Examples

- [events/consumer-group](../consumer-group/) — load-balancing with `x-opt-kubemq-group`
- [events/selector](../selector/) — JMS/SQL-92 message selectors
- [queues/basic-send-receive](../../queues/basic-send-receive/) — at-least-once Queues (with replay-like redelivery)

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
