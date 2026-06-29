# JavaScript — Events / Basic Pub/Sub

Fan-out, at-most-once pub/sub over KubeMQ **Events** with the native
`rhea` / `rhea-promise` client. A receiver subscribes to `events/<ch>` with
standing credit, then a pre-settled sender publishes 20 events; the subscriber
drains every event on the happy path.

Events are a **fire-hose**: deliveries are pre-settled (no DISPOSITION feedback),
there is **no replay**, and a message that arrives at a subscriber with **zero
credit is silently dropped**. This example shows the two disciplines that keep a
subscriber loss-free: **subscribe before publish** and **grant standing credit**.

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
npx tsx events/basic-pubsub/index.ts
```

Override the broker URL (SASL **ANONYMOUS** by default):

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 npx tsx events/basic-pubsub/index.ts
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

1. **OPEN / BEGIN** — `connection.open()` (SASL ANONYMOUS by default).
2. **ATTACH (receiver) FIRST** — `createReceiver({source:{address:"events/<ch>"},
   credit_window:0, ...})` attaches the subscriber link, then `addCredit(100)`
   grants standing credit **before any publish**. Without credit nothing is
   delivered. The `message` handler is registered before `addCredit`.
3. **Wait for the pump** — the attach reply confirms the link, but not that the
   connector's subscription pump has run its `SubscribeEvents` yet. The program
   sleeps ~750 ms; a publish that races the subscription would be **lost** (no
   replay).
4. **ATTACH (sender)** — `createSender({target:{...}, snd_settle_mode:1})` attaches
   a pre-settled producer.
5. **TRANSFER (publish)** — each `send()` writes a settled frame and returns
   immediately; there is no `accepted` DISPOSITION (events are at-most-once,
   fire-and-forget). We `await` `sendable` before each send.
6. **TRANSFER (consume)** — the connector pumps each event to the subscriber.
   The example tops credit back up as each event arrives, so the subscriber is
   never at 0 credit, so nothing is dropped. `delivery.accept()` is a no-op on
   pre-settled pub/sub deliveries but is harmless.
7. **DETACH / CLOSE** — the receiver detaches and the connection closes.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events/<ch>` | `settled` (`snd_settle_mode:1`) | server-granted | NONE — pre-settled fire-and-forget | none | `Data` | at-most-once publish, no replay |
| receiver (KubeMQ → client) | source `events/<ch>` | `first` (default) | client-granted standing credit, topped up per delivery | deliveries pre-settled (`accept` is a no-op) | none | `Data` | 0-credit ⇒ silent drop (see gotcha) |

## Related Examples

- [events/consumer-group](../consumer-group/) — `x-opt-kubemq-group` load balancing vs independent groups
- [events/selector](../selector/) — JMS/SQL-92 message selectors
- [queues/settlement-modes](../../queues/settlement-modes/) — at-most-once vs at-least-once on Queues

## Gotcha

> **Events at 0 credit are SILENTLY DROPPED.** Events are at-most-once with no
> replay. If an event arrives at a subscriber that currently has **zero link
> credit**, the connector **drops the message** and increments the server metric
> `kubemq_amqp10_events_dropped_no_credit_total` — the loss is **never** surfaced
> as a client-side error. Always grant **continuous standing credit** (a large
> initial credit that you top up as deliveries settle) so the subscriber is never
> starved. A slow consumer that lets credit drain to 0 loses events.
>
> **Subscribe before publish.** The attach reply confirms only the link, not the
> connector subscription pump. Publish too soon and the first events race the
> subscription and are lost (no replay to recover them). This example waits
> ~750 ms after attach.
>
> **No replay, no durability.** Events are fire-and-forget. For durable
> subscriptions with resume/replay use **Events Store** (`events-store/<ch>`)
> instead of Events.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
