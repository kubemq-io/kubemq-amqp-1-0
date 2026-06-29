# JavaScript — Events / Consumer Group

Consumer-group load-balancing over KubeMQ **Events** with the native
`rhea` / `rhea-promise` client. The `x-opt-kubemq-group` receiver link property
places a subscriber in a named load-balancing group:

- **Within one group**, the connector round-robins the event stream across the
  group's members — each event goes to exactly **one** member (no duplication).
- **A distinct group** is an independent virtual-topic subscriber that receives
  the **full** stream.

This example runs two receivers in group `g1` (they split the stream) and one
receiver in group `g2` (it gets every event).

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
npx tsx events/consumer-group/index.ts
```

Override the broker URL (SASL **ANONYMOUS** by default):

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 npx tsx events/consumer-group/index.ts
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: events/amqp10.examples.consumergroup  (KubeMQ pattern=events, channel=amqp10.examples.consumergroup)

[recv] 3 subscribers attached: g1a+g1b (group g1), g2 (group g2)
[send] Published 30 events (pre-settled)
[recv] g2 (group g2, independent): 30/30 events — FULL stream
[recv] g1a (group g1): 16 events; g1b (group g1): 14 events
[recv] g1a+g1b together: 30/30 events, 0 duplicates — group SPLIT the stream

Done.
```

The exact split between `g1a` and `g1b` varies run to run (the embedded NATS
queue group distributes round-robin across active members); the invariants are
fixed: `g2` gets all 30, and `g1a + g1b` together get all 30 with zero overlap.

## What's Happening

1. **OPEN / BEGIN** — `connection.open()` (SASL ANONYMOUS by default). Each of the
   three subscribers runs on **its own `Session`** (created via
   `connection.createSession()`); none is shared.
2. **ATTACH (3 receivers)** — each `createReceiver({source:{address:"events/<ch>"},
   credit_window:0, properties:{"x-opt-kubemq-group":"<g>"}})` places the link in a
   load-balancing group. `g1a` and `g1b` use `"g1"`; `g2` uses `"g2"`. Each
   subscriber registers its `message` handler before granting standing credit.
3. **Wait for the pumps** — after all three links attach, the program sleeps
   ~750 ms so the connector subscription pumps go live before publishing (events
   have no replay — a publish that races a subscription is lost).
4. **ATTACH (sender) + TRANSFER** — a pre-settled sender publishes 30 events to
   `events/<ch>` (fire-and-forget) on a dedicated session.
5. **Fan-out + load-balance** — the connector delivers each event to **every
   distinct group** (`g1` once, `g2` once). Within `g1` it round-robins across
   `g1a`/`g1b`, so each event reaches exactly one of them.
6. **Assertions** — `g2` received all 30; `g1a + g1b` together received all 30
   with **no body delivered to both** (proves the group split, not duplicated, the
   stream) and both members got a non-empty share.
7. **DETACH / CLOSE** — each receiver detaches; the connection closes.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events/<ch>` | `settled` (pre-settled) | server-granted | NONE — fire-and-forget | none | `Data` | at-most-once publish |
| receiver g1a / g1b (KubeMQ → client) | source `events/<ch>` | `first` (default) | client-granted standing credit | deliveries pre-settled | **`x-opt-kubemq-group: "g1"`** | `Data` | round-robin within group g1 (no dup) |
| receiver g2 (KubeMQ → client) | source `events/<ch>` | `first` (default) | client-granted standing credit | deliveries pre-settled | **`x-opt-kubemq-group: "g2"`** | `Data` | independent group ⇒ full stream |

## Related Examples

- [events/basic-pubsub](../basic-pubsub/) — fan-out at-most-once, continuous credit, subscribe-before-publish
- [events/selector](../selector/) — JMS/SQL-92 message selectors
- [queues/basic-send-receive](../../queues/basic-send-receive/) — competing consumers on Queues (move semantics)

## Gotcha

> **Group membership is a receiver link property, not an address.** All three
> receivers attach to the **same** `events/<ch>` source; the `x-opt-kubemq-group`
> property (set via `createReceiver({properties:{...}})`) is what splits or
> duplicates the stream. Omit the property and a receiver is a standalone (lone)
> subscriber that gets the full stream — equivalent to its own one-member group.
>
> **Events still drop at 0 credit.** Consumer groups do not change the
> at-most-once fire-hose contract: a group member at zero credit is **skipped**,
> and within a group that means its share is round-robined elsewhere or dropped
> (counted on `kubemq_amqp10_events_dropped_no_credit_total`). Grant standing
> credit to every member — see [events/basic-pubsub](../basic-pubsub/).
>
> **No replay across reconnect.** A group member that disconnects loses its
> in-flight share; events published while no member is attached are lost. For
> durable group-style resume use **Events Store** (`events-store/<ch>`).

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
