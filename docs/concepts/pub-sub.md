# Publish / Subscribe (Events)

## Concept

The **events** pattern (`events/<ch>`) is a **fire-hose, at-most-once** fan-out. Every active
subscriber gets a **copy** of every message (this is the opposite of [queues](work-queues.md),
where a message goes to exactly one consumer). There is **no replay and no persistence** — events
are delivered to whoever is listening *right now* and then they are gone.

```
publisher ──TRANSFER──▶ events/telemetry ──▶ subscriber-1 (copy)
 ──▶ subscriber-2 (copy)
 ──▶ subscriber-3 (copy)
```

- **Produce:** attach a **sender** to `events/<ch>`; each `TRANSFER` becomes `SendEvents`
 (`Store=false`).
- **Consume:** attach a **receiver** from `events/<ch>` and grant standing credit. Deliveries are
 **always pre-settled** (`Settled:true`) — at-most-once, no DISPOSITION round-trip.

## Subscribe Before You Publish

Events have **no replay**. A message published before your subscriber's link is attached *and has
credit* is lost to you — there is no buffer to catch up from. So:

1. Attach the receiver and grant credit **first**.
2. Then start publishing.

The connector's own integration tests sleep ~500–750 ms between attach and the first publish to
avoid the attach↔publish race. Real applications should establish subscriptions before producers
start, or accept that early messages are missed.

## The 0-Credit Drop — a First-Class Footgun

> ### ⚠ Footgun #1 — Events at 0 credit are silently DROPPED
>
> On an events consume link, a message that arrives while your **link credit is 0 is silently
> dropped**. No error. No `DISPOSITION`. No log on your side. The event is simply gone — this is
> what "at-most-once" means here. It bites whenever a slow consumer, a paused receive loop, or a
> forgotten replenish lets credit reach zero, even briefly.
>
> The connector counts every dropped event in `kubemq_amqp10_events_dropped_no_credit_total`. The
> drop path is `subPump.runEvents`: if `tryDeliver` finds no credit, the message is counted and
> discarded.
>
> **Defense:**
> - Grant credit **continuously** and keep it well above zero; replenish eagerly on each delivery.
> - Subscribe **before** publishing (no replay).
> - If you cannot keep up, you want a **durable subscription** ([durable-subscriptions.md](durable-subscriptions.md))
> — but note *that* pattern has its own stalled-credit footgun.
>
> See [flow-control-and-credit.md](flow-control-and-credit.md) and
> [../guides/flow-control.md](../guides/flow-control.md).

A selector non-match is *also* silently not delivered (copy semantics) — that is by design and is
not a drop; see [selectors.md](selectors.md).

## Consumer Groups

Set the link property **`x-opt-kubemq-group`** on a receiver to join a consumer group. Within one
group, the message stream is **load-balanced** across the group's members (each message goes to one
member of the group — no duplicate within the group). Different groups each get the **full** stream
independently.

```
events/orders
 ├── group "g1": receiver-A ┐ (split — no duplicate within g1)
 │ receiver-B ┘
 └── group "g2": receiver-C (full stream)
```

Two `g1` receivers split the stream between them; a lone `g2` receiver gets every message. A
subscriber with **no** group is a plain fan-out subscriber (the KubeMQ default). The group property
is honored on `events`, `events-store`, and the RPC consume patterns.

> **Java (Qpid JMS) is `N/A` for consumer groups today.** The connector advertises no `SHARED-SUBS`
> capability (so Qpid JMS `createSharedConsumer` / `createSharedDurableConsumer` throws *"Remote peer
> does not support shared subscriptions"*), and Qpid JMS exposes no API to set the `x-opt-kubemq-group`
> link property directly — so there is no Qpid-JMS path to consumer groups pending a connector
> `SHARED-SUBS` advertisement (or a JMS-friendly group alias). The other languages
> (Go/Python/C#/JS/Rust) set `x-opt-kubemq-group` on the receiver's ATTACH and support groups fully.
> See [`examples/java/events/consumer-group/README.md`](../../examples/java/events/consumer-group/).

## Examples

| Language | Example |
|----------|---------|
| Go | [events/basic-pubsub](../../examples/go/events/basic-pubsub/) · [events/consumer-group](../../examples/go/events/consumer-group/) |
| Python | [events/basic_pubsub](../../examples/python/events/basic_pubsub/) · [events/consumer_group](../../examples/python/events/consumer_group/) |
| Java | [events/basic-pubsub](../../examples/java/events/basic-pubsub/) · [events/consumer-group](../../examples/java/events/consumer-group/) (**N/A** — no `SHARED-SUBS`; see README) |
| C# | [events/basic-pubsub](../../examples/csharp/events/basic-pubsub/) · [events/consumer-group](../../examples/csharp/events/consumer-group/) |
| JavaScript / TS | [events/basic-pubsub](../../examples/javascript/events/basic-pubsub/) · [events/consumer-group](../../examples/javascript/events/consumer-group/) |
| Rust | [events/basic-pubsub](../../examples/rust/events/basic-pubsub/) · [events/consumer-group](../../examples/rust/events/consumer-group/) |

For durable, replayable subscriptions instead of the fire-hose, see
[durable-subscriptions.md](durable-subscriptions.md).

Grounding: .
