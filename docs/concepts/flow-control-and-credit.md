# Flow Control and Credit

## Concept

AMQP 1.0 has **no publisher confirms and no `basic.qos` prefetch**. Flow is governed by
**link credit**: a receiver grants the sender a number of deliveries it is willing to accept, and
the sender may transfer at most that many. Credit is the AMQP replacement for prefetch/QoS, and
because of [role inversion](links-sessions-connections.md) it flows in opposite directions for the
two link kinds.

| Link kind | Who grants credit | Why |
|-----------|-------------------|-----|
| **Server-receiver** (you produce / publish) | the **server** grants you credit | the server is the receiver, so it controls how much you may send |
| **Server-sender** (you consume) | **you** grant the server credit | you are the receiver, so you control how fast KubeMQ pushes to you |

## Producing — the Server Grants You Credit

When you attach a **sender** (a produce link), the server immediately emits a link `FLOW` with
`link-credit = MaxUnsettledPerLink` (default **1024**, clamped to `[1, 1<<20]`, falling back to 256
if unset). **You must not send a `TRANSFER` before you have received credit.** As you consume
credit the server replenishes back to the full window whenever the remaining credit drops below
half, so a steady producer never starves. The session `incoming-window` (2048) is the secondary
bound; if you blast past it you get `END(amqp:session:window-violation)`.

## Consuming — You Grant the Server Credit

When you attach a **receiver** (a consume link), **nothing is delivered until you grant credit**
with a `FLOW` (`link-credit > 0`). Effective credit is
`(flow.delivery-count + flow.link-credit) − server.delivery-count`, clamped at ≥ 0.

There are two ways to manage it:

1. **Standing credit (recommended for most consumers).** Open the receiver with a modest standing
 credit (e.g. 100–1000, but never above `MaxUnsettledPerLink` = 1024) and let the client library
 replenish on settlement. Simple and robust.
2. **Manual credit.** Open the receiver with `Credit: -1` (the manual-credit mode in most
 libraries) and call `IssueCredit` / `DrainCredit` yourself. Use this when you want exact-credit
 delivery (no over-delivery) — for example, request exactly N messages, settle them, then request
 N more.

For **queue** consume links the server runs a credit-driven `Get` long-poll: per `Get` it reserves
`min(credit, GetBatchSize = 32, MaxUnsettledPerLink − unsettled)` items, issues a downstream `Get`
with `AutoAck:false` and a `WaitTimeout` of **1000 ms**, and frames each returned message as a
`TRANSFER`. The 1000 ms long-poll is the practical empty-queue latency floor (see
[work-queues.md](work-queues.md)).

## Drain

A `FLOW` with `drain=true` tells the server: deliver whatever is immediately available, then
report back. The server advances `delivery-count` by the remaining credit, zeroes credit, and
echoes a `FLOW` with `link-credit=0, drain=true`. Drain completes promptly — it does **not** hang
waiting for messages — and a held remainder resumes on a fresh `IssueCredit`. (Your `FLOW` frames
must carry `next-incoming-id` once the session is established; the client libraries handle this.)

## The Two Data-Loss Footguns

These are the two most expensive mistakes you can make against this connector. Both are silent.

> ### ⚠ Footgun #1 — Events at 0 credit are silently DROPPED
>
> On an **events** (`events/<ch>`) consume link, a message that arrives while your **credit is 0 is
> silently dropped** — no error, no `DISPOSITION`, nothing. This is true at-most-once: the event is
> gone. It happens whenever a slow consumer, a paused loop, or a forgotten replenish lets credit
> reach zero. The connector counts every drop in
> `kubemq_amqp10_events_dropped_no_credit_total`.
>
> **Defense:** grant credit *continuously* and replenish eagerly; keep credit well above zero.
> Subscribe **before** you publish — events have no replay, so anything published before your
> link's credit is in place is lost to the race. See [pub-sub.md](pub-sub.md) and
> [../guides/flow-control.md](../guides/flow-control.md).

> ### ⚠ Footgun #2 — Events-Store stalled credit loses the buffered window
>
> An **events-store** (`events-store/<ch>`) consume link fronts the durable subscription with a
> **deliver-first ring buffer** (capacity `MaxUnsettledPerLink` ≈ 1024) so the broker callback
> never stalls. But "deliver-first" means the buffered window's positions are **already auto-acked
> in the broker** before you take delivery. If the buffer fills while your credit stays at 0, the
> link `DETACH`es with `amqp:resource-limit-exceeded` ("credit stalled") and **the entire buffered,
> already-acked window is lost** — a durable re-attach resumes *after* it. The connector counts the
> lost window in `kubemq_amqp10_events_store_dropped_stalled_total`.
>
> **Defense:** size `MaxUnsettledPerLink` to your real prefetch and replenish credit aggressively
> so the buffer never fills with credit at zero. See
> [durable-subscriptions.md](durable-subscriptions.md) and
> [../guides/flow-control.md](../guides/flow-control.md).

The contrast is important: **queue** consume links never drop on low credit — when you stop
granting, KubeMQ simply stops delivering and the messages wait in the queue. The drop footguns are
specific to the pre-settled pub/sub patterns (events, events-store), which deliver at-most-once.

## Examples

| Language | Example |
|----------|---------|
| Go | [queues/basic-send-receive](../../examples/go/queues/basic-send-receive/) |
| Python | [queues/basic_send_receive](../../examples/python/queues/basic_send_receive/) |
| Java | [queues/basic-send-receive](../../examples/java/queues/basic-send-receive/) |
| C# | [queues/basic-send-receive](../../examples/csharp/queues/basic-send-receive/) |
| JavaScript / TS | [queues/basic-send-receive](../../examples/javascript/queues/basic-send-receive/) |
| Rust | [queues/basic-send-receive](../../examples/rust/queues/basic-send-receive/) |

For manual `IssueCredit`/`DrainCredit` and drain, see the burn-in `credit_flow` worker and
[../guides/flow-control.md](../guides/flow-control.md).

Grounding: .
