# Work Queues

## Concept

The **queues** pattern (`queues/<ch>`) is a durable, destructive, competing-consumer work queue. A
message goes to **exactly one** consumer (it is *moved*, not copied), survives until it is settled,
and is delivered at **at-least-once** by default. This is the right pattern for task distribution
where each unit of work must be done once.

```
producer ──TRANSFER──▶ queues/tasks
 │ │ │
 ▼ ▼ ▼
 worker-1 worker-2 worker-3 (grant credit, accept on success)
```

- **Produce:** attach a **sender** to `queues/<ch>`; each `TRANSFER` becomes a KubeMQ
 `SendQueueMessage`.
- **Consume:** attach a **receiver** from `queues/<ch>`. The server runs a credit-driven `Get`
 long-poll and frames each returned message as a `TRANSFER`. Grant credit, do the work, and
 `accept` (see [settlement-and-delivery-state.md](settlement-and-delivery-state.md)).

## Competing Consumers and Move Semantics

Many consumers can attach to the same `queues/<ch>`. The broker hands each message to one of them
it is a **move** (competing-consumer), not a fan-out. Add consumers to scale throughput; each
message is still processed once. This is the opposite of [events](pub-sub.md), where every
subscriber gets a copy.

## Credit-Driven Get Long-Poll

Queue consume is **destructive credit-based consume only**. The server reserves
`min(credit, GetBatchSize = 32, MaxUnsettledPerLink − unsettled)` per downstream `Get`, with a
`WaitTimeout` of **1000 ms** (`AutoAck:false`). The 1000 ms is the practical latency floor on an
empty queue: an idle long-poll returns after ~1 s and the loop re-issues. Grant a standing credit
and the messages flow as fast as the broker has them; stop granting and the messages simply wait in
the queue (queues never drop on low credit — unlike events). See
[flow-control-and-credit.md](flow-control-and-credit.md).

## At-Least-Once vs Pre-Settled

| Mode | How | Guarantee | Use when |
|------|-----|-----------|----------|
| **At-least-once (default)** | unsettled deliveries; you `accept` after the work succeeds | message survives a crash; may be **redelivered** | the work must not be lost |
| **Pre-settled (at-most-once)** | `snd-settle-mode=settled` on the sender / consume; server settles at send | fast, no DISPOSITION round-trip; a failed publish/consume is dropped | loss is acceptable for speed |

At-least-once means a consumer crash before `accept` requeues the message, so workers **must be
idempotent** — a message can arrive more than once.

## Redelivery and Receive-Count

A redelivered queue message carries `header.delivery-count = ReceiveCount − 1` and
`first-acquirer = (ReceiveCount == 1)`. So a fresh message has `delivery-count=0,
first-acquirer=true`; a once-redelivered copy has `delivery-count≥1, first-acquirer=false`. Use
these to detect and de-duplicate redeliveries.

Redelivery is triggered by:

- a `release` or `modify` DISPOSITION (you ask for requeue) — **increments receive-count** (gotcha
 #3, see [settlement-and-delivery-state.md](settlement-and-delivery-state.md)),
- a disconnect with unsettled deliveries — the connector NAcks them all to the tail.

> **Teardown loses nothing.** On detach / close / shutdown, every unsettled queue delivery is
> NAcked once (returned to the queue **tail**; you see it as `released`). A disconnecting worker's
> in-flight work is recovered by the next consumer. A hard TCP kill behaves the same: the broker
> requeues and a fresh consumer receives the message.

## What Queues Do NOT Have

The queues pattern is deliberately minimal — none of these exist:

- **No peek / browse / FIFO-ordered consume / visibility-timeout verb.** Receive is destructive
 credit-based consume only. There is no "look without taking".
- **No connector dead-letter exchange (DLX).** A `rejected` message is discarded; poison handling
 is a **broker-side `MaxReceiveQueue` policy** (broker-configured, not AMQP-controllable per link).
 Do not look for a per-link DLX — there is none.
- **No `copy` distribution-mode.** Requesting `copy` on a `queues/` link →
 `DETACH(amqp:invalid-field)`; queues are move-only.
- **No selectors.** A selector filter on a `queues/` link → `amqp:not-implemented` (selectors are
 pub/sub only — see [selectors.md](selectors.md)).
- **No transactions.** There is no AMQP transaction coordinator; reliability comes from settlement,
 not `SESSION_TRANSACTED`.

## Examples

| Language | Example |
|----------|---------|
| Go | [queues/basic-send-receive](../../examples/go/queues/basic-send-receive/) |
| Python | [queues/basic_send_receive](../../examples/python/queues/basic_send_receive/) |
| Java | [queues/basic-send-receive](../../examples/java/queues/basic-send-receive/) |
| C# | [queues/basic-send-receive](../../examples/csharp/queues/basic-send-receive/) |
| JavaScript / TS | [queues/basic-send-receive](../../examples/javascript/queues/basic-send-receive/) |
| Rust | [queues/basic-send-receive](../../examples/rust/queues/basic-send-receive/) |

See also [queues/ack-release-redelivery](../../examples/go/queues/ack-release-redelivery/),
[../guides/reliability.md](../guides/reliability.md).

Grounding: , .
