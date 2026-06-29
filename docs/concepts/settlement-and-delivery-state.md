# Settlement and Delivery State

## Concept

AMQP 1.0 reliability is **settlement modes + delivery state**, not publisher confirms and not
numeric reason codes. A delivery is *settled* when both peers agree they no longer need to track it,
and its *delivery state* (accepted, released, rejected, modified) tells the other side what
happened. The KubeMQ connector maps these states onto its queue Ack/NAck machinery.

## Settlement Modes

Settlement modes are negotiated at `ATTACH`:

- **`snd-settle-mode`** (controls the produce/out path):
 - requested **`settled`** is honored — each outbound `TRANSFER` carries `settled=true` and the
 server `AckRange`s immediately at send. This is **at-most-once** (pre-settled): fast, no
 DISPOSITION round-trip, and a publish failure is dropped (counted, no DISPOSITION) rather than
 retried.
 - `mixed`, `unsettled`, or absent → the server uses **`unsettled`**: it sends deliveries
 unsettled, tracks them, and waits for your `DISPOSITION`. This is **at-least-once** (the
 default).
- **`rcv-settle-mode`** is always replied as **`first`**. Requesting **`second`** →
 `DETACH(amqp:not-implemented)` before the link is even built.

> **Gotcha #7 — `rcv-settle-mode=second` is unsupported.** A client that requests second-stage
> receiver settlement gets a clean `DETACH(amqp:not-implemented)`. Pin `rcv-settle-mode=first`
> (the only mode the server echoes).

## Delivery-State Outcome Mapping

When you consume, you settle each delivery by sending a `DISPOSITION` with a delivery state. The
connector classifies that state and emits the corresponding KubeMQ queue request:

| AMQP delivery state (your DISPOSITION) | Client call (typical) | KubeMQ action | Request | Effect |
|----------------------------------------|-----------------------|---------------|---------|--------|
| `accepted` | `AcceptMessage` | discard (consume) | **AckRange** | message removed |
| `rejected` | `RejectMessage` | discard | **AckRange** | discarded; poison handled by **broker `MaxReceiveQueue` policy — NO connector DLX** |
| `released` | `ReleaseMessage` | requeue to tail | **NAckRange** | redelivered, `delivery-count` grows, `first-acquirer=false`; **increments receive-count** |
| `modified{delivery-failed}` / `modified{undeliverable-here}` | `ModifyMessage` | requeue to tail | **NAckRange** | requeued (no per-consumer exclusion) |
| nil state (settled, no outcome) | (settle without a state) | treat as success | **AckRange** | — |
| unknown terminal state | — | conservatively requeue | **NAckRange** | **never silently dropped** |

The key design choice is that an **unknown** state is conservatively NAcked (requeued), never
dropped — so an unexpected state from an exotic client costs you at most a redelivery, never a lost
message.

A `DISPOSITION` may cover a `first..last` delivery-id range; the connector resolves it against the
per-link unsettled map, groups by `RefTransactionId`, and emits one `AckRange`/`NAckRange` per group
with ascending sequence ranges. Unknown ids are ignored (re-settlement is idempotent). A single wide
disposition is bounded by a 65536-id scan ceiling; any over-cap remainder stays unsettled and is
NAcked on teardown (no loss).

> **Gotcha #3 — `released` / `modified` increment the receive-count.** Every release/modify for
> redelivery bumps `ReceiveCount` toward the broker's `MaxReceiveQueue` cap. A message you keep
> NAcking eventually hits that cap and is removed **even though you never `rejected` it**. There is
> no requeue-without-increment. If you genuinely want to discard, `reject`; if you want to retry,
> understand the count climbs. See [../guides/reliability.md](../guides/reliability.md).

## Inbound Settlement (When You Produce)

When you publish, the **server is the receiver** and emits a settled `DISPOSITION(role=receiver)`
per delivery: `accepted` on broker success, `rejected{condition}` on failure. A **pre-settled**
inbound delivery (or one with no delivery-id) gets **no** disposition — a failure is dropped,
logged, and counted in `kubemq_amqp10_transfers_in_dropped_total`. Inbound failure conditions map
as: broker-not-ready → `amqp:not-allowed`; decode error → `amqp:decode-error`; translate error →
`amqp:invalid-field`; array/broker error → `amqp:internal-error` (broker text sanitized to ≤ 512
chars).

## Teardown NAck-All

On link detach, connection close, or graceful shutdown, **every unsettled delivery is NAcked exactly
once** (returned to the queue tail; you see them as `released`). A disconnecting consumer therefore
**loses nothing** — a fresh consumer recovers the work. See [work-queues.md](work-queues.md) and
[../guides/reliability.md](../guides/reliability.md).

## Body Sections

A message body must be one of two AMQP sections:

- **`Data`** (binary) — the default; multiple `Data` sections concatenate.
- **`AmqpValue`** (a typed value) — use for typed bodies.

An **empty body is valid** (it becomes an empty `Data` body downstream).

> **Gotcha #5 — `AmqpSequence` bodies are rejected.** Only `Data` and `AmqpValue` are supported. A
> message carrying an `AmqpSequence` body section gets a `rejected` DISPOSITION then a `DETACH`,
> with `amqp:not-implemented`. Emit `Data` by default and `AmqpValue` for typed bodies; never
> `AmqpSequence`. See [../reference/capabilities.md](../reference/capabilities.md).

## Examples

| Language | Example |
|----------|---------|
| Go | [queues/ack-release-redelivery](../../examples/go/queues/ack-release-redelivery/) · [queues/settlement-modes](../../examples/go/queues/settlement-modes/) |
| Python | [queues/ack_release_redelivery](../../examples/python/queues/ack_release_redelivery/) · [queues/settlement_modes](../../examples/python/queues/settlement_modes/) |
| Java | [queues/ack-release-redelivery](../../examples/java/queues/ack-release-redelivery/) · [queues/settlement-modes](../../examples/java/queues/settlement-modes/) |
| C# | [queues/ack-release-redelivery](../../examples/csharp/queues/ack-release-redelivery/) · [queues/settlement-modes](../../examples/csharp/queues/settlement-modes/) |
| JavaScript / TS | [queues/ack-release-redelivery](../../examples/javascript/queues/ack-release-redelivery/) · [queues/settlement-modes](../../examples/javascript/queues/settlement-modes/) |
| Rust | [queues/ack-release-redelivery](../../examples/rust/queues/ack-release-redelivery/) · [queues/settlement-modes](../../examples/rust/queues/settlement-modes/) |

Grounding: , , .
