# Reliability

AMQP 1.0 reliability is **settlement modes + delivery state**, not publisher confirms and not
numeric reason codes. This guide is the practical playbook for getting the delivery guarantee you
want from the KubeMQ AMQP 1.0 connector: which settlement modes exist (and which is rejected), how
each client delivery-state outcome maps to a KubeMQ queue action, how redelivery and the
receive-count behave, what happens on disconnect (nothing is lost), and why there is **no connector
dead-letter exchange or visibility timeout**.

For the conceptual model see
[Settlement and Delivery State](../concepts/settlement-and-delivery-state.md); for the credit
machinery that governs *when* deliveries arrive see [flow-control.md](flow-control.md).

---

## 1. Settlement modes — pick your guarantee at ATTACH

Settlement is negotiated when the link attaches.

### `snd-settle-mode` (the produce / out path)

| Requested | Server behavior | Guarantee |
|---|---|---|
| **`settled`** | honored — each outbound `TRANSFER` carries `settled=true` and the server `AckRange`s it immediately at send | **at-most-once** (pre-settled) |
| `unsettled` / `mixed` / absent | server uses **`unsettled`** — sends deliveries unsettled, tracks them, waits for your `DISPOSITION` | **at-least-once** (default) |

- **At-least-once (default):** leave `snd-settle-mode` unset (or `unsettled`). The server keeps each
 delivery tracked until you settle it; if you never do (you disconnect), it is requeued (§4).
- **At-most-once:** request `snd-settle-mode=settled`. Use this for high-throughput fire-and-forget
 where occasional loss is acceptable. (Events are *always* pre-settled regardless — see
 [pub-sub](../concepts/pub-sub.md).)

### `rcv-settle-mode` (the consume / in path)

- The server **always** replies `rcv-settle-mode=first`.
- **`rcv-settle-mode=second` is NOT implemented.** Requesting it closes the link with
 `DETACH(amqp:not-implemented)` **before the link is built**. Pin `first` (gotcha #7).

> **First settlement only.** There is no two-stage (`second`) receiver settlement. Your consumer
> sends a `DISPOSITION` with a terminal delivery state and that settles the delivery in one step.

---

## 2. Delivery-state outcomes → KubeMQ actions

When your client is the **receiver** (consuming a queue), it settles each delivery by sending a
`DISPOSITION` carrying a terminal **delivery state**. The connector maps that state to a KubeMQ
queue `AckRange` (settle/remove) or `NAckRange` (requeue) via `classifyState` :

| Client delivery state (go-amqp call) | KubeMQ action | Queue request | Effect on the message |
|---|---|---|---|
| **`accepted`** (`AcceptMessage`) | settle / consume | **AckRange** | removed from the queue |
| **`rejected`** (`RejectMessage`) | discard | **AckRange** | discarded; **poison handled by the broker `MaxReceiveQueue` policy — there is NO connector DLX** |
| **`released`** (`ReleaseMessage`) | requeue to tail | **NAckRange** | redelivered with a grown `delivery-count`, `first-acquirer=false`; **increments receive-count** toward `MaxReceiveCount` |
| **`modified{delivery-failed}` / `modified{undeliverable-here}`** (`ModifyMessage`) | requeue to tail | **NAckRange** | requeued (no per-consumer exclusion; `undeliverable-here` is treated as `delivery-failed`) |
| **nil state** (settled, no outcome) | treat as success | **AckRange** | removed |
| **unknown terminal state** | conservatively requeue | **NAckRange** | requeued — **never silently dropped** |

Notes:

- The connector resolves a `DISPOSITION` over a `first..last` delivery-id range against the per-link
 unsettled map, groups it by `RefTransactionId`, and emits **one `AckRange`/`NAckRange` per group**
 with ascending `SequenceRange` lists. Re-settling an already-settled id is **idempotent** (unknown
 ids are ignored). A single frame walks at most **65536** ids (an absolute safety ceiling).
- **`rejected` does NOT dead-letter through the connector.** It `AckRange`s (discards) the message;
 whether a repeatedly-failing message is moved aside is a **broker-side `MaxReceiveQueue` poison
 policy**, not an AMQP-controllable per-link feature. See §5.
- **`released`/`modified` increment the receive-count** (gotcha #3): every NAck-for-redelivery bumps
 `ReceiveCount`, so a message you keep releasing will eventually hit the broker's `MaxReceiveCount`
 cap and be removed even though you never `rejected` it. There is **no requeue-without-increment**.

---

## 3. Inbound (when your client produces)

When your client is the **sender**, the *server* is the receiver and settles your delivery for you:

- On broker success it emits a settled `DISPOSITION(role=receiver, accepted)`.
- On failure it emits `rejected{condition}`: broker-not-ready → `amqp:not-allowed`; decode error →
 `amqp:decode-error`; translate error → `amqp:invalid-field`; array/broker error →
 `amqp:internal-error` (broker text sanitized to ≤512 chars).
- A **pre-settled** inbound delivery (or one with no delivery-id) gets **no** disposition; a failure
 is dropped, logged, and counted in `kubemq_amqp10_transfers_in_dropped_total` (which also counts
 oversize and no-consumer drops). If you need to know a produce succeeded, do **not** pre-settle
 send unsettled and read the server's `DISPOSITION`.

---

## 4. Redelivery, receive-count, and teardown NAck-all

### Redelivery

`released`, `modified`, and **disconnect-with-unsettled** all requeue the delivery to the tail. A
fresh consumer recovers it. A redelivered copy carries a grown `header.delivery-count`
(`= ReceiveCount − 1`) and `first-acquirer=false` (`applyReceiveCount`), so your
consumer can detect a redelivery and apply its own poison handling if it wants finer control than
the broker's `MaxReceiveQueue` cap.

### Teardown NAck-all — nothing is lost on disconnect

This is the key reliability guarantee for at-least-once consumers:

> **On link detach, connection close, or graceful shutdown, every unsettled delivery is
> `NAckRange`'d exactly once** — returned to the queue tail (the client experiences it as
> `released`). A disconnecting consumer **loses nothing**: whatever it had not yet `accepted` is
> simply redelivered to the next consumer (`nackAllUnsettled`). A per-connection
> downstream `Dispose` is a final safety-net NAck-all.

So with at-least-once (unsettled) consume, a crash, a kill, or a clean shutdown all converge on the
same outcome: in-flight-but-unsettled messages return to the queue. Combined with redelivery, this
gives you the at-least-once contract. (.)

> **Caveat for the pre-settled patterns.** The NAck-all guarantee applies to **queue** consume links
> (unsettled, at-least-once). **Events and Events-Store are pre-settled** — they deliver
> at-most-once and have their own *data-loss footguns* (0-credit drop; stalled-credit buffer loss)
> that NAck-all does **not** protect against. Those are covered first-class in
> [flow-control.md](flow-control.md).

---

## 5. No connector DLX, no visibility timeout

The connector has **no dead-letter exchange and no visibility/redelivery timeout**:

- `rejected` discards via `AckRange`; it does **not** route to a dead-letter destination.
- There is no per-link "make this message invisible for N seconds" verb. Queue receive is
 **destructive, credit-based consume only** — no peek, no browse, no FIFO/visibility primitives.
- **Poison handling is entirely broker-side**: the broker's `MaxReceiveQueue` / `MaxReceiveCount`
 policy removes a message that has been redelivered too many times. This is broker-configured, not
 AMQP-controllable per link.

Design your consumer accordingly: use `accept` on success, `reject` for a permanently-bad message
(it is discarded, and the broker policy is your only poison backstop), and `release`/`modify` for a
transient failure you want retried — knowing each retry bumps the receive-count toward the broker's
cap.

---

## 6. Decision guide

| You want… | Do this |
|---|---|
| At-least-once consume (no loss on disconnect) | Consume **unsettled** (default `rcv-settle-mode=first`); `accept` on success; rely on teardown NAck-all |
| At-most-once produce (fire-and-forget) | Request `snd-settle-mode=settled` |
| Confirm a produce succeeded | Send **unsettled** and read the server's `DISPOSITION(accepted/rejected)` |
| Retry a transient failure | `release` (or `modify`) — but each retry **increments the receive-count** |
| Discard a permanently-bad message | `reject` — discarded via AckRange; broker `MaxReceiveQueue` is the poison backstop (no connector DLX) |
| Two-stage receiver settlement | **Not supported** — `rcv-settle-mode=second` → `amqp:not-implemented`; pin `first` |

---

## Related

- [Settlement and Delivery State](../concepts/settlement-and-delivery-state.md) — the conceptual model
- [Flow Control and Credit](flow-control.md) — the data-loss footguns on the pre-settled patterns
- [Work Queues](../concepts/work-queues.md) — competing-consumer semantics
- [Error Conditions](../reference/error-conditions.md) — the 13 `amqp:*` conditions

Examples:
[Go `queues/ack-release-redelivery`](../../examples/go/queues/ack-release-redelivery/) ·
[`queues/settlement-modes`](../../examples/go/queues/settlement-modes/) ·
[Python `queues/ack_release_redelivery`](../../examples/python/queues/ack_release_redelivery/) ·
[Java](../../examples/java/queues/ack-release-redelivery/) ·
[C#](../../examples/csharp/queues/ack-release-redelivery/) ·
[JS/TS](../../examples/javascript/queues/ack-release-redelivery/) ·
[Rust](../../examples/rust/queues/ack-release-redelivery/)
