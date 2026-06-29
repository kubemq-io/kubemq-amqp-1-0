# Flow Control and Credit

In AMQP 1.0, **link credit** is what publisher confirms and consumer prefetch/QoS are in other
protocols: it is the unit of flow control. A sender may not transmit a `TRANSFER` until the receiver
has granted `link-credit > 0` via a `FLOW`. This guide is the practical playbook for driving the
KubeMQ AMQP 1.0 connector's credit machinery — and, first and foremost, for **avoiding the two ways
credit mismanagement silently loses data**.

---

## ⚠️ The two data-loss footguns — read this first

These are the **two most expensive mistakes** you can make against this connector. Both lose
messages **silently** — no error, no `DISPOSITION`, no DETACH (footgun A) — because the events and
events-store patterns deliver **pre-settled** (at-most-once). Internalize these before you write a
subscriber.

> ### Footgun A — Events at 0 credit are silently DROPPED
>
> On an **`events/<ch>`** consume link (server-sender, fire-hose), a message that arrives while your
> **link-credit is 0 is silently dropped and counted** — there is **no error and no DISPOSITION**.
> This is true at-most-once: the message is simply gone. It bites a slow consumer that lets its
> credit drain, or a subscriber that attaches *after* a publish (subscribe-before-publish, since
> events have no replay).
>
> **Defense: grant credit continuously.** Open the receiver with a healthy standing credit and
> replenish *eagerly* (well before it hits 0). Never let an events consumer sit at 0 credit while
> publishers are active. The drop is counted in `kubemq_amqp10_events_dropped_no_credit_total`
> watch it; a non-zero value is silent data loss.
> (`ReportAmqp10EventDroppedNoCredit`.)

> ### Footgun B — Events-Store stalled credit loses the buffered, already-acked window
>
> An **`events-store/<ch>`** consume link fronts the durable subscription with a **deliver-first
> ring buffer** (cap `MaxUnsettledPerLink` ≈ 1024) so the broker callback never stalls. The buffer's
> STAN positions are **auto-acked *before* you take delivery**. If the buffer fills while your credit
> stays at 0, the link `DETACH`es with `amqp:resource-limit-exceeded` (`"credit stalled"`) and the
> **entire buffered window — already auto-acked — is lost**; a durable re-attach resumes *after* it.
> A genuinely stalled durable subscriber loses data.
>
> **Defense: size `MaxUnsettledPerLink` to your real prefetch and replenish credit aggressively** so
> the buffer never fills with credit at zero. The lost window is counted in
> `kubemq_amqp10_events_store_dropped_stalled_total`. (
> `ReportAmqp10EventsStoreDroppedStalled`.)

> **Why queues are safe by contrast.** A **queue** consume link never drops on low credit: when you
> stop granting, KubeMQ simply stops delivering and the messages wait in the queue. The drop
> footguns are specific to the **pre-settled pub/sub** patterns (events, events-store). See
> [reliability.md](reliability.md) for the queue at-least-once guarantees and
> [Flow Control and Credit](../concepts/flow-control-and-credit.md) for the conceptual model.

---

## 1. Who grants credit — the rule that governs everything

The single most important distinction: **on a *consume* (server-sender) link, YOU must grant
credit; on a *produce* (server-receiver) link, the *server* grants credit.**

| Your link | Server role | Who grants credit | If credit is 0… |
|---|---|---|---|
| **Receiver** from `<pattern>/<ch>` (you *consume*) | server-sender | **you (the client)** must `FLOW` `link-credit > 0` | **queues**: delivery pauses, messages wait. **events**: dropped (footgun A). **events-store**: buffered then stalled-DETACH (footgun B) |
| **Sender** to `<pattern>/<ch>` (you *produce*) | server-receiver | **the server** grants you credit | you may not `TRANSFER` until you receive the server's `FLOW` |

### Produce path — the server grants you credit

On attach, a server-receiver link **immediately emits a `FLOW`** granting
`link-credit = MaxUnsettledPerLink` (default 1024, clamped `[1, 1<<20]`; fallback 256 if unset). You
**MUST NOT** send a `TRANSFER` before you receive that credit. As you complete deliveries the server
**replenishes** the window (a fresh `FLOW`) when remaining credit falls below half, so a steady
producer never stalls. The session `incoming-window` (2048) is a secondary bound.

### Consume path — you grant the server credit

A server-sender link delivers **nothing** until you send a `FLOW` with `link-credit > 0`. The
effective credit the server may use is `(flow.delivery-count + flow.link-credit) − server.delivery-count`,
clamped ≥ 0. **Practical recipe:**

- For automatic credit, grant a modest standing credit (e.g. **100–1000**, but **≤
 `MaxUnsettledPerLink` = 1024**) when you open the receiver, and let your client replenish on
 settlement.
- For **manual** credit control (`IssueCredit` / `DrainCredit`), open the receiver with
 **`Credit: -1`** (manual credit management) — otherwise your client auto-manages credit and your
 manual calls fight it.

---

## 2. Prefetch, `GetBatchSize`, and `MaxUnsettledPerLink`

These three knobs shape how the connector pulls from the broker and how much it will hold unsettled:

| Knob | Default | Meaning |
|---|---|---|
| **link-credit (prefetch)** | you choose | the standing credit you grant a consume link = your prefetch depth |
| **`GetBatchSize`** | `32` | the per-`Get` MaxItems ceiling on a **queue** consume link |
| **`MaxUnsettledPerLink`** | `1024` | the per-link unsettled / pub-sub buffer cap; also drives the **inbound** (produce) credit window |

For a **queue** consume link, each `Get` reserves
`min(credit, GetBatchSize=32, MaxUnsettledPerLink − unsettled)` and issues a downstream `Get` with
`AutoAck:false` and a `WaitTimeout:1000`ms long-poll. So even with high standing credit, a single
`Get` pulls at most 32 messages — credit controls overall in-flight depth, `GetBatchSize` controls
batch granularity.

> **`MaxUnsettledPerLink` is the dial that protects you from footgun B.** It is the events-store
> ring-buffer cap. Size it to your real prefetch so a brief credit stall doesn't overflow the buffer
> and lose the already-acked window. It is a server config field (`CONNECTORS_AMQP10_MAX_UNSETTLED_PER_LINK`,
> see [configuration](../configuration.md)) — but you respect it client-side by keeping your standing
> credit ≤ it and replenishing eagerly.

---

## 3. Drain

Drain is how a consumer says "give me everything you have, then stop." Send a `FLOW` with
`drain=true`. The server then:

1. **advances `delivery-count`** by the remaining credit,
2. **zeroes the credit**, and
3. **echoes a `FLOW`** with `link-credit=0, drain=true` (the drain response).

Drain completes **promptly** — it does not hang. A held remainder (messages that didn't fit the
drained credit) **resumes on a fresh `IssueCredit`**. Two requirements your client must honor:

- A `FLOW` **MUST carry `next-incoming-id`** once the session is established (go-amqp rejects a
 malformed one).
- To drive drain manually, the receiver must be in manual-credit mode (`Credit: -1`).

The burn-in [`credit_flow`](../../burnin/) worker exercises manual
`IssueCredit`/`DrainCredit` and drain under load.

---

## 4. Putting it together — patterns

| Pattern | Consume-side credit hygiene |
|---|---|
| **Queues** | Safe: grant standing credit; if you stop granting, messages wait. No drop. |
| **Events** | **Footgun A.** Grant generous standing credit, replenish *before* it hits 0, subscribe before publishing. A gap at 0 credit = silent loss. |
| **Events-Store** | **Footgun B.** Size `MaxUnsettledPerLink` to your prefetch; replenish aggressively. A genuine stall overflows the deliver-first buffer and loses the already-acked window. |
| **Commands / Queries (RPC)** | The responder pump runs under credit and pauses at `RpcMaxPending`; grant the responder credit and grant the dynamic reply node credit so replies can land. |

---

## 5. Monitoring

Watch these metrics — a non-zero value on the first two **is silent data loss**:

| Metric | Meaning |
|---|---|
| `kubemq_amqp10_events_dropped_no_credit_total` | **Footgun A** — events dropped at 0 credit |
| `kubemq_amqp10_events_store_dropped_stalled_total` | **Footgun B** — events-store buffered window lost to a credit stall |
| `kubemq_amqp10_transfers_in_dropped_total` | inbound transfers dropped (oversize / no-consumer / pre-settled failure) |

See [../reference/connections-endpoint.md](../reference/connections-endpoint.md) for the full metric
and dashboard surface (and note the connector does **not** emit connect/disconnect audit events
[authentication.md](authentication.md#5-audit-events--exactly-two-and-what-is-not-emitted)).

---

## Related

- [Flow Control and Credit (concept)](../concepts/flow-control-and-credit.md) — the credit model
- [Publish / Subscribe (Events)](../concepts/pub-sub.md) — subscribe-before-publish, footgun A
- [Durable Subscriptions](../concepts/durable-subscriptions.md) — events-store, footgun B
- [Reliability](reliability.md) — queue at-least-once and teardown NAck-all
- [Configuration](../configuration.md) — `MaxUnsettledPerLink`, `GetBatchSize`

Examples:
[Go `events/basic-pubsub`](../../examples/go/events/basic-pubsub/) ·
[`events-store/durable-replay`](../../examples/go/events-store/durable-replay/) ·
[Python `events/basic_pubsub`](../../examples/python/events/basic_pubsub/) ·
[Java](../../examples/java/events/basic-pubsub/) ·
[C#](../../examples/csharp/events/basic-pubsub/) ·
[JS/TS](../../examples/javascript/events/basic-pubsub/) ·
[Rust](../../examples/rust/events/basic-pubsub/)
