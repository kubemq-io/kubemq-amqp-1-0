# Durable Subscriptions (Events Store)

## Concept

The **events-store** pattern (`events-store/<ch>`) is a **durable, replayable** subscription. Unlike
plain [events](pub-sub.md) (fire-hose, no replay), an events-store subscriber can **resume** where
it left off after a disconnect and can **replay** history from a chosen start position. This is the
pattern for a consumer that must not miss messages while it was offline.

- **Produce:** attach a **sender** to `events-store/<ch>`; each `TRANSFER` becomes
 `SendEventsStore` (`Store=true`), so the message is persisted.
- **Consume durably:** attach a **receiver** from `events-store/<ch>` with terminus
 **`expiry-policy = never`** (this is what makes the subscription durable), a **stable
 container-id**, and a **stable link `Name`**. On reconnect with the same identity, the
 subscription resumes.

## Durable Identity

The durable subscription's identity is derived from your connection's container-id and the link
name:

```
durableID = sanitize40(containerID) + "_" + sanitize40(linkName) + "_" + fnv1a32hex(containerID + "|" + linkName)
```

where `sanitize40` maps every character outside `[A-Za-z0-9_-]` to `_` and truncates to 40 chars,
and `fnv1a32hex` is the 8-hex-digit FNV-1a-32 of the raw `containerID|linkName`.

The practical consequence: **to resume, reconnect with the same container-id AND the same link
name.** Change either and you get a *different* durable subscription that starts fresh.

> **Stable container-id is mandatory.** Because the container-id is half the durable identity, a
> durable subscriber that lets its container-id drift across reconnects (e.g. a randomly generated
> one) will never resume — it creates a new subscription each time. Pin a stable container-id. See
> [links-sessions-connections.md](links-sessions-connections.md) and
> [../guides/authentication.md](../guides/authentication.md).

## Single Live Attach — Node-Local

A durable identity may have at most **one live attach per node**. A second live attach of the same
identity → `DETACH(amqp:not-allowed, "durable subscription in use")`.

> **Gotcha #6 — durable subscriptions (and dynamic nodes) are node-local.** The durable registry is
> **per-node**. A durable subscription created on node A is not visible from node B, and a dynamic
> reply node ([rpc.md](rpc.md)) lives only on the node that minted it. Cluster-wide *uniqueness* of
> the durable identity is still enforced (via the STAN ClientID), but a durable subscriber must
> reconnect to the **same node** to resume (use load-balancer session affinity / a sticky
> connection). RPC *replies* travel the broker reply path and are cluster-safe; direct cross-node
> dynamic sends are not. See [../reference/migration-from-activemq.md](../reference/migration-from-activemq.md).

## Start Positions — `x-opt-kubemq-start`

A durable receiver's start position is set with the link property **`x-opt-kubemq-start`**. It only
applies to `events-store` (it is ignored on plain events / RPC, which have no replay). The grammar:

| `x-opt-kubemq-start` value | Meaning |
|----------------------------|---------|
| `""` or `new-only` | only messages published **after** the subscription starts (the default) |
| `first` | replay from the **beginning** of stored history |
| `last` | start from the **most recent** stored message |
| `sequence:<n>` | start at sequence number `<n>` (**1-based**, non-negative) |
| `time:<RFC3339\|unix-seconds>` | start at a wall-clock time |
| `time-delta:<seconds>` | start `<seconds>` ago from now |

Notes:

- **Time is sent as RFC3339 or whole seconds; the broker stores nanoseconds.** The connector parses
 your `time:` value (RFC3339 first, then a bare integer as Unix seconds) into a `time.Time` and
 stores `t.UnixNano`. You always send seconds-granularity; the connector does the nanosecond
 conversion. `time-delta:` is whole seconds, used verbatim.
- **No "last N by count".** There is no way to ask for "the last N messages" — use `sequence:`,
 `time:`, or `time-delta:` to bound a replay.
- **Malformed values are rejected** at attach: `sequence:abc`, `time:not-a-time`, or an unknown
 token → `DETACH(amqp:invalid-field)` naming the offending token.

## The Stalled-Credit Footgun

> ### ⚠ Footgun #2 — Events-Store stalled credit loses the buffered window
>
> An events-store consume link fronts the subscription with a **deliver-first ring buffer**
> (capacity `MaxUnsettledPerLink` ≈ 1024) so the broker callback never stalls. "Deliver-first"
> means the buffered window is **already auto-acked in the broker before you take delivery**. If the
> buffer fills while your credit stays at 0, the link `DETACH`es with
> `amqp:resource-limit-exceeded` ("credit stalled"), and **the entire buffered, already-acked window
> is lost** — a durable re-attach resumes *after* it. A genuinely stalled durable subscriber loses
> data, silently, with a metric (`kubemq_amqp10_events_store_dropped_stalled_total`) as the only
> signal.
>
> **Defense:** size `MaxUnsettledPerLink` to your real prefetch and **replenish credit
> aggressively** so the buffer never fills with credit at zero. See
> [flow-control-and-credit.md](flow-control-and-credit.md) and
> [../guides/flow-control.md](../guides/flow-control.md).

Like plain events, outbound events-store deliveries are **pre-settled** (at-most-once on the wire);
durability is on the *store* side (replay/resume), not on the wire-settlement side.

## Examples

| Language | Example |
|----------|---------|
| Go | [events-store/durable-replay](../../examples/go/events-store/durable-replay/) · [events-store/start-positions](../../examples/go/events-store/start-positions/) |
| Python | [events_store/durable_replay](../../examples/python/events_store/durable_replay/) · [events_store/start_positions](../../examples/python/events_store/start_positions/) |
| Java | [events-store/durable-replay](../../examples/java/events-store/durable-replay/) · [events-store/start-positions](../../examples/java/events-store/start-positions/) — start-positions is **N/A** (Qpid JMS has no arbitrary-link-property API for `x-opt-kubemq-start`; README points to durable-replay's native `new-only`) |
| C# | [events-store/durable-replay](../../examples/csharp/events-store/durable-replay/) · [events-store/start-positions](../../examples/csharp/events-store/start-positions/) |
| JavaScript / TS | [events-store/durable-replay](../../examples/javascript/events-store/durable-replay/) · [events-store/start-positions](../../examples/javascript/events-store/start-positions/) |
| Rust | [events-store/durable-replay](../../examples/rust/events-store/durable-replay/) · [events-store/start-positions](../../examples/rust/events-store/start-positions/) |

Grounding: , .
