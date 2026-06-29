# Rust — Events / Selector

JMS / SQL-92 message selectors over KubeMQ **Events** with the native `fe2o3-amqp`
client. A receiver attaches with the selector `color = 'red' AND size > 2` as a
source filter; the connector evaluates it against each event's application
properties and delivers only the matching events. The program also demonstrates
that a selector on a `queues/` source is rejected at ATTACH.

## Prerequisites

- Rust 1.94+ (stable) with `cargo`.
- `fe2o3-amqp` 0.15.1 + `fe2o3-amqp-types` 0.14.0 + `serde_amqp` 0.14.1 (pinned
  exact in the workspace `examples/rust/Cargo.toml`).
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/rust
cargo run -p selector
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 cargo run -p selector
```

## Expected Output

```
Broker:   amqp://localhost:5672
Address:  events/amqp10.examples.selector  (KubeMQ pattern=events, channel=amqp10.examples.selector)
Selector: color = 'red' AND size > 2

[recv] Subscribed to events/amqp10.examples.selector with selector filter (standing credit 100)
[recv] Subscription pump settled (waited 750ms before publishing)
[send] match-1       color=red size=5       -> should MATCH (color=red AND size>2)
[send] miss-blue     color=blue size=9      -> should be FILTERED OUT (color!=red)
[send] miss-small    color=red size=1       -> should be FILTERED OUT (size not >2)
[send] match-2       color=red size=3       -> should MATCH (color=red AND size>2)
[send] miss-nocolor  size=8                 -> should be FILTERED OUT (color IS NULL => UNKNOWN (3-valued))
[recv] delivered: match-1
[recv] delivered: match-2
[recv] Received exactly 2 matching event(s); 3 non-matching event(s) were silently withheld

[gotcha] Selector on queues/amqp10.examples.selector.q correctly REJECTED at ATTACH:
         IllegalSessionState
         (selectors are supported only on events/ and events-store/ — queues/ is move-only)

Done.
```

## What's Happening

1. **OPEN / BEGIN** — connect (ANONYMOUS) and begin one session.
2. **ATTACH (receiver) with a selector filter** — `selector_source(addr, selector)`
   builds a `Source` whose filter map carries one entry: key
   `apache.org:selector-filter:string`, value a **described** value (descriptor =
   the same symbol, value = the selector string), added via
   `SourceBuilder::add_to_filter_using_legacy_format`. A successful attach means the
   connector accepted (and echoed) the filter; a parse error would have DETACHed.
3. **Wait, then publish** — after the pump settles, publish 5 pre-settled events
   with `color` / `size` application properties.
4. **Three-valued logic** — `miss-nocolor` has no `color`, so the predicate is NULL
   ⇒ UNKNOWN (not true) and the event is withheld even though it has no color to
   disqualify it.
5. **RECEIVE matches only** — drain exactly the 2 matching events, then prove a
   final `recv` times out (the 3 non-matching events were silently withheld via copy
   semantics — they remain available to other subscribers).
6. **GOTCHA demo** — attaching a receiver with the same selector on a `queues/`
   source is rejected: the connector DETACHes with `amqp:not-implemented`, surfaced
   by fe2o3-amqp as an attach error. Cleanup after the rejected attach is
   best-effort (the connector also tears the session down).

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events/<ch>` | `Settled` (pre-settled) | server-granted | none | app-props `color`, `size` | `Data` | selector evaluated against app-props |
| receiver (KubeMQ → client) | source `events/<ch>` + **filter** `apache.org:selector-filter:string` | `First` (default) | client `CreditMode::Auto(100)` | n/a (pre-settled) | source filter (selector) | `Data` | only matching events delivered; 3-valued logic |

## Gotchas

> **Selectors are rejected on `/queues/`.** A selector source-filter is honoured
> only on `events/` and `events-store/` consume links. Requesting one on a
> `queues/` source makes the connector DETACH the link with `amqp:not-implemented`
> ("selector filter not supported on this address"). The program demonstrates this
> at the end.

- **Filter encoding is the fe2o3 churn point.** In `fe2o3-amqp-types` 0.14 the
  selector must be a **described** `Value` (descriptor
  `apache.org:selector-filter:string`) added via
  `add_to_filter_using_legacy_format`. This was dial-tested against the live
  connector; re-verify it on any version bump.
- **NULL ⇒ UNKNOWN.** A missing application property makes its predicate UNKNOWN,
  so the event is withheld (SQL-92 three-valued logic).
- **Events drop silently at 0 credit.** Keep standing credit so matching events are
  not dropped (`kubemq_amqp10_events_dropped_no_credit_total`).

## Related Examples

- [events/basic-pubsub](../basic-pubsub/) — unfiltered fan-out
- [events/consumer-group](../consumer-group/) — group-based load balancing
- [events-store/durable-replay](../../events-store/durable-replay/) — selectors also work on durable subscriptions

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
