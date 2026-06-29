# Go — Events / Selector

JMS / SQL-92 **message selectors** over KubeMQ **Events** with the native
`github.com/Azure/go-amqp` client. A receiver attaches to `events/<ch>` with a
selector source-filter; the connector evaluates it against each event's
**application properties** and delivers only the matching events. The
non-matching events are silently withheld (copy semantics — they remain
available to other subscribers, they are not consumed or discarded).

The selector used here is:

```sql
color = 'red' AND size > 2
```

## Prerequisites

- Go 1.24+
- `github.com/Azure/go-amqp` v1.7.0 (pinned in `examples/go/go.mod`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/go
go run ./events/selector
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 go run ./events/selector
```

## Expected Output

```
Broker:   amqp://localhost:5672
Address:  events/amqp10.examples.selector  (KubeMQ pattern=events, channel=amqp10.examples.selector)
Selector: color = 'red' AND size > 2

[recv] Subscribed to events/amqp10.examples.selector with selector filter (standing credit 100)
[recv] Subscription pump settled (waited 750ms before publishing)
[send] match-1       map[color:red size:5]        → should MATCH (color=red AND size>2)
[send] miss-blue     map[color:blue size:9]       → should be FILTERED OUT (color≠red)
[send] miss-small    map[color:red size:1]        → should be FILTERED OUT (size≯2)
[send] match-2       map[color:red size:3]        → should MATCH (color=red AND size>2)
[send] miss-nocolor  map[size:8]                  → should be FILTERED OUT (color IS NULL ⇒ UNKNOWN (3-valued))
[recv] delivered: match-1
[recv] delivered: match-2
[recv] Received exactly 2 matching event(s); 3 non-matching event(s) were silently withheld

[gotcha] Selector on queues/amqp10.examples.selector.q correctly REJECTED at ATTACH:
         received detach frame with unknown link handle 0
         (selectors are supported only on events/ and events-store/ — queues/ is move-only)

Done.
```

> The connector DETACHes the rejected attach with `amqp:not-implemented`
> ("selector filter not supported on this address"). go-amqp v1.7.0 surfaces a
> detach that races link registration as the generic "received detach frame with
> unknown link handle 0"; either way the selector link never attaches, which is
> the contract.

## What's Happening

1. **OPEN / BEGIN** — `amqp.Dial` (SASL ANONYMOUS by default) then `NewSession`.
2. **ATTACH (receiver) with a selector FIRST** —
   `NewReceiver("events/<ch>", {Credit:100, Filters:[]amqp.LinkFilter{amqp.NewSelectorFilter(...)}})`.
   `NewSelectorFilter` encodes the selector under the OASIS-standard filter key
   **`apache.org:selector-filter:string`** (descriptor code `0x0000468C00000004`).
   A successful `NewReceiver` means the connector accepted the filter and echoed
   it in the reply-ATTACH (Qpid JMS requires the echo to trust enforcement); a
   parse error or an unsupported pattern would have DETACHed the link.
3. **ATTACH (sender)** — a pre-settled producer on `events/<ch>`.
4. **TRANSFER (publish)** — each `Send` carries `Message.ApplicationProperties`
   (`color`, `size`). The connector evaluates the selector against these
   properties on the delivery path.
5. **TRANSFER (consume)** — only the events whose properties satisfy the
   predicate are delivered. `match-1` and `match-2` arrive; `miss-blue`,
   `miss-small`, and `miss-nocolor` are silently withheld.
6. **Three-valued logic** — `miss-nocolor` has no `color` property, so
   `color = 'red'` evaluates to **NULL → UNKNOWN** (not `true`), and the event is
   **not** delivered. SQL-92 selectors use three-valued logic: only `TRUE`
   passes; `FALSE` and `UNKNOWN` are both withheld.
7. **Gotcha demo** — the program then attempts the same selector on a `queues/`
   source and shows the connector rejecting it at ATTACH with
   `amqp:not-implemented`.
8. **DETACH / CLOSE** — the receiver detaches and the connection closes.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events/<ch>` | `settled` (`SenderSettleModeSettled`) | server-granted | NONE — pre-settled fire-and-forget | `ApplicationProperties{color, size}` (selector operands) | `Data` | at-most-once publish, no replay |
| receiver (KubeMQ → client) | source `events/<ch>` + selector filter `apache.org:selector-filter:string` | `first` (default) | client-granted standing `Credit:100`, auto-replenished | deliveries pre-settled (Accept is a no-op) | source filter (selector); evaluated against each event's app properties | `Data` | only matching events delivered; non-matching silently withheld; 0-credit ⇒ silent drop |

## Gotchas

> **Selectors are rejected on `queues/`.** A selector source-filter is honoured
> **only** on `events/` and `events-store/` consume links. Requesting one on a
> `queues/` source (queues are move-only) is rejected at **ATTACH** with
> `amqp:not-implemented` ("selector filter not supported on this address"). The
> program demonstrates this at the end. Filter messages with selectors on the
> pub/sub patterns; for queues, route at publish time instead.

- **Three-valued (SQL-92) logic — NULL is withheld.** A missing application
  property evaluates to NULL, making the predicate **UNKNOWN**, not `false`. An
  event with no `color` property does **not** match `color = 'red'` and is **not**
  delivered. Only `TRUE` passes the filter.
- **Selectors evaluate APPLICATION PROPERTIES, not the body.** Operands name
  application-property keys (`color`, `size`), so the publisher must stamp the
  properties it wants filterable — the body bytes are opaque to the selector.
- **A malformed selector is rejected with `amqp:invalid-field`** ("invalid
  selector: …") at ATTACH, naming the offending token — the link never attaches.
- **0-credit ⇒ silent drop still applies.** Events are at-most-once with no
  replay; a matching event that arrives when the subscriber has zero credit is
  dropped. Keep continuous standing credit.

## Related Examples

- [events/basic-pubsub](../basic-pubsub/) — fan-out at-most-once; subscribe-before-publish; continuous credit
- [events/consumer-group](../consumer-group/) — `x-opt-kubemq-group` load balancing vs independent groups
- [events-store/start-positions](../../events-store/start-positions/) — durable replay with `x-opt-kubemq-start` (selectors also work on `events-store/`)

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
