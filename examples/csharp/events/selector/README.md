# C# — Events / Selector

JMS / SQL-92 message selectors over KubeMQ **Events** with the native
`AMQPNetLite.Core` client. A receiver attaches with a selector source-filter; the
connector evaluates it against each event's application properties and delivers
**only** the matching events. The example also demonstrates that a selector on a
`queues/` source is rejected at ATTACH.

## Prerequisites

- .NET SDK **8.0**
- `AMQPNetLite.Core` **2.5.3** (pinned in `examples/csharp/Directory.Packages.props`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd events/selector
dotnet run
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 dotnet run
```

## Expected Output

```
Broker:   amqp://localhost:5672
Address:  events/amqp10.examples.selector  (KubeMQ pattern=events, channel=amqp10.examples.selector)
Selector: color = 'red' AND size > 2

[recv] Subscribed to events/amqp10.examples.selector with selector filter (standing credit 100)
[recv] Subscription pump settled (waited 750ms before publishing)
[send] match-1       {color:red size:5} -> should MATCH (color=red AND size>2)
[send] miss-blue     {color:blue size:9} -> should be FILTERED OUT (color!=red)
[send] miss-small    {color:red size:1} -> should be FILTERED OUT (size not >2)
[send] match-2       {color:red size:3} -> should MATCH (color=red AND size>2)
[send] miss-nocolor  {size:8} -> should be FILTERED OUT (color IS NULL => UNKNOWN (3-valued))
[recv] delivered: match-1
[recv] delivered: match-2
[recv] Received exactly 2 matching event(s); 3 non-matching event(s) were silently withheld

[gotcha] Selector on queues/amqp10.examples.selector.q correctly REJECTED at ATTACH:
         amqp:not-found (link detached by broker)
         (selectors are supported only on events/ and events-store/ — queues/ is move-only)

Done.
```

## What's Happening

1. **OPEN / BEGIN** — connect (ANONYMOUS) and open one session.
2. **ATTACH (receiver) with a selector filter** — AMQPNetLite has no selector
   helper, so the filter is plumbed by hand: `Source.FilterSet` is an
   `Amqp.Types.Map` with one entry —
   `{ Symbol("apache.org:selector-filter:string") => DescribedValue(0x0000468C00000004, "color = 'red' AND size > 2") }`.
   A successful attach means the connector accepted (and echoed) the filter; a
   parse error or unsupported pattern would DETACH the link.
3. **Pump settle** — wait ~750ms before publishing (Events have no replay).
4. **ATTACH (sender)** — pre-settled. Publish 5 events, each stamped with
   `ApplicationProperties` (`color`, `size`). The connector evaluates the
   selector against these properties on the delivery path.
5. **TRANSFER (receive)** — exactly the 2 matching events (`match-1`, `match-2`)
   are delivered; a final `Receive` returns `null`, proving the 3 non-matching
   events were silently withheld (copy semantics — they remain available to
   other subscribers, they are not consumed/discarded).
6. **Gotcha demo** — a receiver requests the same selector on a `queues/`
   source. The connector DETACHes the attach; the link ends up closed with an
   error (AMQPNetLite surfaces it as `amqp:not-found`). Run on its own session so
   the rejection does not disturb the main session.
7. **CLOSE** — links detach and the connection closes.

### Three-valued logic

A property that is **absent** evaluates to NULL, so the predicate is UNKNOWN (not
true) and the event is **not** delivered. That is why `miss-nocolor` (no `color`)
is withheld even though it has no color to disqualify it.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events/<ch>` | `SndSettleMode = Settled` | server-granted | none (pre-settled) | app-props `color`, `size` (stamped per event) | `Data` | at-most-once fan-out |
| receiver (KubeMQ → client) | source `events/<ch>` + `FilterSet` selector | `first` (default) | client standing credit `SetCredit(100, autoRestore)` | `Accept` no-op (pre-settled) | source filter key `apache.org:selector-filter:string` | `Data` | only matching events delivered; NULL ⇒ withheld |
| receiver (rejected) | source `queues/<ch>.q` + `FilterSet` selector | n/a (attach refused) | n/a | DETACH `amqp:not-implemented` (surfaced as `amqp:not-found`) | source filter selector | n/a | selectors not supported on `queues/` |

## Gotcha

> **Selectors are honoured only on `events/` and `events-store/` consume links.**
> Requesting a selector on a `queues/` source is rejected at ATTACH: the
> connector DETACHes the link with `amqp:not-implemented` ("selector filter not
> supported on this address"). AMQPNetLite races the detach against link
> registration and surfaces it as a closed link with `amqp:not-found`; either way
> the selector link never attaches. (`queues/` is move-only — there is no
> per-consumer copy to filter.)

## Related Examples

- [events/basic-pubsub](../basic-pubsub/) — fan-out without a selector
- [events/consumer-group](../consumer-group/) — load-balancing with `x-opt-kubemq-group`
- [queues/basic-send-receive](../../queues/basic-send-receive/) — Queues (move-only; no selectors)

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
