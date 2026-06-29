# Python — Events / Selector

JMS / SQL-92 message selectors over KubeMQ **Events** using the native
`python-qpid-proton` blocking client. A receiver attaches with a source filter
keyed by the OASIS descriptor `apache.org:selector-filter:string`; the connector
evaluates the selector against each event's **application properties** and delivers
only matching events. Non-matching events are silently withheld (copy semantics —
they stay available to other subscribers).

The selector here is `color = 'red' AND size > 2`.

## Prerequisites

- Python 3.10+
- `python-qpid-proton` 0.40.0 (pinned in `examples/python/pyproject.toml`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/python
uv sync
uv run python events/selector/main.py
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 uv run python events/selector/main.py
```

## Expected Output

```
Broker:   amqp://localhost:5672
Address:  events/amqp10.examples.selector  (KubeMQ pattern=events, channel=amqp10.examples.selector)
Selector: color = 'red' AND size > 2

[recv] Subscribed to events/amqp10.examples.selector with selector filter (standing credit 100)
[recv] Subscription pump settled (waited 750ms before publishing)
[send] match-1       {'color': 'red', 'size': 5}  -> should MATCH (color=red AND size>2)
[send] miss-blue     {'color': 'blue', 'size': 9} -> should be FILTERED OUT (color != red)
[send] miss-small    {'color': 'red', 'size': 1}  -> should be FILTERED OUT (size not > 2)
[send] match-2       {'color': 'red', 'size': 3}  -> should MATCH (color=red AND size>2)
[send] miss-nocolor  {'size': 8}                  -> should be FILTERED OUT (color IS NULL -> UNKNOWN (3-valued))
[recv] delivered: match-1
[recv] delivered: match-2
[recv] Received exactly 2 matching event(s); 3 non-matching event(s) were silently withheld

[gotcha] Selector on queues/amqp10.examples.selector.q correctly REJECTED at ATTACH:
         Connection amqp://localhost:5672 disconnected: Condition('amqp:invalid-field', 'no such handle: 0')
         (selectors are supported only on events/ and events-store/ -- queues/ is move-only)

Done.
```

## What's Happening

1. **SUBSCRIBE FIRST with the selector** — a custom `ReceiverOption`
   (`SelectorFilter`) sets the source filter:
   ```python
   receiver.source.filter.put_dict({
       symbol("apache.org:selector-filter:string"):
           Described(symbol("apache.org:selector-filter:string"), selector)
   })
   ```
   The filter-set **key** is the OASIS descriptor symbol itself (this is the key
   the connector reads in `sourceSelectorText`, and the one go-amqp's
   `NewSelectorFilter` emits). A successful `create_receiver` means the connector
   accepted and echoed the filter.
2. **PUBLISH 5 events** with application properties (`AtMostOnce`, fire-and-forget).
3. **RECEIVE only matches** — exactly `match-1` and `match-2` are delivered; the
   three non-matching events never arrive (`miss-nocolor` is withheld because a
   missing property is **NULL** → the predicate is UNKNOWN under 3-valued logic).
4. **Gotcha demo** — a selector requested on a `queues/` source is refused; the
   attach never completes.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events/<ch>` | **settled** (`AtMostOnce`) | server-granted | none (fire-and-forget) | app-props `color`, `size` (selector inputs) | `Data` | the selector evaluates these app-props |
| receiver (KubeMQ → client) | source `events/<ch>`, **source filter** `apache.org:selector-filter:string` | pre-settled by connector | client-granted `credit=100` | none to settle | filter: SQL-92 selector | `Data` | only matching events delivered; non-matches silently withheld |

## Gotchas

> **Selectors are honoured ONLY on `events/` and `events-store/`.** Requesting a
> selector on a `queues/` source is rejected at ATTACH with `amqp:not-implemented`
> ("selector filter not supported on this address") — queues are move-only. proton
> commonly surfaces the refusal as a connection-level `amqp:invalid-field` /
> "no such handle" message (it races the DETACH), but either way the link never
> attaches.

- **3-valued logic.** A missing property evaluates to NULL, so the predicate is
  UNKNOWN (not true) and the event is withheld — `miss-nocolor` is filtered out
  even though it has no `color` to disqualify it.
- **Filter the connector ignores ⇒ no filtering.** The filter-set must be keyed by
  `apache.org:selector-filter:string` (or `jms-selector`). Keying it under any
  other symbol (e.g. a bare `selector`) means the connector applies no filter and
  every event is delivered.

## Related Examples

- [events/basic_pubsub](../basic_pubsub/) — unfiltered fan-out
- [events/consumer_group](../consumer_group/) — load-balance one stream across a group
- [events_store/durable_replay](../../events_store/durable_replay/) — selectors also work on `events-store/`

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
