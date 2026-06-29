# Selectors

## Concept

A **selector** is a server-side filter that delivers only the messages matching a JMS / SQL92
boolean expression. You set it as a link **source filter** on a consume link, using the AMQP filter
key **`apache.org:selector-filter:string`** (the `jms-selector` alias is also accepted). The
connector parses the expression and evaluates it against each message's properties; non-matching
messages are not delivered to your link.

Selectors are a **pub/sub-only** feature — they work on `events` and `events-store`, and they are
rejected on `queues`.

```
events/orders + selector "color = 'red' AND size > 2"
 publish {color:'red', size:5} ──▶ delivered
 publish {color:'blue', size:9} ──▶ withheld (color mismatch)
 publish {size:5} ──▶ withheld (color is NULL → UNKNOWN)
```

## Where Selectors Are (and Aren't) Honored

| Link | Selector behavior |
|------|-------------------|
| `events/<ch>` consume | **honored** — filter evaluated per message |
| `events-store/<ch>` consume | **honored** — filter evaluated per message (including on replay) |
| `queues/<ch>` consume | **rejected** → `DETACH(amqp:not-implemented)` ("selector filter not supported on this address") |
| any **sender** (produce) link | rejected — a selector is only meaningful on a consume link |

> **Gotcha #4 — selectors are rejected on `/queues/`.** A JMS app that attaches a selector to a
> queue consumer gets a clean `DETACH(amqp:not-implemented)`, **not** a silent no-op. This is
> deliberate: queues are move-only, so a server-side filter would silently *drop* (and
> receive-count-churn) every non-matching message it pulled. Selectors belong on pub/sub. If you
> need filtered work distribution, publish to distinct channels or filter on the consumer side.

The connector echoes your selector text back in the reply `ATTACH` (Qpid JMS requires this to trust
that the server is enforcing the filter).

## Three-Valued (SQL92) Logic

Selector evaluation uses **SQL92 three-valued logic**: every predicate is `TRUE`, `FALSE`, or
`UNKNOWN` (NULL). A message is delivered **only when the top-level result is `TRUE`** — both
`FALSE` and `UNKNOWN` **withhold** it.

This is the single most common selector surprise:

- A reference to a **property that is absent** evaluates to NULL → the comparison is `UNKNOWN` →
 the message is **withheld**. So `price > 100` does not match a message that has no `price` at
 all.
- A **type mismatch** in a comparison yields `UNKNOWN` (no match) — never an error.
- **Division by zero** yields `UNKNOWN` (no match) — never a panic.
- `NULL IN (...)` is `UNKNOWN`.

In short: if you are not sure a property is present and well-typed, the message is *withheld*, not
delivered. Use `IS NULL` / `IS NOT NULL` explicitly when you need to match on presence.

## What You Can Filter On

The selector evaluates against the message's standard AMQP properties and application-properties
(the JMS-mapped header fields — e.g. `JMSPriority` maps to `header.priority`). Selector matching is
case-sensitive on string values.

## Bounds

A selector is bounded at parse time and rejected with `amqp:invalid-field` if it exceeds either
limit:

- source text longer than **4 KiB** (`maxSelectorBytes`), or
- an expression that parses to more than **64 AST nodes** (`maxSelectorNodes`).

A malformed expression is also `amqp:invalid-field`, with the description naming the offending
token.

## Examples

| Language | Example |
|----------|---------|
| Go | [events/selector](../../examples/go/events/selector/) |
| Python | [events/selector](../../examples/python/events/selector/) |
| Java | [events/selector](../../examples/java/events/selector/) |
| C# | [events/selector](../../examples/csharp/events/selector/) |
| JavaScript / TS | [events/selector](../../examples/javascript/events/selector/) |
| Rust | [events/selector](../../examples/rust/events/selector/) |

See also [pub-sub.md](pub-sub.md), [../guides/addressing.md](../guides/addressing.md).

Grounding: , (,).
