# C# — Events / Consumer Group

Consumer-group load-balancing over KubeMQ **Events** with the native
`AMQPNetLite.Core` client. The `x-opt-kubemq-group` link property places a
subscriber in a named group: members of **one** group split the stream (no
duplication), while a **distinct** group is an independent subscriber that gets
the full stream.

## Prerequisites

- .NET SDK **8.0**
- `AMQPNetLite.Core` **2.5.3** (pinned in `examples/csharp/Directory.Packages.props`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd events/consumer-group
dotnet run
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 dotnet run
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: events/amqp10.examples.consumergroup  (KubeMQ pattern=events, channel=amqp10.examples.consumergroup)

[recv] 3 subscribers attached: g1a+g1b (group g1), g2 (group g2)
[send] Published 30 events (pre-settled)
[recv] g2 (group g2, independent): 30/30 events — FULL stream
[recv] g1a (group g1): 16 events; g1b (group g1): 14 events
[recv] g1a+g1b together: 30/30 events, 0 duplicates — group SPLIT the stream

Done.
```

(The g1a/g1b split varies run to run; the totals are fixed: g1a+g1b together =
30 with 0 duplicates, g2 = 30.)

## What's Happening

1. **OPEN** — connect (ANONYMOUS).
2. **ATTACH three group receivers FIRST**, each on **its own session** (AMQPNetLite
   sessions/links are not concurrency-safe, so each link gets a dedicated
   session). The group is set on the ATTACH frame's link `Properties` map
   (`Fields` keyed by `Symbol("x-opt-kubemq-group")`):
   - `g1a`, `g1b` → group `g1`
   - `g2` → group `g2`
   Each grants standing credit (`SetCredit(200, autoRestore: true)`).
3. **Pump settle** — wait ~750ms so all subscription pumps go live (Events have
   no replay).
4. **ATTACH (sender)** — a dedicated session + pre-settled sender publishes 30
   events to `events/<ch>`.
5. **TRANSFER (receive)** — each receiver is drained within an idle window; a
   `Receive` that returns `null` (no more events before the timeout) ends that
   drain.
6. **Assertions** — `g2` (distinct group) receives all 30; `g1a` + `g1b`
   together receive all 30 with **zero** bodies delivered to both (the group
   round-robins the stream).
7. **CLOSE** — all links detach and the connection closes.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `events/<ch>` | `SndSettleMode = Settled` | server-granted | none (pre-settled) | none | `Data` | at-most-once fan-out |
| receiver g1a / g1b | source `events/<ch>` | `first` (default) | client standing credit `SetCredit(200, autoRestore)` | `Accept` no-op (pre-settled) | link prop `x-opt-kubemq-group: g1` | `Data` | members of `g1` split the stream (no dup) |
| receiver g2 | source `events/<ch>` | `first` (default) | client standing credit `SetCredit(200, autoRestore)` | `Accept` no-op (pre-settled) | link prop `x-opt-kubemq-group: g2` | `Data` | distinct group ⇒ independent full stream |

## Gotcha

> **0-credit ⇒ silent drop applies per member.** As with all Events consume,
> each group member must keep standing credit; an event delivered to a member at
> 0 credit is dropped silently (and counted on
> `kubemq_amqp10_events_dropped_no_credit_total`). Subscribe-before-publish and
> standing credit avoid the loss. Also note AMQPNetLite sessions/links are not
> concurrency-safe — give each receiver its own session.

## Related Examples

- [events/basic-pubsub](../basic-pubsub/) — single-subscriber fan-out
- [events/selector](../selector/) — JMS/SQL-92 message selectors
- [queues/basic-send-receive](../../queues/basic-send-receive/) — competing consumers on Queues (at-least-once)

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
