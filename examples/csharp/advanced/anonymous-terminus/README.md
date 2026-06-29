# C# — Advanced / Anonymous Terminus

An **anonymous sender** (a link attached with a **null target**) carries no fixed
channel. Each message selects its own destination via `Properties.To`, and the
KubeMQ connector routes it **per-message** to the right pattern/channel. One link,
many destinations. This example sends to a queue and an events topic over the SAME
anonymous link, consumes both back, then shows a bad `to` and a missing `to` rejected
with `amqp:precondition-failed`. Native `AMQPNetLite.Core`; NO KubeMQ SDK.

## Prerequisites

- .NET SDK **8.0**
- `AMQPNetLite.Core` **2.5.3** (pinned in `examples/csharp/Directory.Packages.props`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd advanced/anonymous-terminus
dotnet run
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 dotnet run
```

Runs ANONYMOUS by default (no userinfo in the URL).

## Expected Output

```
Broker: amqp://localhost:5672
Anonymous sender (null target) — routes per-message via Properties.To
  msg #1 to: queues/amqp10.examples.anon.q
  msg #2 to: events/amqp10.examples.anon.e

[attach] Anonymous sender attached (null target)
[send] msg #1 routed to queues/amqp10.examples.anon.q (accepted)
[send] msg #2 routed to events/amqp10.examples.anon.e (accepted)
[send] msg with bad `to`="bogus/prefix/x" rejected as expected: amqp:precondition-failed (unknown address prefix)
[send] msg with NO `to` rejected as expected: amqp:precondition-failed (anonymous terminus message has no `to`)
[recv] queue queues/amqp10.examples.anon.q delivered: "to-queue"
[recv] events events/amqp10.examples.anon.e delivered: "to-events"

Done.
```

## What's Happening

1. **OPEN / BEGIN** — connect and open a session.
2. **ATTACH (anonymous sender)** — a `SenderLink` built from an `Attach` whose
   `Target` is **null**. There is no bound channel; routing happens per-message.
3. **Subscribe first (events)** — events are fire-and-forget (no replay), so the
   events receiver attaches and settles BEFORE the publish. (The queue message is
   durable, so it is consumed after sending.)
4. **TRANSFER (send #1 → queue)** — `Message.Properties.To = "queues/<ch>"`. The
   connector resolves the prefix, authorizes WRITE for this connection (per-message
   Casbin — there is no per-link grant for an anonymous terminus), stores it, and
   returns `accepted`.
5. **TRANSFER (send #2 → events)** — the SAME anonymous link, `Properties.To =
   "events/<ch>"` — a DIFFERENT pattern. The subscriber receives it.
6. **Negative cases** — a bad prefix (`bogus/prefix/x`) and a missing `to` are both
   rejected by the connector with `amqp:precondition-failed`, surfaced as an
   `AmqpException` on `Send`. The anonymous link stays usable afterwards.
7. **Verify** — the queue message is consumed back and the event is received.
8. **DETACH / CLOSE** — links detach; the connection closes.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| anonymous sender (client → KubeMQ) | target **null** (anonymous terminus) | unsettled (default) | server-granted | `accepted` per resolved send; bad/missing `to` ⇒ `amqp:precondition-failed` | msg: `Properties.To` per message | `Data` | per-message routing + per-message Casbin WRITE |
| receiver (queue) | source `queues/<ch>` | `first` (default) | client-granted `SetCredit(1)` | `Accept` ⇒ AckRange (removed) | none | `Data` | verifies the queue send landed |
| receiver (events) | source `events/<ch>` | `first` (default) | client-granted `SetCredit(5)` | at-most-once (no replay) | none | `Data` | subscribed BEFORE the publish |

## Related Examples

- [commands/request-reply-dynamic-node](../../commands/request-reply-dynamic-node/) — the anonymous sender used for RPC replies (responder side)
- [queues/basic-send-receive](../../queues/basic-send-receive/) — a fixed-target queue sender (contrast)
- [events/basic-pubsub](../../events/basic-pubsub/) — a fixed-target events sender (contrast)

## Gotcha

> **The KubeMQ connector advertises NO `ANONYMOUS-RELAY` capability.** A client must
> emit a raw null-target ATTACH itself (as AMQPNetLite does here via `Target = null`)
> — the connector routes on `Properties.To` regardless of the missing capability.
> Higher-level JMS clients (e.g. Qpid JMS) that gate anonymous producers on the
> advertised capability fall back to per-destination links instead, which is why this
> variant is N/A for Java. Every anonymous message MUST carry a valid
> `<pattern>/<channel>` `to`; a bad prefix or a missing `to` is rejected with
> `amqp:precondition-failed` (the link survives — only that send fails).
>
> **AMQPNetLite interaction:** a connector REJECT settles the rejected delivery with
> an error on the anonymous sender's **session**, and reusing that same session for a
> subsequent `Receive` can stall delivery (and closing the disturbed link can throw
> while unwinding the rejected delivery). This example verifies routing on a **fresh
> session** and closes the disturbed links best-effort — an AMQPNetLite/connector
> interaction, not an AMQP 1.0 requirement.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT
see `connectivity/auth` + `guides/authentication.md`.
