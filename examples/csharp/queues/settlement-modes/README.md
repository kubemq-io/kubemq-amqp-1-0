# C# — Queues / Settlement Modes

The two producer reliability tiers, side by side, over KubeMQ **Queues** with the
native `AMQPNetLite.Core` client: a **pre-settled** sender (at-most-once,
fire-and-forget) and an **unsettled** sender (at-least-once, blocks for the
broker's `accepted` confirmation). Both drain to the same consumer, which uses
the only receiver settle-mode the connector supports: `first`.

## Prerequisites

- .NET SDK **8.0**
- `AMQPNetLite.Core` **2.5.3** (pinned in `examples/csharp/Directory.Packages.props`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd queues/settlement-modes
dotnet run
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 dotnet run
```

## Expected Output

```
Broker:  amqp://localhost:5672
Address: queues/amqp10.examples.settlement

[send] Pre-settled (at-most-once): produced 10 messages — NO DISPOSITION awaited
[send] Unsettled (at-least-once): produced 10 messages — each accepted DISPOSITION
[recv] Drained 20 total — 10 pre-settled + 10 unsettled (rcv-settle-mode=first)
[recv] Queue drained to empty (no further messages)

Done.
```

## What's Happening

1. **OPEN / BEGIN** — connect (ANONYMOUS) and open one session.
2. **ATTACH (pre-settled sender)** — built from an `Attach` frame with
   `SndSettleMode = SenderSettleMode.Settled`. Every TRANSFER is marked settled
   by the client, so `Send` returns **without** waiting for a server DISPOSITION
   (at-most-once). Fast, but no delivery confirmation and no redelivery.
3. **ATTACH (unsettled sender)** — the default plain `SenderLink`. Each `Send`
   blocks until the connector returns an `accepted` DISPOSITION confirming the
   broker stored the message (at-least-once — the variant #1 contract).
4. **ATTACH (receiver)** — built from an `Attach` frame with
   `RcvSettleMode = ReceiverSettleMode.First`. This is the **only** receiver
   settle-mode the connector supports — the server settles the delivery on the
   first transfer.
5. **TRANSFER (consume)** — both senders' messages drain to the same consumer;
   `Accept` each. The program counts 10 pre-settled + 10 unsettled = 20 total.
6. **Drain check** — a final `Receive` returns `null` (queue empty).
7. **DETACH / CLOSE** — the links detach and the connection closes.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| pre-settled sender | target `queues/<ch>` | `SndSettleMode = Settled` | server-granted | none (settled at source) | none | `Data` | at-most-once: `Send` returns before any broker confirmation |
| unsettled sender | target `queues/<ch>` | unsettled (default) | server-granted | `accepted` DISPOSITION per send | none | `Data` | at-least-once: `Send` blocks for storage confirm |
| receiver | source `queues/<ch>` | `RcvSettleMode = First` | client-granted `SetCredit(20)` | `Accept` ⇒ AckRange (removed) | none | `Data` | `first` is the only supported receiver settle-mode |

## Gotcha

> **`rcv-settle-mode=second` is not supported.** Requesting
> `ReceiverSettleMode.Second` on a consume link is rejected by the connector with
> a DETACH carrying `amqp:not-implemented`. Always use `First` (the default and
> the only accepted value).

On a healthy broker pre-settled messages also drain — the difference is the
**producer** guarantee, not the happy-path result. A pre-settled `Send` returns
before any broker confirmation, so a drop on the way in is invisible to the
producer; an unsettled `Send` blocks until the broker confirms storage.

## Related Examples

- [queues/basic-send-receive](../basic-send-receive/) — the unsettled at-least-once flow
- [queues/ack-release-redelivery](../ack-release-redelivery/) — accept / release / reject
- [events/basic-pubsub](../../events/basic-pubsub/) — pre-settled at-most-once on Events

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
