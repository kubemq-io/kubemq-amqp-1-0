# Go — Queues / Settlement Modes

Producer reliability tiers side by side over KubeMQ **Queues** with the native
`github.com/Azure/go-amqp` client:

- **Pre-settled sender** (`SenderSettleModeSettled`) — **at-most-once**. Each
  `Send` returns immediately without waiting for a server DISPOSITION. No
  delivery confirmation, no redelivery; a drop on the way in is invisible to the
  producer.
- **Unsettled sender** (the default) — **at-least-once**. Each `Send` blocks
  until the connector returns an `accepted` DISPOSITION confirming the broker
  stored the message (the variant #1 contract).

The consumer requests `ReceiverSettleModeFirst` — the only receiver settle-mode
the connector supports. Both senders' messages drain to the same consumer.

## Prerequisites

- Go 1.24+
- `github.com/Azure/go-amqp` v1.7.0 (pinned in `examples/go/go.mod`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/go
go run ./queues/settlement-modes
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 go run ./queues/settlement-modes
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

On a healthy broker pre-settled messages also drain — the difference is the
**producer guarantee**, not the happy-path result. A pre-settled `Send` returns
before any broker confirmation, so a drop on the way in (oversize / no capacity)
is invisible to the producer; an unsettled `Send` blocks until the broker
confirms storage.

## What's Happening

1. **OPEN / BEGIN** — `amqp.Dial` (SASL ANONYMOUS by default) then `NewSession`.
2. **ATTACH (pre-settled sender)** — `NewSender("queues/<ch>",
   {SettlementMode: SenderSettleModeSettled.Ptr()})` attaches a link whose
   sender-settle-mode is `settled`. The client marks every TRANSFER as already
   settled.
3. **TRANSFER (pre-settled)** — each `Send` writes the frame and returns
   **without** waiting for a DISPOSITION. There is no `accepted`/`rejected`
   feedback loop — at-most-once.
4. **ATTACH (unsettled sender)** — `NewSender("queues/<ch>", nil)` uses the
   default (`unsettled`). Each `Send` blocks until the connector's `accepted`
   DISPOSITION — at-least-once.
5. **ATTACH (receiver)** — `NewReceiver("queues/<ch>", {Credit:20,
   SettlementMode: ReceiverSettleModeFirst.Ptr()})`. `first` means the server
   settles the delivery on the first transfer (the only mode the connector
   supports).
6. **TRANSFER / DISPOSITION (consume)** — `Receive` + `AcceptMessage` drains
   every message (both tiers land in the same queue) and removes it (AckRange).
7. **Drain check** — a final short-deadline `Receive` times out, proving the
   queue is empty.
8. **DETACH / CLOSE** — links detach and the connection closes.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender — pre-settled (client → KubeMQ) | target `queues/<ch>` | `settled` (`SenderSettleModeSettled`) | server-granted | NONE — no DISPOSITION awaited | none | `Data` | at-most-once produce (fire-and-forget) |
| sender — unsettled (client → KubeMQ) | target `queues/<ch>` | `unsettled` (default) | server-granted | server emits `accepted` DISPOSITION per send | none | `Data` | at-least-once produce |
| receiver (KubeMQ → client) | source `queues/<ch>` | `first` (`ReceiverSettleModeFirst`) | client-granted `Credit:20` | `AcceptMessage` ⇒ AckRange (removed) | none | `Data` | `second` rejected (see gotcha) |

## Gotchas

> **`rcv-settle-mode=second` is not implemented.** The connector supports
> sender-settle `unsettled`/`settled` and receiver-settle **`first` only**. A
> receiver that requests `ReceiverSettleModeSecond` (two-phase settlement, where
> the consumer settles only after the server settles) is refused at attach: the
> connector emits a **DETACH carrying `amqp:not-implemented`** before the link is
> ever constructed. Always attach receivers with `ReceiverSettleModeFirst` (the
> default) — `second` will fail the link.

- **Pre-settled has no redelivery.** Because there is no DISPOSITION feedback, a
  pre-settled producer cannot be told the message was lost and cannot trigger a
  resend. Use pre-settled only for data where loss is acceptable (metrics,
  telemetry); use unsettled for anything that must not be lost.
- **`kubemq_amqp10_transfers_in_dropped_total`** counts inbound transfers
  dropped (oversize / no-consumer / pre-settled failure). A pre-settled drop
  shows up only on this server metric — never as a client-side error.

## Related Examples

- [queues/basic-send-receive](../basic-send-receive/) — at-least-once produce + accept drain
- [queues/ack-release-redelivery](../ack-release-redelivery/) — `accept` vs `release` vs `reject`
- [events/basic-pubsub](../../events/basic-pubsub/) — fan-out at-most-once (Events)

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
