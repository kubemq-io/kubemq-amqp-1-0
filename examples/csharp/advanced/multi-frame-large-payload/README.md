# C# — Advanced / Multi-Frame Large Payload

A single AMQP 1.0 message whose body is larger than the connection's
**max-frame-size** is fragmented across multiple TRANSFER frames
(`More:true` … `More:false`) by the sender and reassembled **bit-exact** by the
receiver — transparently, with **no application-level chunking**. This example uses a
deliberately tiny 4 KiB frame and a ~1 MB body so the body spans ~257 frames, then
verifies length + CRC32 on the way back. Native `AMQPNetLite.Core`; NO KubeMQ SDK.

## Prerequisites

- .NET SDK **8.0**
- `AMQPNetLite.Core` **2.5.3** (pinned in `examples/csharp/Directory.Packages.props`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd advanced/multi-frame-large-payload
dotnet run
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 dotnet run
```

Runs ANONYMOUS by default (no userinfo in the URL).

## Expected Output

```
Broker:        amqp://localhost:5672
Address:       queues/amqp10.examples.multiframe  (KubeMQ pattern=queues, channel=amqp10.examples.multiframe)
MaxFrameSize:  4096 bytes
Payload:       1048576 bytes (~1024 KiB)

[prep] Built payload: len=1048576 crc32=0x........
[send] Sent the 1048576-byte body in ONE Send (fragmented across ~257 frames, accepted)
[recv] Reassembled body: len=1048576 crc32=0x........
[verify] Length and CRC32 match — multi-frame body round-tripped bit-exact

Done.
```

(`0x........` is the run's CRC32 — the sent and received values are identical.)

## What's Happening

1. **prep** — build a deterministic ~1 MB payload (`byte = i % 251`) and record its
   length + CRC32.
2. **OPEN (tiny frame)** — both the producer and consumer connect via a
   `ConnectionFactory` whose `AMQP.MaxFrameSize = 4096`. The connector advertises its
   own max-frame-size in the OPEN reply; AMQPNetLite fragments transfers using the
   **smaller** of the two.
3. **ATTACH (sender)** — a `SenderLink` to `queues/<ch>`.
4. **TRANSFER (fragmented send)** — one `Send` carries the whole ~1 MB `Data` body.
   AMQPNetLite splits it across ~257 transfer frames (`More:true` … final
   `More:false`); the connector reassembles them into a **single** stored message and
   returns one `accepted` DISPOSITION.
5. **ATTACH (receiver) + FLOW** — a `ReceiverLink` with `SetCredit(1)` on the second
   tiny-frame connection. One `Receive` yields the **full reassembled body** (the
   receive path exercises reassembly too).
6. **verify** — the received length AND CRC32 must equal the originals — proving a
   bit-exact round-trip across the fragment boundary.
7. **DETACH / CLOSE** — links detach; both connections close.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `queues/<ch>` | unsettled (default) | server-granted | one `accepted` for the whole reassembled message | conn: `AMQP.MaxFrameSize=4096` | `Data` (~1 MB) | one `Send` ⇒ ~257 `More:true…More:false` transfer frames |
| receiver (KubeMQ → client) | source `queues/<ch>` | `first` (default) | client-granted `SetCredit(1)` | `Accept` ⇒ AckRange (removed) | conn: `AMQP.MaxFrameSize=4096` | `Data` (~1 MB) | one `Receive` yields the full reassembled body; CRC + length verified |

## Related Examples

- [queues/basic-send-receive](../../queues/basic-send-receive/) — the plain queue round-trip without fragmentation
- [queues/settlement-modes](../../queues/settlement-modes/) — unsettled vs pre-settled produce

## Gotcha

> **There is a hard ~100 MiB message-size cap.** Fragmentation is transparent up to
> the connector's maximum message size (~100 MiB). A body that exceeds the cap is
> rejected with `amqp:link:message-size-exceeded` — fragmenting it across more frames
> does not raise the ceiling. The frame size only controls how the bytes are split on
> the wire; the total message-size limit is independent. Send genuinely large blobs
> by reference (store the object elsewhere, send a pointer) rather than inline.

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT
see `connectivity/auth` + `guides/authentication.md`.
