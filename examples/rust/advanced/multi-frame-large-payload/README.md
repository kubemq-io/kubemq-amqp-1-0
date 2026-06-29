# Rust — Advanced / Multi-Frame Large Payload

A single AMQP 1.0 message whose body exceeds the connection's max-frame-size is
fragmented across many TRANSFER frames (`More:true … More:false`) by the sender and
reassembled bit-exact by the receiver — transparently, with NO application-level
chunking. This example drives that path with the native `fe2o3-amqp` client: a ~1 MB
body over a deliberately tiny 4 KiB frame, verified with CRC32 and length.

## Prerequisites

- Rust 1.94+ (stable) with `cargo`.
- `fe2o3-amqp` 0.15.1 + `fe2o3-amqp-types` 0.14.0 + `crc32fast` 1.5.0 (pinned exact in
  the workspace `examples/rust/Cargo.toml`).
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/rust
cargo run -p multi-frame-large-payload
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 cargo run -p multi-frame-large-payload
```

## Expected Output

```
Broker:       amqp://localhost:5672
Address:      queues/amqp10.examples.multiframe  (KubeMQ pattern=queues, channel=amqp10.examples.multiframe)
MaxFrameSize: 4096 bytes
Payload:      1048576 bytes (~1024 KiB)

[prep] Built payload: len=1048576 crc32=0x........
[send] Sent the 1048576-byte body in ONE send (fragmented across ~257 frames, accepted)
[recv] Reassembled body: len=1048576 crc32=0x........
[verify] Length and CRC32 match — multi-frame body round-tripped bit-exact

Done.
```

## What's Happening

1. **PREP** — build a deterministic ~1 MB payload (`byte = i % 251`, a prime so there
   is no short repeating period) and record its length + CRC32.
2. **OPEN (producer)** — `Connection::builder().max_frame_size(4096)` opens with a tiny
   4 KiB frame. The connector advertises its own max-frame-size in the OPEN reply;
   fe2o3-amqp uses the smaller of the two when fragmenting.
3. **ATTACH + TRANSFER (send)** — a sender on `queues/<ch>` (`Unsettled`) sends the
   whole body in ONE `send`. fe2o3-amqp splits it across ~257 transfer frames
   (`More:true … More:false`); the connector reassembles them into a single stored
   message and returns `Accepted`.
4. **OPEN (consumer)** — a second connection, same tiny max-frame-size, so reassembly
   is exercised on the receive path too.
5. **ATTACH + TRANSFER (receive)** — a receiver with `CreditMode::Auto(1)`; one `recv`
   yields the FULL reassembled body.
6. **VERIFY** — the received length AND CRC32 must equal the originals — proving a
   bit-exact round-trip across the fragment boundary.
7. **DETACH / CLOSE** — both connections are torn down cleanly.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `queues/<ch>` | `Unsettled` (explicit) | server-granted | `Accepted` after reassembly | none | `Data` | one `send` → many transfer frames (`More:true…false`) |
| receiver (KubeMQ → client) | source `queues/<ch>` | `First` (default) | client `CreditMode::Auto(1)` | `accept` ⇒ AckRange (removed) | none | `Data` | one `recv` → full reassembled body |

Both connections set `max_frame_size = 4096` so a ~1 MB body fragments across ~256
frames in each direction.

## Related Examples

- [queues/basic-send-receive](../../queues/basic-send-receive/) — the same queue produce/consume at a normal frame size
- [advanced/anonymous-terminus](../anonymous-terminus/) — per-message routing via `properties.to`

## Gotchas

> **The connector caps a single message at 100 MiB.** A body over the cap is refused
> with `amqp:link:message-size-exceeded` — there is no application chunking to work
> around it. Split very large payloads into multiple messages instead.

- **Fragmentation is transparent.** You never chunk in application code; one `send`
  and one `recv` move the whole body. The frame size only controls how many transfer
  frames the body is split into on the wire.
- **Both peers negotiate the frame size.** The effective max-frame-size is the smaller
  of the client's and the connector's advertised values.
- **Sender settle-mode must be explicit** (`Unsettled` here); `fe2o3-amqp` defaults to
  `mixed`, which the connector rejects at ATTACH (`amqp:not-implemented`).

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT see
`connectivity/auth` + `guides/authentication.md`.
