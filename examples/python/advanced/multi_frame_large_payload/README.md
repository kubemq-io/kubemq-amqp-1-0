# Python — Advanced / Multi-Frame Large Payload

A single AMQP 1.0 message whose body is larger than the connection's
**max-frame-size** is fragmented across multiple `TRANSFER` frames
(`more=true … more=false`) by the sender and reassembled **bit-exact** by the
receiver — transparently, with **no application-level chunking**. Driven with the
native `python-qpid-proton` blocking client.

This example forces heavy fragmentation by pinning a deliberately tiny 4 KiB
max-frame-size while sending a ~1 MB body, then verifies the round-trip with a
CRC32 + length check.

## Prerequisites

- Python 3.10+
- `python-qpid-proton` 0.40.0 (pinned in `examples/python/pyproject.toml`; the
  prebuilt `python-qpid-proton-wheel` installs without a C toolchain)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/python
uv sync
uv run python advanced/multi_frame_large_payload/main.py
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 uv run python advanced/multi_frame_large_payload/main.py
```

## Expected Output

```
Broker:        amqp://localhost:5672
Address:       queues/amqp10.examples.multiframe  (KubeMQ pattern=queues, channel=amqp10.examples.multiframe)
MaxFrameSize:  4096 bytes
Payload:       1048576 bytes (~1024 KiB)

[prep] Built payload: len=1048576 crc32=0x........
[send] Sent the 1048576-byte body in ONE send (fragmented across ~257 frames, accepted)
[recv] Reassembled body: len=1048576 crc32=0x........
[verify] Length and CRC32 match -- multi-frame body round-tripped bit-exact

Done.
```

(The `crc32` value is deterministic for the fixed payload; both lines print the
same hex value.)

## What's Happening

1. **OPEN with a tiny max-frame-size** — `BlockingConnection(url,
   max_frame_size=4096)` forwards `max_frame_size` to `Container.connect()` on both
   the producer and consumer connections. The connector advertises its own
   max-frame-size in the OPEN reply; proton fragments using the **smaller** of the
   two.
2. **ATTACH + one send** — `sender.send(Message(body=<1 MB bytes>))`. proton
   splits the body across ~257 `TRANSFER` frames (`more=true` … final
   `more=false`); the connector reassembles them into a **single** stored message.
3. **ATTACH receiver (`credit=1`) + one receive** — `receiver.receive()` yields
   the **full** reassembled body in a single `Message`; the example accepts it.
4. **Verify** — the received length and CRC32 must equal the originals, proving a
   bit-exact round-trip across the fragment boundary.
5. **DETACH / CLOSE** — both connections close.

Frame-level: `OPEN(max-frame=4096) → BEGIN → ATTACH → TRANSFER(more) × N →
DISPOSITION(accept) → DETACH/CLOSE`.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `queues/<ch>` | unsettled (default) | server-granted | `accepted` (one disposition for the whole message) | none | `Data` (~1 MB) | one logical message spans many `TRANSFER` frames (`more` flag) |
| receiver (KubeMQ → client) | source `queues/<ch>` | `first` (default) | client-granted `credit=1` | `accepted` | none | `Data` (~1 MB) | reassembled bit-exact; max-frame-size = 4096 on the transport |

## Gotchas

> **The connector enforces a 100 MiB message-size cap.** A body that exceeds it is
> rejected with **`amqp:link:message-size-exceeded`** (the link is detached). The
> ~1 MB body here is well within the cap; fragmentation is about frame size, not
> the cap.

- **Frame size ≠ message size.** A tiny `max_frame_size` only controls how the
  one logical message is *chopped into frames*; the message itself is still a
  single accepted delivery.
- **Set max-frame-size at connect time.** Pass `max_frame_size=` to
  `BlockingConnection` (it is forwarded to `Container.connect()`); it must be in
  place before the OPEN is sent.

## Related Examples

- [queues/basic_send_receive](../../queues/basic_send_receive/) — the plain at-least-once queue round-trip (default frame size)
- [queues/settlement_modes](../../queues/settlement_modes/) — unsettled vs pre-settled delivery
- [advanced/anonymous_terminus](../anonymous_terminus/) — per-message routing via the null-target sender

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
