# JavaScript — Advanced / Multi-Frame Large Payload

A single ~1 MB AMQP 1.0 message sent over a connection with a deliberately tiny
4 KiB `max_frame_size`, so its body is **fragmented across many TRANSFER frames**
(`more:true` … `more:false`) by the sender and **reassembled bit-exact** by the
receiver — transparently, with no application-level chunking. Uses the native
`rhea` / `rhea-promise` client over KubeMQ **Queues**.

The example verifies the round-trip by comparing the received length **and** a
CRC32 of the bytes against the original.

## Prerequisites

- Node.js 20+ (developed against Node 26; uses `zlib.crc32`, available on Node 18+).
- `rhea` 3.0.4 + `rhea-promise` 3.0.3 (pinned in `examples/javascript/package.json`);
  run via `tsx`.
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

Install once from `examples/javascript`:

```bash
npm install
```

## How to Run

```bash
cd examples/javascript
npx tsx advanced/multi-frame-large-payload/index.ts
```

Override the broker URL (SASL **ANONYMOUS** by default):

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 npx tsx advanced/multi-frame-large-payload/index.ts
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
[verify] Length and CRC32 match — multi-frame body round-tripped bit-exact

Done.
```

## What's Happening

1. **Prepare** — build a deterministic ~1 MB payload and record its length + CRC32
   so the round-trip can be proven bit-exact.
2. **OPEN (producer)** — `new Connection({max_frame_size: 4096})`. The connector
   advertises its own max-frame-size in the OPEN reply; rhea fragments transfers
   using the **smaller** of the two values.
3. **BEGIN / ATTACH (sender)** — `createAwaitableSender({target:{address:"queues/<ch>"}})`.
4. **TRANSFER (fragmented)** — a single `send({body: payload})` writes the body
   across many transfer frames: every frame except the last carries `more=true`;
   the last carries `more=false`. The connector stitches them back into one stored
   message and returns an `accepted` DISPOSITION (the `AwaitableSender` promise
   resolves on it).
5. **OPEN / BEGIN / ATTACH (consumer)** — a second connection, also with
   `max_frame_size:4096`, attaches a receiver with manual credit (`addCredit(1)`,
   handler registered first).
6. **TRANSFER (reassembled) / DISPOSITION** — one `message` event yields the
   **full** reassembled body; `delivery.accept()` settles it (AckRange, removed from
   the queue).
7. **Verify** — the received length and CRC32 are compared to the originals; any
   mismatch fails the program.
8. **DETACH / CLOSE** — links detach and both connections close.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `queues/<ch>` | unsettled (default) | server-granted | server emits `accepted` DISPOSITION after the final fragment | none | `Data` (~1 MB) | body fragmented across many TRANSFER frames (`more=true`…`more=false`) at `max_frame_size:4096` |
| receiver (KubeMQ → client) | source `queues/<ch>` | `first` (default) | client-granted (`addCredit(1)`) | `accept()` ⇒ AckRange (removed) | none | `Data` (~1 MB) | one `message` event yields the fully reassembled body |

## Gotchas

> **The connector caps a single message at `MaxMessageSize` = 100 MiB
> (104857600 bytes) by default.** Fragmentation lets a body exceed the per-frame
> limit, but the *total* message size is still bounded. A message one byte over the
> cap is refused with **`amqp:link:message-size-exceeded`** — the sender's `send()`
> rejects with that error (the link advertises its `max-message-size` in the reply
> ATTACH, readable via `sender.maxMessageSize`). Raise the limit with
> `CONNECTORS_AMQP10_MAXMESSAGESIZE` if you genuinely need larger messages.

- **No application-level chunking.** Multi-frame transfer is an AMQP 1.0 protocol
  feature handled entirely by the client and connector. You send one message; you
  receive one message. Do **not** hand-split payloads.
- **`max_frame_size` is per-connection, not per-message.** Both peers negotiate it
  at OPEN; the effective value is the minimum. A small frame size trades more frames
  (overhead) for lower per-frame memory.
- **The received body is a `Buffer`.** rhea surfaces a `Data` section as a Node
  `Buffer`; CRC and length checks run directly over those bytes.

## Related Examples

- [queues/basic-send-receive](../../queues/basic-send-receive/) — at-least-once produce + accept drain
- [queues/settlement-modes](../../queues/settlement-modes/) — pre-settled vs unsettled producers
- [advanced/anonymous-terminus](../anonymous-terminus/) — per-message routing by `to`

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
