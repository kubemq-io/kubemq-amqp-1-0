# Java — Advanced / Multi-Frame Large Payload

A single ~1 MB AMQP 1.0 message sent over a connection with a deliberately tiny
4 KiB max-frame-size, so its body is **fragmented across many TRANSFER frames**
(`More:true` … `More:false`) by the sender and **reassembled bit-exact** by the
receiver — transparently, with no application-level chunking. Uses **Apache Qpid
JMS** (`javax.jms`) over KubeMQ **Queues** — NO KubeMQ SDK.

The example verifies the round-trip by comparing the received length **and** a
CRC32 of the bytes against the original.

## Prerequisites

- Java 21+ and Maven 3.8+
- `org.apache.qpid:qpid-jms-client` **1.16.0** (the latest `javax.jms` line —
  pinned in the parent `examples/java/pom.xml`)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/java
mvn -pl advanced/multi-frame-large-payload exec:java
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 mvn -pl advanced/multi-frame-large-payload exec:java
```

The example appends `?amqp.maxFrameSize=4096` to whatever `KUBEMQ_AMQP_URL` you
provide, so the 4 KiB frame size is in effect regardless of the broker host.

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

1. **Prepare** — build a deterministic ~1 MB payload and record its length +
   CRC32 so the round-trip can be proven bit-exact.
2. **OPEN (producer)** — `JmsConnectionFactory("amqp://...?amqp.maxFrameSize=4096")`
   + `createConnection()`. The connector advertises its own max-frame-size at
   OPEN; Qpid JMS fragments transfers using the **smaller** of the two values.
3. **BEGIN / ATTACH (sender)** — `createSession` then `createProducer(queue)` on
   `queues/<ch>`.
4. **TRANSFER (fragmented)** — a single `producer.send(bytesMessage)` writes the
   body across many transfer frames: every frame except the last carries
   `more=true`; the last carries `more=false`. The connector stitches them back
   into one stored message and returns an `accepted` DISPOSITION.
5. **OPEN / BEGIN / ATTACH (consumer)** — a second connection, also with
   `amqp.maxFrameSize=4096`, attaches a `CLIENT_ACKNOWLEDGE` consumer.
6. **TRANSFER (reassembled) / DISPOSITION** — one `consumer.receive()` yields the
   **full** reassembled `BytesMessage`; `message.acknowledge()` settles it
   (AckRange, removed from the queue).
7. **Verify** — the received length and CRC32 are compared to the originals; any
   mismatch fails the program.
8. **DETACH / CLOSE** — try-with-resources closes the consumer, sessions, and both
   connections.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | target `queues/<ch>` | unsettled (default) | server-granted | server emits `accepted` DISPOSITION after the final fragment | none | `Data` (~1 MB) | body fragmented across many TRANSFER frames (`more=true`…`more=false`) at `amqp.maxFrameSize=4096` |
| receiver (KubeMQ → client) | source `queues/<ch>` | `first` (JMS default) | client-granted (Qpid JMS prefetch) | `message.acknowledge()` ⇒ AckRange (removed) | none | `Data` (~1 MB) | one `receive()` yields the fully reassembled body |

## Gotchas

> **The connector caps a single message at `MaxMessageSize` = 100 MiB
> (104857600 bytes) by default.** Fragmentation lets a body exceed the per-frame
> limit, but the *total* message size is still bounded. A message one byte over
> the cap is refused with **`amqp:link:message-size-exceeded`** — the sender's
> `send` surfaces that as a `JMSException`. Raise the limit with
> `CONNECTORS_AMQP10_MAXMESSAGESIZE` if you genuinely need larger messages.

- **No application-level chunking.** Multi-frame transfer is an AMQP 1.0 protocol
  feature handled entirely by Qpid JMS and the connector. You send one
  `BytesMessage`; you receive one `BytesMessage`. Do **not** hand-split payloads.
- **`amqp.maxFrameSize` is per-connection, not per-message.** Both peers
  negotiate it at OPEN; the effective value is the minimum. A small frame size
  trades more frames (overhead) for lower per-frame memory.

## Related Examples

- [queues/basic-send-receive](../../queues/basic-send-receive/) — at-least-once produce + accept drain
- [queues/settlement-modes](../../queues/settlement-modes/) — pre-settled vs unsettled producers
- [advanced/anonymous-terminus](../anonymous-terminus/) — per-message routing by `to` (Java N/A — see its README)

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ JWT
see `connectivity/auth` + `guides/authentication.md`.
