/**
 * Example: advanced/multi-frame-large-payload (master-table variant #11)
 *
 * A single AMQP 1.0 message whose body is larger than the connection's
 * max-frame-size is fragmented across multiple TRANSFER frames (more:true …
 * more:false) by the sender and reassembled bit-exact by the receiver — all
 * transparently, with NO application-level chunking. This example drives that path
 * against the KubeMQ AMQP 1.0 connector using the native rhea / rhea-promise client
 * (NO KubeMQ SDK).
 *
 * Flow:
 *   - Open with max_frame_size:4096 on BOTH the producer and consumer connections —
 *     a deliberately tiny 4 KiB frame so a ~1 MB body forces heavy fragmentation in
 *     both directions.
 *   - Sender → "queues/<ch>" (unsettled, AwaitableSender): one send() carries a
 *     ~1 MB Data body. rhea splits it across many transfer frames; the connector
 *     reassembles it and stores a single message.
 *   - Receiver ← "queues/<ch>" credit 1: one delivery yields the full body. The
 *     example verifies the received length AND a CRC32 of the bytes match the
 *     original — proving a bit-exact round-trip across the fragment boundary.
 *
 * Grounded in connector test TestQueueMultiFrameLargePayload
 * (connectors/amqp10/integration_test.go).
 *
 * Run:
 *   export KUBEMQ_AMQP_URL=amqp://localhost:5672
 *   npx tsx advanced/multi-frame-large-payload/index.ts
 */
import { crc32 } from "node:zlib";
import {
  Connection,
  ReceiverEvents,
  type ConnectionOptions,
  type EventContext,
  type Receiver,
} from "rhea-promise";

// channel is the KubeMQ queue channel; the link address is "queues/" + channel
// (explicit prefix — never rely on a default pattern).
const channel = "amqp10.examples.multiframe";

// payloadSize is ~1 MB — comfortably larger than maxFrameSize so the body must span
// many transfer frames (more:true … more:false).
const payloadSize = 1 * 1024 * 1024;
// maxFrameSize is a deliberately tiny 4 KiB so the ~1 MB body fragments across ~256
// frames in each direction.
const maxFrameSize = 4096;

function brokerEndpoint(): { host: string; port: number } {
  const raw = process.env["KUBEMQ_AMQP_URL"] ?? "amqp://localhost:5672";
  const url = new URL(raw);
  return { host: url.hostname, port: url.port ? Number(url.port) : 5672 };
}

function connectionOptions(suffix: string): ConnectionOptions {
  const { host, port } = brokerEndpoint();
  return {
    host,
    port,
    container_id: `kubemq-amqp10-js-multiframe-${suffix}-${process.pid}`,
    // A small max_frame_size forces multi-frame fragmentation; the connector
    // advertises its own max-frame-size in the OPEN reply and the effective value
    // is the minimum of the two.
    max_frame_size: maxFrameSize,
    reconnect: false,
  };
}

/** crc32hex returns the unsigned CRC32 of buf as an 8-char zero-padded hex string. */
function crc32hex(buf: Buffer): string {
  return (crc32(buf) >>> 0).toString(16).padStart(8, "0");
}

function toBuffer(body: unknown): Buffer {
  if (Buffer.isBuffer(body)) {
    return body;
  }
  // rhea may surface a Data-section body wrapped as { typecode, content } where
  // content is a Buffer (or a { type:"Buffer", data:[...] } shape); normalise both
  // to a Buffer for the CRC check.
  if (body && typeof body === "object" && "content" in body) {
    const content = (body as { content: unknown }).content;
    if (Buffer.isBuffer(content)) {
      return content;
    }
    if (content && typeof content === "object" && "data" in content && Array.isArray((content as { data: unknown }).data)) {
      return Buffer.from((content as { data: number[] }).data);
    }
  }
  if (typeof body === "string") {
    return Buffer.from(body, "binary");
  }
  throw new Error(`unexpected message body type: ${typeof body}`);
}

async function main(): Promise<void> {
  const { host, port } = brokerEndpoint();
  const address = `queues/${channel}`;
  console.log(`Broker:        amqp://${host}:${port}`);
  console.log(`Address:       ${address}  (KubeMQ pattern=queues, channel=${channel})`);
  console.log(`MaxFrameSize:  ${maxFrameSize} bytes`);
  console.log(`Payload:       ${payloadSize} bytes (~${payloadSize / 1024} KiB)\n`);

  // Build a deterministic, non-trivial payload and remember its CRC + length so we
  // can prove a bit-exact round-trip after reassembly.
  const payload = Buffer.allocUnsafe(payloadSize);
  for (let i = 0; i < payload.length; i++) {
    payload[i] = i % 251; // 251 is prime → no short repeating period
  }
  const wantLen = payload.length;
  const wantCRC = crc32hex(payload);
  console.log(`[prep] Built payload: len=${wantLen} crc32=0x${wantCRC}`);

  // =========================================================================
  // 1. PRODUCER connection — OPEN with a tiny max_frame_size. One send() carries
  //    the whole body; rhea transparently splits it across many transfer frames
  //    (more:true … final more:false). The connector reassembles them into a
  //    single stored message and returns an accepted DISPOSITION.
  // =========================================================================
  const prodConnection = new Connection(connectionOptions("prod"));
  await prodConnection.open();
  try {
    const sender = await prodConnection.createAwaitableSender({ target: { address } });
    await sender.send({ body: payload }, { timeoutInSeconds: 60 });
    await sender.close();
    const approxFrames = Math.floor(wantLen / maxFrameSize) + 1;
    console.log(`[send] Sent the ${wantLen}-byte body in ONE send (fragmented across ~${approxFrames} frames, accepted)`);

    // =======================================================================
    // 2. CONSUMER connection — same tiny max_frame_size so reassembly is exercised
    //    on the receive path too. One delivery yields the FULL reassembled body.
    // =======================================================================
    const consConnection = new Connection(connectionOptions("cons"));
    await consConnection.open();
    try {
      const receiver = await consConnection.createReceiver({
        source: { address },
        credit_window: 0,
        autoaccept: false,
        autosettle: false,
      });

      const received = await receiveOne(receiver, 60_000);
      await receiver.close();

      const got = toBuffer(received);
      const gotLen = got.length;
      const gotCRC = crc32hex(got);
      console.log(`[recv] Reassembled body: len=${gotLen} crc32=0x${gotCRC}`);

      // =====================================================================
      // 3. Verify the round-trip is bit-exact: length AND CRC32 must match.
      // =====================================================================
      if (gotLen !== wantLen) {
        throw new Error(`length mismatch: sent ${wantLen}, received ${gotLen}`);
      }
      if (gotCRC !== wantCRC) {
        throw new Error(`CRC mismatch: sent 0x${wantCRC}, received 0x${gotCRC}`);
      }
      console.log("[verify] Length and CRC32 match — multi-frame body round-tripped bit-exact");
    } finally {
      await consConnection.close();
    }
  } finally {
    await prodConnection.close();
  }

  console.log("\nDone.");
}

/**
 * Grants 1 credit, waits for one delivery, accepts it (AckRange ⇒ removed), and
 * returns the message body. The handler is registered before credit is granted.
 */
function receiveOne(receiver: Receiver, timeoutMs: number): Promise<unknown> {
  return new Promise<unknown>((resolve, reject) => {
    const timer = setTimeout(() => {
      receiver.removeListener(ReceiverEvents.message, handler);
      reject(new Error("timed out waiting for the multi-frame message"));
    }, timeoutMs);

    const handler = (ctx: EventContext): void => {
      clearTimeout(timer);
      receiver.removeListener(ReceiverEvents.message, handler);
      ctx.delivery?.accept(); // AckRange ⇒ removed from the queue
      resolve(ctx.message?.body);
    };

    receiver.on(ReceiverEvents.message, handler);
    receiver.addCredit(1);
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

// Expected output:
//
// Broker:        amqp://localhost:5672
// Address:       queues/amqp10.examples.multiframe  (KubeMQ pattern=queues, channel=amqp10.examples.multiframe)
// MaxFrameSize:  4096 bytes
// Payload:       1048576 bytes (~1024 KiB)
//
// [prep] Built payload: len=1048576 crc32=0x........
// [send] Sent the 1048576-byte body in ONE send (fragmented across ~257 frames, accepted)
// [recv] Reassembled body: len=1048576 crc32=0x........
// [verify] Length and CRC32 match — multi-frame body round-tripped bit-exact
//
// Done.
