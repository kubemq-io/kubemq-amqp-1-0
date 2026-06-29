/**
 * Example: queues/basic-send-receive (master-table variant #1)
 *
 * At-least-once produce + credit-based consume against the KubeMQ AMQP 1.0
 * connector using the native rhea / rhea-promise client (NO KubeMQ SDK).
 *
 * Flow:
 *   - AwaitableSender -> "queues/<ch>" (unsettled): each send() awaits the
 *     server's receiver DISPOSITION (accepted) before resolving.
 *   - Receiver <- "queues/<ch>" with manual credit (credit_window:0 +
 *     addCredit(10)): each message handler calls delivery.accept() => the
 *     connector emits an AckRange and removes the message from the queue.
 *   - After draining, the queue is empty (a further receive times out).
 *
 * Grounded in connector test TestQueueProduceConsumeAtLeastOnce
 * (connectors/amqp10/integration_test.go).
 *
 * Run:
 *   export KUBEMQ_AMQP_URL=amqp://localhost:5672
 *   npx tsx queues/basic-send-receive/index.ts
 */
import {
  Connection,
  ReceiverEvents,
  type ConnectionOptions,
  type EventContext,
  type Receiver,
} from "rhea-promise";

// channel is the KubeMQ queue channel; the link address is "queues/" + channel
// (explicit prefix — never rely on a default pattern).
const channel = "amqp10.examples.basic";

const total = 10;

/** Build SASL-ANONYMOUS connection options from KUBEMQ_AMQP_URL (default amqp://localhost:5672). */
function connectionOptions(): { options: ConnectionOptions; host: string; port: number } {
  const raw = process.env["KUBEMQ_AMQP_URL"] ?? "amqp://localhost:5672";
  const url = new URL(raw);
  const host = url.hostname;
  const port = url.port ? Number(url.port) : 5672;
  return {
    host,
    port,
    options: {
      host,
      port,
      // A non-empty container-id is required by the connector; rhea sends a
      // unique one by default, but we set an explicit, stable id for clarity.
      container_id: `kubemq-amqp10-js-basic-${process.pid}`,
      // Disable auto-reconnect so a deliberate close stays closed.
      reconnect: false,
    },
  };
}

function bodyToString(body: unknown): string {
  return Buffer.isBuffer(body) ? body.toString("utf8") : String(body);
}

async function main(): Promise<void> {
  const { options, host, port } = connectionOptions();
  const address = `queues/${channel}`;
  console.log(`Broker:  amqp://${host}:${port}`);
  console.log(`Address: ${address}  (KubeMQ pattern=queues, channel=${channel})\n`);

  // OPEN: connect (SASL ANONYMOUS by default — no username/password). The
  // connection carries the producer + consumer links below.
  const connection = new Connection(options);
  await connection.open();

  try {
    // =======================================================================
    // 1. Produce — attach an AwaitableSender (the server sees a receiver link).
    //    Each send() is unsettled and resolves only after the connector returns
    //    an `accepted` DISPOSITION, confirming the broker stored the message.
    // =======================================================================
    const sender = await connection.createAwaitableSender({
      target: { address },
    });
    for (let i = 0; i < total; i++) {
      const body = `msg-${String(i).padStart(3, "0")}`;
      await sender.send({ body }, { timeoutInSeconds: 15 });
    }
    await sender.close();
    console.log(`[send] Produced ${total} messages to ${address} (accepted DISPOSITION each)`);

    // =======================================================================
    // 2. Consume — attach a Receiver (the server sees a sender link). We grant
    //    credit manually (credit_window:0 + addCredit) and settle manually
    //    (autoaccept:false). The message handler MUST be registered BEFORE
    //    addCredit, or early deliveries are missed.
    // =======================================================================
    const receiver = await connection.createReceiver({
      source: { address },
      credit_window: 0,
      autoaccept: false,
      autosettle: false,
    });

    const seen = new Set<string>();
    await receiveUntil(receiver, (ctx) => {
      const body = bodyToString(ctx.message?.body);
      ctx.delivery?.accept(); // accept => AckRange => removed from the queue
      seen.add(body);
      return seen.size >= total;
    }, 30_000);
    console.log(`[recv] Consumed and accepted ${seen.size} messages (no loss)`);

    // =======================================================================
    // 3. Assert the queue is empty — a further receive must time out.
    // =======================================================================
    const drained = await expectNoMessage(receiver, 2_000);
    if (!drained) {
      throw new Error("expected an empty queue, but received another message");
    }
    console.log("[recv] Queue drained to empty (no further messages)");

    await receiver.close();
  } finally {
    await connection.close();
  }

  console.log("\nDone.");
}

/**
 * Grants 1 credit per outstanding message and invokes `onMessage` for each
 * delivery until it returns true. The handler is attached BEFORE credit is
 * issued so no early delivery is dropped. Rejects on timeout.
 */
function receiveUntil(
  receiver: Receiver,
  onMessage: (ctx: EventContext) => boolean,
  timeoutMs: number,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      receiver.removeListener(ReceiverEvents.message, handler);
      reject(new Error("timed out waiting for messages"));
    }, timeoutMs);

    const handler = (ctx: EventContext): void => {
      let done = false;
      try {
        done = onMessage(ctx);
      } catch (err) {
        clearTimeout(timer);
        receiver.removeListener(ReceiverEvents.message, handler);
        reject(err instanceof Error ? err : new Error(String(err)));
        return;
      }
      if (done) {
        clearTimeout(timer);
        receiver.removeListener(ReceiverEvents.message, handler);
        resolve();
      }
    };

    receiver.on(ReceiverEvents.message, handler);
    receiver.addCredit(total + 1);
  });
}

/**
 * Resolves true if NO message arrives within `timeoutMs` (queue drained), or
 * false if an unexpected message is delivered. Any such message is accepted to
 * keep the link clean.
 */
function expectNoMessage(receiver: Receiver, timeoutMs: number): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    const timer = setTimeout(() => {
      receiver.removeListener(ReceiverEvents.message, handler);
      resolve(true);
    }, timeoutMs);

    const handler = (ctx: EventContext): void => {
      clearTimeout(timer);
      receiver.removeListener(ReceiverEvents.message, handler);
      ctx.delivery?.accept();
      resolve(false);
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
// Broker:  amqp://localhost:5672
// Address: queues/amqp10.examples.basic  (KubeMQ pattern=queues, channel=amqp10.examples.basic)
//
// [send] Produced 10 messages to queues/amqp10.examples.basic (accepted DISPOSITION each)
// [recv] Consumed and accepted 10 messages (no loss)
// [recv] Queue drained to empty (no further messages)
//
// Done.
