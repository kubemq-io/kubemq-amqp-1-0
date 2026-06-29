/**
 * Example: queues/ack-release-redelivery (master-table variant #2)
 *
 * The three queue settlement outcomes, side by side, against the KubeMQ AMQP 1.0
 * connector using the native rhea / rhea-promise client (NO KubeMQ SDK):
 *
 *   - release (delivery.release()) => NAckRange: the message is requeued to the
 *     tail and REDELIVERED with a grown delivery-count (delivery_count >= 1) and
 *     first_acquirer no longer true. Each release also increments the broker
 *     receive-count toward MaxReceiveQueue (see the README gotcha).
 *   - reject  (delivery.reject(error)) => AckRange/discard: the message is
 *     removed and NOT redelivered to this receiver (poison handling is a broker
 *     MaxReceiveQueue policy — there is no connector DLX).
 *   - accept  (delivery.accept()) => AckRange: the message is removed (success).
 *
 * Grounded in connector tests TestQueueReleasedRedelivery and
 * TestQueueRejectedDiscard (connectors/amqp10/integration_test.go).
 *
 * Run:
 *   export KUBEMQ_AMQP_URL=amqp://localhost:5672
 *   npx tsx queues/ack-release-redelivery/index.ts
 */
import {
  Connection,
  ReceiverEvents,
  type ConnectionOptions,
  type EventContext,
  type Receiver,
} from "rhea-promise";

// A per-run-unique suffix keeps the redelivery / delivery-count assertions
// deterministic: this example releases a message (which requeues it) and reads
// its grown delivery-count, so a leftover copy from a previous interrupted run
// on a shared channel would skew the counts. A fresh channel per run avoids that
// without any cross-run cleanup.
const channel = `amqp10.examples.ack.${process.pid}`;

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
      container_id: `kubemq-amqp10-js-ack-${process.pid}`,
      reconnect: false,
    },
  };
}

function bodyToString(body: unknown): string {
  return Buffer.isBuffer(body) ? body.toString("utf8") : String(body);
}

/**
 * Extracts the AMQP header delivery-count and first-acquirer flag. The connector
 * maps the KubeMQ broker receive-count onto these header fields:
 * delivery-count = ReceiveCount-1, first-acquirer = (ReceiveCount==1). rhea omits
 * first_acquirer on a redelivery, so absent/false both mean "not first".
 */
function deliveryInfo(ctx: EventContext): { deliveryCount: number; firstAcquirer: boolean } {
  const deliveryCount = ctx.message?.delivery_count ?? 0;
  const firstAcquirer = ctx.message?.first_acquirer === true;
  return { deliveryCount, firstAcquirer };
}

async function main(): Promise<void> {
  const { options, host, port } = connectionOptions();
  const address = `queues/${channel}`;
  console.log(`Broker:  amqp://${host}:${port}`);
  console.log(`Address: ${address}\n`);

  const connection = new Connection(options);
  await connection.open();

  try {
    // Produce three distinct messages: one we release, one we reject, one we accept.
    const sender = await connection.createAwaitableSender({ target: { address } });
    for (const body of ["release-me", "reject-me", "accept-me"]) {
      await sender.send({ body }, { timeoutInSeconds: 15 });
    }
    await sender.close();
    console.log("[send] Produced: release-me, reject-me, accept-me");

    // Consume with manual credit + manual settlement. The handler is registered
    // BEFORE addCredit so no delivery is missed.
    const receiver = await connection.createReceiver({
      source: { address },
      credit_window: 0,
      autoaccept: false,
      autosettle: false,
    });

    // Track which terminal outcome we still owe each body. A released message is
    // redelivered, so "release-me" appears twice (released, then accepted).
    const remaining = new Set<string>(["release-me", "reject-me", "accept-me"]);
    let releasedOnce = false;

    await consume(receiver, (ctx) => {
      const body = bodyToString(ctx.message?.body);
      const { deliveryCount, firstAcquirer } = deliveryInfo(ctx);
      const pad = body.padEnd(12);

      if (body === "release-me" && !releasedOnce) {
        // First sight: RELEASE it back to the queue tail (NAckRange).
        ctx.delivery?.release();
        releasedOnce = true;
        console.log(`[recv] ${pad} delivery-count=${deliveryCount} first-acquirer=${firstAcquirer}  -> RELEASED (requeued)`);
      } else if (body === "release-me") {
        // Redelivery: grown delivery-count, no longer first-acquirer. Accept it now.
        if (deliveryCount < 1 || firstAcquirer) {
          throw new Error(
            `expected redelivered copy to have delivery-count>=1 and first-acquirer=false, got dc=${deliveryCount} first=${firstAcquirer}`,
          );
        }
        ctx.delivery?.accept();
        console.log(`[recv] ${pad} delivery-count=${deliveryCount} first-acquirer=${firstAcquirer} -> REDELIVERED, then ACCEPTED`);
        remaining.delete(body);
      } else if (body === "reject-me") {
        // REJECT it (AckRange/discard). It will NOT be redelivered here.
        ctx.delivery?.reject({ condition: "amqp:internal-error", description: "example rejection" });
        console.log(`[recv] ${pad} delivery-count=${deliveryCount} first-acquirer=${firstAcquirer}  -> REJECTED (discarded, no requeue)`);
        remaining.delete(body);
      } else {
        // "accept-me"
        ctx.delivery?.accept();
        console.log(`[recv] ${pad} delivery-count=${deliveryCount} first-acquirer=${firstAcquirer}  -> ACCEPTED (removed)`);
        remaining.delete(body);
      }
      return remaining.size === 0;
    }, 30_000);

    // The rejected body must NOT come back to this receiver.
    const noRedelivery = await expectNoMessage(receiver, 2_000);
    if (!noRedelivery) {
      throw new Error("rejected message was unexpectedly redelivered");
    }
    console.log("[recv] Rejected message was not redelivered (discarded)");

    await receiver.close();
  } finally {
    await connection.close();
  }

  console.log("\nDone.");
}

/**
 * Drains messages, invoking `onMessage` per delivery until it returns true. The
 * handler is attached before credit is granted. A generous standing credit is
 * issued so a redelivered message is delivered without a manual top-up.
 */
function consume(
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
    receiver.addCredit(10);
  });
}

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

// Expected output (the channel carries a per-run process-id suffix):
//
// Broker:  amqp://localhost:5672
// Address: queues/amqp10.examples.ack.<pid>
//
// [send] Produced: release-me, reject-me, accept-me
// [recv] release-me   delivery-count=0 first-acquirer=true  -> RELEASED (requeued)
// [recv] reject-me    delivery-count=0 first-acquirer=true  -> REJECTED (discarded, no requeue)
// [recv] accept-me    delivery-count=0 first-acquirer=true  -> ACCEPTED (removed)
// [recv] release-me   delivery-count=1 first-acquirer=false -> REDELIVERED, then ACCEPTED
// [recv] Rejected message was not redelivered (discarded)
//
// Done.
//
// (Delivery order between the original and the redelivered copy can vary; the
// redelivered "release-me" always carries delivery-count>=1 / first-acquirer=false.)
