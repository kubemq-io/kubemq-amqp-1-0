/**
 * Example: queues/settlement-modes (master-table variant #3)
 *
 * The two producer reliability tiers, side by side, against the KubeMQ AMQP 1.0
 * connector using the native rhea / rhea-promise client (NO KubeMQ SDK):
 *
 *   - PRE-SETTLED sender (snd_settle_mode:1, "settled"): at-MOST-once. Each
 *     TRANSFER is marked settled by the client, so the send returns WITHOUT
 *     waiting for a server DISPOSITION. Fast and fire-and-forget — if the broker
 *     drops the transfer (oversize, no capacity), the producer never learns.
 *     There is no redelivery and no delivery confirmation. We use the plain
 *     Sender here (its send() returns immediately); an AwaitableSender would hang
 *     waiting for a disposition that a pre-settled link never produces.
 *   - UNSETTLED sender (default): at-LEAST-once. Each AwaitableSender.send()
 *     resolves only after the connector returns an `accepted` DISPOSITION,
 *     confirming the broker stored the message. This is the variant #1 contract.
 *
 * On the consume side this example requests rcv_settle_mode:0 ("first" — the
 * only receiver settle-mode the connector supports): the server settles the
 * delivery on the first transfer. rcv-settle-mode=second is rejected by the
 * connector with a DETACH carrying amqp:not-implemented (see the README gotcha).
 *
 * Both senders' messages drain to the same consumer; the program proves no loss
 * on this happy path while explaining the reliability difference.
 *
 * Grounded in connector test TestQueuePreSettled
 * (connectors/amqp10/integration_test.go).
 *
 * Run:
 *   export KUBEMQ_AMQP_URL=amqp://localhost:5672
 *   npx tsx queues/settlement-modes/index.ts
 */
import {
  Connection,
  ReceiverEvents,
  SenderEvents,
  type ConnectionOptions,
  type EventContext,
  type Receiver,
  type Sender,
} from "rhea-promise";

const channel = "amqp10.examples.settlement";

// We produce this many messages on each sender (pre-settled, then unsettled).
const perSender = 10;

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
      container_id: `kubemq-amqp10-js-settlement-${process.pid}`,
      reconnect: false,
    },
  };
}

function bodyToString(body: unknown): string {
  return Buffer.isBuffer(body) ? body.toString("utf8") : String(body);
}

/** Resolves once the sender has link credit to transmit. */
function waitSendable(sender: Sender, timeoutMs: number): Promise<void> {
  if (sender.sendable()) {
    return Promise.resolve();
  }
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      sender.removeListener(SenderEvents.sendable, onSendable);
      reject(new Error("timed out waiting for sender credit"));
    }, timeoutMs);
    const onSendable = (): void => {
      clearTimeout(timer);
      resolve();
    };
    sender.once(SenderEvents.sendable, onSendable);
  });
}

async function main(): Promise<void> {
  const { options, host, port } = connectionOptions();
  const address = `queues/${channel}`;
  console.log(`Broker:  amqp://${host}:${port}`);
  console.log(`Address: ${address}\n`);

  const connection = new Connection(options);
  await connection.open();

  try {
    // =======================================================================
    // 1. PRE-SETTLED sender (at-most-once). snd_settle_mode:1 marks every
    //    TRANSFER as already settled, so send() does NOT wait for a server
    //    DISPOSITION — it returns as soon as the frame is queued. Fast, but no
    //    delivery confirmation and no redelivery.
    // =======================================================================
    const settledSender = await connection.createSender({
      target: { address },
      snd_settle_mode: 1, // "settled" (pre-settled)
      autosettle: true,
    });
    for (let i = 0; i < perSender; i++) {
      const body = `presettled-${String(i).padStart(2, "0")}`;
      await waitSendable(settledSender, 15_000);
      settledSender.send({ body });
    }
    await settledSender.close();
    console.log(`[send] Pre-settled (at-most-once): produced ${perSender} messages — NO DISPOSITION awaited`);

    // =======================================================================
    // 2. UNSETTLED sender (at-least-once — the default). Each send() resolves
    //    only after the connector returns an `accepted` DISPOSITION confirming
    //    the broker stored the message. This is the variant #1 reliability
    //    contract.
    // =======================================================================
    const unsettledSender = await connection.createAwaitableSender({ target: { address } });
    for (let i = 0; i < perSender; i++) {
      const body = `unsettled-${String(i).padStart(2, "0")}`;
      await unsettledSender.send({ body }, { timeoutInSeconds: 15 });
    }
    await unsettledSender.close();
    console.log(`[send] Unsettled (at-least-once): produced ${perSender} messages — each accepted DISPOSITION`);

    // =======================================================================
    // 3. Consume with rcv_settle_mode:0 ("first"). This is the ONLY receiver
    //    settle-mode the connector supports — the server settles on the first
    //    transfer. (rcv-settle-mode=second => DETACH amqp:not-implemented; see
    //    the README gotcha.) Accept each message to drain the queue.
    // =======================================================================
    const receiver = await connection.createReceiver({
      source: { address },
      credit_window: 0,
      autoaccept: false,
      autosettle: false,
      rcv_settle_mode: 0, // "first" — the only supported mode
    });

    const total = 2 * perSender;
    let presettledSeen = 0;
    let unsettledSeen = 0;
    const seen = new Set<string>();
    await drainExactly(receiver, total, (ctx) => {
      const body = bodyToString(ctx.message?.body);
      ctx.delivery?.accept();
      if (!seen.has(body)) {
        seen.add(body);
        if (body.startsWith("presettled")) {
          presettledSeen++;
        } else {
          unsettledSeen++;
        }
      }
    }, 30_000);
    console.log(`[recv] Drained ${seen.size} total — ${presettledSeen} pre-settled + ${unsettledSeen} unsettled (rcv-settle-mode=first)`);

    // =======================================================================
    // 4. Assert the queue is empty — a further receive must time out.
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
 * Drains exactly `count` distinct deliveries, invoking `onMessage` for each. The
 * handler is attached before credit is issued. Standing credit equal to `count`
 * is granted up front.
 */
function drainExactly(
  receiver: Receiver,
  count: number,
  onMessage: (ctx: EventContext) => void,
  timeoutMs: number,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let received = 0;
    const timer = setTimeout(() => {
      receiver.removeListener(ReceiverEvents.message, handler);
      reject(new Error(`timed out after ${received}/${count} messages`));
    }, timeoutMs);

    const handler = (ctx: EventContext): void => {
      try {
        onMessage(ctx);
      } catch (err) {
        clearTimeout(timer);
        receiver.removeListener(ReceiverEvents.message, handler);
        reject(err instanceof Error ? err : new Error(String(err)));
        return;
      }
      received++;
      if (received >= count) {
        clearTimeout(timer);
        receiver.removeListener(ReceiverEvents.message, handler);
        resolve();
      }
    };

    receiver.on(ReceiverEvents.message, handler);
    receiver.addCredit(count);
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

// Expected output:
//
// Broker:  amqp://localhost:5672
// Address: queues/amqp10.examples.settlement
//
// [send] Pre-settled (at-most-once): produced 10 messages — NO DISPOSITION awaited
// [send] Unsettled (at-least-once): produced 10 messages — each accepted DISPOSITION
// [recv] Drained 20 total — 10 pre-settled + 10 unsettled (rcv-settle-mode=first)
// [recv] Queue drained to empty (no further messages)
//
// Done.
//
// (On a healthy broker pre-settled messages also drain — the difference is the
// PRODUCER guarantee, not the happy-path result: a pre-settled send returns
// before any broker confirmation, so a drop on the way in is invisible to the
// producer. Unsettled sends block until the broker confirms storage.)
