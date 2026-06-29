/**
 * Example: events/basic-pubsub (master-table variant #4)
 *
 * Fan-out, at-most-once pub/sub over KubeMQ **Events** with the native
 * rhea / rhea-promise client (NO KubeMQ SDK).
 *
 * Events are a fire-hose: deliveries are pre-settled (no DISPOSITION feedback),
 * there is NO replay, and a message that arrives at a subscriber with zero
 * credit is SILENTLY DROPPED (counted by the server metric
 * kubemq_amqp10_events_dropped_no_credit_total). Two rules follow:
 *
 *   - SUBSCRIBE BEFORE PUBLISH. The attach reply only confirms the link, not
 *     that the connector's subscription pump is live. A publish that races the
 *     subscription is lost (no replay). This example waits ~750ms after attach
 *     before producing.
 *   - GRANT STANDING CREDIT. The receiver attaches with a large standing credit
 *     (replenished as messages settle) so the subscriber is never at 0 credit
 *     when an event arrives.
 *
 * The sender publishes pre-settled to events/<ch> (fire-and-forget); the
 * receiver drains every event on the happy path.
 *
 * Grounded in connector test TestEventsPubSubGroupFanout (the lone-subscriber
 * fan-out leg) (connectors/amqp10/integration_pubsub_test.go).
 *
 * Run:
 *   export KUBEMQ_AMQP_URL=amqp://localhost:5672
 *   npx tsx events/basic-pubsub/index.ts
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

const channel = "amqp10.examples.pubsub";

const total = 20;

// standingCredit is granted up front so the subscriber is never at 0 credit when
// an event arrives. We top it up as messages are consumed.
const standingCredit = 100;

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
      container_id: `kubemq-amqp10-js-pubsub-${process.pid}`,
      reconnect: false,
    },
  };
}

function bodyToString(body: unknown): string {
  return Buffer.isBuffer(body) ? body.toString("utf8") : String(body);
}

function sleep(ms: number): Promise<void> {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

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
  const address = `events/${channel}`;
  console.log(`Broker:  amqp://${host}:${port}`);
  console.log(`Address: ${address}  (KubeMQ pattern=events, channel=${channel})\n`);

  const connection = new Connection(options);
  await connection.open();

  try {
    // =======================================================================
    // 1. SUBSCRIBE FIRST. Attach the receiver with standing credit BEFORE any
    //    publish. Events have no replay — a publish that beats the subscription
    //    is lost forever. The message handler is registered before credit is
    //    granted so no early delivery is missed.
    // =======================================================================
    const receiver = await connection.createReceiver({
      source: { address },
      credit_window: 0,
      autoaccept: false,
      autosettle: false,
    });

    const seen = new Set<string>();
    const received = drainEvents(receiver, (ctx) => {
      const body = bodyToString(ctx.message?.body);
      ctx.delivery?.accept(); // no-op for pre-settled fan-out, but harmless
      seen.add(body);
      return seen.size >= total;
    }, standingCredit, 30_000);
    console.log(`[recv] Subscribed to ${address} with standing credit ${standingCredit}`);

    // The attach reply confirms the link, not that the connector's subscription
    // pump has run its SubscribeEvents yet. Wait for the pump to go live before
    // publishing, or the first events race the subscription and are dropped.
    await sleep(750);
    console.log("[recv] Subscription pump settled (waited 750ms before publishing)");

    // =======================================================================
    // 2. PUBLISH pre-settled. snd_settle_mode:1 marks every TRANSFER as settled
    //    (fire-and-forget) — events are at-most-once, so there is no DISPOSITION
    //    to await and no produce confirmation.
    // =======================================================================
    const sender = await connection.createSender({
      target: { address },
      snd_settle_mode: 1,
      autosettle: true,
    });
    for (let i = 0; i < total; i++) {
      const body = `event-${String(i).padStart(3, "0")}`;
      await waitSendable(sender, 15_000);
      sender.send({ body });
    }
    await sender.close();
    console.log(`[send] Published ${total} events (pre-settled, fire-and-forget)`);

    // =======================================================================
    // 3. RECEIVE. With standing credit the subscriber drains every event.
    // =======================================================================
    await received;
    console.log(`[recv] Received all ${seen.size} events (continuous credit => no 0-credit drop)`);

    await receiver.close();
  } finally {
    await connection.close();
  }

  console.log("\nDone.");
}

/**
 * Subscribes with a standing credit and invokes `onMessage` per delivery until
 * it returns true. Credit is topped back up to `credit` as messages arrive so
 * the subscriber is never starved (a 0-credit event is silently dropped). The
 * handler is registered before the initial credit grant.
 */
function drainEvents(
  receiver: Receiver,
  onMessage: (ctx: EventContext) => boolean,
  credit: number,
  timeoutMs: number,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      receiver.removeListener(ReceiverEvents.message, handler);
      reject(new Error("timed out waiting for events"));
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
        return;
      }
      // Replenish so standing credit never drains to 0.
      receiver.addCredit(1);
    };

    receiver.on(ReceiverEvents.message, handler);
    receiver.addCredit(credit);
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

// Expected output:
//
// Broker:  amqp://localhost:5672
// Address: events/amqp10.examples.pubsub  (KubeMQ pattern=events, channel=amqp10.examples.pubsub)
//
// [recv] Subscribed to events/amqp10.examples.pubsub with standing credit 100
// [recv] Subscription pump settled (waited 750ms before publishing)
// [send] Published 20 events (pre-settled, fire-and-forget)
// [recv] Received all 20 events (continuous credit => no 0-credit drop)
//
// Done.
//
// (Events are at-most-once with no replay: if the subscriber were at 0 credit
// when an event arrived, that event would be SILENTLY DROPPED and counted on the
// server metric kubemq_amqp10_events_dropped_no_credit_total — never surfaced as
// a client error. Standing credit + subscribe-before-publish avoid both losses.)
