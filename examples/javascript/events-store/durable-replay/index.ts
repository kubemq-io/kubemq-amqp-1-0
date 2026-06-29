/**
 * Example: events-store/durable-replay (master-table variant #7)
 *
 * Durable subscriptions with RESUME over KubeMQ **Events Store** using the native
 * rhea / rhea-promise client (NO KubeMQ SDK).
 *
 * Unlike Events (fire-and-forget, no replay), Events Store PERSISTS the stream and
 * lets a DURABLE subscriber resume where it left off. A durable subscription is
 * identified by the pair:
 *
 *     (connection container-id, link name)
 *
 * To make a subscriber durable and resumable:
 *
 *   - open a connection with a STABLE container_id
 *   - attach the receiver with a STABLE link `name`
 *   - request a non-expiring source: source.expiry_policy = "never"
 *   - set the start position once: receiver `properties` x-opt-kubemq-start: new-only
 *
 * On a clean disconnect the connector preserves the durable cursor. Re-attaching
 * with the SAME (container-id, link name) RESUMES the subscription and delivers
 * every event published while the subscriber was away — no loss, no replay of
 * already-consumed events.
 *
 * Flow:
 *  1. Open with container-id "amqp10-examples-durable-container"; attach durable
 *     receiver "durable-sub" (start new-only). Publish 3 events; receive all 3.
 *  2. Disconnect (close the durable connection).
 *  3. Publish 5 MORE events while the durable subscriber is away.
 *  4. Re-open with the SAME container-id; re-attach "durable-sub". The subscription
 *     RESUMES and delivers exactly the 5 missed events.
 *
 * Grounded in connector test TestEventsStoreDurableReplay
 * (connectors/amqp10/integration_pubsub_test.go).
 *
 * Run:
 *   export KUBEMQ_AMQP_URL=amqp://localhost:5672
 *   npx tsx events-store/durable-replay/index.ts
 */
import {
  Connection,
  ReceiverEvents,
  type ConnectionOptions,
  type EventContext,
  type Receiver,
} from "rhea-promise";

const channel = "amqp10.examples.durable";

// The durable identity = (containerID, linkName). Both MUST be stable across
// reconnects for the subscription to resume.
const containerId = "amqp10-examples-durable-container";
const linkName = "durable-sub";

// standingCredit keeps the durable receiver continuously credited so a delivered
// event is never dropped at 0 credit.
const standingCredit = 100;

function brokerEndpoint(): { host: string; port: number } {
  const raw = process.env["KUBEMQ_AMQP_URL"] ?? "amqp://localhost:5672";
  const url = new URL(raw);
  return { host: url.hostname, port: url.port ? Number(url.port) : 5672 };
}

/** Producer connection: a plain connection (no stable id needed) that publishes. */
function producerOptions(): ConnectionOptions {
  const { host, port } = brokerEndpoint();
  return {
    host,
    port,
    container_id: `kubemq-amqp10-js-durable-prod-${process.pid}`,
    reconnect: false,
  };
}

/** Durable subscriber connection: the STABLE container-id is half the durable identity. */
function durableOptions(): ConnectionOptions {
  const { host, port } = brokerEndpoint();
  return {
    host,
    port,
    container_id: containerId, // STABLE — half the durable identity
    reconnect: false,
  };
}

function bodyToString(body: unknown): string {
  return Buffer.isBuffer(body) ? body.toString("utf8") : String(body);
}

function sleep(ms: number): Promise<void> {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

async function main(): Promise<void> {
  const { host, port } = brokerEndpoint();
  const address = `events-store/${channel}`;
  console.log(`Broker:        amqp://${host}:${port}`);
  console.log(`Address:       ${address}  (KubeMQ pattern=events-store, channel=${channel})`);
  console.log(`Durable id:    container-id="${containerId}"  link-name="${linkName}"\n`);

  // =========================================================================
  // 0. PRODUCER — a separate, plain connection that publishes to the
  //    events-store stream throughout the demo (it does not need a stable id).
  //    Each send is unsettled (at-least-once); events-store persists each
  //    accepted transfer. AwaitableSender awaits the connector's `accepted`.
  // =========================================================================
  const prodConnection = new Connection(producerOptions());
  await prodConnection.open();
  const sender = await prodConnection.createAwaitableSender({
    target: { address },
  });

  // publish sends es-<lo>..es-<hi-1>, each awaiting its accepted DISPOSITION.
  const publish = async (lo: number, hi: number): Promise<void> => {
    for (let i = lo; i < hi; i++) {
      await sender.send({ body: `es-${String(i).padStart(3, "0")}` }, { timeoutInSeconds: 15 });
    }
  };

  try {
    // =======================================================================
    // 1. DURABLE SUBSCRIBE (first attach). Stable container-id + link name +
    //    non-expiring source make this subscription durable. start=new-only
    //    means "deliver events from now on" (this attach establishes the cursor).
    // =======================================================================
    const first = await attachDurable("first attach");
    await publish(0, 3); // 3 events while the durable subscriber is live

    const firstBodies = await drain(first.receiver, 3, 30_000);
    if (firstBodies.length !== 3) {
      throw new Error(`durable subscriber expected the first 3 events, got ${firstBodies.length}: ${firstBodies.join(",")}`);
    }
    console.log(`[recv] First attach received ${firstBodies.length} events: [${firstBodies.join(" ")}]\n`);

    // =======================================================================
    // 2. DISCONNECT. A clean Close detaches the durable link; the connector
    //    preserves the durable cursor for this (container-id, link name).
    // =======================================================================
    await first.connection.close();
    console.log("[conn] Durable subscriber DISCONNECTED (cursor preserved by the connector)");
    await sleep(1_000); // let the detach + unsubscribe settle

    // =======================================================================
    // 3. PUBLISH WHILE AWAY. 5 more events arrive at the persisted stream while
    //    the durable subscriber is offline.
    // =======================================================================
    await publish(3, 8);
    console.log("[send] Published 5 more events WHILE the durable subscriber was away");

    // =======================================================================
    // 4. RE-ATTACH with the SAME durable identity. The subscription RESUMES and
    //    delivers exactly the 5 events published while away (not the first 3
    //    again, and nothing lost).
    // =======================================================================
    const second = await attachDurable("re-attach");
    try {
      const resumed = await drain(second.receiver, 5, 30_000);
      const resumedSet = new Set(resumed);
      for (let i = 3; i < 8; i++) {
        const body = `es-${String(i).padStart(3, "0")}`;
        if (!resumedSet.has(body)) {
          throw new Error(`durable resume missing event ${body} (got ${resumed.join(",")})`);
        }
      }
      console.log(`[recv] Re-attach RESUMED and received the ${resumedSet.size} events published while away: [${resumed.join(" ")}]`);
      console.log("[recv] No loss, no re-delivery of the already-consumed first 3 — the durable cursor resumed exactly");
    } finally {
      await second.connection.close();
    }
  } finally {
    await sender.close();
    await prodConnection.close();
  }

  console.log("\nDone.");
}

interface DurableAttach {
  connection: Connection;
  receiver: Receiver;
}

/**
 * Opens the durable connection (stable container-id) and attaches the durable
 * receiver (stable link name + non-expiring source + start position). The handler
 * is attached BEFORE credit so no early delivery is dropped.
 */
async function attachDurable(phase: string): Promise<DurableAttach> {
  const connection = new Connection(durableOptions());
  await connection.open();
  const receiver = await connection.createReceiver({
    name: linkName, // stable link name = half the durable identity
    source: {
      address: `events-store/${channel}`,
      expiry_policy: "never", // never expire the durable source
    },
    properties: { "x-opt-kubemq-start": "new-only" }, // start cursor (honoured on first attach)
    credit_window: 0,
    autoaccept: false,
    autosettle: false,
  });
  console.log(`[recv] Durable receiver attached (${phase}): container-id="${containerId}" name="${linkName}" expiry=never`);
  // Let the connector's subscription pump go live before producing.
  await sleep(750);
  return { connection, receiver };
}

/**
 * Drains up to `max` events within `windowMs`, returning their bodies. Each
 * delivery is accepted (advances the durable cursor). The handler is registered
 * before the standing credit is granted.
 */
function drain(receiver: Receiver, max: number, windowMs: number): Promise<string[]> {
  return new Promise<string[]>((resolve, reject) => {
    const out: string[] = [];
    const timer = setTimeout(() => {
      receiver.removeListener(ReceiverEvents.message, handler);
      // Partial result is allowed to surface as a caller-side length check.
      resolve(out);
    }, windowMs);

    const handler = (ctx: EventContext): void => {
      try {
        ctx.delivery?.accept(); // accept advances the durable cursor
        out.push(bodyToString(ctx.message?.body));
        if (out.length >= max) {
          clearTimeout(timer);
          receiver.removeListener(ReceiverEvents.message, handler);
          resolve(out);
          return;
        }
        receiver.addCredit(1); // keep standing credit topped up
      } catch (err) {
        clearTimeout(timer);
        receiver.removeListener(ReceiverEvents.message, handler);
        reject(err instanceof Error ? err : new Error(String(err)));
      }
    };

    receiver.on(ReceiverEvents.message, handler);
    receiver.addCredit(standingCredit);
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

// Expected output:
//
// Broker:        amqp://localhost:5672
// Address:       events-store/amqp10.examples.durable  (KubeMQ pattern=events-store, channel=amqp10.examples.durable)
// Durable id:    container-id="amqp10-examples-durable-container"  link-name="durable-sub"
//
// [recv] Durable receiver attached (first attach): container-id="amqp10-examples-durable-container" name="durable-sub" expiry=never
// [recv] First attach received 3 events: [es-000 es-001 es-002]
//
// [conn] Durable subscriber DISCONNECTED (cursor preserved by the connector)
// [send] Published 5 more events WHILE the durable subscriber was away
// [recv] Durable receiver attached (re-attach): container-id="amqp10-examples-durable-container" name="durable-sub" expiry=never
// [recv] Re-attach RESUMED and received the 5 events published while away: [es-003 es-004 es-005 es-006 es-007]
// [recv] No loss, no re-delivery of the already-consumed first 3 — the durable cursor resumed exactly
//
// Done.
//
// NOTE: durable subscriptions are NODE-LOCAL (see README gotcha). In a cluster the
// durable cursor lives on the node that owned the original attach; reconnect to the
// SAME node (or run a single-node dev broker, as here) to resume.
