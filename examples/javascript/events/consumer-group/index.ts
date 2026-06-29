/**
 * Example: events/consumer-group (master-table variant #5)
 *
 * Consumer-group load-balancing over KubeMQ **Events** with the native
 * rhea / rhea-promise client (NO KubeMQ SDK).
 *
 * The x-opt-kubemq-group receiver link property places a subscriber in a named
 * load-balancing group. Within ONE group, the connector round-robins the event
 * stream across the group's members (no duplication). A DISTINCT group is an
 * independent virtual-topic subscriber that gets the FULL stream.
 *
 * This example opens:
 *   - g1a, g1b — two receivers in group "g1" => together they receive every
 *     event with NO body delivered to both (the group splits the stream).
 *   - g2       — one receiver in group "g2" => gets EVERY event (independent).
 *
 * Each receiver runs on its own Session: a session/link should not be shared
 * across independent consumers.
 *
 * Grounded in connector test TestEventsPubSubGroupFanout
 * (connectors/amqp10/integration_pubsub_test.go).
 *
 * Run:
 *   export KUBEMQ_AMQP_URL=amqp://localhost:5672
 *   npx tsx events/consumer-group/index.ts
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

const channel = "amqp10.examples.consumergroup";

const total = 30;

const groupProp = "x-opt-kubemq-group";

// standingCredit is granted to each member so none is ever at 0 credit.
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
      container_id: `kubemq-amqp10-js-consumergroup-${process.pid}`,
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

/**
 * Drives one group receiver on its own session. It collects bodies into `got`
 * for up to `windowMs`, replenishing credit as messages arrive, then closes.
 */
class GroupSubscriber {
  readonly label: string;
  readonly group: string;
  readonly got = new Set<string>();
  private receiver?: Receiver;

  constructor(label: string, group: string) {
    this.label = label;
    this.group = group;
  }

  async attach(connection: Connection, address: string): Promise<void> {
    const session = await connection.createSession();
    this.receiver = await connection.createReceiver({
      session,
      source: { address },
      credit_window: 0,
      autoaccept: false,
      autosettle: false,
      properties: { [groupProp]: this.group },
    });
    const receiver = this.receiver;
    receiver.on(ReceiverEvents.message, (ctx: EventContext) => {
      this.got.add(bodyToString(ctx.message?.body));
      ctx.delivery?.accept(); // no-op for pre-settled fan-out
      receiver.addCredit(1); // keep standing credit topped up
    });
    receiver.addCredit(standingCredit);
  }

  async collect(windowMs: number): Promise<void> {
    await sleep(windowMs);
    if (this.receiver) {
      await this.receiver.close();
    }
  }
}

async function main(): Promise<void> {
  const { options, host, port } = connectionOptions();
  const address = `events/${channel}`;
  console.log(`Broker:  amqp://${host}:${port}`);
  console.log(`Address: ${address}  (KubeMQ pattern=events, channel=${channel})\n`);

  const connection = new Connection(options);
  await connection.open();

  try {
    // Three group subscribers, each on its own session.
    const g1a = new GroupSubscriber("g1a", "g1");
    const g1b = new GroupSubscriber("g1b", "g1");
    const g2 = new GroupSubscriber("g2", "g2");
    const subs = [g1a, g1b, g2];

    for (const sub of subs) {
      await sub.attach(connection, address);
    }

    // Let the subscription pumps go live (events have no replay — a publish that
    // races a subscription is lost).
    await sleep(750);
    console.log("[recv] 3 subscribers attached: g1a+g1b (group g1), g2 (group g2)");

    // Publish on a dedicated session. Pre-settled fire-and-forget.
    const prodSession = await connection.createSession();
    const sender = await connection.createSender({
      session: prodSession,
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
    console.log(`[send] Published ${total} events (pre-settled)`);

    // Let the subscribers drain their windows.
    await Promise.all(subs.map((s) => s.collect(5_000)));

    // --- Assert the consumer-group semantics --------------------------------

    // g2 (a distinct group) receives EVERY event.
    if (g2.got.size !== total) {
      throw new Error(`group g2 (independent) expected all ${total} events, got ${g2.got.size}`);
    }
    console.log(`[recv] g2 (group g2, independent): ${g2.got.size}/${total} events — FULL stream`);

    // g1a + g1b TOGETHER receive every event, with NO body delivered to both.
    const combined = new Set<string>(g1a.got);
    let dups = 0;
    for (const body of g1b.got) {
      if (g1a.got.has(body)) {
        dups++;
      }
      combined.add(body);
    }
    if (dups !== 0) {
      throw new Error(`group g1 load-balancing broken: ${dups} event(s) delivered to BOTH g1a and g1b`);
    }
    if (combined.size !== total) {
      throw new Error(`group g1 members together expected all ${total} events, got ${combined.size}`);
    }
    if (g1a.got.size === 0 || g1b.got.size === 0) {
      throw new Error(`group g1 not load-balanced: g1a=${g1a.got.size} g1b=${g1b.got.size} (one member got nothing)`);
    }
    console.log(`[recv] g1a (group g1): ${g1a.got.size} events; g1b (group g1): ${g1b.got.size} events`);
    console.log(`[recv] g1a+g1b together: ${combined.size}/${total} events, 0 duplicates — group SPLIT the stream`);
  } finally {
    await connection.close();
  }

  console.log("\nDone.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

// Expected output (the g1a/g1b split varies run to run; the totals are fixed):
//
// Broker:  amqp://localhost:5672
// Address: events/amqp10.examples.consumergroup  (KubeMQ pattern=events, channel=amqp10.examples.consumergroup)
//
// [recv] 3 subscribers attached: g1a+g1b (group g1), g2 (group g2)
// [send] Published 30 events (pre-settled)
// [recv] g2 (group g2, independent): 30/30 events — FULL stream
// [recv] g1a (group g1): 16 events; g1b (group g1): 14 events
// [recv] g1a+g1b together: 30/30 events, 0 duplicates — group SPLIT the stream
//
// Done.
