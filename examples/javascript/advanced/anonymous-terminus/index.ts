/**
 * Example: advanced/anonymous-terminus (master-table variant #12)
 *
 * An ANONYMOUS sender (a link attached with a NULL target — createSender({target:{}}))
 * carries no fixed channel. Instead, EACH message selects its own destination via
 * its `to` field, and the KubeMQ connector routes it per-message to the right
 * pattern/channel. One link, many destinations. Driven with the native rhea /
 * rhea-promise client (NO KubeMQ SDK).
 *
 * Flow:
 *   - ATTACH an anonymous sender: createSender({target:{}}) → null target. Sends are
 *     UNSETTLED (AwaitableSender) so each routing decision surfaces as an accepted
 *     or rejected DISPOSITION.
 *   - Send #1: message { to: "queues/<ch>" } routes to a queue.
 *   - Send #2: message { to: "events/<ch>" } routes to an events topic (a subscriber
 *     is attached BEFORE the send — events are fire-and-forget).
 *   - The queue message is then consumed back to prove it landed correctly.
 *   - (Demonstrated as expected errors) a BAD `to` (unknown prefix) and a MISSING
 *     `to` are both rejected by the connector: the send rejects carrying
 *     amqp:precondition-failed.
 *
 * Per-message authorization: each anonymous send is authorized for WRITE on the
 * resolved (pattern, channel) via the connector's Casbin policy — there is no
 * per-link grant for an anonymous terminus.
 *
 * Grounded in connector test TestAnonymousTerminusRouting
 * (connectors/amqp10/integration_test.go).
 *
 * Run:
 *   export KUBEMQ_AMQP_URL=amqp://localhost:5672
 *   npx tsx advanced/anonymous-terminus/index.ts
 */
import {
  Connection,
  ConnectionEvents,
  ReceiverEvents,
  type ConnectionOptions,
  type EventContext,
  type Receiver,
} from "rhea-promise";

// Explicit <pattern>/<channel> destinations selected per-message via `to`.
const queueChannel = "amqp10.examples.anon.q";
const eventsChannel = "amqp10.examples.anon.e";

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
      container_id: `kubemq-amqp10-js-anon-${process.pid}`,
      reconnect: false,
    },
  };
}

// bodyToString normalises a received message body to a string. rhea surfaces a
// Data section either as a raw Buffer or as a typed wrapper { typecode, content:
// Buffer } depending on how the bytes were stored; unwrap both.
function bodyToString(body: unknown): string {
  if (Buffer.isBuffer(body)) {
    return body.toString("utf8");
  }
  if (typeof body === "string") {
    return body;
  }
  if (body && typeof body === "object") {
    const content = (body as { content?: unknown }).content;
    if (Buffer.isBuffer(content)) {
      return content.toString("utf8");
    }
    if (content && typeof content === "object" && "data" in content && Array.isArray((content as { data: unknown }).data)) {
      return Buffer.from((content as { data: number[] }).data).toString("utf8");
    }
  }
  return String(body);
}

function sleep(ms: number): Promise<void> {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

// describeAmqpError extracts the AMQP error condition. rhea nests a rejected-send
// disposition's condition under `innerError` (e.g. amqp:precondition-failed); fall
// back to the top-level condition/message otherwise.
function describeAmqpError(err: unknown): string {
  if (err && typeof err === "object") {
    const e = err as {
      condition?: string;
      description?: string;
      message?: string;
      innerError?: { condition?: string; description?: string };
    };
    const inner = e.innerError;
    if (inner?.condition) {
      return inner.description ? `${inner.condition} (${inner.description})` : inner.condition;
    }
    if (e.condition) {
      return e.description ? `${e.condition} (${e.description})` : e.condition;
    }
    if (e.message) {
      return e.message;
    }
  }
  return String(err);
}

async function main(): Promise<void> {
  const { options, host, port } = connectionOptions();
  const queueTo = `queues/${queueChannel}`;
  const eventsTo = `events/${eventsChannel}`;
  console.log(`Broker: amqp://${host}:${port}`);
  console.log("Anonymous sender (null target) — routes per-message via `to`");
  console.log(`  msg #1 to: ${queueTo}`);
  console.log(`  msg #2 to: ${eventsTo}\n`);

  const connection = new Connection(options);
  // A connector rejection of a bad/missing `to` can surface as a connection-level
  // `error` event (rhea v3) in addition to rejecting the send; swallow it so Node
  // does not crash on an unhandled error. We assert the rejection via the send
  // promise itself below.
  connection.on(ConnectionEvents.error, () => {
    /* swallowed — the rejection is asserted on the send() promise */
  });
  await connection.open();

  try {
    // =======================================================================
    // 1. ATTACH an anonymous sender. The empty target ({}) attaches a link with a
    //    NULL target — there is no bound channel. Every message routes by its own
    //    `to`. Unsettled so each routing decision returns a DISPOSITION.
    // =======================================================================
    const anon = await connection.createAwaitableSender({ target: {} });
    console.log("[attach] Anonymous sender attached (null target)");

    // A consumer for the EVENTS channel must be subscribed BEFORE we publish to it —
    // events are fire-and-forget (no replay). The queue message, by contrast, is
    // durable, so we consume it after sending.
    const eventReceiver = await connection.createReceiver({
      source: { address: eventsTo },
      credit_window: 0,
      autoaccept: false,
      autosettle: false,
    });
    const eventDelivered = receiveOne(eventReceiver, 30_000);
    await sleep(500); // give the fresh subscription a moment to register

    // =======================================================================
    // 2. Send #1 — route to a QUEUE via `to`. The connector resolves
    //    "queues/<ch>", authorizes WRITE for this connection, and stores it.
    // =======================================================================
    await anon.send({ body: "to-queue", to: queueTo }, { timeoutInSeconds: 15 });
    console.log(`[send] msg #1 routed to ${queueTo} (accepted)`);

    // =======================================================================
    // 3. Send #2 — route to an EVENTS topic via `to`. Same anonymous link, a
    //    DIFFERENT pattern. The subscriber attached above receives it.
    // =======================================================================
    await anon.send({ body: "to-events", to: eventsTo }, { timeoutInSeconds: 15 });
    console.log(`[send] msg #2 routed to ${eventsTo} (accepted)`);

    // =======================================================================
    // 4. Negative cases (expected errors) — the connector rejects a bad/missing
    //    `to` with amqp:precondition-failed, surfaced to the client as a rejected
    //    send. The anonymous link stays usable afterwards.
    // =======================================================================
    const badTo = "bogus/prefix/x";
    try {
      await anon.send({ body: "nowhere", to: badTo }, { timeoutInSeconds: 15 });
      throw new Error("expected a bad `to` to be rejected, but the send succeeded");
    } catch (err) {
      console.log(`[send] msg with bad \`to\`="${badTo}" rejected as expected: ${describeAmqpError(err)}`);
    }

    try {
      await anon.send({ body: "orphan" }, { timeoutInSeconds: 15 }); // NO `to` at all
      throw new Error("expected a missing `to` to be rejected, but the send succeeded");
    } catch (err) {
      console.log(`[send] msg with NO \`to\` rejected as expected: ${describeAmqpError(err)}`);
    }
    await anon.close();

    // =======================================================================
    // 5. Verify routing — consume the queue message back, and receive the event.
    // =======================================================================
    const queueReceiver = await connection.createReceiver({
      source: { address: queueTo },
      credit_window: 0,
      autoaccept: false,
      autosettle: false,
    });
    const qBody = await receiveOne(queueReceiver, 30_000);
    console.log(`[recv] queue ${queueTo} delivered: "${qBody}"`);
    await queueReceiver.close();

    const eBody = await eventDelivered;
    console.log(`[recv] events ${eventsTo} delivered: "${eBody}"`);
    await eventReceiver.close();
  } finally {
    try {
      await connection.close();
    } catch {
      /* connection may already be torn down */
    }
  }

  console.log("\nDone.");
}

/**
 * Grants 1 credit, waits for one delivery, accepts it, and returns the body string.
 * The handler is registered before credit is granted.
 */
function receiveOne(receiver: Receiver, timeoutMs: number): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    const timer = setTimeout(() => {
      receiver.removeListener(ReceiverEvents.message, handler);
      reject(new Error("timed out waiting for a message"));
    }, timeoutMs);

    const handler = (ctx: EventContext): void => {
      clearTimeout(timer);
      receiver.removeListener(ReceiverEvents.message, handler);
      ctx.delivery?.accept();
      resolve(bodyToString(ctx.message?.body));
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
// Broker: amqp://localhost:5672
// Anonymous sender (null target) — routes per-message via `to`
//   msg #1 to: queues/amqp10.examples.anon.q
//   msg #2 to: events/amqp10.examples.anon.e
//
// [attach] Anonymous sender attached (null target)
// [send] msg #1 routed to queues/amqp10.examples.anon.q (accepted)
// [send] msg #2 routed to events/amqp10.examples.anon.e (accepted)
// [send] msg with bad `to`="bogus/prefix/x" rejected as expected: amqp:precondition-failed (unknown address prefix)
// [send] msg with NO `to` rejected as expected: amqp:precondition-failed (anonymous terminus message has no `to`)
// [recv] queue queues/amqp10.examples.anon.q delivered: "to-queue"
// [recv] events events/amqp10.examples.anon.e delivered: "to-events"
//
// Done.
