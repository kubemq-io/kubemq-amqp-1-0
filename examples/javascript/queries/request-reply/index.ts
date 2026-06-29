/**
 * Example: queries/request-reply (master-table variant #10)
 *
 * Native AMQP 1.0 request/reply over KubeMQ **Queries** (RPC) with the native
 * rhea / rhea-promise client — NO KubeMQ SDK, NO gRPC. The whole round-trip stays
 * in-protocol over a single broker connection per role.
 *
 * The reply path is IDENTICAL to commands (variant #9): the requester opens a
 * DYNAMIC reply node (createReceiver({source:{dynamic:true}}) → .address), sends to
 * queries/<ch> with message.reply_to = that node + a correlation_id; the responder
 * receives on queries/<ch> and replies via an ANONYMOUS sender
 * (createSender({target:{}})) with message.to = the request's reply_to + the echoed
 * correlation_id.
 *
 * The CONTRAST with commands (the whole point of variant #10):
 *
 *   - A query reply carries ONLY the body + metadata — NO x-opt-kubemq-executed /
 *     x-opt-kubemq-error application-properties. A query is a "fetch a value" call;
 *     there is no executed/error envelope.
 *   - A FAILED query delivers NOTHING. The connector's runRequest delivers no reply
 *     when a query fails or times out (MQTT-bridge parity), so the requester simply
 *     TIMES OUT. (A failed command, by contrast, always replies executed=false so
 *     its requester is never left waiting.)
 *
 * This example demonstrates BOTH: a successful query (reply round-trips, body
 * intact) and a query the responder ignores (no reply ⇒ the requester times out on
 * a short demo deadline; in production the connector default is ~30s).
 *
 * Grounded in connector test TestRPCRequesterResponderViaAMQP10 (queries leg)
 * (connectors/amqp10/integration_test.go).
 *
 * Run:
 *   export KUBEMQ_AMQP_URL=amqp://localhost:5672
 *   npx tsx queries/request-reply/index.ts
 */
import {
  Connection,
  ReceiverEvents,
  type AwaitableSender,
  type ConnectionOptions,
  type EventContext,
  type Receiver,
} from "rhea-promise";

// channel is the KubeMQ queries channel; the link address is "queries/" + channel
// (explicit prefix — never rely on a default pattern).
const channel = "amqp10.examples.queries";

// demoTimeoutMs is the short per-request deadline used here so the "no reply" leg
// surfaces a timeout quickly. The connector's own default RPC timeout is ~30s; in
// production set the request `ttl` to choose the per-request budget.
const demoTimeoutMs = 5_000;

function connectionOptions(suffix: string): ConnectionOptions {
  const raw = process.env["KUBEMQ_AMQP_URL"] ?? "amqp://localhost:5672";
  const url = new URL(raw);
  return {
    host: url.hostname,
    port: url.port ? Number(url.port) : 5672,
    container_id: `kubemq-amqp10-js-queries-${suffix}-${process.pid}`,
    reconnect: false,
  };
}

function bodyToString(body: unknown): string {
  return Buffer.isBuffer(body) ? body.toString("utf8") : String(body);
}

async function main(): Promise<void> {
  const raw = process.env["KUBEMQ_AMQP_URL"] ?? "amqp://localhost:5672";
  const url = new URL(raw);
  const address = `queries/${channel}`;
  console.log(`Broker:  amqp://${url.hostname}:${url.port || "5672"}`);
  console.log(`Address: ${address}  (KubeMQ pattern=queries, channel=${channel})\n`);

  const responder = await Responder.start(address);
  try {
    await runRequester(address);
  } finally {
    await responder.stop();
  }

  console.log("\nDone.");
}

// =============================================================================
// RESPONDER — receives queries on queries/<ch>, replies via an anonymous sender.
// A query whose body is "ignore" gets NO reply (so its requester times out).
// =============================================================================

class Responder {
  private constructor(
    private readonly connection: Connection,
    private readonly receiver: Receiver,
    private readonly replySender: AwaitableSender,
  ) {}

  static async start(address: string): Promise<Responder> {
    const connection = new Connection(connectionOptions("responder"));
    await connection.open();

    // ATTACH a receiver on queries/<ch> (server-sender link — the client consumes
    // requests). Manual credit; handler registered before credit.
    const receiver = await connection.createReceiver({
      source: { address },
      credit_window: 0,
      autoaccept: false,
      autosettle: false,
    });

    // ATTACH an ANONYMOUS sender (null target — target:{}). Each reply sets
    // message.to to the request's reply_to so it routes back to the requester's
    // dynamic node.
    const replySender = await connection.createAwaitableSender({ target: {} });

    const responder = new Responder(connection, receiver, replySender);
    receiver.on(ReceiverEvents.message, (ctx: EventContext) => {
      responder.onRequest(ctx).catch((err) => {
        console.error(`[responder] reply error: ${err instanceof Error ? err.message : String(err)}`);
      });
    });
    receiver.addCredit(10);

    console.log(`[responder] Listening on ${address} (anonymous reply sender ready)`);
    return responder;
  }

  private async onRequest(ctx: EventContext): Promise<void> {
    const req = ctx.message;
    ctx.delivery?.accept();
    if (!req || req.reply_to === undefined || req.reply_to === null) {
      console.log("[responder] request with no reply-to; cannot reply");
      return;
    }
    const body = bodyToString(req.body);
    console.log(`[responder] Received query "${body}" (correlation-id=${String(req.correlation_id)})`);

    // Business logic: a query body of "ignore" is dropped on the floor — the
    // responder sends NOTHING. The requester will time out. (A real responder
    // would only fail to reply on a crash / unreachable backend; "ignore" makes the
    // contrast deterministic for the demo.)
    if (body === "ignore") {
      console.log(`[responder] Ignoring "${body}" — NO reply sent (requester will time out)`);
      return;
    }

    // A QUERY reply carries ONLY the body + metadata — NO executed/error
    // application-properties (the Commands-vs-Queries contrast).
    const correlationId = req.correlation_id ?? req.message_id;
    await this.replySender.send(
      {
        body: `result:${body}`,
        to: req.reply_to,
        correlation_id: correlationId,
      },
      { timeoutInSeconds: 10 },
    );
    console.log(`[responder] Replied to "${body}" (body + metadata only, no executed/error props)`);
  }

  async stop(): Promise<void> {
    await this.replySender.close();
    await this.receiver.close();
    await this.connection.close();
  }
}

// =============================================================================
// REQUESTER — dynamic reply node + sender on queries/<ch>; correlates replies.
// =============================================================================

async function runRequester(address: string): Promise<void> {
  const connection = new Connection(connectionOptions("requester"));
  await connection.open();

  try {
    // ATTACH a DYNAMIC reply node (dynamic:true). The server creates a transient
    // node and echoes its address (read with .address after attach).
    const replyReceiver = await connection.createReceiver({
      source: { address: "", dynamic: true },
      credit_window: 0,
      autoaccept: false,
      autosettle: false,
    });
    replyReceiver.addCredit(5);
    const replyNode = replyReceiver.address || replyReceiver.source.address;
    if (!replyNode) {
      throw new Error("[requester] server did not assign a dynamic reply-node address");
    }
    console.log(`[requester] Dynamic reply node: ${replyNode}`);

    // ATTACH a sender on queries/<ch> (server-receiver link — the client produces
    // requests). The server grants credit on attach.
    const sender = await connection.createAwaitableSender({ target: { address } });

    // 1. A SUCCESSFUL query: round-trips, body intact, no executed/error props.
    await doQuery(sender, replyReceiver, replyNode, "get-temp-sensor-3", "corr-qry-1");

    // 2. A query the responder ignores: NOTHING is delivered, so the requester
    //    TIMES OUT. This is the core Queries contrast — a failed/unanswered query
    //    has no error envelope; the absence of a reply IS the failure signal.
    await doQueryExpectTimeout(sender, replyReceiver, replyNode, "ignore", "corr-qry-2");

    await sender.close();
    await replyReceiver.close();
  } finally {
    await connection.close();
  }
}

/** Sends one query, awaits the correlated reply, and prints the result body. */
async function doQuery(
  sender: AwaitableSender,
  replyReceiver: Receiver,
  replyNode: string,
  body: string,
  corr: string,
): Promise<void> {
  const replyPromise = awaitReply(replyReceiver, demoTimeoutMs);
  await sendQuery(sender, replyNode, body, corr);

  const reply = await replyPromise;
  const gotCorr = reply.correlation_id;
  if (String(gotCorr) !== corr) {
    throw new Error(`[requester] correlation-id mismatch: want ${corr} got ${String(gotCorr)}`);
  }
  console.log(`[requester] Reply for "${body}" (correlation-id=${String(gotCorr)}): body="${bodyToString(reply.body)}"`);
}

/** Sends a query the responder will ignore and shows the requester timing out. */
async function doQueryExpectTimeout(
  sender: AwaitableSender,
  replyReceiver: Receiver,
  replyNode: string,
  body: string,
  corr: string,
): Promise<void> {
  const replyPromise = awaitReply(replyReceiver, demoTimeoutMs);
  await sendQuery(sender, replyNode, body, corr);

  try {
    await replyPromise;
    throw new Error(`[requester] expected NO reply for "${body}", but one arrived`);
  } catch (err) {
    if (err instanceof Error && err.message === REPLY_TIMEOUT) {
      // A timeout here is the EXPECTED outcome for an unanswered query.
      console.log(
        `[requester] No reply for "${body}" within ${demoTimeoutMs / 1000}s — query timed out (expected; failed queries deliver nothing)`,
      );
      return;
    }
    throw err;
  }
}

/** Sends one query message naming the dynamic reply node + correlation-id. */
async function sendQuery(sender: AwaitableSender, replyNode: string, body: string, corr: string): Promise<void> {
  await sender.send(
    {
      body,
      reply_to: replyNode, // MUST name a node this connection owns (snooping guard)
      correlation_id: corr,
    },
    { timeoutInSeconds: 15 },
  );
  console.log(`[requester] Sent query "${body}" (reply-to=dynamic node, correlation-id=${corr})`);
}

interface ReplyMessage {
  body: unknown;
  correlation_id?: unknown;
}

const REPLY_TIMEOUT = "reply-timeout";

/**
 * Resolves with the next reply on the dynamic node (accepting it), or rejects with
 * a REPLY_TIMEOUT error if none arrives within `timeoutMs`. The handler is removed
 * either way so a later reply for a different request is not mis-delivered.
 */
function awaitReply(replyReceiver: Receiver, timeoutMs: number): Promise<ReplyMessage> {
  return new Promise<ReplyMessage>((resolve, reject) => {
    const timer = setTimeout(() => {
      replyReceiver.removeListener(ReceiverEvents.message, handler);
      reject(new Error(REPLY_TIMEOUT));
    }, timeoutMs);

    const handler = (ctx: EventContext): void => {
      clearTimeout(timer);
      replyReceiver.removeListener(ReceiverEvents.message, handler);
      ctx.delivery?.accept();
      replyReceiver.addCredit(1); // replenish for the next request
      resolve({ body: ctx.message?.body, correlation_id: ctx.message?.correlation_id });
    };

    replyReceiver.on(ReceiverEvents.message, handler);
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

// Expected output:
//
// Broker:  amqp://localhost:5672
// Address: queries/amqp10.examples.queries  (KubeMQ pattern=queries, channel=amqp10.examples.queries)
//
// [responder] Listening on queries/amqp10.examples.queries (anonymous reply sender ready)
// [requester] Dynamic reply node: _amqp10.tmp.<connID>.<uuid>
// [requester] Sent query "get-temp-sensor-3" (reply-to=dynamic node, correlation-id=corr-qry-1)
// [responder] Received query "get-temp-sensor-3" (correlation-id=corr-qry-1)
// [responder] Replied to "get-temp-sensor-3" (body + metadata only, no executed/error props)
// [requester] Reply for "get-temp-sensor-3" (correlation-id=corr-qry-1): body="result:get-temp-sensor-3"
// [requester] Sent query "ignore" (reply-to=dynamic node, correlation-id=corr-qry-2)
// [responder] Received query "ignore" (correlation-id=corr-qry-2)
// [responder] Ignoring "ignore" — NO reply sent (requester will time out)
// [requester] No reply for "ignore" within 5s — query timed out (expected; failed queries deliver nothing)
//
// Done.
//
// (Unlike a command — which always replies executed=false on failure so the
// requester is never left waiting — a query that fails/goes unanswered delivers
// NOTHING. The requester's timeout IS the failure signal. The connector's own
// default per-request timeout is ~30s; set the request `ttl` to choose it.)
