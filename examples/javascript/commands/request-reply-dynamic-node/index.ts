/**
 * Example: commands/request-reply-dynamic-node (master-table variant #9)
 *
 * Native AMQP 1.0 request/reply over KubeMQ **Commands** (RPC) with the native
 * rhea / rhea-promise client — NO KubeMQ SDK, NO gRPC. The whole round-trip stays
 * in-protocol over a single broker connection per role.
 *
 * The mechanism (spec §2.4/§6.5; connector connectors/amqp10/rpc.go + dynamic.go):
 *
 *   - REQUESTER opens a DYNAMIC reply node: createReceiver({source:{dynamic:true}})
 *     — the server creates a transient node and echoes its address back, read with
 *     replyReceiver.address (a "_amqp10.tmp.<connID>.<uuid>" token). The requester
 *     sends the command to commands/<ch> carrying message.reply_to = that node +
 *     message.correlation_id. The connector verifies the reply-to names a node THIS
 *     connection owns (snooping guard: a reply-to that does not resolve to a
 *     connection-owned node is refused with amqp:not-allowed) and routes the
 *     request to SendCommand. The broker Response is delivered out-of-band onto the
 *     dynamic node; the requester correlates it by correlation-id (the connector
 *     falls back to message-id when absent).
 *
 *   - RESPONDER receives requests on commands/<ch> (a server-sender link pumped
 *     under credit) and replies via an ANONYMOUS sender — createSender({target:{}})
 *     (null target) — setting message.to = the request's reply_to (the connector
 *     stamps that as "/responses/<RequestID>") + the echoed correlation_id. A
 *     command reply ALSO carries application_properties:
 *     x-opt-kubemq-executed (bool) + x-opt-kubemq-error (string).
 *
 * Commands vs Queries (the #9 vs #10 contrast): a command that FAILS still produces
 * a reply (executed=false + error text) so the requester is NEVER left waiting.
 * This example demonstrates BOTH: a successful command (executed=true) and a failed
 * command (executed=false) — both round-trip, neither hangs.
 *
 * Grounded in connector tests TestRPCRequesterResponderViaAMQP10 (commands leg) and
 * TestRPCInteropAMQP10RequesterGRPCResponder (the executed/error app-props)
 * (connectors/amqp10/integration_test.go).
 *
 * Run:
 *   export KUBEMQ_AMQP_URL=amqp://localhost:5672
 *   npx tsx commands/request-reply-dynamic-node/index.ts
 */
import {
  Connection,
  ReceiverEvents,
  type AwaitableSender,
  type ConnectionOptions,
  type EventContext,
  type Receiver,
} from "rhea-promise";

// channel is the KubeMQ commands channel; the link address is "commands/" + channel
// (explicit prefix — never rely on a default pattern).
const channel = "amqp10.examples.commands";

function connectionOptions(suffix: string): ConnectionOptions {
  const raw = process.env["KUBEMQ_AMQP_URL"] ?? "amqp://localhost:5672";
  const url = new URL(raw);
  return {
    host: url.hostname,
    port: url.port ? Number(url.port) : 5672,
    container_id: `kubemq-amqp10-js-commands-${suffix}-${process.pid}`,
    reconnect: false,
  };
}

function bodyToString(body: unknown): string {
  if (Buffer.isBuffer(body)) {
    return body.toString("utf8");
  }
  if (typeof body === "string") {
    return body;
  }
  // A COMMAND response carries the executed/error outcome, NOT a body — the
  // connector drops the reply body, so rhea surfaces a null/typed placeholder. Show
  // it as an empty string (matching the Go reference's `body=""`).
  if (body === null || body === undefined) {
    return "";
  }
  const content = (body as { content?: unknown }).content;
  if (Buffer.isBuffer(content)) {
    return content.toString("utf8");
  }
  if (typeof content === "string") {
    return content;
  }
  return "";
}

async function main(): Promise<void> {
  const raw = process.env["KUBEMQ_AMQP_URL"] ?? "amqp://localhost:5672";
  const url = new URL(raw);
  const address = `commands/${channel}`;
  console.log(`Broker:  amqp://${url.hostname}:${url.port || "5672"}`);
  console.log(`Address: ${address}  (KubeMQ pattern=commands, channel=${channel})\n`);

  // Two connections: one per role (separate connections honour the snooping
  // guard — the requester's dynamic node is owned by the requester connection).
  const responder = await Responder.start(address);
  try {
    await runRequester(address);
  } finally {
    await responder.stop();
  }

  console.log("\nDone.");
}

// =============================================================================
// RESPONDER — receives commands on commands/<ch>, replies via an anonymous sender.
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

    // ATTACH a receiver on commands/<ch> (a server-sender link — the client
    // consumes requests). Manual credit so we pump requests under credit; the
    // message handler is registered before credit is granted.
    const receiver = await connection.createReceiver({
      source: { address },
      credit_window: 0,
      autoaccept: false,
      autosettle: false,
    });

    // ATTACH an ANONYMOUS sender (null target — target:{}). Each reply sets
    // message.to to the request's reply_to, so the reply routes back to the
    // requester's dynamic node. Replies are unsettled (await the disposition).
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
    ctx.delivery?.accept(); // settle the inbound request (the reply travels out-of-band)
    if (!req || req.reply_to === undefined || req.reply_to === null) {
      console.log("[responder] request with no reply-to; cannot reply");
      return;
    }
    const body = bodyToString(req.body);
    console.log(`[responder] Received command "${body}" (correlation-id=${String(req.correlation_id)})`);

    // Business logic: a command body of "fail" is rejected (executed=false); any
    // other body succeeds (executed=true). BOTH paths send a reply — a command
    // failure must NOT leave the requester waiting (unlike a query, variant #10).
    //
    // NOTE: a COMMAND response carries the EXECUTED/ERROR outcome, not data. The
    // broker's command-response path round-trips executed + error (and the echoed
    // correlation-id) but NOT a reply body — the requester observes an empty
    // command body. Use a QUERY (variant #10) when you need to return a value.
    const ok = body !== "fail";
    const errText = ok ? "" : "command rejected by handler";

    // Echo the correlation-id (fall back to message-id, the connector convention)
    // so the requester can match the reply to its request.
    const correlationId = req.correlation_id ?? req.message_id;

    await this.replySender.send(
      {
        body: `ack:${body}`,
        to: req.reply_to,
        correlation_id: correlationId,
        // A COMMAND reply carries the execution outcome as application-properties.
        application_properties: {
          "x-opt-kubemq-executed": ok,
          "x-opt-kubemq-error": errText,
        },
      },
      { timeoutInSeconds: 10 },
    );
    console.log(`[responder] Replied to "${body}" (executed=${ok}, error="${errText}")`);
  }

  async stop(): Promise<void> {
    await this.replySender.close();
    await this.receiver.close();
    await this.connection.close();
  }
}

// =============================================================================
// REQUESTER — dynamic reply node + sender on commands/<ch>; correlates replies.
// =============================================================================

async function runRequester(address: string): Promise<void> {
  const connection = new Connection(connectionOptions("requester"));
  await connection.open();

  try {
    // ATTACH a DYNAMIC reply node: dynamic:true asks the server to create a
    // transient node and echo its address. We read that server-assigned address
    // and use it as the reply-to on every request. The address field is "" so the
    // server (not the client) names the node. Manual credit; the message handler
    // is wired per-request below (we correlate by correlation-id).
    const replyReceiver = await connection.createReceiver({
      source: { address: "", dynamic: true },
      credit_window: 0,
      autoaccept: false,
      autosettle: false,
    });
    replyReceiver.addCredit(5);
    // After attach, the resolved dynamic node address is on the link.
    const replyNode = replyReceiver.address || replyReceiver.source.address;
    if (!replyNode) {
      throw new Error("[requester] server did not assign a dynamic reply-node address");
    }
    console.log(`[requester] Dynamic reply node: ${replyNode}`);

    // ATTACH a sender on commands/<ch> (a server-receiver link — the client
    // produces requests). Unsettled: each send awaits the connector's accepted
    // DISPOSITION once the request is routed.
    const sender = await connection.createAwaitableSender({ target: { address } });

    // 1. A SUCCESSFUL command: round-trips with executed=true.
    await doRequest(sender, replyReceiver, replyNode, "reboot-node-7", "corr-cmd-1");

    // 2. A FAILED command ("fail"): the responder replies executed=false + an
    //    error text — the requester is NOT left waiting (the key Commands contrast
    //    vs Queries, where a failure delivers nothing and the requester times out).
    await doRequest(sender, replyReceiver, replyNode, "fail", "corr-cmd-2");

    await sender.close();
    await replyReceiver.close();
  } finally {
    await connection.close();
  }
}

/**
 * Sends one command naming the dynamic reply node + a correlation-id, then awaits
 * the correlated reply on the dynamic node and prints the executed/error outcome.
 */
async function doRequest(
  sender: AwaitableSender,
  replyReceiver: Receiver,
  replyNode: string,
  body: string,
  corr: string,
): Promise<void> {
  // Arm the reply waiter BEFORE sending so the out-of-band reply is never missed.
  const replyPromise = awaitReply(replyReceiver, 30_000);

  await sender.send(
    {
      body,
      reply_to: replyNode, // MUST name a node this connection owns (snooping guard)
      correlation_id: corr,
    },
    { timeoutInSeconds: 15 },
  );
  console.log(`[requester] Sent command "${body}" (reply-to=dynamic node, correlation-id=${corr})`);

  const reply = await replyPromise;
  const gotCorr = reply.correlation_id;
  if (String(gotCorr) !== corr) {
    throw new Error(`[requester] correlation-id mismatch: want ${corr} got ${String(gotCorr)}`);
  }
  const props = (reply.application_properties ?? {}) as Record<string, unknown>;
  const executed = props["x-opt-kubemq-executed"] === true;
  const errText = typeof props["x-opt-kubemq-error"] === "string" ? (props["x-opt-kubemq-error"] as string) : "";
  console.log(
    `[requester] Reply for "${body}" (correlation-id=${String(gotCorr)}): executed=${executed} error="${errText}" body="${bodyToString(reply.body)}"`,
  );
}

interface ReplyMessage {
  body: unknown;
  correlation_id?: unknown;
  application_properties?: Record<string, unknown>;
}

/**
 * Resolves with the next reply delivered on the dynamic node, accepting it. A
 * command always replies (success OR failure), so this never times out on the
 * happy path. One credit is granted to replenish what each reply consumes.
 */
function awaitReply(replyReceiver: Receiver, timeoutMs: number): Promise<ReplyMessage> {
  return new Promise<ReplyMessage>((resolve, reject) => {
    const timer = setTimeout(() => {
      replyReceiver.removeListener(ReceiverEvents.message, handler);
      reject(new Error("timed out awaiting reply on the dynamic node"));
    }, timeoutMs);

    const handler = (ctx: EventContext): void => {
      clearTimeout(timer);
      replyReceiver.removeListener(ReceiverEvents.message, handler);
      ctx.delivery?.accept();
      replyReceiver.addCredit(1); // replenish for the next request
      resolve({
        body: ctx.message?.body,
        correlation_id: ctx.message?.correlation_id,
        application_properties: ctx.message?.application_properties as Record<string, unknown> | undefined,
      });
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
// Address: commands/amqp10.examples.commands  (KubeMQ pattern=commands, channel=amqp10.examples.commands)
//
// [responder] Listening on commands/amqp10.examples.commands (anonymous reply sender ready)
// [requester] Dynamic reply node: _amqp10.tmp.<connID>.<uuid>
// [requester] Sent command "reboot-node-7" (reply-to=dynamic node, correlation-id=corr-cmd-1)
// [responder] Received command "reboot-node-7" (correlation-id=<RequestID>)
// [responder] Replied to "reboot-node-7" (executed=true, error="")
// [requester] Reply for "reboot-node-7" (correlation-id=corr-cmd-1): executed=true error="" body=""
// [requester] Sent command "fail" (reply-to=dynamic node, correlation-id=corr-cmd-2)
// [responder] Received command "fail" (correlation-id=<RequestID>)
// [responder] Replied to "fail" (executed=false, error="command rejected by handler")
// [requester] Reply for "fail" (correlation-id=corr-cmd-2): executed=false error="command rejected by handler" body=""
//
// Done.
//
// (The responder sees the connector-stamped RequestID as the delivered request's
// correlation-id, while the requester's reply correlation-id is its ORIGINAL
// corr-cmd-N — the connector echoes the requester's correlation-id back on the
// reply. A COMMAND response carries the executed/error outcome, NOT a body — the
// requester observes an empty command reply body; use a QUERY to return a value.)
//
// (A failed command still delivers a reply — executed=false + error text — so the
// requester is NEVER left waiting. Contrast queries/request-reply, where a failed
// query delivers nothing and the requester simply times out.)
