/**
 * Example: events-store/start-positions (master-table variant #8)
 *
 * The x-opt-kubemq-start receiver link property over KubeMQ **Events Store** using
 * the native rhea / rhea-promise client (NO KubeMQ SDK).
 *
 * Events Store persists the stream, so a (non-durable) subscriber can choose WHERE
 * in the history to start consuming via the x-opt-kubemq-start receiver link
 * property. The full grammar (parsed by the connector's parseEventsStoreStart):
 *
 *   (absent) / "new-only"  -> deliver only events published AFTER attach
 *   "first"                -> replay the ENTIRE history from the beginning
 *   "last"                 -> start at the last stored event
 *   "sequence:<n>"         -> start at store sequence n (1-BASED; sequence 1 = the
 *                             first stored event — the connector passes n straight
 *                             to NATS streaming's StartAtSequence)
 *   "time:<RFC3339|secs>"  -> start at a wall-clock instant (RFC3339 or unix-seconds)
 *   "time-delta:<secs>"    -> start <secs> seconds ago (relative to now)
 *
 * IMPORTANT — time encoding: the client sends a `time:` value as RFC3339 OR as unix
 * SECONDS; the connector parses BOTH to the same instant and the broker stores the
 * cursor as unix NANOSECONDS. `time-delta:` is seconds verbatim. A malformed value
 * (e.g. "sequence:abc", "whenever") is rejected at ATTACH with amqp:invalid-field.
 * There is NO native "last N by count" form — to read the tail, compute a sequence
 * or a time window.
 *
 * This program seeds 6 events, then demonstrates four start positions on fresh
 * (non-durable) receivers against the SAME persisted stream:
 *
 *   first              -> all 6 (full replay)
 *   sequence:4         -> from the 4th stored event onward (1-based ⇒ es-003,004,005)
 *   time-delta:3600    -> all 6 (all were published within the last hour)
 *   new-only           -> none of the existing 6; only events published after attach
 *
 * Grounded in connector tests TestEventsStoreDurableReplay (the start:first leg)
 * and TestParseEventsStoreStart (connectors/amqp10/link_pubsub_test.go), and the
 * grammar in connectors/amqp10/link.go (parseEventsStoreStart).
 *
 * Run:
 *   export KUBEMQ_AMQP_URL=amqp://localhost:5672
 *   npx tsx events-store/start-positions/index.ts
 */
import {
  Connection,
  ConnectionEvents,
  ReceiverEvents,
  type AmqpError,
  type ConnectionOptions,
  type CreateReceiverOptions,
  type EventContext,
  type Receiver,
} from "rhea-promise";

// A fresh channel per run keeps the sequence numbers deterministic (this demo
// reads by absolute sequence, which is per-channel and monotonic from 1).
const channel = `amqp10.examples.startpos.${Date.now()}`;

const startProp = "x-opt-kubemq-start";
const seedCount = 6;
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
      container_id: `kubemq-amqp10-js-startpos-${process.pid}`,
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

async function main(): Promise<void> {
  const { options, host, port } = connectionOptions();
  const address = `events-store/${channel}`;
  console.log(`Broker:  amqp://${host}:${port}`);
  console.log(`Address: ${address}  (KubeMQ pattern=events-store, channel=${channel})\n`);

  const connection = new Connection(options);
  // A malformed start value makes the connector DETACH the bad attach. rhea may
  // surface the teardown as a raw, connection-level "error" event; without a
  // listener Node would crash with an unhandled-error. We swallow it here and
  // detect the rejection on the receiver link itself (see attachOrRejection in
  // the malformed-start demo below).
  connection.on(ConnectionEvents.error, () => {
    /* swallowed — the rejection is detected on the receiver link below */
  });
  await connection.open();

  try {
    // =======================================================================
    // 0. SEED — publish 6 events into the persisted events-store stream. They are
    //    stored at 1-based sequences 1..6 (per-channel, monotonic). The
    //    AwaitableSender awaits the connector's accepted DISPOSITION per send.
    // =======================================================================
    const sender = await connection.createAwaitableSender({ target: { address } });
    for (let i = 0; i < seedCount; i++) {
      await sender.send({ body: `es-${String(i).padStart(3, "0")}` }, { timeoutInSeconds: 15 });
    }
    await sender.close();
    console.log(`[seed] Published ${seedCount} events (stored at 1-based sequences 1..${seedCount})\n`);

    // =======================================================================
    // 1. start=first → FULL REPLAY (all 6 events from the beginning).
    // =======================================================================
    let got = await readFrom(connection, address, "first", seedCount, 15_000);
    expectExactly(got, ["es-000", "es-001", "es-002", "es-003", "es-004", "es-005"], "first");
    console.log(`[start=first]           replayed full history: [${got.join(" ")}]`);

    // =======================================================================
    // 2. start=sequence:4 → from the 4th stored event onward. Sequences are
    //    1-BASED (the connector passes the value straight to NATS streaming's
    //    StartAtSequence; sequence 1 = the first event), so the 4th stored event
    //    is es-003, delivering es-003, es-004, es-005.
    // =======================================================================
    got = await readFrom(connection, address, "sequence:4", seedCount, 15_000);
    expectExactly(got, ["es-003", "es-004", "es-005"], "sequence:4");
    console.log(`[start=sequence:4]      from the 4th stored event (1-based): [${got.join(" ")}]`);

    // =======================================================================
    // 3. start=time-delta:3600 → everything from the last hour (all 6, since the
    //    seed was published seconds ago). time-delta is SECONDS verbatim.
    // =======================================================================
    got = await readFrom(connection, address, "time-delta:3600", seedCount, 15_000);
    expectExactly(got, ["es-000", "es-001", "es-002", "es-003", "es-004", "es-005"], "time-delta:3600");
    console.log(`[start=time-delta:3600] last hour (all 6): [${got.join(" ")}]`);

    // (You can also start at an absolute instant, e.g.
    //   properties: { [startProp]: "time:" + new Date(Date.now() - 3_600_000).toISOString() }
    // or with unix-seconds: "time:1623578400". Both forms resolve to the same
    // instant; the broker stores the cursor as nanoseconds.)

    // =======================================================================
    // 4. start=new-only → NONE of the 6 existing events; only what is published
    //    AFTER this attach. We attach, then publish one more event and prove only
    //    that one is delivered.
    // =======================================================================
    await demoNewOnly(connection, address);

    // =======================================================================
    // 5. GOTCHA — a malformed start value is rejected at ATTACH with
    //    amqp:invalid-field.
    // =======================================================================
    console.log("");
    await demoMalformed(connection, address, "sequence:abc");
    await demoMalformed(connection, address, "whenever");
  } finally {
    try {
      await connection.close();
    } catch {
      /* connection may already be torn down by a rejected attach */
    }
  }

  console.log("\nDone.");
}

/**
 * Opens a fresh (non-durable) receiver at the given start position and drains up
 * to `max` events within `windowMs`, returning their bodies in order. The handler
 * is registered before the standing credit grant.
 */
async function readFrom(
  connection: Connection,
  address: string,
  start: string,
  max: number,
  windowMs: number,
): Promise<string[]> {
  const receiver = await connection.createReceiver({
    source: { address },
    properties: { [startProp]: start },
    credit_window: 0,
    autoaccept: false,
    autosettle: false,
  });
  try {
    return await drain(receiver, max, windowMs);
  } finally {
    await receiver.close();
  }
}

/**
 * Attaches a new-only receiver, then publishes one event and proves ONLY the
 * post-attach event is delivered (the 6 existing events are skipped).
 */
async function demoNewOnly(connection: Connection, address: string): Promise<void> {
  const fresh = "es-new-after-attach";
  const receiver = await connection.createReceiver({
    source: { address },
    properties: { [startProp]: "new-only" },
    credit_window: 0,
    autoaccept: false,
    autosettle: false,
  });

  // Collect: only the post-attach event must arrive; the 6 existing events must
  // NOT. The handler is registered before credit.
  const collected: string[] = [];
  const settled = new Promise<void>((resolve, reject) => {
    // Allow a short grace AFTER the post-attach event to catch any leaked history.
    const timer = setTimeout(() => {
      receiver.removeListener(ReceiverEvents.message, handler);
      resolve();
    }, 3_000);
    const handler = (ctx: EventContext): void => {
      ctx.delivery?.accept();
      const body = bodyToString(ctx.message?.body);
      collected.push(body);
      receiver.addCredit(1);
      if (collected.length > 1) {
        clearTimeout(timer);
        receiver.removeListener(ReceiverEvents.message, handler);
        reject(new Error(`an existing event leaked (new-only must skip history): got ${collected.join(",")}`));
      }
    };
    receiver.on(ReceiverEvents.message, handler);
    receiver.addCredit(standingCredit);
  });

  await sleep(750); // let the new-only cursor settle before publishing

  // Publish one fresh event AFTER the new-only attach.
  const sender = await connection.createAwaitableSender({ target: { address } });
  await sender.send({ body: fresh }, { timeoutInSeconds: 15 });
  await sender.close();

  await settled;
  await receiver.close();

  if (collected.length !== 1 || collected[0] !== fresh) {
    throw new Error(`[start=new-only] expected only [${fresh}] (post-attach), but got [${collected.join(",")}]`);
  }
  console.log(`[start=new-only]        skipped all ${seedCount} existing events; delivered only the post-attach event: [${fresh}]`);
}

/**
 * Proves a bad start value is rejected at ATTACH with amqp:invalid-field (the
 * receiver never attaches). See attachOrRejection for the rhea quirk this works
 * around — a refused attach RESOLVES createReceiver, so the refusal is detected
 * on the receiver link rather than as a promise rejection.
 */
async function demoMalformed(connection: Connection, address: string, badStart: string): Promise<void> {
  const rejection = await attachOrRejection(connection, {
    source: { address },
    properties: { [startProp]: badStart },
    credit_window: 0,
    autoaccept: false,
    autosettle: false,
  });
  if (!rejection) {
    throw new Error(`[malformed] expected start="${badStart}" to be rejected, but the attach succeeded`);
  }
  console.log(`[gotcha] start="${badStart}" correctly REJECTED at ATTACH (${describeAmqpError(rejection)})`);
}

/**
 * Attempts a receiver attach and returns the AMQP error if the connector REFUSED
 * it at attach, or `undefined` if the link genuinely attached.
 *
 * rhea QUIRK: when the connector refuses an attach the canonical AMQP §2.6.3 way
 * (reply ATTACH with NO source/target, then DETACH(closed) with the condition),
 * rhea RESOLVES createReceiver instead of rejecting it. The refusal is observable
 * only on the receiver link AFTERWARDS: the refused link carries no remote source,
 * and a `receiver_error` / `receiver_close` carrying the condition arrives a tick
 * later (the DETACH follows the reply ATTACH essentially immediately). So a
 * resolved createReceiver does NOT mean the link attached.
 *
 * The state at the instant createReceiver resolves is racy (isOpen() can briefly
 * read true before the DETACH is processed, and `error` may not be populated yet),
 * so we DON'T trust a single synchronous snapshot. Instead we register the
 * error/close listeners and then settle: a refusal is signalled by `receiver.error`
 * becoming set or a receiver_close/receiver_error firing. If neither happens within
 * the grace window AND the link is still open with a remote source, the attach was
 * genuine.
 */
function attachOrRejection(
  connection: Connection,
  options: CreateReceiverOptions,
): Promise<AmqpError | Error | undefined> {
  return new Promise((resolve, reject) => {
    connection.createReceiver(options).then((receiver) => {
      let settled = false;
      let timer: ReturnType<typeof setTimeout> | undefined;
      const finish = (err: AmqpError | Error | undefined): void => {
        if (settled) {
          return;
        }
        settled = true;
        if (timer) {
          clearTimeout(timer);
        }
        receiver.removeListener(ReceiverEvents.receiverError, onRefused);
        receiver.removeListener(ReceiverEvents.receiverClose, onRefused);
        // Best-effort tidy-up; on a refusal the link is already detached server-side.
        void receiver.close().catch(() => {
          /* already detached / never attached */
        });
        resolve(err);
      };

      // A refused attach surfaces as a receiver_error / receiver_close carrying the
      // condition. The connector emits the DETACH right after the reply ATTACH.
      const onRefused = (): void => {
        finish(receiver.error ?? { condition: "amqp:invalid-field", description: "attach refused" });
      };
      receiver.on(ReceiverEvents.receiverError, onRefused);
      receiver.on(ReceiverEvents.receiverClose, onRefused);

      // If the error/close already landed before we attached the listeners, surface it.
      if (receiver.error) {
        onRefused();
        return;
      }

      // Otherwise wait a short grace. If the link is still open with a remote source
      // and no error arrived, it genuinely attached (=> undefined). A refused link has
      // no remote source even after the grace.
      timer = setTimeout(() => {
        const attached = receiver.isOpen() && receiver.source != null && receiver.error == null;
        finish(attached ? undefined : (receiver.error ?? { condition: "amqp:invalid-field", description: "attach refused (no error surfaced)" }));
      }, 1_500);
    }, reject);
  });
}

function describeAmqpError(err: unknown): string {
  if (err && typeof err === "object") {
    const e = err as { condition?: string; description?: string; message?: string };
    if (e.condition) {
      return e.description ? `${e.condition}: ${e.description}` : e.condition;
    }
    if (e.message) {
      return e.message;
    }
  }
  return String(err);
}

/**
 * Drains up to `max` events within `windowMs`, accepting each, returning bodies.
 * The handler is registered before the standing credit grant.
 */
function drain(receiver: Receiver, max: number, windowMs: number): Promise<string[]> {
  return new Promise<string[]>((resolve, reject) => {
    const out: string[] = [];
    const timer = setTimeout(() => {
      receiver.removeListener(ReceiverEvents.message, handler);
      resolve(out);
    }, windowMs);

    const handler = (ctx: EventContext): void => {
      try {
        ctx.delivery?.accept();
        out.push(bodyToString(ctx.message?.body));
        if (out.length >= max) {
          clearTimeout(timer);
          receiver.removeListener(ReceiverEvents.message, handler);
          resolve(out);
          return;
        }
        receiver.addCredit(1);
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

function expectExactly(got: string[], want: string[], label: string): void {
  if (got.length !== want.length) {
    throw new Error(`[start=${label}] expected ${want.length} events [${want.join(" ")}], got ${got.length}: [${got.join(" ")}]`);
  }
  const set = new Set(got);
  for (const w of want) {
    if (!set.has(w)) {
      throw new Error(`[start=${label}] missing expected event ${w} (got [${got.join(" ")}])`);
    }
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

// Expected output (the channel suffix is a timestamp, so it varies per run):
//
// Broker:  amqp://localhost:5672
// Address: events-store/amqp10.examples.startpos.<ts>  (KubeMQ pattern=events-store, channel=amqp10.examples.startpos.<ts>)
//
// [seed] Published 6 events (stored at 1-based sequences 1..6)
//
// [start=first]           replayed full history: [es-000 es-001 es-002 es-003 es-004 es-005]
// [start=sequence:4]      from the 4th stored event (1-based): [es-003 es-004 es-005]
// [start=time-delta:3600] last hour (all 6): [es-000 es-001 es-002 es-003 es-004 es-005]
// [start=new-only]        skipped all 6 existing events; delivered only the post-attach event: [es-new-after-attach]
//
// [gotcha] start="sequence:abc" correctly REJECTED at ATTACH (amqp:invalid-field: invalid start sequence: abc)
// [gotcha] start="whenever" correctly REJECTED at ATTACH (amqp:invalid-field: unknown start position: whenever)
//
// Done.
//
// The connector DETACHes the bad attach with amqp:invalid-field (description
// "invalid start sequence: abc" / "unknown start position: whenever"). rhea
// resolves createReceiver even on this canonical §2.6.3 refusal (reply ATTACH
// with no source, then DETACH), so this example detects the rejection by
// inspecting the receiver link (null source / not open) and reads the condition
// off the link's `receiver_error` / `receiver_close` — the link never attaches.
