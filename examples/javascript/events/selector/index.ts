/**
 * Example: events/selector (master-table variant #6)
 *
 * JMS / SQL-92 message selectors over KubeMQ **Events** with the native
 * rhea / rhea-promise client (NO KubeMQ SDK).
 *
 * A receiver attaches to events/<ch> carrying a selector source-filter
 * (rhea's filter.selector), which rhea encodes under the OASIS-standard
 * descriptor 0x0000468C00000004 ("apache.org:selector-filter:string", emitted
 * with the "jms-selector" key the connector also accepts). The connector
 * evaluates the selector against each event's APPLICATION PROPERTIES and
 * delivers ONLY the matching events; non-matching events are silently withheld
 * (copy semantics — they stay available to OTHER subscribers, they are not
 * consumed/discarded).
 *
 * The selector here is:  color = 'red' AND size > 2
 *
 * We publish 5 events and assert exactly 2 are delivered:
 *
 *   match-1      {color:red,  size:5}  delivered
 *   miss-blue    {color:blue, size:9}  color != red
 *   miss-small   {color:red,  size:1}  size not > 2
 *   match-2      {color:red,  size:3}  delivered
 *   miss-nocolor {           size:8}   color IS NULL  (3-valued logic: UNKNOWN => withheld)
 *
 * THREE-VALUED LOGIC: a property that is absent evaluates to NULL, so the
 * predicate is UNKNOWN (not true) and the event is NOT delivered — this is why
 * miss-nocolor is withheld even though it has no color to disqualify it.
 *
 * GOTCHA: a selector is honoured ONLY on events/ and events-store/ consume
 * links. Requesting one on a queues/ source is rejected at ATTACH with
 * amqp:not-implemented ("selector filter not supported on this address") —
 * this program demonstrates that rejection at the end.
 *
 * Grounded in connector test TestEventsSelector
 * (connectors/amqp10/integration_pubsub_test.go) and the selector-on-queues
 * rejection in connectors/amqp10/link.go (applySourceSelector).
 *
 * Run:
 *   export KUBEMQ_AMQP_URL=amqp://localhost:5672
 *   npx tsx events/selector/index.ts
 */
import {
  Connection,
  ConnectionEvents,
  filter,
  ReceiverEvents,
  SenderEvents,
  type AmqpError,
  type ConnectionOptions,
  type CreateReceiverOptions,
  type EventContext,
  type Receiver,
  type Sender,
} from "rhea-promise";

const channel = "amqp10.examples.selector";

// selector is a standard SQL-92 / JMS message selector evaluated against each
// event's application properties.
const selector = "color = 'red' AND size > 2";

// standingCredit is granted up front so the subscriber is never at 0 credit when
// a matching event arrives (events are at-most-once; 0-credit => silent drop).
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
      container_id: `kubemq-amqp10-js-selector-${process.pid}`,
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

interface EventSpec {
  body: string;
  props: Record<string, unknown>;
  match: boolean;
  why: string;
}

async function main(): Promise<void> {
  const { options, host, port } = connectionOptions();
  const address = `events/${channel}`;
  console.log(`Broker:   amqp://${host}:${port}`);
  console.log(`Address:  ${address}  (KubeMQ pattern=events, channel=${channel})`);
  console.log(`Selector: ${selector}\n`);

  const connection = new Connection(options);
  // The selector-on-queues gotcha below makes the connector DETACH the bad
  // attach. rhea may surface the teardown as a raw, connection-level "error"
  // event; without a listener Node would crash with an unhandled-error. We
  // swallow it here and detect the rejection on the receiver link itself (see
  // attachOrRejection in step 4).
  connection.on(ConnectionEvents.error, () => {
    /* swallowed — the rejection is detected on the receiver link in step 4 */
  });
  await connection.open();

  try {
    // =======================================================================
    // 1. SUBSCRIBE FIRST with the selector filter. rhea encodes the selector as
    //    the OASIS selector-filter descriptor. A successful createReceiver means
    //    the connector accepted the filter — a parse error or unsupported
    //    pattern would have DETACHed the link. Events have no replay, so we
    //    subscribe before publishing. The handler is registered before credit.
    // =======================================================================
    const matches = new Set<string>();
    const wantMatches = 2;
    const receiver = await connection.createReceiver({
      source: { address, filter: filter.selector(selector) },
      credit_window: 0,
      autoaccept: false,
      autosettle: false,
    });
    const matched = collectMatches(receiver, wantMatches, (body) => {
      console.log(`[recv] delivered: ${body}`);
      matches.add(body);
    }, standingCredit, 15_000);
    console.log(`[recv] Subscribed to ${address} with selector filter (standing credit ${standingCredit})`);

    // Wait for the connector's subscription pump to go live before publishing.
    await sleep(750);
    console.log("[recv] Subscription pump settled (waited 750ms before publishing)");

    // =======================================================================
    // 2. PUBLISH 5 events with application properties. The sender is pre-settled
    //    (fire-and-forget). The connector evaluates the selector against each
    //    event's application properties on the delivery path.
    // =======================================================================
    const sender = await connection.createSender({
      target: { address },
      snd_settle_mode: 1,
      autosettle: true,
    });

    const events: EventSpec[] = [
      { body: "match-1", props: { color: "red", size: 5 }, match: true, why: "color=red AND size>2" },
      { body: "miss-blue", props: { color: "blue", size: 9 }, match: false, why: "color!=red" },
      { body: "miss-small", props: { color: "red", size: 1 }, match: false, why: "size not > 2" },
      { body: "match-2", props: { color: "red", size: 3 }, match: true, why: "color=red AND size>2" },
      { body: "miss-nocolor", props: { size: 8 }, match: false, why: "color IS NULL => UNKNOWN (3-valued)" },
    ];

    for (const e of events) {
      await waitSendable(sender, 15_000);
      sender.send({ body: e.body, application_properties: e.props });
      const propsText = JSON.stringify(e.props);
      const verdict = e.match ? "should MATCH" : "should be FILTERED OUT";
      console.log(`[send] ${e.body.padEnd(13)}${propsText.padEnd(28)} -> ${verdict} (${e.why})`);
    }
    await sender.close();

    // =======================================================================
    // 3. RECEIVE only the matching events. Drain exactly wantMatches; then prove
    //    nothing else arrives (the non-matching events were silently withheld).
    // =======================================================================
    await matched;

    const leaked = await expectNoMessage(receiver, 2_000);
    if (leaked) {
      throw new Error("selector leak: an extra (non-matching) event was delivered");
    }
    const nonMatching = events.length - wantMatches;
    console.log(`[recv] Received exactly ${matches.size} matching event(s); ${nonMatching} non-matching event(s) were silently withheld`);

    await receiver.close();

    // =======================================================================
    // 4. GOTCHA demo — a selector on a queues/ source is rejected at ATTACH.
    //    Selectors are honoured ONLY on events/ and events-store/ consume links;
    //    on queues/ (move-only) the connector DETACHes with amqp:not-implemented.
    // =======================================================================
    console.log("");
    const queueAddress = `queues/${channel}.q`;

    // rhea QUIRK: the connector refuses a bad attach the canonical AMQP §2.6.3
    // way — it replies with an ATTACH carrying NO source/target, then DETACHes
    // with the condition. rhea treats that reply-ATTACH as "open" and RESOLVES
    // createReceiver anyway (it does NOT reject the promise), then surfaces the
    // refusal on the receiver itself: receiver.source stays null, isOpen() is
    // false, and a `receiver_error` / `receiver_close` carrying the condition
    // arrives a tick later. So a resolved createReceiver does NOT mean the link
    // attached — we must inspect the link afterwards. attachOrRejection() below
    // does exactly that, returning the AMQP error when the attach was refused.
    const rejection = await attachOrRejection(connection, {
      source: { address: queueAddress, filter: filter.selector(selector) },
      credit_window: 0,
      autoaccept: false,
      autosettle: false,
    });
    if (!rejection) {
      throw new Error(`expected the selector on ${queueAddress} to be rejected, but the attach succeeded`);
    }
    console.log(`[gotcha] Selector on ${queueAddress} correctly REJECTED at ATTACH:`);
    console.log(`         ${describeAmqpError(rejection)}`);
    console.log("         (selectors are supported only on events/ and events-store/ — queues/ is move-only)");
  } finally {
    // The connection may already be torn down by the gotcha-demo detach; closing
    // a half-closed connection can reject, which is fine here.
    try {
      await connection.close();
    } catch {
      /* connection already closed by the rejected attach */
    }
  }

  console.log("\nDone.");
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
        finish(receiver.error ?? { condition: "amqp:not-implemented", description: "attach refused" });
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
        finish(attached ? undefined : (receiver.error ?? { condition: "amqp:not-implemented", description: "attach refused (no error surfaced)" }));
      }, 1_500);
    }, reject);
  });
}

/**
 * Subscribes with standing credit and collects matching deliveries until `want`
 * have arrived. Credit is topped up as messages arrive. The handler is attached
 * before the initial credit grant.
 */
function collectMatches(
  receiver: Receiver,
  want: number,
  onMatch: (body: string) => void,
  credit: number,
  timeoutMs: number,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let got = 0;
    const timer = setTimeout(() => {
      receiver.removeListener(ReceiverEvents.message, handler);
      reject(new Error(`timed out after ${got}/${want} matching events`));
    }, timeoutMs);

    const handler = (ctx: EventContext): void => {
      const body = bodyToString(ctx.message?.body);
      ctx.delivery?.accept(); // no-op for pre-settled fan-out
      onMatch(body);
      got++;
      if (got >= want) {
        clearTimeout(timer);
        receiver.removeListener(ReceiverEvents.message, handler);
        resolve();
        return;
      }
      receiver.addCredit(1);
    };

    receiver.on(ReceiverEvents.message, handler);
    receiver.addCredit(credit);
  });
}

function expectNoMessage(receiver: Receiver, timeoutMs: number): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    const timer = setTimeout(() => {
      receiver.removeListener(ReceiverEvents.message, handler);
      resolve(false);
    }, timeoutMs);

    const handler = (ctx: EventContext): void => {
      clearTimeout(timer);
      receiver.removeListener(ReceiverEvents.message, handler);
      ctx.delivery?.accept();
      resolve(true);
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
// Broker:   amqp://localhost:5672
// Address:  events/amqp10.examples.selector  (KubeMQ pattern=events, channel=amqp10.examples.selector)
// Selector: color = 'red' AND size > 2
//
// [recv] Subscribed to events/amqp10.examples.selector with selector filter (standing credit 100)
// [recv] Subscription pump settled (waited 750ms before publishing)
// [send] match-1      {"color":"red","size":5}    -> should MATCH (color=red AND size>2)
// [send] miss-blue    {"color":"blue","size":9}   -> should be FILTERED OUT (color!=red)
// [send] miss-small   {"color":"red","size":1}    -> should be FILTERED OUT (size not > 2)
// [send] match-2      {"color":"red","size":3}    -> should MATCH (color=red AND size>2)
// [send] miss-nocolor {"size":8}                  -> should be FILTERED OUT (color IS NULL => UNKNOWN (3-valued))
// [recv] delivered: match-1
// [recv] delivered: match-2
// [recv] Received exactly 2 matching event(s); 3 non-matching event(s) were silently withheld
//
// [gotcha] Selector on queues/amqp10.examples.selector.q correctly REJECTED at ATTACH:
//          amqp:not-implemented: selector filter not supported on this address
//          (selectors are supported only on events/ and events-store/ — queues/ is move-only)
//
// Done.
