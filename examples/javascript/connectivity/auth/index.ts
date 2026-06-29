/**
 * Example: connectivity/auth (master-table variant #13)
 *
 * The ONE runnable authentication variant. It connects to the KubeMQ AMQP 1.0
 * connector with SASL PLAIN — the username is AUDIT-ONLY and the password is a
 * KubeMQ JWT — then runs a queues/<ch> round-trip. Driven with the native rhea /
 * rhea-promise client (NO KubeMQ SDK).
 *
 * Identity precedence (connector contract):
 *   - With authentication ENABLED, the JWT in the SASL PLAIN *password* must
 *     validate; the ClientID/identity is derived from the verified token. The SASL
 *     *username* is recorded for audit (auth.success / auth.failure) only.
 *   - With authentication DISABLED (the stock dev-broker default), the SASL PLAIN
 *     *username* becomes the ClientID and any password is accepted; with ANONYMOUS,
 *     a default identity is used.
 *
 * CLONE-AND-RUN behavior: a stock dev broker has authentication OFF and accepts
 * ANONYMOUS, so this example reads the credentials from the environment and falls
 * back to ANONYMOUS when they are unset — it runs cleanly either way.
 *
 *   KUBEMQ_AMQP_USER  — SASL PLAIN username (audit identity; defaults to a label)
 *   KUBEMQ_AMQP_JWT   — SASL PLAIN password = a KubeMQ JWT (required to use PLAIN)
 *
 * If KUBEMQ_AMQP_JWT is set, the example dials SASL PLAIN (rhea negotiates PLAIN
 * when both username + password are present); otherwise it dials ANONYMOUS and
 * prints a clear note.
 *
 * Grounded in connector tests TestAuthorizationReadDenied and
 * TestAuthenticationBadCredential (connectors/amqp10/integration_test.go).
 *
 * Run (ANONYMOUS, stock dev broker):
 *   export KUBEMQ_AMQP_URL=amqp://localhost:5672
 *   npx tsx connectivity/auth/index.ts
 *
 * Run (SASL PLAIN with a KubeMQ JWT, auth-enabled broker):
 *   export KUBEMQ_AMQP_USER=my-service
 *   export KUBEMQ_AMQP_JWT=<a-kubemq-jwt>
 *   npx tsx connectivity/auth/index.ts
 */
import {
  Connection,
  ReceiverEvents,
  type ConnectionOptions,
  type EventContext,
  type Receiver,
} from "rhea-promise";

// channel is the KubeMQ queue channel; the link address is "queues/" + channel
// (explicit prefix — never rely on a default pattern).
const channel = "amqp10.examples.auth";

interface AuthChoice {
  options: ConnectionOptions;
  host: string;
  port: number;
  description: string;
  anonymous: boolean;
}

/**
 * Builds connection options. With a JWT in KUBEMQ_AMQP_JWT it sets username +
 * password so rhea negotiates SASL PLAIN (username audit-only, password = the JWT);
 * otherwise it leaves credentials unset so rhea negotiates ANONYMOUS.
 */
function authChoice(): AuthChoice {
  const raw = process.env["KUBEMQ_AMQP_URL"] ?? "amqp://localhost:5672";
  const url = new URL(raw);
  const host = url.hostname;
  const port = url.port ? Number(url.port) : 5672;

  const jwt = process.env["KUBEMQ_AMQP_JWT"];
  let user = process.env["KUBEMQ_AMQP_USER"];

  const base: ConnectionOptions = {
    host,
    port,
    container_id: `kubemq-amqp10-js-auth-${process.pid}`,
    reconnect: false,
  };

  if (jwt) {
    if (!user) {
      user = "amqp10-example"; // audit-only label; identity comes from the JWT
    }
    // SASL PLAIN: username is AUDIT-ONLY; password is the KubeMQ JWT. Setting both
    // makes rhea select the PLAIN mechanism during the SASL handshake.
    return {
      options: { ...base, username: user, password: jwt },
      host,
      port,
      description: `SASL PLAIN  (username="${user}" audit-only; password=<KubeMQ JWT>)`,
      anonymous: false,
    };
  }

  // Stock dev-broker default: ANONYMOUS (no username/password ⇒ rhea negotiates
  // ANONYMOUS).
  return {
    options: base,
    host,
    port,
    description: "ANONYMOUS  (KUBEMQ_AMQP_JWT unset — falling back to the dev-broker default)",
    anonymous: true,
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

async function main(): Promise<void> {
  const { options, host, port, description, anonymous } = authChoice();
  const address = `queues/${channel}`;
  console.log(`Broker:  amqp://${host}:${port}`);
  console.log(`Address: ${address}  (KubeMQ pattern=queues, channel=${channel})`);
  console.log(`Auth:    ${description}`);
  if (anonymous) {
    console.log("         Set KUBEMQ_AMQP_USER + KUBEMQ_AMQP_JWT to dial SASL PLAIN with a KubeMQ JWT.");
  }
  console.log("");

  // =========================================================================
  // 1. OPEN — the SASL handshake happens here. With auth ENABLED, a JWT that
  //    fails validation makes open() reject (amqp:unauthorized-access at the SASL
  //    layer — see TestAuthenticationBadCredential). With auth DISABLED, any
  //    credential is accepted.
  // =========================================================================
  const connection = new Connection(options);
  try {
    await connection.open();
  } catch (err) {
    console.error(`dial (SASL handshake failed — bad/expired JWT? auth-disabled broker?): ${err instanceof Error ? err.message : String(err)}`);
    process.exit(1);
  }
  console.log("[open] Connected — SASL handshake accepted");

  try {
    // =======================================================================
    // 2. ATTACH + SEND — the WRITE authorization check runs at sender attach /
    //    send. With authorization ENABLED, an identity without a WRITE grant on
    //    this channel is refused with amqp:unauthorized-access (see
    //    TestAuthorizationReadDenied for the READ-attach counterpart).
    // =======================================================================
    const sender = await connection.createAwaitableSender({ target: { address } });
    await sender.send({ body: "auth-round-trip" }, { timeoutInSeconds: 15 });
    await sender.close();
    console.log(`[send] Produced 1 message to ${address} (accepted)`);

    // =======================================================================
    // 3. ATTACH + RECEIVE — the READ authorization check runs at receiver attach.
    //    A denied identity's receiver attach is refused with
    //    amqp:unauthorized-access (TestAuthorizationReadDenied).
    // =======================================================================
    const receiver = await connection.createReceiver({
      source: { address },
      credit_window: 0,
      autoaccept: false,
      autosettle: false,
    });
    const body = await receiveOne(receiver, 30_000);
    await receiver.close();
    console.log(`[recv] Consumed and accepted 1 message: "${body}"`);
  } finally {
    await connection.close();
  }

  console.log("\nDone.");
}

/**
 * Grants 1 credit, waits for one delivery, accepts it (AckRange ⇒ removed), and
 * returns the body string. The handler is registered before credit is granted.
 */
function receiveOne(receiver: Receiver, timeoutMs: number): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    const timer = setTimeout(() => {
      receiver.removeListener(ReceiverEvents.message, handler);
      reject(new Error("timed out waiting for the message"));
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

// Expected output (ANONYMOUS, stock dev broker — no env set):
//
// Broker:  amqp://localhost:5672
// Address: queues/amqp10.examples.auth  (KubeMQ pattern=queues, channel=amqp10.examples.auth)
// Auth:    ANONYMOUS  (KUBEMQ_AMQP_JWT unset — falling back to the dev-broker default)
//          Set KUBEMQ_AMQP_USER + KUBEMQ_AMQP_JWT to dial SASL PLAIN with a KubeMQ JWT.
//
// [open] Connected — SASL handshake accepted
// [send] Produced 1 message to queues/amqp10.examples.auth (accepted)
// [recv] Consumed and accepted 1 message: "auth-round-trip"
//
// Done.
//
// Expected output (SASL PLAIN, KUBEMQ_AMQP_USER + KUBEMQ_AMQP_JWT set):
//
// Broker:  amqp://localhost:5672
// Address: queues/amqp10.examples.auth  (KubeMQ pattern=queues, channel=amqp10.examples.auth)
// Auth:    SASL PLAIN  (username="my-service" audit-only; password=<KubeMQ JWT>)
//
// [open] Connected — SASL handshake accepted
// [send] Produced 1 message to queues/amqp10.examples.auth (accepted)
// [recv] Consumed and accepted 1 message: "auth-round-trip"
//
// Done.
