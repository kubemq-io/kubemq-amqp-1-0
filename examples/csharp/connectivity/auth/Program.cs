// Example: connectivity/auth (master-table variant #13)
//
// The ONE runnable authentication variant. It connects to the KubeMQ AMQP 1.0
// connector with SASL PLAIN — the username is AUDIT-ONLY and the password is a
// KubeMQ JWT — then runs a queues/<ch> round-trip. Driven with the native
// AMQPNetLite.Core client. NO KubeMQ SDK.
//
// Identity precedence (connector contract):
//   - With authentication ENABLED, the JWT in the SASL PLAIN *password* must
//     validate; the ClientID/identity is derived from the verified token. The SASL
//     *username* is recorded for audit (auth.success / auth.failure) only.
//   - With authentication DISABLED (the stock dev-broker default), the SASL PLAIN
//     *username* becomes the ClientID and any password is accepted; with ANONYMOUS,
//     a default identity is used.
//
// CLONE-AND-RUN behavior: a stock dev broker has authentication OFF and accepts
// ANONYMOUS, so this example reads the credentials from the environment and falls
// back to ANONYMOUS when they are unset — it runs cleanly either way.
//
//     KUBEMQ_AMQP_USER  — SASL PLAIN username (audit identity; defaults to a label)
//     KUBEMQ_AMQP_JWT   — SASL PLAIN password = a KubeMQ JWT (required to use PLAIN)
//
// If KUBEMQ_AMQP_JWT is set, the example dials SASL PLAIN; otherwise it dials
// ANONYMOUS and prints a clear note.
//
// HOW SASL PLAIN IS SELECTED IN AMQPNetLite.Core: the public path is the userinfo on
// the Address — when an Address carries a User + Password, the client negotiates SASL
// PLAIN; with neither it negotiates ANONYMOUS. (Amqp.Sasl.SaslPlainProfile is
// internal, so we drive PLAIN through the Address, which avoids URL-encoding a JWT.)
//
// Grounded in connector tests TestAuthorizationReadDenied and
// TestAuthenticationBadCredential (connectors/amqp10/integration_test.go).
//
// Run (ANONYMOUS, stock dev broker):
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   cd connectivity/auth && dotnet run
//
// Run (SASL PLAIN with a KubeMQ JWT, auth-enabled broker):
//   export KUBEMQ_AMQP_USER=my-service
//   export KUBEMQ_AMQP_JWT=<a-kubemq-jwt>
//   cd connectivity/auth && dotnet run

using System.Text;
using Amqp;
using Amqp.Framing;
using Amqp.Sasl;

// channel is the KubeMQ queue channel; the link address is "queues/" + channel
// (explicit prefix — never rely on a default pattern).
const string channel = "amqp10.examples.auth";

static string AmqpUrl() =>
    Environment.GetEnvironmentVariable("KUBEMQ_AMQP_URL") is { Length: > 0 } v
        ? v
        : "amqp://localhost:5672";

var addr = "queues/" + channel;
Console.WriteLine($"Broker:  {AmqpUrl()}");
Console.WriteLine($"Address: {addr}  (KubeMQ pattern=queues, channel={channel})");

// Choose the SASL mechanism from the environment so the example clone-and-runs on a
// stock dev broker (auth OFF, ANONYMOUS) yet also demonstrates SASL PLAIN with a
// KubeMQ JWT when credentials are provided.
var user = Environment.GetEnvironmentVariable("KUBEMQ_AMQP_USER");
var jwt = Environment.GetEnvironmentVariable("KUBEMQ_AMQP_JWT");

// Parse the broker URL once so we can rebuild an Address that carries userinfo for
// the SASL PLAIN path (the host/port/user/password ctor avoids URL-encoding the JWT).
var baseAddress = new Address(AmqpUrl());

Address connectAddress;
ConnectionFactory? factory = null;
if (!string.IsNullOrEmpty(jwt))
{
    if (string.IsNullOrEmpty(user))
        user = "amqp10-example"; // audit-only label; identity comes from the JWT
    // SASL PLAIN: username is AUDIT-ONLY; password is the KubeMQ JWT. An Address that
    // carries User + Password makes AMQPNetLite negotiate PLAIN.
    connectAddress = new Address(
        baseAddress.Host,
        baseAddress.Port,
        user,
        jwt,
        baseAddress.Path is { Length: > 0 } p ? p : "/",
        baseAddress.Scheme);
    Console.WriteLine($"Auth:    SASL PLAIN  (username=\"{user}\" audit-only; password=<KubeMQ JWT>)");
    Console.WriteLine();
}
else
{
    // Stock dev-broker default: ANONYMOUS. We pin the ANONYMOUS profile explicitly via
    // a ConnectionFactory for clarity (a plain Address with no userinfo also
    // negotiates ANONYMOUS).
    connectAddress = baseAddress;
    factory = new ConnectionFactory();
    factory.SASL.Profile = SaslProfile.Anonymous;
    Console.WriteLine("Auth:    ANONYMOUS  (KUBEMQ_AMQP_JWT unset — falling back to the dev-broker default)");
    Console.WriteLine("         Set KUBEMQ_AMQP_USER + KUBEMQ_AMQP_JWT to dial SASL PLAIN with a KubeMQ JWT.");
    Console.WriteLine();
}

// =========================================================================
// 1. OPEN — the SASL handshake happens here. With auth ENABLED, a JWT that fails
//    validation makes the connect fail (amqp:unauthorized-access at the SASL layer —
//    see TestAuthenticationBadCredential). With auth DISABLED, any credential is
//    accepted.
// =========================================================================
var connection = factory is not null
    ? await factory.CreateAsync(connectAddress)
    : await Connection.Factory.CreateAsync(connectAddress);
try
{
    Console.WriteLine("[open] Connected — SASL handshake accepted");

    var session = new Session(connection);

    // =====================================================================
    // 2. ATTACH + SEND — the WRITE authorization check runs at sender attach / send.
    //    With authorization ENABLED, an identity without a WRITE grant on this
    //    channel is refused with amqp:unauthorized-access (see TestAuthorizationRead-
    //    Denied for the READ-attach counterpart).
    // =====================================================================
    var sender = new SenderLink(session, "auth-sender", addr);
    const string body = "auth-round-trip";
    sender.Send(new Message { BodySection = new Data { Binary = Encoding.UTF8.GetBytes(body) } }, TimeSpan.FromSeconds(15));
    Console.WriteLine($"[send] Produced 1 message to {addr} (accepted)");

    // =====================================================================
    // 3. ATTACH + RECEIVE — the READ authorization check runs at receiver attach. A
    //    denied identity's receiver attach is refused with amqp:unauthorized-access
    //    (TestAuthorizationReadDenied).
    //
    //    NOTE: the sender link is kept OPEN through the consume (closed at the very
    //    end). With this connector + AMQPNetLite, detaching the producer before the
    //    sibling receiver on the same connection has drained can stall delivery to
    //    that receiver — see queues/basic-send-receive's gotcha.
    // =====================================================================
    var receiver = new ReceiverLink(session, "auth-receiver", addr);
    receiver.SetCredit(1, autoRestore: true);
    var message = receiver.Receive(TimeSpan.FromSeconds(30));
    if (message is null)
        throw new InvalidOperationException("receive timed out");
    receiver.Accept(message);
    Console.WriteLine($"[recv] Consumed and accepted 1 message: \"{BodyString(message)}\"");

    await receiver.CloseAsync();
    await sender.CloseAsync();
    await session.CloseAsync();
}
finally
{
    await connection.CloseAsync();
}

Console.WriteLine();
Console.WriteLine("Done.");

// BodyString extracts the UTF-8 payload from whichever body section the connector
// delivered. The message may arrive as either a Data section (binary) or an
// AmqpValue section (binary/string) depending on the producing client, so we
// pattern-match instead of casting unconditionally.
static string BodyString(Message message) => message.BodySection switch
{
    Data d => Encoding.UTF8.GetString(d.Binary),
    AmqpValue { Value: byte[] bytes } => Encoding.UTF8.GetString(bytes),
    AmqpValue { Value: string str } => str,
    AmqpValue v => v.Value?.ToString() ?? string.Empty,
    _ => string.Empty,
};

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
