// Example: advanced/anonymous-terminus (master-table variant #12)
//
// An ANONYMOUS sender (a link attached with a NULL target — an Attach whose Target
// is null) carries no fixed channel. Instead, EACH message selects its own
// destination via its Properties.To field, and the KubeMQ connector routes it
// per-message to the right pattern/channel. One link, many destinations. Driven with
// the native AMQPNetLite.Core client. NO KubeMQ SDK.
//
// Flow:
//   - ATTACH an anonymous sender: a SenderLink built from an Attach with Target=null.
//   - Send #1: Message{Properties:{To: "queues/<ch>"}} routes to a queue.
//   - Send #2: Message{Properties:{To: "events/<ch>"}} routes to an events topic
//     (a subscriber is attached BEFORE the send — events are fire-and-forget).
//   - The queue message is then consumed back to prove it landed correctly.
//   - (Demonstrated as expected errors) a BAD `to` (unknown prefix) and a MISSING
//     `to` are both rejected by the connector: the Send throws an AmqpException
//     carrying amqp:precondition-failed.
//
// Per-message authorization: each anonymous send is authorized for WRITE on the
// resolved (pattern, channel) via the connector's Casbin policy — there is no
// per-link grant for an anonymous terminus.
//
// Grounded in connector test TestAnonymousTerminusRouting
// (connectors/amqp10/integration_test.go).
//
// Run:
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   cd advanced/anonymous-terminus && dotnet run

using System.Text;
using Amqp;
using Amqp.Framing;

// Explicit <pattern>/<channel> destinations selected per-message via Properties.To.
const string queueChannel = "amqp10.examples.anon.q";
const string eventsChannel = "amqp10.examples.anon.e";

static string AmqpUrl() =>
    Environment.GetEnvironmentVariable("KUBEMQ_AMQP_URL") is { Length: > 0 } v
        ? v
        : "amqp://localhost:5672";

var queueTo = "queues/" + queueChannel;
var eventsTo = "events/" + eventsChannel;
Console.WriteLine($"Broker: {AmqpUrl()}");
Console.WriteLine("Anonymous sender (null target) — routes per-message via Properties.To");
Console.WriteLine($"  msg #1 to: {queueTo}");
Console.WriteLine($"  msg #2 to: {eventsTo}");
Console.WriteLine();

var connection = await Connection.Factory.CreateAsync(new Address(AmqpUrl()));
try
{
    var session = new Session(connection);

    // =====================================================================
    // 1. ATTACH an anonymous sender. An Attach with Target=null attaches a link with
    //    a NULL target — there is no bound channel. Every message routes by its own
    //    Properties.To.
    // =====================================================================
    var anonAttach = new Attach { Source = new Source(), Target = null };
    var anon = new SenderLink(session, "anonymous-sender", anonAttach, null);
    Console.WriteLine("[attach] Anonymous sender attached (null target)");

    // A consumer for the EVENTS channel must be subscribed BEFORE we publish to it —
    // events are fire-and-forget (no replay). The queue message, by contrast, is
    // durable, so we consume it after sending.
    var eventRcv = new ReceiverLink(session, "anon-events-receiver", eventsTo);
    eventRcv.SetCredit(5, autoRestore: true);
    // Give the fresh subscription a moment to register before the publish.
    await Task.Delay(500);

    // sendWith issues one Send and returns the connector's outcome: null on success,
    // or the AmqpException (precondition-failed) on a rejected `to`.
    static AmqpException? SendWith(SenderLink sender, Message message)
    {
        try
        {
            sender.Send(message, TimeSpan.FromSeconds(15));
            return null;
        }
        catch (AmqpException ex)
        {
            return ex;
        }
    }

    static Message TextMessage(string body, string? to)
    {
        var message = new Message { BodySection = new Data { Binary = Encoding.UTF8.GetBytes(body) } };
        if (to is not null)
            message.Properties = new Properties { To = to };
        return message;
    }

    // =====================================================================
    // 2. Send #1 — route to a QUEUE via Properties.To. The connector resolves
    //    "queues/<ch>", authorizes WRITE for this connection, and stores it.
    // =====================================================================
    if (SendWith(anon, TextMessage("to-queue", queueTo)) is { } qErr)
        throw new InvalidOperationException($"send to queue failed: {qErr.Error?.Condition} {qErr.Error?.Description}");
    Console.WriteLine($"[send] msg #1 routed to {queueTo} (accepted)");

    // =====================================================================
    // 3. Send #2 — route to an EVENTS topic via Properties.To. Same anonymous link,
    //    a DIFFERENT pattern. The subscriber attached above receives it.
    // =====================================================================
    if (SendWith(anon, TextMessage("to-events", eventsTo)) is { } eErr)
        throw new InvalidOperationException($"send to events failed: {eErr.Error?.Condition} {eErr.Error?.Description}");
    Console.WriteLine($"[send] msg #2 routed to {eventsTo} (accepted)");

    // =====================================================================
    // 4. Negative cases (expected errors) — the connector rejects a bad/missing `to`
    //    with amqp:precondition-failed, surfaced to the client as an AmqpException on
    //    Send. The anonymous link stays usable afterwards.
    // =====================================================================
    const string badTo = "bogus/prefix/x";
    if (SendWith(anon, TextMessage("nowhere", badTo)) is { } badRej)
        Console.WriteLine($"[send] msg with bad `to`=\"{badTo}\" rejected as expected: {badRej.Error?.Condition} ({badRej.Error?.Description})");
    else
        throw new InvalidOperationException("expected a bad `to` to be rejected, but the send succeeded");

    if (SendWith(anon, TextMessage("orphan", to: null)) is { } orphanRej) // NO Properties.To at all
        Console.WriteLine($"[send] msg with NO `to` rejected as expected: {orphanRej.Error?.Condition} ({orphanRej.Error?.Description})");
    else
        throw new InvalidOperationException("expected a missing `to` to be rejected, but the send succeeded");

    // =====================================================================
    // 5. Verify routing — consume the queue message back, and receive the event.
    //    The verification receivers use a SEPARATE session: a connector REJECT
    //    (precondition-failed) settles the rejected delivery with an error on the
    //    anonymous sender's session, and reusing that session for a receive can stall
    //    delivery in AMQPNetLite. A fresh session sidesteps that interaction. The
    //    successful queue + events sends already happened, so both messages are there.
    // =====================================================================
    var verifySession = new Session(connection);
    var qRcv = new ReceiverLink(verifySession, "anon-queue-receiver", queueTo);
    qRcv.SetCredit(1, autoRestore: true);
    var qGot = qRcv.Receive(TimeSpan.FromSeconds(30));
    if (qGot is null)
        throw new InvalidOperationException("receive queue message timed out");
    qRcv.Accept(qGot);
    Console.WriteLine($"[recv] queue {queueTo} delivered: \"{BodyString(qGot)}\"");

    var eGot = eventRcv.Receive(TimeSpan.FromSeconds(30));
    if (eGot is null)
        throw new InvalidOperationException("receive event message timed out");
    Console.WriteLine($"[recv] events {eventsTo} delivered: \"{BodyString(eGot)}\"");

    // Close the anonymous sender + receivers best-effort: a rejected delivery can make
    // AMQPNetLite throw while unwinding the link, but the demo's work is already done.
    try { await qRcv.CloseAsync(); } catch (Exception) { /* best-effort */ }
    try { await eventRcv.CloseAsync(); } catch (Exception) { /* best-effort */ }
    try { await anon.CloseAsync(); } catch (Exception) { /* rejected delivery cleanup */ }
    try { await verifySession.CloseAsync(); } catch (Exception) { /* best-effort */ }
    try { await session.CloseAsync(); } catch (Exception) { /* best-effort */ }
}
finally
{
    // The rejected anonymous delivery above can also disturb the connection teardown;
    // the close is best-effort once the demo has printed its results.
    try { await connection.CloseAsync(); } catch (Exception) { /* link/delivery already gone */ }
}

Console.WriteLine();
Console.WriteLine("Done.");

// BodyString extracts the UTF-8 payload from whichever body section the connector
// delivered. A routed message may arrive as either a Data section (binary) or an
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

// Expected output:
//
// Broker: amqp://localhost:5672
// Anonymous sender (null target) — routes per-message via Properties.To
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
