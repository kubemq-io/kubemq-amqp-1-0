// Example: events/consumer-group (master-table variant #5)
//
// Consumer-group load-balancing over KubeMQ Events with the native
// AMQPNetLite.Core client. NO KubeMQ SDK.
//
// The x-opt-kubemq-group receiver link property places a subscriber in a named
// load-balancing group. Within ONE group, the connector round-robins the event
// stream across the group's members (no duplication). A DISTINCT group is an
// independent virtual-topic subscriber that gets the FULL stream.
//
// This example opens:
//   - g1a, g1b — two receivers in group "g1" => together they receive every
//     event with NO body delivered to both (the group splits the stream).
//   - g2       — one receiver in group "g2" => gets EVERY event (independent).
//
// The group is requested via the ATTACH frame's link Properties map (key
// "x-opt-kubemq-group"), which AMQPNetLite exposes via the Attach constructor.
// Each receiver runs on its own Session: AMQPNetLite sessions/links are not
// concurrency-safe, so we never share one across links.
//
// Grounded in connector test TestEventsPubSubGroupFanout
// (connectors/amqp10/integration_pubsub_test.go).
//
// Run:
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   cd events/consumer-group && dotnet run

using System.Text;
using Amqp;
using Amqp.Framing;
using Amqp.Types;

const string channel = "amqp10.examples.consumergroup";
const int total = 30;
const string groupProp = "x-opt-kubemq-group";
const int standingCredit = 200;

static string AmqpUrl() =>
    Environment.GetEnvironmentVariable("KUBEMQ_AMQP_URL") is { Length: > 0 } v
        ? v
        : "amqp://localhost:5672";

var addr = "events/" + channel;
Console.WriteLine($"Broker:  {AmqpUrl()}");
Console.WriteLine($"Address: {addr}  (KubeMQ pattern=events, channel={channel})");
Console.WriteLine();

var connection = await Connection.Factory.CreateAsync(new Address(AmqpUrl()));
try
{
    // Build one group receiver on its own Session. The x-opt-kubemq-group value
    // is set in the ATTACH link Properties (Fields map keyed by Symbol).
    ReceiverLink MakeGroupReceiver(string label, string group)
    {
        var session = new Session(connection);
        var properties = new Fields { { new Symbol(groupProp), group } };
        var attach = new Attach
        {
            Source = new Source { Address = addr },
            Target = new Target(),
            Properties = properties,
        };
        var receiver = new ReceiverLink(session, $"cg-{label}", attach, null);
        receiver.SetCredit(standingCredit, autoRestore: true);
        return receiver;
    }

    // =====================================================================
    // 1. SUBSCRIBE all three group members FIRST (events have no replay).
    // =====================================================================
    var g1a = MakeGroupReceiver("g1a", "g1");
    var g1b = MakeGroupReceiver("g1b", "g1");
    var g2 = MakeGroupReceiver("g2", "g2");

    // Let the connector's subscription pumps go live before publishing.
    await Task.Delay(750);
    Console.WriteLine("[recv] 3 subscribers attached: g1a+g1b (group g1), g2 (group g2)");

    // =====================================================================
    // 2. PUBLISH on a dedicated session/link. Pre-settled fire-and-forget.
    // =====================================================================
    var producerSession = new Session(connection);
    var senderAttach = new Attach
    {
        Source = new Source(),
        Target = new Target { Address = addr },
        SndSettleMode = SenderSettleMode.Settled,
    };
    var sender = new SenderLink(producerSession, "cg-sender", senderAttach, null);
    for (var i = 0; i < total; i++)
    {
        var body = $"event-{i:D3}";
        var message = new Message { BodySection = new Data { Binary = Encoding.UTF8.GetBytes(body) } };
        sender.Send(message, TimeSpan.FromSeconds(15));
    }
    Console.WriteLine($"[send] Published {total} events (pre-settled)");

    // =====================================================================
    // 3. DRAIN each receiver within a window. A receiver returns null when no
    //    further event arrives before the timeout — that ends its drain.
    // =====================================================================
    static HashSet<string> Drain(ReceiverLink receiver, int max, TimeSpan idleTimeout)
    {
        var got = new HashSet<string>();
        while (got.Count < max)
        {
            var message = receiver.Receive(idleTimeout);
            if (message is null)
                break; // window elapsed / no more messages
            receiver.Accept(message); // no-op for pre-settled fan-out
            got.Add(BodyString(message));
        }
        return got;
    }

    // BodyString extracts the UTF-8 payload from whichever body section the
    // connector delivered. KubeMQ events may arrive as either a Data section
    // (binary) or an AmqpValue section (binary/string) depending on the
    // producing client, so we pattern-match instead of casting unconditionally.
    static string BodyString(Message message) => message.BodySection switch
    {
        Data d => Encoding.UTF8.GetString(d.Binary),
        AmqpValue { Value: byte[] bytes } => Encoding.UTF8.GetString(bytes),
        AmqpValue { Value: string str } => str,
        AmqpValue v => v.Value?.ToString() ?? string.Empty,
        _ => string.Empty,
    };

    var idle = TimeSpan.FromSeconds(3);
    var g2Got = Drain(g2, total, idle);
    var g1aGot = Drain(g1a, total, idle);
    var g1bGot = Drain(g1b, total, idle);

    await g1a.CloseAsync();
    await g1b.CloseAsync();
    await g2.CloseAsync();
    await sender.CloseAsync();

    // =====================================================================
    // 4. Assert the consumer-group semantics.
    // =====================================================================

    // g2 (a distinct group) receives EVERY event.
    if (g2Got.Count != total)
        throw new InvalidOperationException($"group g2 (independent) expected all {total} events, got {g2Got.Count}");
    Console.WriteLine($"[recv] g2 (group g2, independent): {g2Got.Count}/{total} events — FULL stream");

    // g1a + g1b TOGETHER receive every event, with NO body delivered to both.
    var dups = g1aGot.Intersect(g1bGot).Count();
    if (dups != 0)
        throw new InvalidOperationException($"group g1 load-balancing broken: {dups} event(s) delivered to BOTH g1a and g1b");

    var combined = new HashSet<string>(g1aGot);
    combined.UnionWith(g1bGot);
    if (combined.Count != total)
        throw new InvalidOperationException($"group g1 members together expected all {total} events, got {combined.Count}");
    if (g1aGot.Count == 0 || g1bGot.Count == 0)
        throw new InvalidOperationException($"group g1 not load-balanced: g1a={g1aGot.Count} g1b={g1bGot.Count} (one member got nothing)");

    Console.WriteLine($"[recv] g1a (group g1): {g1aGot.Count} events; g1b (group g1): {g1bGot.Count} events");
    Console.WriteLine($"[recv] g1a+g1b together: {combined.Count}/{total} events, 0 duplicates — group SPLIT the stream");
}
finally
{
    await connection.CloseAsync();
}

Console.WriteLine();
Console.WriteLine("Done.");

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
