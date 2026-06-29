// Example: events-store/durable-replay (master-table variant #7)
//
// Durable subscriptions with resume over KubeMQ Events Store using the native
// AMQPNetLite.Core client (Connection / Session / SenderLink / ReceiverLink).
// NO KubeMQ SDK.
//
// Unlike Events (fire-and-forget, no replay), Events Store PERSISTS the stream
// and lets a DURABLE subscriber resume where it left off. A durable subscription
// is identified by the pair:
//
//     (connection container-id, link name)
//
// To make a subscriber durable and resumable:
//
//   - connect with a STABLE container-id -> ConnectionFactory.AMQP.ContainerId
//   - attach with a STABLE link name     -> Attach.LinkName = "durable-sub"
//   - request a non-expiring source      -> Source.ExpiryPolicy = Symbol("never")
//   - set the start position once         -> Attach.Properties["x-opt-kubemq-start"] = "new-only"
//
// On a clean disconnect the connector preserves the durable position. Re-attaching
// with the SAME (container-id, link name) RESUMES the subscription and delivers
// every event published while the subscriber was away — no loss, no replay of
// already-consumed events.
//
// Flow:
//  1. Connect with container-id "amqp10-examples-durable-container"; attach durable
//     receiver "durable-sub" (start new-only). Publish 3 events; receive all 3.
//  2. Disconnect (close the connection).
//  3. Publish 5 MORE events while the durable subscriber is away.
//  4. Re-connect with the SAME container-id; re-attach "durable-sub". The
//     subscription RESUMES and delivers the 5 missed events.
//
// Grounded in connector test TestEventsStoreDurableReplay
// (connectors/amqp10/integration_pubsub_test.go).
//
// Run:
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   cd events-store/durable-replay && dotnet run

using System.Text;
using Amqp;
using Amqp.Framing;
using Amqp.Types;

const string channel = "amqp10.examples.durable";

// The durable identity = (containerID, linkName). Both MUST be stable across
// reconnects for the subscription to resume.
const string containerId = "amqp10-examples-durable-container";
const string linkName = "durable-sub";

const int standingCredit = 100;

// "never" is the AMQP terminus-expiry-policy symbol that asks the connector to
// keep the durable source (and its cursor) alive across detach/disconnect.
var expiryNever = new Symbol("never");

static string AmqpUrl() =>
    Environment.GetEnvironmentVariable("KUBEMQ_AMQP_URL") is { Length: > 0 } v
        ? v
        : "amqp://localhost:5672";

var addr = "events-store/" + channel;
Console.WriteLine($"Broker:        {AmqpUrl()}");
Console.WriteLine($"Address:       {addr}  (KubeMQ pattern=events-store, channel={channel})");
Console.WriteLine($"Durable id:    container-id=\"{containerId}\"  link-name=\"{linkName}\"");
Console.WriteLine();

// =========================================================================
// 0. PRODUCER — a separate, plain connection that publishes to the
//    events-store stream throughout the demo (it does not need a stable id).
// =========================================================================
var prodConnection = await Connection.Factory.CreateAsync(new Address(AmqpUrl()));
var prodSession = new Session(prodConnection);
var sender = new SenderLink(prodSession, "durable-producer", addr);

// publish sends events es-<lo>..es-<hi-1> (unsettled — events-store persists each
// accepted transfer).
void Publish(int lo, int hi)
{
    for (var i = lo; i < hi; i++)
    {
        var body = $"es-{i:D3}";
        var message = new Message { BodySection = new Data { Binary = Encoding.UTF8.GetBytes(body) } };
        sender.Send(message, TimeSpan.FromSeconds(15));
    }
}

// attachDurable connects with the stable container-id and attaches the durable
// receiver (stable link name + non-expiring source + start position). The returned
// Connection must be closed to disconnect the subscriber.
(ReceiverLink Receiver, Connection Connection) AttachDurable(string phase)
{
    // A fresh ConnectionFactory carrying the STABLE container-id — half the durable
    // identity. (AMQPNetLite otherwise generates a random container-id per connect.)
    var factory = new ConnectionFactory();
    factory.AMQP.ContainerId = containerId;
    var connection = factory.CreateAsync(new Address(AmqpUrl())).GetAwaiter().GetResult();
    var session = new Session(connection);

    var attach = new Attach
    {
        // ExpiryPolicy "never" => the connector keeps the durable source alive.
        Source = new Source { Address = addr, ExpiryPolicy = expiryNever },
        Target = new Target(),
        // Stable link name = the OTHER half of the durable identity.
        LinkName = linkName,
        // x-opt-kubemq-start sets the cursor on FIRST attach; new-only = "from now".
        Properties = new Fields { { new Symbol("x-opt-kubemq-start"), "new-only" } },
    };
    var receiver = new ReceiverLink(session, linkName, attach, null);
    receiver.SetCredit(standingCredit, autoRestore: true);
    Console.WriteLine($"[recv] Durable receiver attached ({phase}): container-id=\"{containerId}\" name=\"{linkName}\" expiry=never");

    // Let the connector's subscription pump go live before producing.
    Thread.Sleep(750);
    return (receiver, connection);
}

// drain receives up to max events within window, returning their bodies.
List<string> Drain(ReceiverLink receiver, int max, TimeSpan window)
{
    var outp = new List<string>(max);
    var deadline = DateTime.UtcNow + window;
    while (outp.Count < max && DateTime.UtcNow < deadline)
    {
        var message = receiver.Receive(TimeSpan.FromSeconds(2));
        if (message is null)
            continue;
        receiver.Accept(message);
        outp.Add(BodyString(message));
    }
    return outp;
}

try
{
    // =====================================================================
    // 1. DURABLE SUBSCRIBE (first attach). Stable container-id + link name +
    //    non-expiring source make this subscription durable. start=new-only means
    //    "deliver events from now on" (this attach establishes the cursor).
    // =====================================================================
    var (durRcv, durConn) = AttachDurable("first attach");
    Publish(0, 3); // 3 events while the durable subscriber is live

    var first = Drain(durRcv, 3, TimeSpan.FromSeconds(30));
    if (first.Count != 3)
        throw new InvalidOperationException($"durable subscriber expected the first 3 events, got {first.Count}: [{string.Join(" ", first)}]");
    Console.WriteLine($"[recv] First attach received {first.Count} events: [{string.Join(" ", first)}]");
    Console.WriteLine();

    // =====================================================================
    // 2. DISCONNECT. A clean Close detaches the durable link; the connector
    //    preserves the durable cursor for this (container-id, link name).
    // =====================================================================
    await durConn.CloseAsync();
    Console.WriteLine("[conn] Durable subscriber DISCONNECTED (cursor preserved by the connector)");
    await Task.Delay(1000); // let the detach + unsubscribe settle

    // =====================================================================
    // 3. PUBLISH WHILE AWAY. 5 more events arrive at the persisted stream while
    //    the durable subscriber is offline.
    // =====================================================================
    Publish(3, 8);
    Console.WriteLine("[send] Published 5 more events WHILE the durable subscriber was away");

    // =====================================================================
    // 4. RE-ATTACH with the SAME durable identity. The subscription RESUMES and
    //    delivers exactly the 5 events published while away (not the first 3
    //    again, and nothing lost).
    // =====================================================================
    var (durRcv2, durConn2) = AttachDurable("re-attach");
    try
    {
        var resumed = Drain(durRcv2, 5, TimeSpan.FromSeconds(30));
        var resumedSet = new HashSet<string>(resumed);
        for (var i = 3; i < 8; i++)
        {
            var body = $"es-{i:D3}";
            if (!resumedSet.Contains(body))
                throw new InvalidOperationException($"durable resume missing event {body} (got [{string.Join(" ", resumed)}])");
        }
        Console.WriteLine($"[recv] Re-attach RESUMED and received the {resumedSet.Count} events published while away: [{string.Join(" ", resumed)}]");
        Console.WriteLine("[recv] No loss, no re-delivery of the already-consumed first 3 — the durable cursor resumed exactly");

        await durRcv2.CloseAsync();
    }
    finally
    {
        await durConn2.CloseAsync();
    }
}
finally
{
    await sender.CloseAsync();
    await prodSession.CloseAsync();
    await prodConnection.CloseAsync();
}

Console.WriteLine();
Console.WriteLine("Done.");

// BodyString extracts the UTF-8 payload from whichever body section the connector
// delivered. KubeMQ events-store may arrive as either a Data section (binary) or an
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
// Broker:        amqp://localhost:5672
// Address:       events-store/amqp10.examples.durable  (KubeMQ pattern=events-store, channel=amqp10.examples.durable)
// Durable id:    container-id="amqp10-examples-durable-container"  link-name="durable-sub"
//
// [recv] Durable receiver attached (first attach): container-id="amqp10-examples-durable-container" name="durable-sub" expiry=never
// [recv] First attach received 3 events: [es-000 es-001 es-002]
//
// [conn] Durable subscriber DISCONNECTED (cursor preserved by the connector)
// [send] Published 5 more events WHILE the durable subscriber was away
// [recv] Durable receiver attached (re-attach): container-id="amqp10-examples-durable-container" name="durable-sub" expiry=never
// [recv] Re-attach RESUMED and received the 5 events published while away: [es-003 es-004 es-005 es-006 es-007]
// [recv] No loss, no re-delivery of the already-consumed first 3 — the durable cursor resumed exactly
//
// Done.
//
// NOTE: durable subscriptions are NODE-LOCAL (see README gotcha). In a cluster the
// durable cursor lives on the node that owned the original attach; reconnect to the
// SAME node (or run a single-node dev broker, as here) to resume.
