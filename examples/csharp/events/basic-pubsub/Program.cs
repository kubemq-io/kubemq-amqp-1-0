// Example: events/basic-pubsub (master-table variant #4)
//
// Fan-out, at-most-once pub/sub over KubeMQ Events with the native
// AMQPNetLite.Core client. NO KubeMQ SDK.
//
// Events are a fire-hose: deliveries are pre-settled (no DISPOSITION feedback),
// there is NO replay, and a message that arrives at a subscriber with zero
// credit is SILENTLY DROPPED (counted by the server metric
// kubemq_amqp10_events_dropped_no_credit_total). Two rules follow:
//
//   - SUBSCRIBE BEFORE PUBLISH. The attach reply only confirms the link, not
//     that the connector's subscription pump is live. A publish that races the
//     subscription is lost (no replay). This example waits ~750ms after attach
//     before producing.
//   - GRANT STANDING CREDIT. The receiver attaches with a large standing credit
//     (SetCredit auto-restores as messages settle) so the subscriber is never at
//     0 credit when an event arrives.
//
// The sender publishes pre-settled to events/<ch> (fire-and-forget); the
// receiver drains every event on the happy path.
//
// Grounded in connector test TestEventsPubSubGroupFanout (the lone-subscriber
// fan-out leg) (connectors/amqp10/integration_pubsub_test.go).
//
// Run:
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   cd events/basic-pubsub && dotnet run

using System.Text;
using Amqp;
using Amqp.Framing;

const string channel = "amqp10.examples.pubsub";
const int total = 20;

// standingCredit is granted up front so the subscriber is never at 0 credit when
// an event arrives. SetCredit(autoRestore: true) auto-replenishes as deliveries
// settle.
const int standingCredit = 100;

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
    var session = new Session(connection);

    // =====================================================================
    // 1. SUBSCRIBE FIRST. Attach the receiver with standing credit BEFORE any
    //    publish. Events have no replay — a publish that beats the subscription
    //    is lost forever.
    // =====================================================================
    var receiver = new ReceiverLink(session, "pubsub-receiver", addr);
    receiver.SetCredit(standingCredit, autoRestore: true);
    Console.WriteLine($"[recv] Subscribed to {addr} with standing credit {standingCredit}");

    // The attach reply confirms the link, not that the connector's subscription
    // pump has run its SubscribeEvents yet. Wait for the pump to go live before
    // publishing, or the first events race the subscription and are dropped.
    await Task.Delay(750);
    Console.WriteLine("[recv] Subscription pump settled (waited 750ms before publishing)");

    // =====================================================================
    // 2. PUBLISH pre-settled. The sender marks every TRANSFER as settled
    //    (fire-and-forget) — events are at-most-once, so there is no DISPOSITION
    //    to await and no produce confirmation. The mode is set on the ATTACH
    //    frame (SndSettleMode.Settled).
    // =====================================================================
    var senderAttach = new Attach
    {
        Source = new Source(),
        Target = new Target { Address = addr },
        SndSettleMode = SenderSettleMode.Settled,
    };
    var sender = new SenderLink(session, "pubsub-sender", senderAttach, null);
    for (var i = 0; i < total; i++)
    {
        var body = $"event-{i:D3}";
        var message = new Message { BodySection = new Data { Binary = Encoding.UTF8.GetBytes(body) } };
        sender.Send(message, TimeSpan.FromSeconds(15));
    }
    Console.WriteLine($"[send] Published {total} events (pre-settled, fire-and-forget)");

    // =====================================================================
    // 3. RECEIVE. With standing credit the subscriber drains every event. Accept
    //    is a no-op on pre-settled pub/sub deliveries but is harmless.
    // =====================================================================
    var seen = new HashSet<string>();
    while (seen.Count < total)
    {
        var message = receiver.Receive(TimeSpan.FromSeconds(30));
        if (message is null)
            throw new InvalidOperationException($"receive timed out ({seen.Count}/{total})");

        receiver.Accept(message); // no-op for pre-settled fan-out
        seen.Add(BodyString(message));
    }
    Console.WriteLine($"[recv] Received all {seen.Count} events (continuous credit => no 0-credit drop)");

    await sender.CloseAsync();
    await receiver.CloseAsync();
    await session.CloseAsync();
}
finally
{
    await connection.CloseAsync();
}

Console.WriteLine();
Console.WriteLine("Done.");

// BodyString extracts the UTF-8 payload from whichever body section the connector
// delivered. KubeMQ events may arrive as either a Data section (binary) or an
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
// Broker:  amqp://localhost:5672
// Address: events/amqp10.examples.pubsub  (KubeMQ pattern=events, channel=amqp10.examples.pubsub)
//
// [recv] Subscribed to events/amqp10.examples.pubsub with standing credit 100
// [recv] Subscription pump settled (waited 750ms before publishing)
// [send] Published 20 events (pre-settled, fire-and-forget)
// [recv] Received all 20 events (continuous credit => no 0-credit drop)
//
// Done.
//
// (Events are at-most-once with no replay: if the subscriber were at 0 credit
// when an event arrived, that event would be SILENTLY DROPPED and counted on the
// server metric kubemq_amqp10_events_dropped_no_credit_total — never surfaced as
// a client error. Standing credit + subscribe-before-publish avoid both losses.)
