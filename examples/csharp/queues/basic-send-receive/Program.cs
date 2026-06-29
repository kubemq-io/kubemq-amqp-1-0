// Example: queues/basic-send-receive (master-table variant #1)
//
// At-least-once produce + credit-based consume against the KubeMQ AMQP 1.0
// connector using the native AMQPNetLite.Core client (Connection / Session /
// SenderLink / ReceiverLink). NO KubeMQ SDK.
//
// Flow:
//   - SenderLink -> "queues/<ch>" (unsettled): each Send blocks until the
//     server's receiver DISPOSITION (accepted) confirms the broker stored the
//     message.
//   - ReceiverLink <- "queues/<ch>" with credit 10: Receive + Accept each =>
//     the connector emits an AckRange and removes the message from the queue.
//   - After draining, the queue is empty (a further Receive times out -> null).
//
// Grounded in connector test TestQueueProduceConsumeAtLeastOnce
// (connectors/amqp10/integration_test.go).
//
// Run:
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   cd queues/basic-send-receive && dotnet run

using System.Text;
using Amqp;
using Amqp.Framing;

// channel is the KubeMQ queue channel; the link address is "queues/" + channel
// (explicit prefix — examples never rely on a bare address / DefaultPattern).
const string channel = "amqp10.examples.basic";
const int total = 10;

static string AmqpUrl() =>
    Environment.GetEnvironmentVariable("KUBEMQ_AMQP_URL") is { Length: > 0 } v
        ? v
        : "amqp://localhost:5672";

var addr = "queues/" + channel;
Console.WriteLine($"Broker:  {AmqpUrl()}");
Console.WriteLine($"Address: {addr}  (KubeMQ pattern=queues, channel={channel})");
Console.WriteLine();

// OPEN: connect (SASL ANONYMOUS by default — no userinfo in the URL).
var connection = await Connection.Factory.CreateAsync(new Address(AmqpUrl()));
try
{
    // BEGIN: one session carries the producer + consumer links below.
    var session = new Session(connection);

    // =====================================================================
    // 1. Produce — ATTACH a sender (server-receiver link). The server grants
    //    credit on attach; each Send is unsettled and blocks until the server
    //    DISPOSITION (accepted) confirms the broker stored the message.
    // =====================================================================
    var sender = new SenderLink(session, "basic-sender", addr);
    for (var i = 0; i < total; i++)
    {
        var body = $"msg-{i:D3}";
        var message = new Message { BodySection = new Data { Binary = Encoding.UTF8.GetBytes(body) } };
        sender.Send(message, TimeSpan.FromSeconds(15)); // blocks for the accepted DISPOSITION
    }
    Console.WriteLine($"[send] Produced {total} messages to {addr} (accepted DISPOSITION each)");

    // =====================================================================
    // 2. Consume — ATTACH a receiver (server-sender link). The CLIENT grants
    //    credit (SetCredit(10), auto-restore on settle). Receive each message
    //    and Accept it => the connector AckRanges it (removed from the queue).
    //
    //    NOTE: the sender link is kept OPEN through the consume phase. With this
    //    connector + AMQPNetLite, detaching the producer on the same connection
    //    before draining can stall delivery to a sibling consumer; we close both
    //    links at the very end. See the README gotcha.
    // =====================================================================
    var receiver = new ReceiverLink(session, "basic-receiver", addr);
    receiver.SetCredit(10, autoRestore: true);

    var seen = new HashSet<string>();
    while (seen.Count < total)
    {
        var message = receiver.Receive(TimeSpan.FromSeconds(30));
        if (message is null)
            throw new InvalidOperationException($"receive timed out ({seen.Count}/{total})");

        var body = BodyString(message);
        receiver.Accept(message); // AckRange -> removed from the queue
        seen.Add(body);
    }
    Console.WriteLine($"[recv] Consumed and accepted {seen.Count} messages (no loss)");

    // =====================================================================
    // 3. Assert the queue is empty — a further Receive must time out (null).
    // =====================================================================
    var extra = receiver.Receive(TimeSpan.FromSeconds(2));
    if (extra is not null)
        throw new InvalidOperationException("expected an empty queue, but received another message");
    Console.WriteLine("[recv] Queue drained to empty (no further messages)");

    // DETACH: close the receiver, then the sender, then the session.
    await receiver.CloseAsync();
    await sender.CloseAsync();
    await session.CloseAsync();
}
finally
{
    // CLOSE the connection.
    await connection.CloseAsync();
}

Console.WriteLine();
Console.WriteLine("Done.");

// BodyString extracts the UTF-8 payload from whichever body section the connector
// delivered. A queue message may arrive as either a Data section (binary) or an
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
// Address: queues/amqp10.examples.basic  (KubeMQ pattern=queues, channel=amqp10.examples.basic)
//
// [send] Produced 10 messages to queues/amqp10.examples.basic (accepted DISPOSITION each)
// [recv] Consumed and accepted 10 messages (no loss)
// [recv] Queue drained to empty (no further messages)
//
// Done.
