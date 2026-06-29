// Example: queues/settlement-modes (master-table variant #3)
//
// The two producer reliability tiers, side by side, against the KubeMQ AMQP 1.0
// connector using the native AMQPNetLite.Core client. NO KubeMQ SDK.
//
//   - PRE-SETTLED sender (SenderSettleMode.Settled): at-MOST-once. Each TRANSFER
//     is marked settled by the client, so Send returns WITHOUT waiting for a
//     server DISPOSITION. Fast and fire-and-forget — if the broker drops the
//     transfer (oversize, no capacity), the producer never learns. There is no
//     redelivery and no delivery confirmation.
//   - UNSETTLED sender (default): at-LEAST-once. Each Send blocks until the
//     connector returns an `accepted` DISPOSITION, confirming the broker stored
//     the message. This is the variant #1 contract.
//
// On the consume side this example requests ReceiverSettleMode.First (the only
// receiver settle-mode the connector supports): the server settles the delivery
// on the first transfer. rcv-settle-mode=second is rejected by the connector
// with a DETACH carrying amqp:not-implemented (see the README gotcha).
//
// Both senders' messages drain to the same consumer; the program proves no loss
// on this happy path while explaining the reliability difference.
//
// Grounded in connector test TestQueuePreSettled
// (connectors/amqp10/integration_test.go).
//
// Run:
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   cd queues/settlement-modes && dotnet run

using System.Text;
using Amqp;
using Amqp.Framing;

const string channel = "amqp10.examples.settlement";
const int perSender = 10; // produced on each sender (pre-settled, then unsettled)

static string AmqpUrl() =>
    Environment.GetEnvironmentVariable("KUBEMQ_AMQP_URL") is { Length: > 0 } v
        ? v
        : "amqp://localhost:5672";

var addr = "queues/" + channel;
Console.WriteLine($"Broker:  {AmqpUrl()}");
Console.WriteLine($"Address: {addr}");
Console.WriteLine();

var connection = await Connection.Factory.CreateAsync(new Address(AmqpUrl()));
try
{
    var session = new Session(connection);

    // =====================================================================
    // 1. PRE-SETTLED sender (at-most-once). SenderSettleMode.Settled marks every
    //    TRANSFER as already settled, so Send does NOT wait for a server
    //    DISPOSITION — it returns as soon as the frame is written. Fast, but no
    //    delivery confirmation and no redelivery. The mode is requested on the
    //    ATTACH frame (SndSettleMode), which AMQPNetLite exposes via the Attach
    //    constructor overload.
    // =====================================================================
    var presettledAttach = new Attach
    {
        Source = new Source(),
        Target = new Target { Address = addr },
        SndSettleMode = SenderSettleMode.Settled,
    };
    var presettledSender = new SenderLink(session, "presettled-sender", presettledAttach, null);
    for (var i = 0; i < perSender; i++)
    {
        var body = $"presettled-{i:D2}";
        var message = new Message { BodySection = new Data { Binary = Encoding.UTF8.GetBytes(body) } };
        presettledSender.Send(message, TimeSpan.FromSeconds(15)); // returns without awaiting a DISPOSITION
    }
    Console.WriteLine($"[send] Pre-settled (at-most-once): produced {perSender} messages — NO DISPOSITION awaited");

    // =====================================================================
    // 2. UNSETTLED sender (at-least-once — the default). Each Send blocks until
    //    the connector returns an `accepted` DISPOSITION confirming the broker
    //    stored the message. This is the variant #1 reliability contract.
    // =====================================================================
    var unsettledSender = new SenderLink(session, "unsettled-sender", addr);
    for (var i = 0; i < perSender; i++)
    {
        var body = $"unsettled-{i:D2}";
        var message = new Message { BodySection = new Data { Binary = Encoding.UTF8.GetBytes(body) } };
        unsettledSender.Send(message, TimeSpan.FromSeconds(15)); // blocks for the accepted DISPOSITION
    }
    Console.WriteLine($"[send] Unsettled (at-least-once): produced {perSender} messages — each accepted DISPOSITION");

    // =====================================================================
    // 3. Consume with ReceiverSettleMode.First. This is the ONLY receiver
    //    settle-mode the connector supports — the server settles on the first
    //    transfer. (rcv-settle-mode=second => DETACH amqp:not-implemented; see
    //    the README gotcha.) Accept each message to drain the queue.
    //
    //    The sender links stay OPEN through the consume phase (README gotcha) and
    //    are closed at the end.
    // =====================================================================
    var receiverAttach = new Attach
    {
        Source = new Source { Address = addr },
        Target = new Target(),
        RcvSettleMode = ReceiverSettleMode.First,
    };
    var receiver = new ReceiverLink(session, "settlement-receiver", receiverAttach, null);
    receiver.SetCredit(20, autoRestore: true);

    var total = 2 * perSender;
    var presettledSeen = 0;
    var unsettledSeen = 0;
    var seen = new HashSet<string>();
    while (seen.Count < total)
    {
        var message = receiver.Receive(TimeSpan.FromSeconds(30));
        if (message is null)
            throw new InvalidOperationException($"receive timed out ({seen.Count}/{total})");

        var body = BodyString(message);
        receiver.Accept(message);
        if (seen.Add(body))
        {
            if (body.StartsWith("presettled", StringComparison.Ordinal))
                presettledSeen++;
            else
                unsettledSeen++;
        }
    }
    Console.WriteLine($"[recv] Drained {seen.Count} total — {presettledSeen} pre-settled + {unsettledSeen} unsettled (rcv-settle-mode=first)");

    // =====================================================================
    // 4. Assert the queue is empty — a further Receive must time out (null).
    // =====================================================================
    var extra = receiver.Receive(TimeSpan.FromSeconds(2));
    if (extra is not null)
        throw new InvalidOperationException("expected an empty queue, but received another message");
    Console.WriteLine("[recv] Queue drained to empty (no further messages)");

    await receiver.CloseAsync();
    await unsettledSender.CloseAsync();
    await presettledSender.CloseAsync();
    await session.CloseAsync();
}
finally
{
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
// Address: queues/amqp10.examples.settlement
//
// [send] Pre-settled (at-most-once): produced 10 messages — NO DISPOSITION awaited
// [send] Unsettled (at-least-once): produced 10 messages — each accepted DISPOSITION
// [recv] Drained 20 total — 10 pre-settled + 10 unsettled (rcv-settle-mode=first)
// [recv] Queue drained to empty (no further messages)
//
// Done.
//
// (On a healthy broker pre-settled messages also drain — the difference is the
// PRODUCER guarantee, not the happy-path result: a pre-settled Send returns
// before any broker confirmation, so a drop on the way in is invisible to the
// producer. Unsettled sends block until the broker confirms storage.)
