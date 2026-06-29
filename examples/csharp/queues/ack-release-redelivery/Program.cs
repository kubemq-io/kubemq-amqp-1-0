// Example: queues/ack-release-redelivery (master-table variant #2)
//
// The three queue settlement outcomes, side by side, against the KubeMQ AMQP 1.0
// connector using the native AMQPNetLite.Core client. NO KubeMQ SDK.
//
//   - release (ReceiverLink.Release) => NAckRange: the message is requeued to the
//     tail and REDELIVERED with a grown delivery-count (Header.DeliveryCount >= 1)
//     and FirstAcquirer=false. Each release also increments the broker
//     receive-count toward MaxReceiveQueue.
//   - reject  (ReceiverLink.Reject)  => AckRange/discard: the message is removed
//     and NOT redelivered to this receiver (poison handling is a broker
//     MaxReceiveQueue policy — there is no connector DLX).
//   - accept  (ReceiverLink.Accept)  => AckRange: the message is removed (success).
//
// Grounded in connector tests TestQueueReleasedRedelivery and
// TestQueueRejectedDiscard (connectors/amqp10/integration_test.go).
//
// Run:
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   cd queues/ack-release-redelivery && dotnet run

using System.Text;
using Amqp;
using Amqp.Framing;
using Amqp.Types;

const string channel = "amqp10.examples.ack";

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

    // Produce three distinct messages: one we release, one we reject, one we accept.
    var sender = new SenderLink(session, "ack-sender", addr);
    foreach (var body in new[] { "release-me", "reject-me", "accept-me" })
    {
        var message = new Message { BodySection = new Data { Binary = Encoding.UTF8.GetBytes(body) } };
        sender.Send(message, TimeSpan.FromSeconds(15));
    }
    Console.WriteLine("[send] Produced: release-me, reject-me, accept-me");

    // The sender link stays OPEN through the consume phase (see README gotcha:
    // detaching the producer before draining can stall delivery with this
    // connector + AMQPNetLite). It is closed at the end.
    var receiver = new ReceiverLink(session, "ack-receiver", addr);
    receiver.SetCredit(10, autoRestore: true);

    // Track which terminal outcome we still owe each body. A released message is
    // redelivered, so "release-me" is seen twice (released, then accepted).
    var remaining = new HashSet<string> { "release-me", "reject-me", "accept-me" };
    var releasedOnce = false;

    while (remaining.Count > 0)
    {
        var message = receiver.Receive(TimeSpan.FromSeconds(30));
        if (message is null)
            throw new InvalidOperationException($"receive timed out (remaining: {string.Join(",", remaining)})");

        var body = BodyString(message);
        var (deliveryCount, firstAcquirer) = DeliveryInfo(message);

        switch (body)
        {
            case "release-me" when !releasedOnce:
                // First sight: RELEASE it back to the queue tail (NAckRange).
                receiver.Release(message);
                releasedOnce = true;
                Console.WriteLine($"[recv] {body,-12} delivery-count={deliveryCount} first-acquirer={firstAcquirer,-5} -> RELEASED (requeued)");
                break;

            case "release-me":
                // Redelivery: grown delivery-count, no longer first-acquirer. Accept now.
                if (deliveryCount < 1 || firstAcquirer)
                    throw new InvalidOperationException(
                        $"expected redelivered copy to have delivery-count>=1 and first-acquirer=false, got dc={deliveryCount} first={firstAcquirer}");
                receiver.Accept(message);
                Console.WriteLine($"[recv] {body,-12} delivery-count={deliveryCount} first-acquirer={firstAcquirer,-5} -> REDELIVERED, then ACCEPTED");
                remaining.Remove(body);
                break;

            case "reject-me":
                // REJECT it (AckRange/discard). It will NOT be redelivered here.
                receiver.Reject(message, new Error(new Symbol("amqp:internal-error")) { Description = "example rejection" });
                Console.WriteLine($"[recv] {body,-12} delivery-count={deliveryCount} first-acquirer={firstAcquirer,-5} -> REJECTED (discarded, no requeue)");
                remaining.Remove(body);
                break;

            default: // "accept-me"
                receiver.Accept(message);
                Console.WriteLine($"[recv] {body,-12} delivery-count={deliveryCount} first-acquirer={firstAcquirer,-5} -> ACCEPTED (removed)");
                remaining.Remove(body);
                break;
        }
    }

    // The rejected body must NOT come back to this receiver.
    var leak = receiver.Receive(TimeSpan.FromSeconds(2));
    if (leak is not null)
        throw new InvalidOperationException("rejected message was unexpectedly redelivered");
    Console.WriteLine("[recv] Rejected message was not redelivered (discarded)");

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

// DeliveryInfo extracts the AMQP header.delivery-count and first-acquirer flag.
// The connector maps the KubeMQ broker receive-count onto these header fields:
// delivery-count = ReceiveCount-1, first-acquirer = (ReceiveCount==1).
static (uint deliveryCount, bool firstAcquirer) DeliveryInfo(Message message) =>
    message.Header is { } h ? (h.DeliveryCount, h.FirstAcquirer) : (0u, true);

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
// Address: queues/amqp10.examples.ack
//
// [send] Produced: release-me, reject-me, accept-me
// [recv] release-me   delivery-count=0 first-acquirer=True  -> RELEASED (requeued)
// [recv] reject-me    delivery-count=0 first-acquirer=True  -> REJECTED (discarded, no requeue)
// [recv] accept-me    delivery-count=0 first-acquirer=True  -> ACCEPTED (removed)
// [recv] release-me   delivery-count=1 first-acquirer=False -> REDELIVERED, then ACCEPTED
// [recv] Rejected message was not redelivered (discarded)
//
// Done.
//
// (Delivery order between the original and the redelivered copy can vary; the
// redelivered "release-me" always carries delivery-count>=1 / first-acquirer=False.)
