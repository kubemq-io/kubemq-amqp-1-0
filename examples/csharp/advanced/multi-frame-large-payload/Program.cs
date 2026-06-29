// Example: advanced/multi-frame-large-payload (master-table variant #11)
//
// A single AMQP 1.0 message whose body is larger than the connection's
// max-frame-size is fragmented across multiple TRANSFER frames (More:true …
// More:false) by the sender and reassembled bit-exact by the receiver — all
// transparently, with NO application-level chunking. This example drives that path
// against the KubeMQ AMQP 1.0 connector using the native AMQPNetLite.Core client.
// NO KubeMQ SDK.
//
// Flow:
//   - OPEN both the producer and consumer connections with AMQP.MaxFrameSize = 4096
//     (via a ConnectionFactory) — a deliberately tiny 4 KiB frame so a ~1 MB body
//     forces heavy fragmentation in both directions.
//   - Sender → "queues/<ch>" (unsettled): one Send carries a ~1 MB Data body.
//     AMQPNetLite splits it across many transfer frames; the connector reassembles
//     it and stores a single message.
//   - Receiver ← "queues/<ch>" credit 1: one Receive yields the full body. The
//     example verifies the received length AND a CRC32 of the bytes match the
//     original — proving a bit-exact round-trip across the fragment boundary.
//
// Grounded in connector test TestQueueMultiFrameLargePayload
// (connectors/amqp10/integration_test.go).
//
// Run:
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   cd advanced/multi-frame-large-payload && dotnet run

using System.Text;
using Amqp;
using Amqp.Framing;

// channel is the KubeMQ queue channel; the link address is "queues/" + channel
// (explicit prefix — never rely on a default pattern).
const string channel = "amqp10.examples.multiframe";

// payloadSize is ~1 MB — comfortably larger than maxFrameSize so the body must span
// many transfer frames (More:true … More:false).
const int payloadSize = 1 * 1024 * 1024;
// maxFrameSize is a deliberately tiny 4 KiB so the ~1 MB body fragments across ~256
// frames in each direction.
const int maxFrameSize = 4096;

static string AmqpUrl() =>
    Environment.GetEnvironmentVariable("KUBEMQ_AMQP_URL") is { Length: > 0 } v
        ? v
        : "amqp://localhost:5672";

// Crc32 computes the IEEE CRC32 of a buffer (reflected, poly 0xEDB88320) — enough to
// prove a bit-exact round-trip without an extra package dependency.
static uint Crc32(byte[] data)
{
    var crc = 0xFFFFFFFFu;
    foreach (var b in data)
    {
        crc ^= b;
        for (var k = 0; k < 8; k++)
            crc = (crc & 1) != 0 ? (crc >> 1) ^ 0xEDB88320u : crc >> 1;
    }
    return ~crc;
}

var addr = "queues/" + channel;
Console.WriteLine($"Broker:        {AmqpUrl()}");
Console.WriteLine($"Address:       {addr}  (KubeMQ pattern=queues, channel={channel})");
Console.WriteLine($"MaxFrameSize:  {maxFrameSize} bytes");
Console.WriteLine($"Payload:       {payloadSize} bytes (~{payloadSize / 1024} KiB)");
Console.WriteLine();

// Build a deterministic, non-trivial payload and remember its CRC + length so we can
// prove a bit-exact round-trip after reassembly.
var payload = new byte[payloadSize];
for (var i = 0; i < payload.Length; i++)
    payload[i] = (byte)(i % 251); // 251 is prime → no short repeating period
var wantLen = payload.Length;
var wantCrc = Crc32(payload);
Console.WriteLine($"[prep] Built payload: len={wantLen} crc32=0x{wantCrc:x8}");

// A ConnectionFactory lets us set the per-connection MaxFrameSize before connecting.
static ConnectionFactory TinyFrameFactory()
{
    var factory = new ConnectionFactory();
    factory.AMQP.MaxFrameSize = maxFrameSize; // tiny 4 KiB frame
    return factory;
}

// =========================================================================
// 1. PRODUCER connection — OPEN with a tiny MaxFrameSize. The connector advertises
//    its own max-frame-size in the OPEN reply; AMQPNetLite uses the smaller of the
//    two when fragmenting transfers.
// =========================================================================
var prodConnection = await TinyFrameFactory().CreateAsync(new Address(AmqpUrl()));
try
{
    var prodSession = new Session(prodConnection);

    // ATTACH a sender and send the whole body in ONE Send. AMQPNetLite transparently
    // splits it across many transfer frames (More:true … final More:false). The
    // connector reassembles them into a single stored message.
    var sender = new SenderLink(prodSession, "multiframe-sender", addr);
    var message = new Message { BodySection = new Data { Binary = payload } };
    sender.Send(message, TimeSpan.FromSeconds(60));
    await sender.CloseAsync();
    var approxFrames = (wantLen / maxFrameSize) + 1;
    Console.WriteLine($"[send] Sent the {wantLen}-byte body in ONE Send (fragmented across ~{approxFrames} frames, accepted)");

    // =====================================================================
    // 2. CONSUMER connection — same tiny MaxFrameSize so reassembly is exercised on
    //    the receive path too. One Receive yields the FULL reassembled body.
    // =====================================================================
    var consConnection = await TinyFrameFactory().CreateAsync(new Address(AmqpUrl()));
    try
    {
        var consSession = new Session(consConnection);
        var receiver = new ReceiverLink(consSession, "multiframe-receiver", addr);
        receiver.SetCredit(1, autoRestore: true);

        var got = receiver.Receive(TimeSpan.FromSeconds(60));
        if (got is null)
            throw new InvalidOperationException("multi-frame receive timed out");
        receiver.Accept(got);

        var gotBytes = BodyBytes(got);
        var gotLen = gotBytes.Length;
        var gotCrc = Crc32(gotBytes);
        Console.WriteLine($"[recv] Reassembled body: len={gotLen} crc32=0x{gotCrc:x8}");

        // =================================================================
        // 3. Verify the round-trip is bit-exact: length AND CRC32 must match.
        // =================================================================
        if (gotLen != wantLen)
            throw new InvalidOperationException($"length mismatch: sent {wantLen}, received {gotLen}");
        if (gotCrc != wantCrc)
            throw new InvalidOperationException($"CRC mismatch: sent 0x{wantCrc:x8}, received 0x{gotCrc:x8}");
        Console.WriteLine("[verify] Length and CRC32 match — multi-frame body round-tripped bit-exact");

        await receiver.CloseAsync();
        await consSession.CloseAsync();
    }
    finally
    {
        await consConnection.CloseAsync();
    }

    await prodSession.CloseAsync();
}
finally
{
    await prodConnection.CloseAsync();
}

Console.WriteLine();
Console.WriteLine("Done.");

// BodyBytes extracts the raw payload bytes from whichever body section the connector
// delivered. The reassembled body may arrive as either a Data section (binary) or an
// AmqpValue section (binary/string) depending on the producing client, so we
// pattern-match instead of casting unconditionally — a CRC32 check needs the bytes.
static byte[] BodyBytes(Message message) => message.BodySection switch
{
    Data d => d.Binary,
    AmqpValue { Value: byte[] bytes } => bytes,
    AmqpValue { Value: string str } => Encoding.UTF8.GetBytes(str),
    AmqpValue v => Encoding.UTF8.GetBytes(v.Value?.ToString() ?? string.Empty),
    _ => Array.Empty<byte>(),
};

// Expected output:
//
// Broker:        amqp://localhost:5672
// Address:       queues/amqp10.examples.multiframe  (KubeMQ pattern=queues, channel=amqp10.examples.multiframe)
// MaxFrameSize:  4096 bytes
// Payload:       1048576 bytes (~1024 KiB)
//
// [prep] Built payload: len=1048576 crc32=0x........
// [send] Sent the 1048576-byte body in ONE Send (fragmented across ~257 frames, accepted)
// [recv] Reassembled body: len=1048576 crc32=0x........
// [verify] Length and CRC32 match — multi-frame body round-tripped bit-exact
//
// Done.
