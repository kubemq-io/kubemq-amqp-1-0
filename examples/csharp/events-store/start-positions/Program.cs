// Example: events-store/start-positions (master-table variant #8)
//
// The x-opt-kubemq-start link property over KubeMQ Events Store using the native
// AMQPNetLite.Core client. NO KubeMQ SDK.
//
// Events Store persists the stream, so a (non-durable) subscriber can choose WHERE
// in the history to start consuming via the x-opt-kubemq-start receiver link
// property. The full grammar (parsed by the connector's parseEventsStoreStart):
//
//     (absent) / "new-only"  -> deliver only events published AFTER attach
//     "first"                -> replay the ENTIRE history from the beginning
//     "last"                 -> start at the last stored event
//     "sequence:<n>"         -> start at store sequence n (1-BASED; sequence 1 = the
//                               first stored event — the connector passes n straight
//                               to NATS streaming's StartAtSequence)
//     "time:<RFC3339|secs>"  -> start at a wall-clock instant (RFC3339 or unix-seconds)
//     "time-delta:<secs>"    -> start <secs> seconds ago (relative to now)
//
// IMPORTANT — time encoding: the client sends a `time:` value as RFC3339 OR as unix
// SECONDS; the connector parses BOTH to the same instant and the broker stores the
// cursor as unix NANOSECONDS. `time-delta:` is seconds verbatim. A malformed value
// (e.g. "sequence:abc", "time:not-a-time", "whenever") is rejected at ATTACH with
// amqp:invalid-field. There is NO native "last N by count" form — to read the tail,
// compute a sequence or a time window.
//
// This program seeds 6 events, then demonstrates four start positions on fresh
// (non-durable) receivers against the SAME persisted stream:
//
//     first              -> all 6 (full replay)
//     sequence:4         -> from the 4th stored event onward (1-based => es-003,004,005)
//     time-delta:3600    -> all 6 (all were published within the last hour)
//     new-only           -> none of the existing 6; only events published after attach
//
// Grounded in connector tests TestEventsStoreDurableReplay (the start:first leg)
// and TestParseEventsStoreStart (connectors/amqp10/link_pubsub_test.go), and the
// grammar in connectors/amqp10/link.go (parseEventsStoreStart).
//
// Run:
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   cd events-store/start-positions && dotnet run

using System.Text;
using Amqp;
using Amqp.Framing;
using Amqp.Types;

// A fresh channel per run keeps the sequence numbers deterministic (this demo reads
// by absolute sequence, which is per-channel and monotonic from 1).
var channel = $"amqp10.examples.startpos.{DateTime.UtcNow.Ticks}";

const string startProp = "x-opt-kubemq-start";
const int seedCount = 6;
const int standingCredit = 100;

static string AmqpUrl() =>
    Environment.GetEnvironmentVariable("KUBEMQ_AMQP_URL") is { Length: > 0 } v
        ? v
        : "amqp://localhost:5672";

var addr = "events-store/" + channel;
Console.WriteLine($"Broker:  {AmqpUrl()}");
Console.WriteLine($"Address: {addr}  (KubeMQ pattern=events-store, channel={channel})");
Console.WriteLine();

var connection = await Connection.Factory.CreateAsync(new Address(AmqpUrl()));
try
{
    // =====================================================================
    // 0. SEED — publish 6 events into the persisted events-store stream. They are
    //    stored at 1-based sequences 1..6 (per-channel, monotonic).
    // =====================================================================
    var seedSession = new Session(connection);
    var sender = new SenderLink(seedSession, "startpos-seed", addr);
    for (var i = 0; i < seedCount; i++)
    {
        var body = $"es-{i:D3}";
        var message = new Message { BodySection = new Data { Binary = Encoding.UTF8.GetBytes(body) } };
        sender.Send(message, TimeSpan.FromSeconds(15));
    }
    await sender.CloseAsync();
    // Let the seed events settle into the persisted events-store stream before any
    // replay attaches. A send is acknowledged when the broker accepts it, but the
    // events-store index that backs a `first`/`sequence:` replay lags that ack by a
    // few hundred ms; a replay receiver that binds its cursor before the index
    // catches up sees an empty history and delivers 0 events. The Go/Rust siblings
    // never hit this because their seed→read transition carries extra broker
    // round-trips; here we make the settle window explicit.
    await Task.Delay(500);
    Console.WriteLine($"[seed] Published {seedCount} events (stored at 1-based sequences 1..{seedCount})");
    Console.WriteLine();

    // readFrom opens a fresh (non-durable) receiver at the given start position and
    // drains up to max events within window, returning their bodies in order.
    List<string> ReadFrom(string start, int max, TimeSpan window)
    {
        var session = new Session(connection);
        var attach = new Attach
        {
            Source = new Source { Address = addr },
            Target = new Target(),
            Properties = new Fields { { new Symbol(startProp), start } },
        };
        var receiver = new ReceiverLink(session, "startpos-rd-" + Guid.NewGuid().ToString("N")[..6], attach, null);
        receiver.SetCredit(standingCredit, autoRestore: true);
        // Let the connector's subscription pump go live so the replay begins.
        Thread.Sleep(750);

        var outp = new List<string>(max);
        var deadline = DateTime.UtcNow + window;
        while (outp.Count < max && DateTime.UtcNow < deadline)
        {
            var message = receiver.Receive(TimeSpan.FromSeconds(2));
            if (message is null)
                break;
            receiver.Accept(message);
            outp.Add(BodyString(message));
        }
        try { receiver.CloseAsync().Wait(2000); session.CloseAsync().Wait(2000); } catch (AmqpException) { /* best-effort */ }
        return outp;
    }

    static void ExpectExactly(List<string> got, string[] want, string label)
    {
        var set = new HashSet<string>(got);
        if (got.Count != want.Length || !want.All(set.Contains))
            throw new InvalidOperationException($"[start={label}] expected {want.Length} events [{string.Join(" ", want)}], got {got.Count}: [{string.Join(" ", got)}]");
    }

    // =====================================================================
    // 1. start=first -> FULL REPLAY (all 6 events from the beginning).
    // =====================================================================
    var got = ReadFrom("first", seedCount, TimeSpan.FromSeconds(15));
    ExpectExactly(got, ["es-000", "es-001", "es-002", "es-003", "es-004", "es-005"], "first");
    Console.WriteLine($"[start=first]           replayed full history: [{string.Join(" ", got)}]");

    // =====================================================================
    // 2. start=sequence:4 -> from the 4th stored event onward. Sequences are
    //    1-BASED (the connector passes the value straight to NATS streaming's
    //    StartAtSequence; sequence 1 = the first event), so the 4th stored event is
    //    es-003, delivering es-003, es-004, es-005.
    // =====================================================================
    got = ReadFrom("sequence:4", seedCount, TimeSpan.FromSeconds(15));
    ExpectExactly(got, ["es-003", "es-004", "es-005"], "sequence:4");
    Console.WriteLine($"[start=sequence:4]      from the 4th stored event (1-based): [{string.Join(" ", got)}]");

    // =====================================================================
    // 3. start=time-delta:3600 -> everything from the last hour (all 6, since the
    //    seed was published seconds ago). time-delta is SECONDS verbatim.
    // =====================================================================
    got = ReadFrom("time-delta:3600", seedCount, TimeSpan.FromSeconds(15));
    ExpectExactly(got, ["es-000", "es-001", "es-002", "es-003", "es-004", "es-005"], "time-delta:3600");
    Console.WriteLine($"[start=time-delta:3600] last hour (all 6): [{string.Join(" ", got)}]");

    // (You can also start at an absolute instant, e.g.
    //   Attach.Properties[startProp] = "time:" + DateTime.UtcNow.AddHours(-1).ToString("o")  // RFC3339
    // or with unix-seconds: "time:1623578400". Both forms resolve to the same
    // instant; the broker stores the cursor as nanoseconds.)

    // =====================================================================
    // 4. start=new-only -> NONE of the 6 existing events; only what is published
    //    AFTER this attach. We attach, then publish one more event and prove only
    //    that one is delivered.
    // =====================================================================
    {
        var newOnlySession = new Session(connection);
        var newOnlyAttach = new Attach
        {
            Source = new Source { Address = addr },
            Target = new Target(),
            Properties = new Fields { { new Symbol(startProp), "new-only" } },
        };
        var newOnlyRcv = new ReceiverLink(newOnlySession, "startpos-newonly", newOnlyAttach, null);
        newOnlyRcv.SetCredit(standingCredit, autoRestore: true);
        await Task.Delay(750); // let the new-only cursor settle before publishing

        var freshSession = new Session(connection);
        var freshSender = new SenderLink(freshSession, "startpos-fresh", addr);
        const string fresh = "es-new-after-attach";
        freshSender.Send(new Message { BodySection = new Data { Binary = Encoding.UTF8.GetBytes(fresh) } }, TimeSpan.FromSeconds(15));
        await freshSender.CloseAsync();

        // Only the post-attach event must arrive.
        var post = newOnlyRcv.Receive(TimeSpan.FromSeconds(15));
        if (post is null)
            throw new InvalidOperationException("[start=new-only] expected the post-attach event, got nothing");
        newOnlyRcv.Accept(post);
        var postBody = BodyString(post);
        if (postBody != fresh)
            throw new InvalidOperationException($"[start=new-only] expected \"{fresh}\" (post-attach), but got \"{postBody}\" (an existing event leaked)");

        // Nothing else (the 6 existing events must NOT be delivered).
        var leak = newOnlyRcv.Receive(TimeSpan.FromSeconds(2));
        if (leak is not null)
            throw new InvalidOperationException($"[start=new-only] an existing event \"{BodyString(leak)}\" leaked (new-only must skip history)");

        Console.WriteLine($"[start=new-only]        skipped all {seedCount} existing events; delivered only the post-attach event: [{fresh}]");
        await newOnlyRcv.CloseAsync();
        await newOnlySession.CloseAsync();
        await freshSession.CloseAsync();
    }
}
finally
{
    try { await connection.CloseAsync(); } catch (AmqpException) { /* best-effort */ }
}

// =====================================================================
// 5. GOTCHA — a malformed start value is rejected at ATTACH with
//    amqp:invalid-field. The connector DETACHes the bad attach, which can tear
//    the WHOLE connection, so each malformed probe runs on its OWN connection.
// =====================================================================
Console.WriteLine();
await DemoMalformed("sequence:abc");
await DemoMalformed("whenever");

async Task DemoMalformed(string badStart)
{
    var badConnection = await Connection.Factory.CreateAsync(new Address(AmqpUrl()));
    try
    {
        var session = new Session(badConnection);
        var attach = new Attach
        {
            Source = new Source { Address = addr },
            Target = new Target(),
            Properties = new Fields { { new Symbol(startProp), badStart } },
        };
        string rejection;
        try
        {
            var rejected = new ReceiverLink(session, "startpos-bad-" + Guid.NewGuid().ToString("N")[..6], attach, null);
            // The connector DETACHes the bad attach asynchronously; poll until the
            // link closes (or carries an error), or time out.
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (!rejected.IsClosed && rejected.Error is null && DateTime.UtcNow < deadline)
                await Task.Delay(50);
            rejection = rejected.IsClosed || rejected.Error is not null
                ? $"{rejected.Error?.Condition ?? new Symbol("amqp:not-found")} (link detached by broker)"
                : throw new InvalidOperationException($"expected start=\"{badStart}\" to be rejected, but the attach succeeded");
        }
        catch (AmqpException ex)
        {
            rejection = $"{ex.Error?.Condition} ({ex.Message})";
        }
        Console.WriteLine($"[gotcha] start=\"{badStart}\" correctly REJECTED at ATTACH: {rejection}");
    }
    finally
    {
        try { await badConnection.CloseAsync(); } catch (AmqpException) { /* link already gone */ }
    }
}

Console.WriteLine();
Console.WriteLine("Done.");

// BodyString extracts the UTF-8 payload from whichever body section the connector
// delivered. Events-store replays may arrive as either a Data section (binary) or
// an AmqpValue section (binary/string) depending on the producing client, so we
// pattern-match instead of casting unconditionally.
static string BodyString(Message message) => message.BodySection switch
{
    Data d => Encoding.UTF8.GetString(d.Binary),
    AmqpValue { Value: byte[] bytes } => Encoding.UTF8.GetString(bytes),
    AmqpValue { Value: string str } => str,
    AmqpValue v => v.Value?.ToString() ?? string.Empty,
    _ => string.Empty,
};

// Expected output (the channel suffix is a timestamp, so it varies per run):
//
// Broker:  amqp://localhost:5672
// Address: events-store/amqp10.examples.startpos.<ts>  (KubeMQ pattern=events-store, channel=amqp10.examples.startpos.<ts>)
//
// [seed] Published 6 events (stored at 1-based sequences 1..6)
//
// [start=first]           replayed full history: [es-000 es-001 es-002 es-003 es-004 es-005]
// [start=sequence:4]      from the 4th stored event (1-based): [es-003 es-004 es-005]
// [start=time-delta:3600] last hour (all 6): [es-000 es-001 es-002 es-003 es-004 es-005]
// [start=new-only]        skipped all 6 existing events; delivered only the post-attach event: [es-new-after-attach]
//
// [gotcha] start="sequence:abc" correctly REJECTED at ATTACH: amqp:not-found (link detached by broker)
// [gotcha] start="whenever" correctly REJECTED at ATTACH: amqp:not-found (link detached by broker)
//
// Done.
//
// The connector DETACHes the bad attach with amqp:invalid-field (description
// "invalid start sequence: abc" / "unknown start position: whenever"). AMQPNetLite
// races the detach against link registration and surfaces it as the amqp:not-found
// handle error above; either way the receiver never attaches.
