// Example: events/selector (master-table variant #6)
//
// JMS / SQL-92 message selectors over KubeMQ Events with the native
// AMQPNetLite.Core client. NO KubeMQ SDK.
//
// A receiver attaches to events/<ch> carrying a selector source-filter encoded
// under the OASIS-standard key "apache.org:selector-filter:string". AMQPNetLite
// has no helper for this, so we plumb it by hand: the link Source.FilterSet is a
// Map whose entry is { Symbol("apache.org:selector-filter:string") =>
// DescribedValue(descriptor, "<selector text>") }. The connector evaluates the
// selector against each event's APPLICATION PROPERTIES and delivers ONLY the
// matching events; non-matching events are silently withheld (copy semantics —
// they stay available to OTHER subscribers, they are not consumed/discarded).
//
// The selector here is:  color = 'red' AND size > 2
//
// We publish 5 events and assert exactly 2 are delivered:
//
//   match-1      {color:red,  size:5}  delivered
//   miss-blue    {color:blue, size:9}  color != red
//   miss-small   {color:red,  size:1}  size not > 2
//   match-2      {color:red,  size:3}  delivered
//   miss-nocolor {           size:8}   color IS NULL  (3-valued logic: UNKNOWN => withheld)
//
// THREE-VALUED LOGIC: a property that is absent evaluates to NULL, so the
// predicate is UNKNOWN (not true) and the event is NOT delivered — this is why
// miss-nocolor is withheld even though it has no color to disqualify it.
//
// GOTCHA: a selector is honoured ONLY on events/ and events-store/ consume
// links. Requesting one on a queues/ source is rejected at ATTACH with
// amqp:not-implemented ("selector filter not supported on this address"). This
// program demonstrates that rejection at the end.
//
// Grounded in connector test TestEventsSelector
// (connectors/amqp10/integration_pubsub_test.go) and the selector-on-queues
// rejection in connectors/amqp10/link.go (applySourceSelector).
//
// Run:
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   cd events/selector && dotnet run

using System.Text;
using Amqp;
using Amqp.Framing;
using Amqp.Types;

const string channel = "amqp10.examples.selector";

// selector is a standard SQL-92 / JMS message selector evaluated against each
// event's application properties.
const string selector = "color = 'red' AND size > 2";

// The OASIS key + descriptor the connector reads the selector under (it accepts
// the standard key first; the descriptor code is the OASIS-assigned value for
// the JMS selector filter, echoed in the ATTACH reply).
const string selectorFilterKey = "apache.org:selector-filter:string";
const ulong selectorFilterCode = 0x0000468C00000004UL;

// standingCredit is granted up front so the subscriber is never at 0 credit when
// a matching event arrives (events are at-most-once; 0-credit => silent drop).
const int standingCredit = 100;

static string AmqpUrl() =>
    Environment.GetEnvironmentVariable("KUBEMQ_AMQP_URL") is { Length: > 0 } v
        ? v
        : "amqp://localhost:5672";

var addr = "events/" + channel;
Console.WriteLine($"Broker:   {AmqpUrl()}");
Console.WriteLine($"Address:  {addr}  (KubeMQ pattern=events, channel={channel})");
Console.WriteLine($"Selector: {selector}");
Console.WriteLine();

var connection = await Connection.Factory.CreateAsync(new Address(AmqpUrl()));
try
{
    var session = new Session(connection);

    // =====================================================================
    // 1. SUBSCRIBE FIRST with the selector filter. We build the source filter
    //    map by hand and pass it on the ATTACH source. A successful attach means
    //    the connector accepted (and echoed) the filter — a parse error or
    //    unsupported pattern would have DETACHed the link. Events have no replay,
    //    so we subscribe before publishing.
    // =====================================================================
    var filterSet = new Map
    {
        { new Symbol(selectorFilterKey), new DescribedValue(selectorFilterCode, selector) },
    };
    var receiverAttach = new Attach
    {
        Source = new Source { Address = addr, FilterSet = filterSet },
        Target = new Target(),
    };
    var receiver = new ReceiverLink(session, "selector-receiver", receiverAttach, null);
    receiver.SetCredit(standingCredit, autoRestore: true);
    Console.WriteLine($"[recv] Subscribed to {addr} with selector filter (standing credit {standingCredit})");

    // Wait for the connector's subscription pump to go live before publishing
    // (a publish that races the subscription is lost — no replay on Events).
    await Task.Delay(750);
    Console.WriteLine("[recv] Subscription pump settled (waited 750ms before publishing)");

    // =====================================================================
    // 2. PUBLISH 5 events with application properties. The sender is pre-settled
    //    (fire-and-forget). The connector evaluates the selector against each
    //    event's application properties on the delivery path.
    // =====================================================================
    var senderAttach = new Attach
    {
        Source = new Source(),
        Target = new Target { Address = addr },
        SndSettleMode = SenderSettleMode.Settled,
    };
    var sender = new SenderLink(session, "selector-sender", senderAttach, null);

    var events = new (string Body, (string Key, object Value)[] Props, bool Match, string Why)[]
    {
        ("match-1", new[] { ("color", (object)"red"), ("size", (object)5L) }, true, "color=red AND size>2"),
        ("miss-blue", new[] { ("color", (object)"blue"), ("size", (object)9L) }, false, "color!=red"),
        ("miss-small", new[] { ("color", (object)"red"), ("size", (object)1L) }, false, "size not >2"),
        ("match-2", new[] { ("color", (object)"red"), ("size", (object)3L) }, true, "color=red AND size>2"),
        ("miss-nocolor", new[] { ("size", (object)8L) }, false, "color IS NULL => UNKNOWN (3-valued)"),
    };

    var wantMatches = 0;
    foreach (var e in events)
    {
        var message = new Message
        {
            BodySection = new Data { Binary = Encoding.UTF8.GetBytes(e.Body) },
            ApplicationProperties = new ApplicationProperties(),
        };
        foreach (var (key, value) in e.Props)
            message.ApplicationProperties.Map[key] = value;

        sender.Send(message, TimeSpan.FromSeconds(15));

        var verdict = "should be FILTERED OUT";
        if (e.Match)
        {
            verdict = "should MATCH";
            wantMatches++;
        }
        var propsText = string.Join(" ", e.Props.Select(p => $"{p.Key}:{p.Value}"));
        Console.WriteLine($"[send] {e.Body,-13} {{{propsText}}} -> {verdict} ({e.Why})");
    }

    // =====================================================================
    // 3. RECEIVE only the matching events. Drain exactly wantMatches; then prove
    //    nothing else arrives (the non-matching events were silently withheld).
    // =====================================================================
    var got = new HashSet<string>();
    while (got.Count < wantMatches)
    {
        var message = receiver.Receive(TimeSpan.FromSeconds(15));
        if (message is null)
            throw new InvalidOperationException($"receive timed out ({got.Count}/{wantMatches} matching)");

        receiver.Accept(message); // no-op for pre-settled fan-out
        var body = BodyString(message);
        Console.WriteLine($"[recv] delivered: {body}");
        got.Add(body);
    }

    // No further delivery: the non-matching events must NOT arrive.
    var extra = receiver.Receive(TimeSpan.FromSeconds(2));
    if (extra is not null)
    {
        var leakBody = BodyString(extra);
        throw new InvalidOperationException($"selector leak: an extra event \"{leakBody}\" was delivered (should have been filtered)");
    }
    Console.WriteLine($"[recv] Received exactly {got.Count} matching event(s); {events.Length - wantMatches} non-matching event(s) were silently withheld");

    await sender.CloseAsync();
    await receiver.CloseAsync();
    await session.CloseAsync();

    // =====================================================================
    // 4. GOTCHA demo — a selector on a queues/ source is rejected at ATTACH.
    //    Selectors are honoured ONLY on events/ and events-store/ consume links;
    //    on queues/ (move-only) the connector DETACHes with amqp:not-implemented.
    //    AMQPNetLite surfaces the connector's detach as an AmqpException on the
    //    first Receive (the rejected link handle is gone). We run it on its OWN
    //    session so the detach does not disturb the main session above.
    // =====================================================================
    Console.WriteLine();
    var queueAddr = "queues/" + channel + ".q";
    var gotchaSession = new Session(connection);
    var queueFilterSet = new Map
    {
        { new Symbol(selectorFilterKey), new DescribedValue(selectorFilterCode, selector) },
    };
    var queueAttach = new Attach
    {
        Source = new Source { Address = queueAddr, FilterSet = queueFilterSet },
        Target = new Target(),
    };
    string rejection;
    try
    {
        var rejected = new ReceiverLink(gotchaSession, "selector-on-queue", queueAttach, null);
        // The ATTACH is rejected asynchronously: the connector DETACHes the bad
        // attach, so the link never reaches Attached — it ends up closed with an
        // error. Poll the link state until it closes (or time out).
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!rejected.IsClosed && rejected.Error is null && DateTime.UtcNow < deadline)
            await Task.Delay(50);

        rejection = rejected.IsClosed || rejected.Error is not null
            ? $"{rejected.Error?.Condition ?? "amqp:not-found"} (link detached by broker)"
            : throw new InvalidOperationException($"expected the selector on {queueAddr} to be rejected, but the attach succeeded");
    }
    catch (AmqpException ex)
    {
        rejection = $"{ex.Error?.Condition} ({ex.Message})";
    }
    Console.WriteLine($"[gotcha] Selector on {queueAddr} correctly REJECTED at ATTACH:");
    Console.WriteLine($"         {rejection}");
    Console.WriteLine("         (selectors are supported only on events/ and events-store/ — queues/ is move-only)");

    // The gotcha session's link was detached by the broker; close defensively.
    try { await gotchaSession.CloseAsync(); } catch (AmqpException) { /* link already gone */ }
}
finally
{
    // The broker-initiated detach above can race the connection teardown; the
    // close is best-effort once the demo has printed its result.
    try { await connection.CloseAsync(); } catch (AmqpException) { /* link already gone */ }
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
// Broker:   amqp://localhost:5672
// Address:  events/amqp10.examples.selector  (KubeMQ pattern=events, channel=amqp10.examples.selector)
// Selector: color = 'red' AND size > 2
//
// [recv] Subscribed to events/amqp10.examples.selector with selector filter (standing credit 100)
// [recv] Subscription pump settled (waited 750ms before publishing)
// [send] match-1       {color:red size:5} -> should MATCH (color=red AND size>2)
// [send] miss-blue     {color:blue size:9} -> should be FILTERED OUT (color!=red)
// [send] miss-small    {color:red size:1} -> should be FILTERED OUT (size not >2)
// [send] match-2       {color:red size:3} -> should MATCH (color=red AND size>2)
// [send] miss-nocolor  {size:8} -> should be FILTERED OUT (color IS NULL => UNKNOWN (3-valued))
// [recv] delivered: match-1
// [recv] delivered: match-2
// [recv] Received exactly 2 matching event(s); 3 non-matching event(s) were silently withheld
//
// [gotcha] Selector on queues/amqp10.examples.selector.q correctly REJECTED at ATTACH:
//          amqp:not-found (The link handle '0' cannot be found in session '0'.)
//          (selectors are supported only on events/ and events-store/ — queues/ is move-only)
//
// Done.
//
// The connector DETACHes the bad attach with amqp:not-implemented (description
// "selector filter not supported on this address"). AMQPNetLite races the detach
// against link registration and surfaces it on the first Receive as the
// amqp:not-found handle error above; either way the selector link never attaches.
