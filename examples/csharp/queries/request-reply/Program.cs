// Example: queries/request-reply (master-table variant #10)
//
// Native AMQP 1.0 request/reply over KubeMQ Queries (RPC) with the native
// AMQPNetLite.Core client — NO kubemq SDK, NO gRPC. The whole round-trip stays
// in-protocol over a single broker connection per role.
//
// The reply path is IDENTICAL to commands (variant #9): the requester opens a
// DYNAMIC reply node (a ReceiverLink with Attach.Source.Dynamic = true → the
// server-assigned address read via the OnAttached callback), sends to queries/<ch>
// with Properties.ReplyTo = that node + a CorrelationId; the responder receives on
// queries/<ch> and replies via an ANONYMOUS sender (SenderLink with a null
// Attach.Target) with Properties.To = the request's reply-to + the echoed
// CorrelationId.
//
// The CONTRAST with commands (the whole point of variant #10):
//
//   - A query reply carries ONLY the body + metadata — NO x-opt-kubemq-executed /
//     x-opt-kubemq-error application-properties. A query is a "fetch a value" call;
//     there is no executed/error envelope.
//   - A FAILED query delivers NOTHING. The connector's runRequest delivers no reply
//     when a query fails or times out (MQTT-bridge parity), so the requester simply
//     TIMES OUT. (A failed command, by contrast, always replies executed=false so
//     its requester is never left waiting.)
//
// This example demonstrates BOTH: a successful query (reply round-trips, body
// intact) and a query the responder ignores (no reply ⇒ the requester times out on
// a short demo deadline; in production the connector default is ~30s).
//
// Grounded in connector test TestRPCRequesterResponderViaAMQP10 (queries leg)
// (connectors/amqp10/integration_test.go).
//
// Run:
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   cd queries/request-reply && dotnet run

using System.Text;
using Amqp;
using Amqp.Framing;

// channel is the KubeMQ queries channel; the link address is "queries/" + channel
// (explicit prefix — never rely on a default pattern).
const string channel = "amqp10.examples.queries";

// demoTimeout is the short per-request deadline used here so the "no reply" leg
// surfaces a timeout quickly. The connector's own default RPC timeout is ~30s; in
// production set the request header.ttl to choose the per-request budget.
var demoTimeout = TimeSpan.FromSeconds(5);

static string AmqpUrl() =>
    Environment.GetEnvironmentVariable("KUBEMQ_AMQP_URL") is { Length: > 0 } v
        ? v
        : "amqp://localhost:5672";

var addr = "queries/" + channel;
Console.WriteLine($"Broker:  {AmqpUrl()}");
Console.WriteLine($"Address: {addr}  (KubeMQ pattern=queries, channel={channel})");
Console.WriteLine();

// Two connections: one per role. The responder runs on its own Task; the responder
// and the requester use SEPARATE sessions on SEPARATE connections.
using var responderDone = new CancellationTokenSource();
var responderReady = new TaskCompletionSource();
var responderTask = Task.Run(() => RunResponder(addr, responderReady, responderDone.Token));

await responderReady.Task.WaitAsync(TimeSpan.FromSeconds(20));

await RunRequester(addr, demoTimeout);

responderDone.Cancel();
await responderTask;

Console.WriteLine();
Console.WriteLine("Done.");

// =============================================================================
// RESPONDER — receives queries on queries/<ch>, replies via an anonymous sender.
// A query whose body is "ignore" gets NO reply (so its requester times out).
// =============================================================================
static void RunResponder(string addr, TaskCompletionSource ready, CancellationToken stop)
{
    var connection = Connection.Factory.CreateAsync(new Address(AmqpUrl())).GetAwaiter().GetResult();
    try
    {
        var session = new Session(connection);

        // ATTACH a receiver on queries/<ch> (server-sender link — the client
        // consumes requests). The client grants credit so the connector pumps them.
        var receiver = new ReceiverLink(session, "query-responder", addr);
        receiver.SetCredit(10, autoRestore: true);

        // ATTACH an ANONYMOUS sender (null Attach.Target). Each reply sets
        // Properties.To to the request's reply-to so it routes back to the
        // requester's dynamic node.
        var anonAttach = new Attach { Source = new Source(), Target = null };
        var sender = new SenderLink(session, "query-reply-sender", anonAttach, null);

        Console.WriteLine($"[responder] Listening on {addr} (anonymous reply sender ready)");
        ready.TrySetResult();

        while (!stop.IsCancellationRequested)
        {
            // Guard the blocking receive against cancellation: on shutdown the
            // connection is torn down, which would otherwise surface the wakeup as
            // an unhandled exception. A null (timeout) just loops back to re-check
            // the token.
            Message? req;
            try
            {
                req = receiver.Receive(TimeSpan.FromSeconds(1));
            }
            catch (Exception) when (stop.IsCancellationRequested)
            {
                break; // link/connection closed during shutdown — expected
            }
            if (req is null)
                continue;
            receiver.Accept(req);

            if (req.Properties?.ReplyTo is not { Length: > 0 } replyTo)
            {
                Console.WriteLine("[responder] request with no reply-to; cannot reply");
                continue;
            }
            var body = BodyString(req);
            Console.WriteLine($"[responder] Received query \"{body}\" (correlation-id={req.Properties.GetCorrelationId()})");

            // Business logic: a query body of "ignore" is dropped on the floor — the
            // responder sends NOTHING. The requester will time out. (A real responder
            // would only fail to reply on a crash / unreachable backend; "ignore"
            // makes the contrast deterministic for the demo.)
            if (body == "ignore")
            {
                Console.WriteLine($"[responder] Ignoring \"{body}\" — NO reply sent (requester will time out)");
                continue;
            }

            // A QUERY reply carries ONLY the body + metadata — NO executed/error
            // application-properties (the Commands-vs-Queries contrast).
            var reply = new Message { BodySection = new Data { Binary = Encoding.UTF8.GetBytes("result:" + body) } };
            reply.Properties = new Properties { To = replyTo };
            var corr = req.Properties.GetCorrelationId() ?? req.Properties.GetMessageId();
            if (corr is not null)
                reply.Properties.SetCorrelationId(corr);

            sender.Send(reply, TimeSpan.FromSeconds(10));
            Console.WriteLine($"[responder] Replied to \"{body}\" (body + metadata only, no executed/error props)");
        }
    }
    catch (Exception ex) when (stop.IsCancellationRequested)
    {
        _ = ex; // connection torn down on shutdown
    }
    finally
    {
        try { connection.CloseAsync().Wait(2000); } catch (AmqpException) { /* best-effort */ }
    }
}

// =============================================================================
// REQUESTER — dynamic reply node + sender on queries/<ch>; correlates replies.
// =============================================================================
static async Task RunRequester(string addr, TimeSpan demoTimeout)
{
    var connection = await Connection.Factory.CreateAsync(new Address(AmqpUrl()));
    try
    {
        var session = new Session(connection);

        // ATTACH a DYNAMIC reply node (Attach.Source.Dynamic = true). The server
        // creates a transient node and echoes its address; AMQPNetLite hands the
        // ATTACH reply to OnAttached, where we read attach.Source.Address.
        string? replyNode = null;
        using var attached = new SemaphoreSlim(0, 1);
        var dynAttach = new Attach
        {
            Source = new Source { Dynamic = true },
            Target = new Target(),
        };
        var replyRcv = new ReceiverLink(session, "query-reply-node", dynAttach, (link, attach) =>
        {
            if (attach.Source is Source s)
                replyNode = s.Address;
            attached.Release();
        });
        replyRcv.SetCredit(5, autoRestore: true);
        await attached.WaitAsync(TimeSpan.FromSeconds(10));
        if (string.IsNullOrEmpty(replyNode))
            throw new InvalidOperationException("server did not assign a dynamic reply-node address");
        Console.WriteLine($"[requester] Dynamic reply node: {replyNode}");

        // ATTACH a sender on queries/<ch> (server-receiver link — the client
        // produces requests). The server grants credit on attach.
        var sender = new SenderLink(session, "query-requester", addr);

        // 1. A SUCCESSFUL query: round-trips, body intact, no executed/error props.
        DoQuery(sender, replyRcv, replyNode!, "get-temp-sensor-3", "corr-qry-1", demoTimeout);

        // 2. A query the responder ignores: NOTHING is delivered, so the requester
        //    TIMES OUT. This is the core Queries contrast — a failed/unanswered query
        //    has no error envelope; the absence of a reply IS the failure signal.
        DoQueryExpectTimeout(sender, replyRcv, replyNode!, "ignore", "corr-qry-2", demoTimeout);

        await sender.CloseAsync();
        await replyRcv.CloseAsync();
        await session.CloseAsync();
    }
    finally
    {
        await connection.CloseAsync();
    }
}

// SendQuery sends one query message naming the dynamic reply node + correlation-id.
static void SendQuery(SenderLink sender, string replyNode, string body, string corr)
{
    var req = new Message { BodySection = new Data { Binary = Encoding.UTF8.GetBytes(body) } };
    req.Properties = new Properties { ReplyTo = replyNode }; // MUST name a node this connection owns (snooping guard)
    req.Properties.SetCorrelationId(corr);
    sender.Send(req, TimeSpan.FromSeconds(15));
    Console.WriteLine($"[requester] Sent query \"{body}\" (reply-to=dynamic node, correlation-id={corr})");
}

// DoQuery sends one query, then awaits the correlated reply and prints the body.
static void DoQuery(SenderLink sender, ReceiverLink replyRcv, string replyNode, string body, string corr, TimeSpan demoTimeout)
{
    SendQuery(sender, replyNode, body, corr);

    var reply = replyRcv.Receive(demoTimeout);
    if (reply is null)
        throw new InvalidOperationException($"await reply for \"{body}\": timed out unexpectedly");
    replyRcv.Accept(reply);

    var gotCorr = reply.Properties?.GetCorrelationId();
    if (!Equals(gotCorr?.ToString(), corr))
        throw new InvalidOperationException($"correlation-id mismatch: want \"{corr}\" got {gotCorr}");
    var replyBody = reply.BodySection is Data d ? Encoding.UTF8.GetString(d.Binary) : "";
    Console.WriteLine($"[requester] Reply for \"{body}\" (correlation-id={gotCorr}): body=\"{replyBody}\"");
}

// DoQueryExpectTimeout sends a query the responder will ignore and shows the
// requester timing out (no reply is the failure signal for queries).
static void DoQueryExpectTimeout(SenderLink sender, ReceiverLink replyRcv, string replyNode, string body, string corr, TimeSpan demoTimeout)
{
    SendQuery(sender, replyNode, body, corr);

    var reply = replyRcv.Receive(demoTimeout);
    if (reply is not null)
        throw new InvalidOperationException($"expected NO reply for \"{body}\", but one arrived");
    // A null (timeout) here is the EXPECTED outcome for an unanswered query.
    Console.WriteLine($"[requester] No reply for \"{body}\" within {demoTimeout.TotalSeconds:0}s — query timed out (expected; failed queries deliver nothing)");
}

// BodyString extracts the UTF-8 payload from whichever body section the message
// carries. A request may arrive as either a Data section (binary) or an AmqpValue
// section (binary/string) depending on the producing client, so we pattern-match
// instead of casting unconditionally.
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
// Address: queries/amqp10.examples.queries  (KubeMQ pattern=queries, channel=amqp10.examples.queries)
//
// [responder] Listening on queries/amqp10.examples.queries (anonymous reply sender ready)
// [requester] Dynamic reply node: _amqp10.tmp.<connID>.<uuid>
// [requester] Sent query "get-temp-sensor-3" (reply-to=dynamic node, correlation-id=corr-qry-1)
// [responder] Received query "get-temp-sensor-3" (correlation-id=corr-qry-1)
// [responder] Replied to "get-temp-sensor-3" (body + metadata only, no executed/error props)
// [requester] Reply for "get-temp-sensor-3" (correlation-id=corr-qry-1): body="result:get-temp-sensor-3"
// [requester] Sent query "ignore" (reply-to=dynamic node, correlation-id=corr-qry-2)
// [responder] Received query "ignore" (correlation-id=corr-qry-2)
// [responder] Ignoring "ignore" — NO reply sent (requester will time out)
// [requester] No reply for "ignore" within 5s — query timed out (expected; failed queries deliver nothing)
//
// Done.
//
// (Unlike a command — which always replies executed=false on failure so the
// requester is never left waiting — a query that fails/goes unanswered delivers
// NOTHING. The requester's timeout IS the failure signal. The connector's own
// default per-request timeout is ~30s; set the request header.ttl to choose it.)
