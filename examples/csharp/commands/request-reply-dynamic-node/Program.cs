// Example: commands/request-reply-dynamic-node (master-table variant #9)
//
// Native AMQP 1.0 request/reply over KubeMQ Commands (RPC) with the native
// AMQPNetLite.Core client — NO kubemq SDK, NO gRPC. The whole round-trip stays
// in-protocol over a single broker connection per role.
//
// The mechanism (spec §2.4/§6.5; connector connectors/amqp10/rpc.go + dynamic.go):
//
//   - REQUESTER opens a DYNAMIC reply node: a ReceiverLink whose Attach.Source has
//     Dynamic = true. The server creates a transient node and echoes its address in
//     the ATTACH reply; AMQPNetLite surfaces that reply via the OnAttached callback,
//     where we read attach.Source.Address (a "_amqp10.tmp.<connID>.<uuid>" token).
//     The requester sends the command to commands/<ch> carrying Properties.ReplyTo =
//     that node + Properties.CorrelationId. The connector verifies the reply-to
//     names a node THIS connection owns (snooping guard: a reply-to that does not
//     resolve to a connection-owned node is refused with amqp:not-allowed) and
//     routes the request to SendCommand. The broker Response is delivered
//     out-of-band onto the dynamic node; the requester correlates it by
//     correlation-id (the connector falls back to message-id when absent).
//
//   - RESPONDER receives requests on commands/<ch> (a server-sender link pumped
//     under credit) and replies via an ANONYMOUS sender — a SenderLink whose
//     Attach.Target is null. It sets Properties.To = the request's ReplyTo (the
//     connector stamps the delivered request's reply-to as "/responses/<RequestID>")
//     + the echoed CorrelationId. A command reply ALSO carries ApplicationProperties:
//     x-opt-kubemq-executed (bool) + x-opt-kubemq-error (string).
//
// Commands vs Queries (the #9 vs #10 contrast): a command that FAILS still produces
// a reply (executed=false + error text) so the requester is NEVER left waiting. This
// example demonstrates BOTH: a successful command (executed=true) and a failed
// command (executed=false) — both round-trip, neither hangs.
//
// Grounded in connector tests TestRPCRequesterResponderViaAMQP10 (commands leg) and
// TestRPCInteropAMQP10RequesterGRPCResponder (the executed/error app-props)
// (connectors/amqp10/integration_test.go).
//
// Run:
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   cd commands/request-reply-dynamic-node && dotnet run

using System.Text;
using Amqp;
using Amqp.Framing;

// channel is the KubeMQ commands channel; the link address is "commands/" + channel
// (explicit prefix — never rely on a default pattern).
const string channel = "amqp10.examples.commands";

static string AmqpUrl() =>
    Environment.GetEnvironmentVariable("KUBEMQ_AMQP_URL") is { Length: > 0 } v
        ? v
        : "amqp://localhost:5672";

var addr = "commands/" + channel;
Console.WriteLine($"Broker:  {AmqpUrl()}");
Console.WriteLine($"Address: {addr}  (KubeMQ pattern=commands, channel={channel})");
Console.WriteLine();

// Two connections: one per role. The responder runs on its own Task so this single
// program is runnable standalone against the broker; the responder and the requester
// use SEPARATE sessions on SEPARATE connections.
using var responderDone = new CancellationTokenSource();
var responderReady = new TaskCompletionSource();
var responderTask = Task.Run(() => RunResponder(addr, responderReady, responderDone.Token));

// Wait for the responder's subscription to go live before sending requests.
await responderReady.Task.WaitAsync(TimeSpan.FromSeconds(20));

await RunRequester(addr);

// Stop the responder loop and wait for it to unwind.
responderDone.Cancel();
await responderTask;

Console.WriteLine();
Console.WriteLine("Done.");

// =============================================================================
// RESPONDER — receives commands on commands/<ch>, replies via an anonymous sender.
// =============================================================================
static void RunResponder(string addr, TaskCompletionSource ready, CancellationToken stop)
{
    var connection = Connection.Factory.CreateAsync(new Address(AmqpUrl())).GetAwaiter().GetResult();
    try
    {
        var session = new Session(connection);

        // ATTACH a receiver on commands/<ch> (a server-sender link — the client
        // consumes requests). The client grants credit so the connector pumps them.
        var receiver = new ReceiverLink(session, "command-responder", addr);
        receiver.SetCredit(10, autoRestore: true);

        // ATTACH an ANONYMOUS sender (null Attach.Target). Each reply sets
        // Properties.To to the request's reply-to so it routes back to the
        // requester's dynamic node.
        var anonAttach = new Attach { Source = new Source(), Target = null };
        var sender = new SenderLink(session, "command-reply-sender", anonAttach, null);

        Console.WriteLine($"[responder] Listening on {addr} (anonymous reply sender ready)");
        ready.TrySetResult();

        while (!stop.IsCancellationRequested)
        {
            var req = receiver.Receive(TimeSpan.FromSeconds(1));
            if (req is null)
                continue; // poll again (so cancellation is checked promptly)

            // Settle the inbound request (accept). The reply travels out-of-band.
            receiver.Accept(req);

            if (req.Properties?.ReplyTo is not { Length: > 0 } replyTo)
            {
                Console.WriteLine("[responder] request with no reply-to; cannot reply");
                continue;
            }
            var body = BodyString(req);
            Console.WriteLine($"[responder] Received command \"{body}\" (correlation-id={req.Properties.GetCorrelationId()})");

            // Business logic: a command body of "fail" is rejected (executed=false),
            // any other body succeeds (executed=true). BOTH paths send a reply — a
            // command failure must NOT leave the requester waiting (the key Commands
            // contrast vs Queries, variant #10).
            //
            // NOTE: a COMMAND response carries the EXECUTED/ERROR outcome, not data.
            // The reply body is sent for completeness but the requester observes an
            // empty command body. Use a QUERY (variant #10) to return a value.
            var ok = body != "fail";
            var errText = ok ? "" : "command rejected by handler";

            var reply = new Message { BodySection = new Data { Binary = Encoding.UTF8.GetBytes("ack:" + body) } };
            reply.Properties = new Properties { To = replyTo };
            // Echo the correlation-id (fall back to message-id, the connector
            // convention) so the requester can match the reply to its request.
            var corr = req.Properties.GetCorrelationId() ?? req.Properties.GetMessageId();
            if (corr is not null)
                reply.Properties.SetCorrelationId(corr);
            // A COMMAND reply carries the execution outcome as application-properties.
            reply.ApplicationProperties = new ApplicationProperties();
            reply.ApplicationProperties.Map["x-opt-kubemq-executed"] = ok;
            reply.ApplicationProperties.Map["x-opt-kubemq-error"] = errText;

            sender.Send(reply, TimeSpan.FromSeconds(10));
            Console.WriteLine($"[responder] Replied to \"{body}\" (executed={ok}, error=\"{errText}\")");
        }
    }
    catch (Exception ex) when (stop.IsCancellationRequested)
    {
        // The connection is torn down on shutdown; swallow the resulting fault.
        _ = ex;
    }
    finally
    {
        try { connection.CloseAsync().Wait(2000); } catch (AmqpException) { /* best-effort */ }
    }
}

// =============================================================================
// REQUESTER — dynamic reply node + sender on commands/<ch>; correlates replies.
// =============================================================================
static async Task RunRequester(string addr)
{
    var connection = await Connection.Factory.CreateAsync(new Address(AmqpUrl()));
    try
    {
        var session = new Session(connection);

        // ATTACH a DYNAMIC reply node: an Attach.Source with Dynamic = true asks the
        // server to create a transient node and echo its address. AMQPNetLite hands
        // the server's ATTACH reply to the OnAttached callback, where we read the
        // server-assigned address from attach.Source.Address.
        string? replyNode = null;
        using var attached = new SemaphoreSlim(0, 1);
        var dynAttach = new Attach
        {
            Source = new Source { Dynamic = true },
            Target = new Target(),
        };
        var replyRcv = new ReceiverLink(session, "command-reply-node", dynAttach, (link, attach) =>
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

        // ATTACH a sender on commands/<ch> (a server-receiver link — the client
        // produces requests). The server grants credit on attach.
        var sender = new SenderLink(session, "command-requester", addr);

        // 1. A SUCCESSFUL command: round-trips with executed=true.
        DoRequest(sender, replyRcv, replyNode!, "reboot-node-7", "corr-cmd-1");

        // 2. A FAILED command ("fail"): the responder replies executed=false + an
        //    error text — the requester is NOT left waiting (the key Commands
        //    contrast vs Queries, where a failure delivers nothing and the requester
        //    times out).
        DoRequest(sender, replyRcv, replyNode!, "fail", "corr-cmd-2");

        await sender.CloseAsync();
        await replyRcv.CloseAsync();
        await session.CloseAsync();
    }
    finally
    {
        await connection.CloseAsync();
    }
}

// DoRequest sends one command naming the dynamic reply node + a correlation-id, then
// awaits the correlated reply on the dynamic node and prints the executed/error
// outcome.
static void DoRequest(SenderLink sender, ReceiverLink replyRcv, string replyNode, string body, string corr)
{
    var req = new Message { BodySection = new Data { Binary = Encoding.UTF8.GetBytes(body) } };
    req.Properties = new Properties { ReplyTo = replyNode }; // MUST name a node this connection owns (snooping guard)
    req.Properties.SetCorrelationId(corr);
    sender.Send(req, TimeSpan.FromSeconds(15));
    Console.WriteLine($"[requester] Sent command \"{body}\" (reply-to=dynamic node, correlation-id={corr})");

    // Await the correlated reply on the dynamic node. A command always replies
    // (success OR failure), so this never times out on the happy path.
    var reply = replyRcv.Receive(TimeSpan.FromSeconds(30));
    if (reply is null)
        throw new InvalidOperationException($"await reply for \"{body}\": timed out");
    replyRcv.Accept(reply);

    var gotCorr = reply.Properties?.GetCorrelationId();
    if (!Equals(gotCorr?.ToString(), corr))
        throw new InvalidOperationException($"correlation-id mismatch: want \"{corr}\" got {gotCorr}");

    var executed = reply.ApplicationProperties?.Map["x-opt-kubemq-executed"] as bool? ?? false;
    var errText = reply.ApplicationProperties?.Map["x-opt-kubemq-error"] as string ?? "";
    var replyBody = BodyString(reply);
    Console.WriteLine($"[requester] Reply for \"{body}\" (correlation-id={gotCorr}): executed={executed} error=\"{errText}\" body=\"{replyBody}\"");
}

// BodyString extracts the UTF-8 payload from whichever body section the connector
// delivered. A command request/reply may arrive as either a Data section (binary) or
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

// Expected output:
//
// Broker:  amqp://localhost:5672
// Address: commands/amqp10.examples.commands  (KubeMQ pattern=commands, channel=amqp10.examples.commands)
//
// [responder] Listening on commands/amqp10.examples.commands (anonymous reply sender ready)
// [requester] Dynamic reply node: _amqp10.tmp.<connID>.<uuid>
// [requester] Sent command "reboot-node-7" (reply-to=dynamic node, correlation-id=corr-cmd-1)
// [responder] Received command "reboot-node-7" (correlation-id=<RequestID>)
// [responder] Replied to "reboot-node-7" (executed=True, error="")
// [requester] Reply for "reboot-node-7" (correlation-id=corr-cmd-1): executed=True error="" body=""
// [requester] Sent command "fail" (reply-to=dynamic node, correlation-id=corr-cmd-2)
// [responder] Received command "fail" (correlation-id=<RequestID>)
// [responder] Replied to "fail" (executed=False, error="command rejected by handler")
// [requester] Reply for "fail" (correlation-id=corr-cmd-2): executed=False error="command rejected by handler" body=""
//
// Done.
//
// (The responder sees the connector-stamped RequestID as the delivered request's
// correlation-id, while the requester's reply correlation-id is its ORIGINAL
// corr-cmd-N — the connector echoes the requester's correlation-id back on the
// reply. A COMMAND response carries the executed/error outcome, NOT a body — the
// requester observes an empty command reply body; use a QUERY to return a value.)
//
// (A failed command still delivers a reply — executed=false + error text — so the
// requester is NEVER left waiting. Contrast queries/request-reply, where a failed
// query delivers nothing and the requester simply times out.)
