//! Example: commands/request-reply-dynamic-node (master-table variant #9)
//!
//! Native AMQP 1.0 request/reply over KubeMQ **Commands** (RPC) with the native
//! `fe2o3-amqp` client — NO KubeMQ SDK, NO gRPC. The whole round-trip stays
//! in-protocol over a single broker connection per role.
//!
//! The mechanism (spec §2.4/§6.5; connector connectors/amqp10/rpc.go + dynamic.go):
//!
//!   - REQUESTER opens a DYNAMIC reply node: a receiver whose source is
//!     `Source::builder().dynamic(true).build()` (no address). The connector
//!     creates a transient node and echoes its address back in the attached
//!     source; the requester reads it from `reply_rcv.source()` (a
//!     "_amqp10.tmp.<connID>.<uuid>" token). Every command is sent to
//!     commands/<ch> carrying `Properties.reply_to` = that node + a `correlation_id`.
//!     The connector verifies the reply-to names a node THIS connection owns
//!     (snooping guard: a reply-to that does not resolve to a connection-owned node
//!     is refused with amqp:not-allowed) and routes the request to SendCommand. The
//!     broker Response is delivered out-of-band onto the dynamic node; the requester
//!     correlates it by correlation-id (the connector falls back to message-id when
//!     absent).
//!
//!   - RESPONDER receives requests on commands/<ch> (a server-sender link pumped
//!     under credit) and replies via an ANONYMOUS sender — a sender attached with a
//!     NULL target (`Target::builder().build()`) — setting `Properties.to` = the
//!     request's reply-to (the connector stamps it as "/responses/<RequestID>") +
//!     the echoed correlation-id. A command reply ALSO carries application
//!     properties: `x-opt-kubemq-executed` (bool) + `x-opt-kubemq-error` (string).
//!
//! Commands vs Queries (the #9 vs #10 contrast): a command that FAILS still produces
//! a reply (executed=false + error text) so the requester is NEVER left waiting.
//! This example demonstrates BOTH: a successful command (executed=true) and a failed
//! command (executed=false) — both round-trip, neither hangs.
//!
//! Grounded in connector tests TestRPCRequesterResponderViaAMQP10 (commands leg) and
//! TestRPCInteropAMQP10RequesterGRPCResponder (the executed/error app-props)
//! (connectors/amqp10/integration_test.go).
//!
//! Run:
//!
//!   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//!   cargo run -p request-reply-dynamic-node

use std::time::Duration;

use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use fe2o3_amqp_types::definitions::SenderSettleMode;
use fe2o3_amqp_types::messaging::{
    ApplicationProperties, Body, Message, MessageId, Properties, Source, Target,
};
use fe2o3_amqp_types::primitives::{SimpleValue, Value};
use tokio::sync::oneshot;

/// The KubeMQ commands channel; the link address is "commands/" + channel
/// (explicit prefix — never rely on a default pattern).
const CHANNEL: &str = "amqp10.examples.commands";

const EXECUTED_PROP: &str = "x-opt-kubemq-executed";
const ERROR_PROP: &str = "x-opt-kubemq-error";

fn amqp_url() -> String {
    std::env::var("KUBEMQ_AMQP_URL").unwrap_or_else(|_| "amqp://localhost:5672".to_string())
}

/// Decode a `Data` body into a UTF-8 string.
fn body_string(body: &Body<Value>) -> String {
    let bytes = match body {
        Body::Data(batch) => {
            let mut out = Vec::new();
            for d in batch.iter() {
                out.extend_from_slice(&d.0);
            }
            out
        }
        Body::Value(v) => match &v.0 {
            Value::Binary(b) => b.to_vec(),
            Value::String(s) => s.clone().into_bytes(),
            other => format!("{other:?}").into_bytes(),
        },
        _ => Vec::new(),
    };
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Render a `MessageId` (correlation-id / message-id) as a string for display and
/// comparison. The connector echoes the requester's correlation-id on the reply.
fn message_id_string(id: &Option<MessageId>) -> Option<String> {
    match id {
        Some(MessageId::String(s)) => Some(s.clone()),
        Some(other) => Some(format!("{other:?}")),
        None => None,
    }
}

/// Read a command reply's execution outcome from its application properties:
/// `x-opt-kubemq-executed` (bool) + `x-opt-kubemq-error` (string). Absent => false / "".
fn command_outcome(msg: &Message<Body<Value>>) -> (bool, String) {
    let Some(props) = &msg.application_properties else {
        return (false, String::new());
    };
    let executed = matches!(props.get(EXECUTED_PROP), Some(SimpleValue::Bool(true)));
    let error = match props.get(ERROR_PROP) {
        Some(SimpleValue::String(s)) => s.clone(),
        _ => String::new(),
    };
    (executed, error)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = amqp_url();
    let addr = format!("commands/{CHANNEL}");
    println!("Broker:  {url}");
    println!("Address: {addr}  (KubeMQ pattern=commands, channel={CHANNEL})\n");

    // Two connections: one per role. The responder runs in a task so this single
    // program is runnable standalone against the broker. A shutdown oneshot stops
    // the responder once the requester is done.
    let (ready_tx, ready_rx) = oneshot::channel::<()>();
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let responder_url = url.clone();
    let responder_addr = addr.clone();
    let responder = tokio::spawn(async move {
        run_responder(&responder_url, &responder_addr, ready_tx, stop_rx).await
    });

    // Wait for the responder's subscription to go live before sending requests, so
    // the first command does not race the responder's attach.
    ready_rx
        .await
        .map_err(|_| "responder failed to become ready")?;

    run_requester(&url, &addr).await?;

    // Stop the responder and join it.
    let _ = stop_tx.send(());
    match responder.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(e),
        Err(e) => return Err(Box::new(e)),
    }

    println!("\nDone.");
    Ok(())
}

// =============================================================================
// RESPONDER — receives commands on commands/<ch>, replies via an anonymous sender.
// =============================================================================

async fn run_responder(
    url: &str,
    addr: &str,
    ready: oneshot::Sender<()>,
    mut stop: oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut connection = Connection::open("amqp10-examples-commands-responder", url).await?;
    let mut session = Session::begin(&mut connection).await?;

    // ATTACH a receiver on commands/<ch> (a server-sender link — the client consumes
    // requests). The client grants credit so the connector pumps requests.
    let mut rcv = Receiver::builder()
        .name("commands-responder-receiver")
        .source(addr)
        .credit_mode(CreditMode::Auto(10))
        .attach(&mut session)
        .await?;

    // ATTACH an ANONYMOUS sender (null target). Each reply sets Properties.to to the
    // request's reply-to (the connector resolves it as /responses/<RequestID>), so
    // the reply routes back to the requester's dynamic node. Senders MUST set an
    // explicit settle-mode (the connector rejects the AMQP default `mixed`).
    let mut snd = Sender::builder()
        .name("commands-responder-anon-sender")
        .target(Target::builder().build()) // null target = anonymous terminus
        .sender_settle_mode(SenderSettleMode::Unsettled)
        .attach(&mut session)
        .await?;

    println!("[responder] Listening on {addr} (anonymous reply sender ready)");
    let _ = ready.send(());

    loop {
        let delivery = tokio::select! {
            biased;
            _ = &mut stop => break,
            res = rcv.recv::<Body<Value>>() => match res {
                Ok(d) => d,
                Err(_) => break, // link torn down / connection closed
            },
        };
        // Settle the inbound request (accept). The reply travels out-of-band.
        rcv.accept(&delivery).await?;

        let msg = delivery.message();
        let Some(reply_to) = msg.properties.as_ref().and_then(|p| p.reply_to.clone()) else {
            println!("[responder] request with no reply-to; cannot reply");
            continue;
        };
        let req_corr = msg
            .properties
            .as_ref()
            .and_then(|p| message_id_string(&p.correlation_id));
        let body = body_string(&msg.body);
        println!(
            "[responder] Received command {body:?} (correlation-id={})",
            req_corr.clone().unwrap_or_else(|| "<none>".into())
        );

        // Business logic: a command body of "fail" is rejected (executed=false); any
        // other body succeeds (executed=true). BOTH paths send a reply — a command
        // failure must NOT leave the requester waiting (unlike a query, variant #10).
        //
        // NOTE: a COMMAND response carries the EXECUTED/ERROR outcome, not data. The
        // broker round-trips executed + error (and the echoed correlation-id) but NOT
        // a reply body — the reply body below is sent for completeness, but the
        // requester observes an empty command body. Use a QUERY (#10) to return data.
        let ok = body != "fail";
        let err_text = if ok {
            String::new()
        } else {
            "command rejected by handler".to_string()
        };

        // Echo the correlation-id (fall back to message-id, the connector convention).
        let corr = msg
            .properties
            .as_ref()
            .and_then(|p| p.correlation_id.clone().or_else(|| p.message_id.clone()));

        let mut props = Properties::builder().to(reply_to);
        if let Some(c) = corr {
            props = props.correlation_id(c);
        }
        let reply = Message::builder()
            .properties(props.build())
            .application_properties(
                ApplicationProperties::builder()
                    .insert(EXECUTED_PROP, ok)
                    .insert(ERROR_PROP, err_text.as_str())
                    .build(),
            )
            .data(format!("ack:{body}").into_bytes())
            .build();

        snd.send(reply).await?;
        println!("[responder] Replied to {body:?} (executed={ok}, error={err_text:?})");
    }

    snd.close().await?;
    rcv.close().await?;
    session.end().await?;
    connection.close().await?;
    Ok(())
}

// =============================================================================
// REQUESTER — dynamic reply node + sender on commands/<ch>; correlates replies.
// =============================================================================

async fn run_requester(
    url: &str,
    addr: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut connection = Connection::open("amqp10-examples-commands-requester", url).await?;
    let mut session = Session::begin(&mut connection).await?;

    // ATTACH a DYNAMIC reply node: an empty source with dynamic=true asks the server
    // to create a transient node and echo its address back in the attached source.
    let mut reply_rcv = Receiver::builder()
        .name("commands-requester-reply-node")
        .source(Source::builder().dynamic(true).build())
        .credit_mode(CreditMode::Auto(5))
        .attach(&mut session)
        .await?;
    let reply_node = reply_rcv
        .source()
        .as_ref()
        .and_then(|s| s.address.clone())
        .ok_or("server did not assign a dynamic reply-node address")?;
    println!("[requester] Dynamic reply node: {reply_node}");

    // ATTACH a sender on commands/<ch> (a server-receiver link — the client produces
    // requests). Explicit settle-mode (the connector rejects the default `mixed`).
    let mut snd = Sender::builder()
        .name("commands-requester-sender")
        .target(addr)
        .sender_settle_mode(SenderSettleMode::Unsettled)
        .attach(&mut session)
        .await?;

    // 1. A SUCCESSFUL command: round-trips with executed=true.
    do_request(
        &mut snd,
        &mut reply_rcv,
        &reply_node,
        "reboot-node-7",
        "corr-cmd-1",
    )
    .await?;

    // 2. A FAILED command ("fail"): the responder replies executed=false + error text
    //    — the requester is NOT left waiting (the key Commands contrast vs Queries,
    //    where a failure delivers nothing and the requester times out).
    do_request(&mut snd, &mut reply_rcv, &reply_node, "fail", "corr-cmd-2").await?;

    snd.close().await?;
    reply_rcv.close().await?;
    session.end().await?;
    connection.close().await?;
    Ok(())
}

/// Send one command naming the dynamic reply node + a correlation-id, then await the
/// correlated reply on the dynamic node and print the executed/error outcome.
async fn do_request(
    snd: &mut Sender,
    reply_rcv: &mut Receiver,
    reply_node: &str,
    body: &str,
    corr: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let req = Message::builder()
        .properties(
            Properties::builder()
                .reply_to(reply_node.to_string()) // MUST name a node this connection owns (snooping guard)
                .correlation_id(corr.to_string())
                .build(),
        )
        .data(body.as_bytes().to_vec())
        .build();
    let outcome = snd.send(req).await?;
    if !outcome.is_accepted() {
        return Err(format!("send command {body:?}: unexpected outcome {outcome:?}").into());
    }
    println!("[requester] Sent command {body:?} (reply-to=dynamic node, correlation-id={corr})");

    // Await the correlated reply on the dynamic node. A command always replies
    // (success OR failure), so this never times out on the happy path.
    let reply = match tokio::time::timeout(Duration::from_secs(30), reply_rcv.recv::<Body<Value>>())
        .await
    {
        Ok(Ok(r)) => r,
        _ => return Err(format!("await reply for {body:?}: timed out").into()),
    };
    reply_rcv.accept(&reply).await?;

    let got_corr = reply
        .message()
        .properties
        .as_ref()
        .and_then(|p| message_id_string(&p.correlation_id));
    if got_corr.as_deref() != Some(corr) {
        return Err(format!("correlation-id mismatch: want {corr:?} got {got_corr:?}").into());
    }
    let (executed, err_text) = command_outcome(reply.message());
    println!(
        "[requester] Reply for {body:?} (correlation-id={}): executed={executed} error={err_text:?} body={:?}",
        got_corr.unwrap_or_default(),
        body_string(&reply.message().body)
    );
    Ok(())
}

// Expected output:
//
// Broker:  amqp://localhost:5672
// Address: commands/amqp10.examples.commands  (KubeMQ pattern=commands, channel=amqp10.examples.commands)
//
// [responder] Listening on commands/amqp10.examples.commands (anonymous reply sender ready)
// [requester] Dynamic reply node: _amqp10.tmp.<connID>.<uuid>
// [requester] Sent command "reboot-node-7" (reply-to=dynamic node, correlation-id=corr-cmd-1)
// [responder] Received command "reboot-node-7" (correlation-id=<RequestID>)
// [responder] Replied to "reboot-node-7" (executed=true, error="")
// [requester] Reply for "reboot-node-7" (correlation-id=corr-cmd-1): executed=true error="" body="..."
// [requester] Sent command "fail" (reply-to=dynamic node, correlation-id=corr-cmd-2)
// [responder] Received command "fail" (correlation-id=<RequestID>)
// [responder] Replied to "fail" (executed=false, error="command rejected by handler")
// [requester] Reply for "fail" (correlation-id=corr-cmd-2): executed=false error="command rejected by handler" body="..."
//
// Done.
//
// (The responder sees the connector-stamped RequestID as the delivered request's
// correlation-id, while the requester's reply correlation-id is its ORIGINAL
// corr-cmd-N — the connector echoes the requester's correlation-id back on the
// reply. A COMMAND response carries executed/error, NOT data — use a QUERY (#10) to
// return a value. A failed command still delivers a reply (executed=false + error)
// so the requester is NEVER left waiting; contrast queries/request-reply.)
