//! Example: queries/request-reply (master-table variant #10)
//!
//! Native AMQP 1.0 request/reply over KubeMQ **Queries** (RPC) with the native
//! `fe2o3-amqp` client — NO KubeMQ SDK, NO gRPC. The whole round-trip stays
//! in-protocol over a single broker connection per role.
//!
//! The reply path is IDENTICAL to commands (variant #9): the requester opens a
//! DYNAMIC reply node (`Source::builder().dynamic(true)` → reads the assigned
//! address from `reply_rcv.source()`), sends to queries/<ch> with
//! `Properties.reply_to` = that node + a `correlation_id`; the responder receives on
//! queries/<ch> and replies via an ANONYMOUS sender (null target) with
//! `Properties.to` = the request's reply-to + the echoed correlation-id.
//!
//! The CONTRAST with commands (the whole point of variant #10):
//!
//!   - A query reply carries ONLY the body + metadata — NO x-opt-kubemq-executed /
//!     x-opt-kubemq-error application properties. A query is a "fetch a value" call;
//!     there is no executed/error envelope.
//!   - A FAILED query delivers NOTHING. The connector delivers no reply when a query
//!     fails or times out (MQTT-bridge parity), so the requester simply TIMES OUT. (A
//!     failed command, by contrast, always replies executed=false so its requester is
//!     never left waiting.)
//!
//! This example demonstrates BOTH: a successful query (reply round-trips, body intact)
//! and a query the responder ignores (no reply ⇒ the requester times out on a short
//! demo deadline; in production the connector default is ~30s).
//!
//! Grounded in connector test TestRPCRequesterResponderViaAMQP10 (queries leg)
//! (connectors/amqp10/integration_test.go).
//!
//! Run:
//!
//!   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//!   cargo run -p request-reply

use std::time::Duration;

use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use fe2o3_amqp_types::definitions::SenderSettleMode;
use fe2o3_amqp_types::messaging::{Body, Message, MessageId, Properties, Source, Target};
use fe2o3_amqp_types::primitives::Value;
use tokio::sync::oneshot;

/// The KubeMQ queries channel; the link address is "queries/" + channel (explicit
/// prefix — never rely on a default pattern).
const CHANNEL: &str = "amqp10.examples.queries";

/// A short per-request deadline so the "no reply" leg surfaces a timeout quickly.
/// The connector's own default RPC timeout is ~30s; in production set the request
/// header.ttl to choose the per-request budget.
const DEMO_TIMEOUT: Duration = Duration::from_secs(5);

fn amqp_url() -> String {
    std::env::var("KUBEMQ_AMQP_URL").unwrap_or_else(|_| "amqp://localhost:5672".to_string())
}

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

fn message_id_string(id: &Option<MessageId>) -> Option<String> {
    match id {
        Some(MessageId::String(s)) => Some(s.clone()),
        Some(other) => Some(format!("{other:?}")),
        None => None,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = amqp_url();
    let addr = format!("queries/{CHANNEL}");
    println!("Broker:  {url}");
    println!("Address: {addr}  (KubeMQ pattern=queries, channel={CHANNEL})\n");

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

    ready_rx
        .await
        .map_err(|_| "responder failed to become ready")?;

    run_requester(&url, &addr).await?;

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
// RESPONDER — receives queries on queries/<ch>, replies via an anonymous sender.
// A query whose body is "ignore" gets NO reply (so its requester times out).
// =============================================================================

async fn run_responder(
    url: &str,
    addr: &str,
    ready: oneshot::Sender<()>,
    mut stop: oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut connection = Connection::open("amqp10-examples-queries-responder", url).await?;
    let mut session = Session::begin(&mut connection).await?;

    // ATTACH a receiver on queries/<ch> (server-sender link — the client consumes
    // requests). The client grants credit so the connector pumps requests.
    let mut rcv = Receiver::builder()
        .name("queries-responder-receiver")
        .source(addr)
        .credit_mode(CreditMode::Auto(10))
        .attach(&mut session)
        .await?;

    // ATTACH an ANONYMOUS sender (null target). Each reply sets Properties.to to the
    // request's reply-to so it routes back to the requester's dynamic node. Explicit
    // settle-mode (the connector rejects the AMQP default `mixed`).
    let mut snd = Sender::builder()
        .name("queries-responder-anon-sender")
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
                Err(_) => break,
            },
        };
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
            "[responder] Received query {body:?} (correlation-id={})",
            req_corr.clone().unwrap_or_else(|| "<none>".into())
        );

        // Business logic: a query body of "ignore" is dropped on the floor — the
        // responder sends NOTHING and the requester times out. (A real responder
        // would only fail to reply on a crash / unreachable backend; "ignore" makes
        // the contrast deterministic for the demo.)
        if body == "ignore" {
            println!("[responder] Ignoring {body:?} — NO reply sent (requester will time out)");
            continue;
        }

        // A QUERY reply carries ONLY the body + metadata — NO executed/error
        // application properties (the Commands-vs-Queries contrast).
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
            .data(format!("result:{body}").into_bytes())
            .build();

        snd.send(reply).await?;
        println!("[responder] Replied to {body:?} (body + metadata only, no executed/error props)");
    }

    snd.close().await?;
    rcv.close().await?;
    session.end().await?;
    connection.close().await?;
    Ok(())
}

// =============================================================================
// REQUESTER — dynamic reply node + sender on queries/<ch>; correlates replies.
// =============================================================================

async fn run_requester(
    url: &str,
    addr: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut connection = Connection::open("amqp10-examples-queries-requester", url).await?;
    let mut session = Session::begin(&mut connection).await?;

    // ATTACH a DYNAMIC reply node (empty source + dynamic=true). The server creates a
    // transient node and echoes its address back in the attached source.
    let mut reply_rcv = Receiver::builder()
        .name("queries-requester-reply-node")
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

    // ATTACH a sender on queries/<ch> (server-receiver link — the client produces
    // requests). Explicit settle-mode (the connector rejects the default `mixed`).
    let mut snd = Sender::builder()
        .name("queries-requester-sender")
        .target(addr)
        .sender_settle_mode(SenderSettleMode::Unsettled)
        .attach(&mut session)
        .await?;

    // 1. A SUCCESSFUL query: round-trips, body intact, no executed/error props.
    do_query(
        &mut snd,
        &mut reply_rcv,
        &reply_node,
        "get-temp-sensor-3",
        "corr-qry-1",
    )
    .await?;

    // 2. A query the responder ignores: NOTHING is delivered, so the requester TIMES
    //    OUT. This is the core Queries contrast — a failed/unanswered query has no
    //    error envelope; the absence of a reply IS the failure signal.
    do_query_expect_timeout(
        &mut snd,
        &mut reply_rcv,
        &reply_node,
        "ignore",
        "corr-qry-2",
    )
    .await?;

    snd.close().await?;
    reply_rcv.close().await?;
    session.end().await?;
    connection.close().await?;
    Ok(())
}

/// Send one query naming the dynamic reply node + correlation-id.
async fn send_query(
    snd: &mut Sender,
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
        return Err(format!("send query {body:?}: unexpected outcome {outcome:?}").into());
    }
    println!("[requester] Sent query {body:?} (reply-to=dynamic node, correlation-id={corr})");
    Ok(())
}

/// Send a query and await the correlated reply on the dynamic node; print the result.
async fn do_query(
    snd: &mut Sender,
    reply_rcv: &mut Receiver,
    reply_node: &str,
    body: &str,
    corr: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    send_query(snd, reply_node, body, corr).await?;

    let reply = match tokio::time::timeout(DEMO_TIMEOUT, reply_rcv.recv::<Body<Value>>()).await {
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
    println!(
        "[requester] Reply for {body:?} (correlation-id={}): body={:?}",
        got_corr.unwrap_or_default(),
        body_string(&reply.message().body)
    );
    Ok(())
}

/// Send a query the responder will ignore and show the requester timing out (no
/// reply is the failure signal for queries).
async fn do_query_expect_timeout(
    snd: &mut Sender,
    reply_rcv: &mut Receiver,
    reply_node: &str,
    body: &str,
    corr: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    send_query(snd, reply_node, body, corr).await?;

    match tokio::time::timeout(DEMO_TIMEOUT, reply_rcv.recv::<Body<Value>>()).await {
        Ok(Ok(_)) => Err(format!("expected NO reply for {body:?}, but one arrived").into()),
        _ => {
            // A timeout here is the EXPECTED outcome for an unanswered query.
            println!(
                "[requester] No reply for {body:?} within {DEMO_TIMEOUT:?} — query timed out (expected; failed queries deliver nothing)"
            );
            Ok(())
        }
    }
}

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
// (Unlike a command — which always replies executed=false on failure so the requester
// is never left waiting — a query that fails/goes unanswered delivers NOTHING. The
// requester's timeout IS the failure signal. The connector's own default per-request
// timeout is ~30s; set the request header.ttl to choose it.)
