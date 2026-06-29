//! Example: advanced/anonymous-terminus (master-table variant #12)
//!
//! An ANONYMOUS sender (a link attached with a NULL target —
//! `Target::builder().build()`) carries no fixed channel. Instead, EACH message
//! selects its own destination via its `properties.to` field, and the KubeMQ
//! connector routes it per-message to the right pattern/channel. One link, many
//! destinations. Driven with the native `fe2o3-amqp` client (NO KubeMQ SDK).
//!
//! Flow:
//!   - ATTACH an anonymous sender: `Sender::builder().target(Target::builder().build())`
//!     → a null target.
//!   - Send #1: Message with `properties.to = "queues/<ch>"` routes to a queue.
//!   - Send #2: Message with `properties.to = "events/<ch>"` routes to an events
//!     topic (a subscriber is attached BEFORE the send — events are fire-and-forget).
//!   - The queue message is then consumed back to prove it landed correctly.
//!   - (Demonstrated as expected errors) a BAD `to` (unknown prefix) and a MISSING
//!     `to` are both rejected by the connector: the `send` returns an error carrying
//!     amqp:precondition-failed.
//!
//! Per-message authorization: each anonymous send is authorized for WRITE on the
//! resolved (pattern, channel) via the connector's Casbin policy — there is no
//! per-link grant for an anonymous terminus.
//!
//! GOTCHA — an anonymous sender MUST set an explicit settle-mode. fe2o3-amqp defaults
//! a sender to `mixed`, which the connector rejects at ATTACH (amqp:not-implemented).
//! This example uses `SenderSettleMode::Unsettled` so each routed send is confirmed.
//!
//! Grounded in connector test TestAnonymousTerminusRouting
//! (connectors/amqp10/integration_test.go).
//!
//! Run:
//!
//!   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//!   cargo run -p anonymous-terminus

use std::time::Duration;

use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use fe2o3_amqp_types::definitions::SenderSettleMode;
use fe2o3_amqp_types::messaging::{Body, Data, Message, Properties, Target};
use fe2o3_amqp_types::primitives::Value;

/// Explicit <pattern>/<channel> destinations selected per-message via properties.to.
const QUEUE_CHANNEL: &str = "amqp10.examples.anon.q";
const EVENTS_CHANNEL: &str = "amqp10.examples.anon.e";

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

/// Build a message routed to `to` via its `properties.to` field; if `to` is None the
/// message carries NO destination (an orphan — the connector rejects it).
fn routed_message(to: Option<&str>, body: &str) -> Message<Data> {
    let mut builder = Message::builder();
    if let Some(dest) = to {
        builder = builder.properties(Properties::builder().to(dest.to_string()).build());
    }
    builder.data(body.as_bytes().to_vec()).build()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = amqp_url();
    let queue_to = format!("queues/{QUEUE_CHANNEL}");
    let events_to = format!("events/{EVENTS_CHANNEL}");
    println!("Broker: {url}");
    println!("Anonymous sender (null target) — routes per-message via properties.to");
    println!("  msg #1 to: {queue_to}");
    println!("  msg #2 to: {events_to}\n");

    let mut connection = Connection::open("amqp10-examples-anon", url.as_str()).await?;
    let mut session = Session::begin(&mut connection).await?;

    // =========================================================================
    // 1. ATTACH an anonymous sender. The null target attaches a link with no bound
    //    channel — every message routes by its own properties.to.
    // =========================================================================
    let mut anon = Sender::builder()
        .name("anonymous-terminus-sender")
        .target(Target::builder().build()) // null target = anonymous terminus
        .sender_settle_mode(SenderSettleMode::Unsettled)
        .attach(&mut session)
        .await?;
    println!("[attach] Anonymous sender attached (null target)");

    // A consumer for the EVENTS channel must be subscribed BEFORE we publish to it —
    // events are fire-and-forget (no replay). The queue message, by contrast, is
    // durable, so we consume it after sending.
    let mut event_rcv = Receiver::builder()
        .name("anonymous-terminus-events-receiver")
        .source(events_to.as_str())
        .credit_mode(CreditMode::Auto(5))
        .attach(&mut session)
        .await?;
    tokio::time::sleep(Duration::from_millis(500)).await; // let the subscription register

    // =========================================================================
    // 2. Send #1 — route to a QUEUE via properties.to. The connector resolves
    //    "queues/<ch>", authorizes WRITE for this connection, and stores it.
    // =========================================================================
    let outcome = anon
        .send(routed_message(Some(&queue_to), "to-queue"))
        .await?;
    if !outcome.is_accepted() {
        return Err(format!("send to queue: unexpected outcome {outcome:?}").into());
    }
    println!("[send] msg #1 routed to {queue_to} (accepted)");

    // =========================================================================
    // 3. Send #2 — route to an EVENTS topic via properties.to. Same anonymous link,
    //    a DIFFERENT pattern. The subscriber attached above receives it.
    // =========================================================================
    anon.send(routed_message(Some(&events_to), "to-events"))
        .await?;
    println!("[send] msg #2 routed to {events_to} (accepted)");

    // =========================================================================
    // 4. Negative cases (expected errors) — the connector rejects a bad/missing `to`
    //    with amqp:precondition-failed, surfaced to the client as a send error. The
    //    anonymous link stays usable afterwards.
    // =========================================================================
    match anon
        .send(routed_message(Some("bogus/prefix/x"), "nowhere"))
        .await
    {
        Ok(outcome) if outcome.is_accepted() => {
            return Err("expected a bad `to` to be rejected, but the send was accepted".into());
        }
        Ok(outcome) => println!(
            "[send] msg with bad `to`=\"bogus/prefix/x\" rejected as expected: {outcome:?}"
        ),
        Err(e) => {
            println!("[send] msg with bad `to`=\"bogus/prefix/x\" rejected as expected: {e:?}")
        }
    }

    match anon.send(routed_message(None, "orphan")).await {
        Ok(outcome) if outcome.is_accepted() => {
            return Err("expected a missing `to` to be rejected, but the send was accepted".into());
        }
        Ok(outcome) => println!("[send] msg with NO `to` rejected as expected: {outcome:?}"),
        Err(e) => println!("[send] msg with NO `to` rejected as expected: {e:?}"),
    }
    anon.close().await?;

    // =========================================================================
    // 5. Verify routing — consume the queue message back, and receive the event.
    // =========================================================================
    let mut q_rcv = Receiver::builder()
        .name("anonymous-terminus-queue-receiver")
        .source(queue_to.as_str())
        .credit_mode(CreditMode::Auto(1))
        .attach(&mut session)
        .await?;
    let q_got =
        match tokio::time::timeout(Duration::from_secs(30), q_rcv.recv::<Body<Value>>()).await {
            Ok(Ok(d)) => d,
            _ => return Err("receive queue message: timed out".into()),
        };
    q_rcv.accept(&q_got).await?;
    println!(
        "[recv] queue {queue_to} delivered: {:?}",
        body_string(&q_got.message().body)
    );

    let e_got = match tokio::time::timeout(Duration::from_secs(30), event_rcv.recv::<Body<Value>>())
        .await
    {
        Ok(Ok(d)) => d,
        _ => return Err("receive event message: timed out".into()),
    };
    let _ = event_rcv.accept(&e_got).await; // no-op for pre-settled fan-out
    println!(
        "[recv] events {events_to} delivered: {:?}",
        body_string(&e_got.message().body)
    );

    q_rcv.close().await?;
    event_rcv.close().await?;
    session.end().await?;
    connection.close().await?;

    println!("\nDone.");
    Ok(())
}

// Expected output:
//
// Broker: amqp://localhost:5672
// Anonymous sender (null target) — routes per-message via properties.to
//   msg #1 to: queues/amqp10.examples.anon.q
//   msg #2 to: events/amqp10.examples.anon.e
//
// [attach] Anonymous sender attached (null target)
// [send] msg #1 routed to queues/amqp10.examples.anon.q (accepted)
// [send] msg #2 routed to events/amqp10.examples.anon.e (accepted)
// [send] msg with bad `to`="bogus/prefix/x" rejected as expected: ... (amqp:precondition-failed)
// [send] msg with NO `to` rejected as expected: ... (amqp:precondition-failed)
// [recv] queue queues/amqp10.examples.anon.q delivered: "to-queue"
// [recv] events events/amqp10.examples.anon.e delivered: "to-events"
//
// Done.
