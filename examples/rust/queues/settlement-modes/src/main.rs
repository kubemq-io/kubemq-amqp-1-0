//! Example: queues/settlement-modes (master-table variant #3)
//!
//! The two producer reliability tiers, side by side, against the KubeMQ AMQP 1.0
//! connector using the native `fe2o3-amqp` client (NO KubeMQ SDK):
//!
//!   - PRE-SETTLED sender (`SenderSettleMode::Settled`): at-MOST-once. Each
//!     TRANSFER is marked settled by the client, so `send` returns WITHOUT a
//!     server outcome. Fast and fire-and-forget — if the broker drops the transfer
//!     (oversize, no capacity), the producer never learns. No redelivery, no
//!     delivery confirmation.
//!   - UNSETTLED sender (`SenderSettleMode::Unsettled`): at-LEAST-once. Each `send`
//!     blocks until the connector returns an `Accepted` outcome, confirming the
//!     broker stored the message. This is the variant #1 contract.
//!
//! On the consume side this example requests `ReceiverSettleMode::First` (the only
//! receiver settle-mode the connector supports): the server settles the delivery
//! on the first transfer. `rcv-settle-mode=second` is rejected by the connector
//! with a DETACH carrying `amqp:not-implemented` (gotcha #7 — see README).
//!
//! Both senders' messages drain to the same consumer; the program proves no loss
//! on this happy path while explaining the reliability difference.
//!
//! Grounded in connector test TestQueuePreSettled
//! (connectors/amqp10/integration_test.go).
//!
//! Run:
//!
//!   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//!   cargo run -p settlement-modes

use std::collections::HashSet;
use std::time::Duration;

use fe2o3_amqp::link::delivery::Delivery;
use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use fe2o3_amqp_types::definitions::{ReceiverSettleMode, SenderSettleMode};
use fe2o3_amqp_types::messaging::{Body, Message};
use fe2o3_amqp_types::primitives::Value;

const CHANNEL: &str = "amqp10.examples.settlement";

/// Messages produced on each sender (pre-settled, then unsettled).
const PER_SENDER: usize = 10;

fn amqp_url() -> String {
    std::env::var("KUBEMQ_AMQP_URL").unwrap_or_else(|_| "amqp://localhost:5672".to_string())
}

fn body_string(msg: &Message<Body<Value>>) -> String {
    let bytes = match &msg.body {
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = amqp_url();
    let addr = format!("queues/{CHANNEL}");
    println!("Broker:  {url}");
    println!("Address: {addr}\n");

    let mut connection = Connection::open("amqp10-examples-settlement", url.as_str()).await?;
    let mut session = Session::begin(&mut connection).await?;

    // =========================================================================
    // 1. PRE-SETTLED sender (at-most-once). SenderSettleMode::Settled marks every
    //    TRANSFER as already settled, so `send` does NOT wait for a server outcome
    //    — it returns as soon as the frame is written. Fast, but no delivery
    //    confirmation and no redelivery.
    // =========================================================================
    let mut settled_sender = Sender::builder()
        .name("settlement-modes-presettled")
        .target(addr.as_str())
        .sender_settle_mode(SenderSettleMode::Settled)
        .attach(&mut session)
        .await?;
    for i in 0..PER_SENDER {
        let body = format!("presettled-{i:02}");
        settled_sender.send(body).await?;
    }
    settled_sender.close().await?;
    println!(
        "[send] Pre-settled (at-most-once): produced {PER_SENDER} messages — NO outcome awaited"
    );

    // =========================================================================
    // 2. UNSETTLED sender (at-least-once). Each `send` blocks until the connector
    //    returns an `Accepted` outcome confirming the broker stored the message.
    // =========================================================================
    let mut unsettled_sender = Sender::builder()
        .name("settlement-modes-unsettled")
        .target(addr.as_str())
        .sender_settle_mode(SenderSettleMode::Unsettled)
        .attach(&mut session)
        .await?;
    for i in 0..PER_SENDER {
        let body = format!("unsettled-{i:02}");
        let outcome = unsettled_sender.send(body.clone()).await?;
        if !outcome.is_accepted() {
            return Err(format!("unsettled send {body}: unexpected outcome {outcome:?}").into());
        }
    }
    unsettled_sender.close().await?;
    println!(
        "[send] Unsettled (at-least-once): produced {PER_SENDER} messages — each Accepted outcome"
    );

    // =========================================================================
    // 3. Consume with ReceiverSettleMode::First — the ONLY receiver settle-mode
    //    the connector supports. (`second` => DETACH amqp:not-implemented; see the
    //    README gotcha.) Accept each message to drain the queue.
    // =========================================================================
    let mut receiver = Receiver::builder()
        .name("settlement-modes-receiver")
        .source(addr.as_str())
        .receiver_settle_mode(ReceiverSettleMode::First)
        .credit_mode(CreditMode::Auto(20))
        .attach(&mut session)
        .await?;

    let total = 2 * PER_SENDER;
    let mut presettled_seen = 0usize;
    let mut unsettled_seen = 0usize;
    let mut seen: HashSet<String> = HashSet::with_capacity(total);
    while seen.len() < total {
        let delivery: Delivery<Body<Value>> = receiver.recv().await?;
        let body = body_string(delivery.message());
        receiver.accept(&delivery).await?;
        if seen.insert(body.clone()) {
            if body.starts_with("presettled") {
                presettled_seen += 1;
            } else {
                unsettled_seen += 1;
            }
        }
    }
    println!(
        "[recv] Drained {} total — {presettled_seen} pre-settled + {unsettled_seen} unsettled (rcv-settle-mode=first)",
        seen.len()
    );

    // =========================================================================
    // 4. Assert the queue is empty — a further recv must time out.
    // =========================================================================
    match tokio::time::timeout(Duration::from_secs(2), receiver.recv::<Body<Value>>()).await {
        Ok(Ok(_)) => return Err("expected an empty queue, but received another message".into()),
        _ => println!("[recv] Queue drained to empty (no further messages)"),
    }

    receiver.close().await?;
    session.end().await?;
    connection.close().await?;

    println!("\nDone.");
    Ok(())
}

// Expected output:
//
// Broker:  amqp://localhost:5672
// Address: queues/amqp10.examples.settlement
//
// [send] Pre-settled (at-most-once): produced 10 messages — NO outcome awaited
// [send] Unsettled (at-least-once): produced 10 messages — each Accepted outcome
// [recv] Drained 20 total — 10 pre-settled + 10 unsettled (rcv-settle-mode=first)
// [recv] Queue drained to empty (no further messages)
//
// Done.
//
// (On a healthy broker pre-settled messages also drain — the difference is the
// PRODUCER guarantee, not the happy-path result: a pre-settled send returns before
// any broker confirmation, so a drop on the way in is invisible to the producer.
// Unsettled sends block until the broker confirms storage.)
