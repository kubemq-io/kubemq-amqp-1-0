//! Example: queues/ack-release-redelivery (master-table variant #2)
//!
//! The three queue settlement outcomes, side by side, against the KubeMQ AMQP 1.0
//! connector using the native `fe2o3-amqp` client (NO KubeMQ SDK):
//!
//!   - release (`receiver.release`) => NAckRange: the message is requeued to the
//!     tail and REDELIVERED with a grown delivery-count (Header.delivery_count >= 1)
//!     and first_acquirer=false. Each release also increments the broker
//!     receive-count toward MaxReceiveQueue (see the gotcha below).
//!   - reject  (`receiver.reject`)  => AckRange/discard: the message is removed and
//!     NOT redelivered to this receiver (poison handling is a broker
//!     MaxReceiveQueue policy — there is no connector DLX).
//!   - accept  (`receiver.accept`)  => AckRange: the message is removed (success).
//!
//! Grounded in connector tests TestQueueReleasedRedelivery and
//! TestQueueRejectedDiscard (connectors/amqp10/integration_test.go).
//!
//! Run:
//!
//!   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//!   cargo run -p ack-release-redelivery

use std::collections::HashSet;
use std::time::Duration;

use fe2o3_amqp::link::delivery::Delivery;
use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use fe2o3_amqp_types::definitions::{AmqpError, Error, SenderSettleMode};
use fe2o3_amqp_types::messaging::{Body, Message};
use fe2o3_amqp_types::primitives::Value;

const CHANNEL: &str = "amqp10.examples.ack";

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

/// Extract the AMQP header.delivery-count and first-acquirer flag. The connector
/// maps the KubeMQ broker receive-count onto these header fields:
/// delivery-count = ReceiveCount-1, first-acquirer = (ReceiveCount == 1).
fn delivery_info(msg: &Message<Body<Value>>) -> (u32, bool) {
    match &msg.header {
        Some(h) => (h.delivery_count, h.first_acquirer),
        None => (0, true),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = amqp_url();
    let addr = format!("queues/{CHANNEL}");
    println!("Broker:  {url}");
    println!("Address: {addr}\n");

    let mut connection = Connection::open("amqp10-examples-ack", url.as_str()).await?;
    let mut session = Session::begin(&mut connection).await?;

    // Produce three distinct messages: one we release, one we reject, one we accept.
    let mut sender = Sender::builder()
        .name("ack-release-redelivery-sender")
        .target(addr.as_str())
        .sender_settle_mode(SenderSettleMode::Unsettled)
        .attach(&mut session)
        .await?;
    for body in ["release-me", "reject-me", "accept-me"] {
        let outcome = sender.send(body.to_string()).await?;
        if !outcome.is_accepted() {
            return Err(format!("send {body}: unexpected outcome {outcome:?}").into());
        }
    }
    sender.close().await?;
    println!("[send] Produced: release-me, reject-me, accept-me");

    let mut receiver = Receiver::builder()
        .name("ack-release-redelivery-receiver")
        .source(addr.as_str())
        .credit_mode(CreditMode::Auto(10))
        .attach(&mut session)
        .await?;

    // Track which terminal outcome we still owe each body. A released message is
    // redelivered, so "release-me" appears twice (released, then accepted).
    let mut remaining: HashSet<&'static str> = ["release-me", "reject-me", "accept-me"]
        .into_iter()
        .collect();
    let mut released_once = false;

    while !remaining.is_empty() {
        let delivery: Delivery<Body<Value>> = receiver.recv().await?;
        let body = body_string(delivery.message());
        let (dc, first) = delivery_info(delivery.message());

        match body.as_str() {
            "release-me" if !released_once => {
                // First sight: RELEASE it back to the queue tail (NAckRange).
                receiver.release(&delivery).await?;
                released_once = true;
                println!("[recv] {body:<12} delivery-count={dc} first-acquirer={first}  -> RELEASED (requeued)");
            }
            "release-me" => {
                // Redelivery: grown delivery-count, no longer first-acquirer.
                if dc < 1 || first {
                    return Err(format!(
                        "expected redelivered copy with delivery-count>=1 and first-acquirer=false, got dc={dc} first={first}"
                    )
                    .into());
                }
                receiver.accept(&delivery).await?;
                println!("[recv] {body:<12} delivery-count={dc} first-acquirer={first} -> REDELIVERED, then ACCEPTED");
                remaining.remove("release-me");
            }
            "reject-me" => {
                // REJECT it (AckRange/discard). It will NOT be redelivered here.
                let reject_err = Error::new(
                    AmqpError::InternalError,
                    Some("example rejection".to_string()),
                    None,
                );
                receiver.reject(&delivery, reject_err).await?;
                println!("[recv] {body:<12} delivery-count={dc} first-acquirer={first}  -> REJECTED (discarded, no requeue)");
                remaining.remove("reject-me");
            }
            _ => {
                receiver.accept(&delivery).await?;
                println!("[recv] {body:<12} delivery-count={dc} first-acquirer={first}  -> ACCEPTED (removed)");
                remaining.remove("accept-me");
            }
        }
    }

    // The rejected body must NOT come back to this receiver.
    match tokio::time::timeout(Duration::from_secs(2), receiver.recv::<Body<Value>>()).await {
        Ok(Ok(_)) => return Err("rejected message was unexpectedly redelivered".into()),
        _ => println!("[recv] Rejected message was not redelivered (discarded)"),
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
// Address: queues/amqp10.examples.ack
//
// [send] Produced: release-me, reject-me, accept-me
// [recv] release-me   delivery-count=0 first-acquirer=true  -> RELEASED (requeued)
// [recv] reject-me    delivery-count=0 first-acquirer=true  -> REJECTED (discarded, no requeue)
// [recv] accept-me    delivery-count=0 first-acquirer=true  -> ACCEPTED (removed)
// [recv] release-me   delivery-count=1 first-acquirer=false -> REDELIVERED, then ACCEPTED
// [recv] Rejected message was not redelivered (discarded)
//
// Done.
//
// (Delivery order between the original and the redelivered copy can vary; the
// redelivered "release-me" always carries delivery-count>=1 / first-acquirer=false.)
