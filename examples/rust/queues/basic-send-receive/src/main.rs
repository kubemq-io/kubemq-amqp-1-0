//! Example: queues/basic-send-receive (master-table variant #1)
//!
//! At-least-once produce + credit-based consume against the KubeMQ AMQP 1.0
//! connector using the native `fe2o3-amqp` client (NO KubeMQ SDK).
//!
//! Flow:
//!   - Sender -> "queues/<ch>" (Unsettled): each `send` waits for the server's
//!     receiver DISPOSITION (Accepted outcome) before returning.
//!   - Receiver <- "queues/<ch>" with auto credit 10: `recv` + `accept` each =>
//!     the connector emits an AckRange and removes the message from the queue.
//!   - After draining, the queue is empty (a further `recv` times out).
//!
//! Grounded in connector test TestQueueProduceConsumeAtLeastOnce
//! (connectors/amqp10/integration_test.go).
//!
//! Run:
//!
//!   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//!   cargo run -p basic-send-receive

use std::collections::HashSet;
use std::time::Duration;

use fe2o3_amqp::link::delivery::Delivery;
use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use fe2o3_amqp_types::definitions::SenderSettleMode;
use fe2o3_amqp_types::messaging::{Body, Message};
use fe2o3_amqp_types::primitives::Value;

/// The KubeMQ queue channel; the link address is "queues/" + CHANNEL (explicit
/// prefix — never rely on DefaultPattern).
const CHANNEL: &str = "amqp10.examples.basic";

const TOTAL: usize = 10;

fn amqp_url() -> String {
    std::env::var("KUBEMQ_AMQP_URL").unwrap_or_else(|_| "amqp://localhost:5672".to_string())
}

/// Concatenate every `Data` body section into a byte vector (a `Data` body may be
/// fragmented into one or more sections).
fn body_bytes(msg: &Message<Body<Value>>) -> Vec<u8> {
    match &msg.body {
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
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = amqp_url();
    let addr = format!("queues/{CHANNEL}");
    println!("Broker:  {url}");
    println!("Address: {addr}  (KubeMQ pattern=queues, channel={CHANNEL})\n");

    // OPEN: connect (SASL ANONYMOUS by default — no userinfo in the URL). A
    // non-empty container-id is required by the connector and supplied here.
    let mut connection = Connection::open("amqp10-examples-basic", url.as_str()).await?;

    // BEGIN: one session carries the producer + consumer links below.
    let mut session = Session::begin(&mut connection).await?;

    // =========================================================================
    // 1. Produce — ATTACH a sender (server-receiver link). We pin
    //    SenderSettleMode::Unsettled: the connector rejects the AMQP default
    //    `mixed`, and Unsettled means each `send` is at-least-once and blocks
    //    until the server returns an `Accepted` outcome (broker stored it).
    // =========================================================================
    let mut sender = Sender::builder()
        .name("basic-send-receive-sender")
        .target(addr.as_str())
        .sender_settle_mode(SenderSettleMode::Unsettled)
        .attach(&mut session)
        .await?;

    for i in 0..TOTAL {
        let body = format!("msg-{i:03}");
        let outcome = sender.send(body.clone()).await?;
        // Unsettled => the server returns a terminal outcome; assert it accepted.
        if !outcome.is_accepted() {
            return Err(format!("send {body}: unexpected outcome {outcome:?}").into());
        }
    }
    sender.close().await?;
    println!("[send] Produced {TOTAL} messages to {addr} (Accepted outcome each)");

    // =========================================================================
    // 2. Consume — ATTACH a receiver (server-sender link). The CLIENT grants
    //    credit. CreditMode::Auto(10) issues 10 credits up front and auto-
    //    replenishes as deliveries settle. `accept` => the connector AckRanges
    //    the delivery and removes it from the queue.
    // =========================================================================
    let mut receiver = Receiver::builder()
        .name("basic-send-receive-receiver")
        .source(addr.as_str())
        .credit_mode(CreditMode::Auto(10))
        .attach(&mut session)
        .await?;

    let mut seen: HashSet<String> = HashSet::with_capacity(TOTAL);
    while seen.len() < TOTAL {
        let delivery: Delivery<Body<Value>> = receiver.recv().await?;
        let body = String::from_utf8_lossy(&body_bytes(delivery.message())).into_owned();
        receiver.accept(&delivery).await?;
        seen.insert(body);
    }
    println!(
        "[recv] Consumed and accepted {} messages (no loss)",
        seen.len()
    );

    // =========================================================================
    // 3. Assert the queue is empty — a further recv must time out.
    // =========================================================================
    match tokio::time::timeout(Duration::from_secs(2), receiver.recv::<Body<Value>>()).await {
        Ok(Ok(_)) => return Err("expected an empty queue, but received another message".into()),
        _ => println!("[recv] Queue drained to empty (no further messages)"),
    }

    // DETACH / CLOSE: clean up the receiver link, the session, then the connection.
    receiver.close().await?;
    session.end().await?;
    connection.close().await?;

    println!("\nDone.");
    Ok(())
}

// Expected output:
//
// Broker:  amqp://localhost:5672
// Address: queues/amqp10.examples.basic  (KubeMQ pattern=queues, channel=amqp10.examples.basic)
//
// [send] Produced 10 messages to queues/amqp10.examples.basic (Accepted outcome each)
// [recv] Consumed and accepted 10 messages (no loss)
// [recv] Queue drained to empty (no further messages)
//
// Done.
