//! Example: events/basic-pubsub (master-table variant #4)
//!
//! Fan-out, at-most-once pub/sub over KubeMQ **Events** with the native
//! `fe2o3-amqp` client (NO KubeMQ SDK).
//!
//! Events are a fire-hose: deliveries are pre-settled (no DISPOSITION feedback),
//! there is NO replay, and a message that arrives at a subscriber with zero credit
//! is SILENTLY DROPPED (counted by the server metric
//! kubemq_amqp10_events_dropped_no_credit_total). Two rules follow:
//!
//!   - SUBSCRIBE BEFORE PUBLISH. The attach reply only confirms the link, not that
//!     the connector's subscription pump is live. A publish that races the
//!     subscription is lost (no replay). This example sleeps ~750ms after attach
//!     before producing.
//!   - GRANT STANDING CREDIT. The receiver attaches with a large standing credit
//!     (CreditMode::Auto auto-replenishes) so the subscriber is never at 0 credit
//!     when an event arrives.
//!
//! The sender publishes pre-settled to events/<ch> (fire-and-forget); the receiver
//! drains every event on the happy path.
//!
//! Grounded in connector test TestEventsPubSubGroupFanout (the lone-subscriber
//! fan-out leg) (connectors/amqp10/integration_pubsub_test.go).
//!
//! Run:
//!
//!   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//!   cargo run -p basic-pubsub

use std::collections::HashSet;
use std::time::Duration;

use fe2o3_amqp::link::delivery::Delivery;
use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use fe2o3_amqp_types::definitions::SenderSettleMode;
use fe2o3_amqp_types::messaging::{Body, Message};
use fe2o3_amqp_types::primitives::Value;

const CHANNEL: &str = "amqp10.examples.pubsub";

const TOTAL: usize = 20;

/// Granted up front so the subscriber is never at 0 credit when an event arrives.
/// CreditMode::Auto auto-replenishes as deliveries settle.
const STANDING_CREDIT: u32 = 100;

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
    let addr = format!("events/{CHANNEL}");
    println!("Broker:  {url}");
    println!("Address: {addr}  (KubeMQ pattern=events, channel={CHANNEL})\n");

    let mut connection = Connection::open("amqp10-examples-pubsub", url.as_str()).await?;
    let mut session = Session::begin(&mut connection).await?;

    // =========================================================================
    // 1. SUBSCRIBE FIRST. Attach the receiver with standing credit BEFORE any
    //    publish. Events have no replay — a publish that beats the subscription is
    //    lost forever.
    // =========================================================================
    let mut receiver = Receiver::builder()
        .name("basic-pubsub-receiver")
        .source(addr.as_str())
        .credit_mode(CreditMode::Auto(STANDING_CREDIT))
        .attach(&mut session)
        .await?;
    println!("[recv] Subscribed to {addr} with standing credit {STANDING_CREDIT}");

    // The attach reply confirms the link, not that the connector's subscription
    // pump has run its SubscribeEvents yet. Wait for the pump to go live before
    // publishing, or the first events race the subscription and are dropped.
    tokio::time::sleep(Duration::from_millis(750)).await;
    println!("[recv] Subscription pump settled (waited 750ms before publishing)");

    // =========================================================================
    // 2. PUBLISH pre-settled. SenderSettleMode::Settled marks every TRANSFER as
    //    settled (fire-and-forget) — events are at-most-once, so there is no
    //    outcome to await and no produce confirmation.
    // =========================================================================
    let mut sender = Sender::builder()
        .name("basic-pubsub-sender")
        .target(addr.as_str())
        .sender_settle_mode(SenderSettleMode::Settled)
        .attach(&mut session)
        .await?;
    for i in 0..TOTAL {
        let body = format!("event-{i:03}");
        sender.send(body).await?;
    }
    sender.close().await?;
    println!("[send] Published {TOTAL} events (pre-settled, fire-and-forget)");

    // =========================================================================
    // 3. RECEIVE. With standing credit the subscriber drains every event.
    // =========================================================================
    let mut seen: HashSet<String> = HashSet::with_capacity(TOTAL);
    while seen.len() < TOTAL {
        let delivery: Delivery<Body<Value>> = receiver.recv().await?;
        // accept is a no-op on pre-settled pub/sub deliveries but is harmless.
        let _ = receiver.accept(&delivery).await;
        seen.insert(body_string(delivery.message()));
    }
    println!(
        "[recv] Received all {} events (continuous credit => no 0-credit drop)",
        seen.len()
    );

    receiver.close().await?;
    session.end().await?;
    connection.close().await?;

    println!("\nDone.");
    Ok(())
}

// Expected output:
//
// Broker:  amqp://localhost:5672
// Address: events/amqp10.examples.pubsub  (KubeMQ pattern=events, channel=amqp10.examples.pubsub)
//
// [recv] Subscribed to events/amqp10.examples.pubsub with standing credit 100
// [recv] Subscription pump settled (waited 750ms before publishing)
// [send] Published 20 events (pre-settled, fire-and-forget)
// [recv] Received all 20 events (continuous credit => no 0-credit drop)
//
// Done.
//
// (Events are at-most-once with no replay: if the subscriber were at 0 credit when
// an event arrived, that event would be SILENTLY DROPPED and counted on the server
// metric kubemq_amqp10_events_dropped_no_credit_total — never surfaced as a client
// error. Standing credit + subscribe-before-publish avoid both losses.)
