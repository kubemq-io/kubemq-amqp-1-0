//! Example: events/consumer-group (master-table variant #5)
//!
//! Consumer-group load-balancing over KubeMQ **Events** with the native
//! `fe2o3-amqp` client (NO KubeMQ SDK).
//!
//! The `x-opt-kubemq-group` receiver link property places a subscriber in a named
//! load-balancing group. Within ONE group, the connector round-robins the event
//! stream across the group's members (no duplication). A DISTINCT group is an
//! independent virtual-topic subscriber that gets the FULL stream.
//!
//! This example opens:
//!   - g1a, g1b — two receivers in group "g1" => together they receive every event
//!     with NO body delivered to both (the group splits the stream).
//!   - g2       — one receiver in group "g2" => gets EVERY event (independent).
//!
//! Each receiver runs on its own Session and tokio task: fe2o3-amqp links are
//! driven from a single task, so we never share one across tasks.
//!
//! Grounded in connector test TestEventsPubSubGroupFanout
//! (connectors/amqp10/integration_pubsub_test.go).
//!
//! Run:
//!
//!   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//!   cargo run -p consumer-group

use std::collections::HashSet;
use std::time::Duration;

use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use fe2o3_amqp_types::definitions::{Fields, SenderSettleMode};
use fe2o3_amqp_types::messaging::{Body, Message};
use fe2o3_amqp_types::primitives::{Symbol, Value};

const CHANNEL: &str = "amqp10.examples.consumergroup";

const TOTAL: usize = 30;

const GROUP_PROP: &str = "x-opt-kubemq-group";

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

/// Build the link-property `Fields` carrying `x-opt-kubemq-group = <group>`.
fn group_properties(group: &str) -> Fields {
    Fields::from_iter([(Symbol::from(GROUP_PROP), Value::String(group.to_string()))])
}

/// Drain up to `max` events from one group receiver within `window`, returning the
/// distinct bodies received.
async fn drain(
    mut receiver: Receiver,
    max: usize,
    window: Duration,
) -> Result<HashSet<String>, Box<dyn std::error::Error + Send + Sync>> {
    let mut got: HashSet<String> = HashSet::new();
    let deadline = tokio::time::Instant::now() + window;
    while got.len() < max {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, receiver.recv::<Body<Value>>()).await {
            Ok(Ok(delivery)) => {
                let _ = receiver.accept(&delivery).await; // no-op for pre-settled fan-out
                got.insert(body_string(delivery.message()));
            }
            _ => break, // window elapsed / no more messages
        }
    }
    receiver.close().await?;
    Ok(got)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = amqp_url();
    let addr = format!("events/{CHANNEL}");
    println!("Broker:  {url}");
    println!("Address: {addr}  (KubeMQ pattern=events, channel={CHANNEL})\n");

    let mut connection = Connection::open("amqp10-examples-consumergroup", url.as_str()).await?;

    // Begin three sessions and attach three group receivers up front (one session
    // per receiver — links are not shared across tasks). Each receiver, with its
    // owning session, is then moved into its own task to drain concurrently.
    let labels = [("g1a", "g1"), ("g1b", "g1"), ("g2", "g2")];
    let mut handles = Vec::new();
    for (label, group) in labels {
        let mut session = Session::begin(&mut connection).await?;
        let receiver = Receiver::builder()
            .name(format!("consumer-group-{label}"))
            .source(addr.as_str())
            .credit_mode(CreditMode::Auto(100))
            .properties(group_properties(group))
            .attach(&mut session)
            .await?;
        // Move the session into the task so it outlives the receiver's link.
        let handle = tokio::spawn(async move {
            let got = drain(receiver, TOTAL, Duration::from_secs(30)).await;
            // Keep the session alive until the receiver has fully drained, then end it.
            let mut session = session;
            let _ = session.end().await;
            (label, got)
        });
        handles.push(handle);
    }

    // Let the three subscription pumps go live before publishing (events have no
    // replay — a publish that races a subscription is lost).
    tokio::time::sleep(Duration::from_millis(750)).await;
    println!("[recv] 3 subscribers attached: g1a+g1b (group g1), g2 (group g2)");

    // Publish on a dedicated session. Pre-settled fire-and-forget.
    let mut prod_session = Session::begin(&mut connection).await?;
    let mut sender = Sender::builder()
        .name("consumer-group-sender")
        .target(addr.as_str())
        .sender_settle_mode(SenderSettleMode::Settled)
        .attach(&mut prod_session)
        .await?;
    for i in 0..TOTAL {
        let body = format!("event-{i:03}");
        sender.send(body).await?;
    }
    sender.close().await?;
    prod_session.end().await?;
    println!("[send] Published {TOTAL} events (pre-settled)");

    // Collect each subscriber's results.
    let mut g1a: HashSet<String> = HashSet::new();
    let mut g1b: HashSet<String> = HashSet::new();
    let mut g2: HashSet<String> = HashSet::new();
    for handle in handles {
        let (label, got) = handle.await?;
        let got = got.map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
        match label {
            "g1a" => g1a = got,
            "g1b" => g1b = got,
            _ => g2 = got,
        }
    }

    // --- Assert the consumer-group semantics ---------------------------------

    // g2 (a distinct group) receives EVERY event.
    if g2.len() != TOTAL {
        return Err(format!(
            "group g2 (independent) expected all {TOTAL} events, got {}",
            g2.len()
        )
        .into());
    }
    println!(
        "[recv] g2 (group g2, independent): {}/{TOTAL} events — FULL stream",
        g2.len()
    );

    // g1a + g1b TOGETHER receive every event, with NO body delivered to both.
    let dups = g1a.intersection(&g1b).count();
    if dups != 0 {
        return Err(format!(
            "group g1 load-balancing broken: {dups} event(s) delivered to BOTH g1a and g1b"
        )
        .into());
    }
    let combined: HashSet<&String> = g1a.union(&g1b).collect();
    if combined.len() != TOTAL {
        return Err(format!(
            "group g1 members together expected all {TOTAL} events, got {}",
            combined.len()
        )
        .into());
    }
    if g1a.is_empty() || g1b.is_empty() {
        return Err(format!(
            "group g1 not load-balanced: g1a={} g1b={} (one member got nothing)",
            g1a.len(),
            g1b.len()
        )
        .into());
    }
    println!(
        "[recv] g1a (group g1): {} events; g1b (group g1): {} events",
        g1a.len(),
        g1b.len()
    );
    println!(
        "[recv] g1a+g1b together: {}/{TOTAL} events, 0 duplicates — group SPLIT the stream",
        combined.len()
    );

    connection.close().await?;

    println!("\nDone.");
    Ok(())
}

// Expected output (the g1a/g1b split varies run to run; the totals are fixed):
//
// Broker:  amqp://localhost:5672
// Address: events/amqp10.examples.consumergroup  (KubeMQ pattern=events, channel=amqp10.examples.consumergroup)
//
// [recv] 3 subscribers attached: g1a+g1b (group g1), g2 (group g2)
// [send] Published 30 events (pre-settled)
// [recv] g2 (group g2, independent): 30/30 events — FULL stream
// [recv] g1a (group g1): 16 events; g1b (group g1): 14 events
// [recv] g1a+g1b together: 30/30 events, 0 duplicates — group SPLIT the stream
//
// Done.
