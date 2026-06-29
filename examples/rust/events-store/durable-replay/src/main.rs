//! Example: events-store/durable-replay (master-table variant #7)
//!
//! Durable subscriptions with resume over KubeMQ **Events Store** using the native
//! `fe2o3-amqp` client (NO KubeMQ SDK).
//!
//! Unlike Events (fire-and-forget, no replay), Events Store PERSISTS the stream
//! and lets a DURABLE subscriber resume where it left off. A durable subscription
//! is identified by the pair:
//!
//!   (connection container-id, link name)
//!
//! To make a subscriber durable and resumable:
//!
//!   - open with a STABLE container-id → `Connection::builder().container_id("...")`
//!   - attach with a STABLE link name  → `Receiver::builder().name("...")`
//!   - request a non-expiring source   → `Source::builder().expiry_policy(TerminusExpiryPolicy::Never)`
//!   - set the start position once      → link property `x-opt-kubemq-start = "new-only"`
//!
//! On a clean disconnect the connector preserves the durable position. Re-attaching
//! with the SAME (container-id, link name) RESUMES the subscription and delivers
//! every event published while the subscriber was away — no loss, no replay of
//! already-consumed events.
//!
//! Flow:
//!   1. Open with container-id "amqp10-examples-durable-container"; attach durable
//!      receiver "durable-sub" (start new-only). Publish 3 events; receive all 3.
//!   2. Disconnect (close the connection).
//!   3. Publish 5 MORE events while the durable subscriber is away.
//!   4. Re-open with the SAME container-id; re-attach "durable-sub". The
//!      subscription RESUMES and delivers the 5 missed events.
//!
//! Grounded in connector test TestEventsStoreDurableReplay
//! (connectors/amqp10/integration_pubsub_test.go).
//!
//! Run:
//!
//!   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//!   cargo run -p durable-replay

use std::collections::HashSet;
use std::time::Duration;

use fe2o3_amqp::connection::ConnectionHandle;
use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::session::SessionHandle;
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use fe2o3_amqp_types::definitions::SenderSettleMode;
use fe2o3_amqp_types::messaging::{Body, Source, TerminusExpiryPolicy};
use fe2o3_amqp_types::primitives::{OrderedMap, Symbol, Value};

const CHANNEL: &str = "amqp10.examples.durable";

/// The durable identity = (container-id, link name). Both MUST be stable across
/// reconnects for the subscription to resume.
const CONTAINER_ID: &str = "amqp10-examples-durable-container";
const LINK_NAME: &str = "durable-sub";

/// Granted up front so the durable subscriber is never at 0 credit when a stored
/// event is replayed.
const STANDING_CREDIT: u32 = 100;

/// The KubeMQ start-position link property.
const START_PROP: &str = "x-opt-kubemq-start";

fn amqp_url() -> String {
    std::env::var("KUBEMQ_AMQP_URL").unwrap_or_else(|_| "amqp://localhost:5672".to_string())
}

/// Concatenate every `Data` body section into a UTF-8 string.
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

/// Build the durable source for `addr`: a non-expiring terminus so the connector
/// preserves the subscription state across detach.
fn durable_source(addr: &str) -> Source {
    Source::builder()
        .address(addr)
        .expiry_policy(TerminusExpiryPolicy::Never)
        .build()
}

/// The `x-opt-kubemq-start` link property carried on the durable attach.
fn start_props(start: &str) -> OrderedMap<Symbol, Value> {
    let mut props = OrderedMap::new();
    props.insert(Symbol::from(START_PROP), Value::String(start.to_string()));
    props
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = amqp_url();
    let addr = format!("events-store/{CHANNEL}");
    println!("Broker:     {url}");
    println!("Address:    {addr}  (KubeMQ pattern=events-store, channel={CHANNEL})");
    println!("Durable id: container-id={CONTAINER_ID:?}  link-name={LINK_NAME:?}\n");

    // =========================================================================
    // 0. PRODUCER — a separate plain connection that publishes to the
    //    events-store stream throughout the demo (it needs no stable identity).
    //    Unsettled so each accepted transfer is confirmed persisted.
    // =========================================================================
    let mut prod_conn = Connection::open("amqp10-examples-durable-producer", url.as_str()).await?;
    let mut prod_sess = Session::begin(&mut prod_conn).await?;
    let mut sender = Sender::builder()
        .name("durable-replay-producer")
        .target(addr.as_str())
        .sender_settle_mode(SenderSettleMode::Unsettled)
        .attach(&mut prod_sess)
        .await?;

    // =========================================================================
    // 1. DURABLE SUBSCRIBE (first attach). Stable container-id + link name +
    //    non-expiring source make this subscription durable. start=new-only means
    //    "deliver events from now on" — this attach establishes the cursor.
    // =========================================================================
    let (mut dur_rcv, mut dur_sess, mut dur_conn) =
        attach_durable(url.as_str(), addr.as_str(), "first attach").await?;
    publish(&mut sender, 0, 3).await?; // 3 events while the durable subscriber is live

    let first = drain(&mut dur_rcv, 3, Duration::from_secs(30)).await?;
    if first.len() != 3 {
        return Err(format!(
            "durable subscriber expected the first 3 events, got {}: {first:?}",
            first.len()
        )
        .into());
    }
    println!(
        "[recv] First attach received {} events: {first:?}\n",
        first.len()
    );

    // =========================================================================
    // 2. DISCONNECT. A clean close detaches the durable link; the connector
    //    preserves the durable cursor for this (container-id, link name). We end
    //    the session and close the connection so the next attach is a genuine
    //    re-connect with the same identity.
    // =========================================================================
    dur_rcv.close().await?;
    dur_sess.end().await?;
    dur_conn.close().await?;
    println!("[conn] Durable subscriber DISCONNECTED (cursor preserved by the connector)");
    tokio::time::sleep(Duration::from_secs(1)).await; // let the detach settle

    // =========================================================================
    // 3. PUBLISH WHILE AWAY. 5 more events arrive at the persisted stream while
    //    the durable subscriber is offline.
    // =========================================================================
    publish(&mut sender, 3, 8).await?;
    println!("[send] Published 5 more events WHILE the durable subscriber was away");

    // =========================================================================
    // 4. RE-ATTACH with the SAME durable identity. The subscription RESUMES and
    //    delivers exactly the 5 events published while away (not the first 3
    //    again, and nothing lost).
    // =========================================================================
    let (mut dur_rcv2, mut dur_sess2, mut dur_conn2) =
        attach_durable(url.as_str(), addr.as_str(), "re-attach").await?;
    let resumed = drain(&mut dur_rcv2, 5, Duration::from_secs(30)).await?;
    let resumed_set: HashSet<String> = resumed.iter().cloned().collect();
    for i in 3..8 {
        let body = format!("es-{i:03}");
        if !resumed_set.contains(&body) {
            return Err(format!("durable resume missing event {body} (got {resumed:?})").into());
        }
    }
    println!(
        "[recv] Re-attach RESUMED and received the {} events published while away: {resumed:?}",
        resumed_set.len()
    );
    println!("[recv] No loss, no re-delivery of the already-consumed first 3 — the durable cursor resumed exactly");

    dur_rcv2.close().await?;
    dur_sess2.end().await?;
    dur_conn2.close().await?;

    // Clean up the producer connection.
    sender.close().await?;
    prod_sess.end().await?;
    prod_conn.close().await?;

    println!("\nDone.");
    Ok(())
}

/// Open a connection with the stable container-id and attach the durable receiver
/// (stable link name + non-expiring source + start position). The caller must keep
/// the returned `SessionHandle` alive for the link's lifetime (dropping it ends the
/// session) and close the connection to disconnect the subscriber.
#[allow(clippy::type_complexity)]
async fn attach_durable(
    url: &str,
    addr: &str,
    phase: &str,
) -> Result<(Receiver, SessionHandle<()>, ConnectionHandle<()>), Box<dyn std::error::Error>> {
    let mut connection = Connection::builder()
        .container_id(CONTAINER_ID) // stable container-id = half the durable identity
        .open(url)
        .await?;
    let mut session = Session::begin(&mut connection).await?;
    let receiver = Receiver::builder()
        .name(LINK_NAME) // stable link name = the other half of the durable identity
        .source(durable_source(addr)) // non-expiring source
        .properties(start_props("new-only")) // start cursor (honoured on first attach)
        .credit_mode(CreditMode::Auto(STANDING_CREDIT))
        .attach(&mut session)
        .await?;
    println!(
        "[recv] Durable receiver attached ({phase}): container-id={CONTAINER_ID:?} name={LINK_NAME:?} expiry=never"
    );
    // Let the connector's subscription pump go live before producing.
    tokio::time::sleep(Duration::from_millis(750)).await;
    Ok((receiver, session, connection))
}

/// Publish events es-<lo>..es-<hi-1> on the producer sender (unsettled —
/// events-store persists each accepted transfer).
async fn publish(
    sender: &mut Sender,
    lo: usize,
    hi: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    for i in lo..hi {
        let body = format!("es-{i:03}");
        let outcome = sender.send(body.clone()).await?;
        if !outcome.is_accepted() {
            return Err(format!("publish {body}: unexpected outcome {outcome:?}").into());
        }
    }
    Ok(())
}

/// Receive up to `max` events within `window`, returning their bodies in order.
async fn drain(
    receiver: &mut Receiver,
    max: usize,
    window: Duration,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut out = Vec::with_capacity(max);
    let deadline = tokio::time::Instant::now() + window;
    while out.len() < max {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, receiver.recv::<Body<Value>>()).await {
            Ok(Ok(delivery)) => {
                receiver.accept(&delivery).await?;
                out.push(body_string(&delivery.message().body));
            }
            _ => break,
        }
    }
    Ok(out)
}

// Expected output:
//
// Broker:     amqp://localhost:5672
// Address:    events-store/amqp10.examples.durable  (KubeMQ pattern=events-store, channel=amqp10.examples.durable)
// Durable id: container-id="amqp10-examples-durable-container"  link-name="durable-sub"
//
// [recv] Durable receiver attached (first attach): container-id="amqp10-examples-durable-container" name="durable-sub" expiry=never
// [recv] First attach received 3 events: ["es-000", "es-001", "es-002"]
//
// [conn] Durable subscriber DISCONNECTED (cursor preserved by the connector)
// [send] Published 5 more events WHILE the durable subscriber was away
// [recv] Durable receiver attached (re-attach): container-id="amqp10-examples-durable-container" name="durable-sub" expiry=never
// [recv] Re-attach RESUMED and received the 5 events published while away: ["es-003", "es-004", "es-005", "es-006", "es-007"]
// [recv] No loss, no re-delivery of the already-consumed first 3 — the durable cursor resumed exactly
//
// Done.
//
// NOTE: durable subscriptions are NODE-LOCAL (see README gotcha). In a cluster the
// durable cursor lives on the node that owned the original attach; reconnect to the
// SAME node (or run a single-node dev broker, as here) to resume.
