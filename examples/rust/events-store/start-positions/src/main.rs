//! Example: events-store/start-positions (master-table variant #8)
//!
//! The `x-opt-kubemq-start` link property over KubeMQ **Events Store** using the
//! native `fe2o3-amqp` client (NO KubeMQ SDK).
//!
//! Events Store persists the stream, so a (non-durable) subscriber can choose WHERE
//! in the history to start consuming via the `x-opt-kubemq-start` receiver link
//! property. The full grammar (parsed by the connector's parseEventsStoreStart):
//!
//!   (absent) / "new-only"  -> deliver only events published AFTER attach
//!   "first"                -> replay the ENTIRE history from the beginning
//!   "last"                 -> start at the last stored event
//!   "sequence:<n>"         -> start at store sequence n (1-BASED; sequence 1 = the
//!                             first stored event — the connector passes n straight
//!                             to NATS streaming's StartAtSequence)
//!   "time:<RFC3339|secs>"  -> start at a wall-clock instant (RFC3339 or unix-seconds)
//!   "time-delta:<secs>"    -> start <secs> seconds ago (relative to now)
//!
//! IMPORTANT — time encoding: the client sends a `time:` value as RFC3339 OR as
//! unix SECONDS; the connector parses BOTH to the same instant and the broker stores
//! the cursor as unix NANOSECONDS. `time-delta:` is seconds verbatim. A malformed
//! value (e.g. "sequence:abc", "time:not-a-time", "whenever") is rejected at ATTACH
//! with amqp:invalid-field. There is NO native "last N by count" form — to read the
//! tail, compute a sequence or a time window.
//!
//! This program seeds 6 events, then demonstrates four start positions on fresh
//! (non-durable) receivers against the SAME persisted stream:
//!
//!   first              -> all 6 (full replay)
//!   sequence:4         -> from the 4th stored event onward (1-based => es-003,004,005)
//!   time-delta:3600    -> all 6 (all were published within the last hour)
//!   new-only           -> none of the existing 6; only events published after attach
//!
//! Grounded in connector tests TestEventsStoreDurableReplay (the start:first leg)
//! and TestParseEventsStoreStart (connectors/amqp10/link_pubsub_test.go).
//!
//! Run:
//!
//!   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//!   cargo run -p start-positions

use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use fe2o3_amqp_types::definitions::SenderSettleMode;
use fe2o3_amqp_types::messaging::{Body, Source};
use fe2o3_amqp_types::primitives::{OrderedMap, Symbol, Value};

const START_PROP: &str = "x-opt-kubemq-start";
const SEED_COUNT: usize = 6;
const STANDING_CREDIT: u32 = 100;

fn amqp_url() -> String {
    std::env::var("KUBEMQ_AMQP_URL").unwrap_or_else(|_| "amqp://localhost:5672".to_string())
}

/// A fresh channel per run keeps the sequence numbers deterministic (this demo
/// reads by absolute sequence, which is per-channel and monotonic).
fn fresh_channel() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("amqp10.examples.startpos.{nanos}")
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

/// Build a plain `Source` for `addr` (the start position travels as a LINK property,
/// not in the source filter — see `start_props`).
fn start_source(addr: &str) -> Source {
    Source::builder().address(addr).build()
}

/// The `x-opt-kubemq-start` link property carried on the consume attach.
fn start_props(start: &str) -> OrderedMap<Symbol, Value> {
    let mut props = OrderedMap::new();
    props.insert(Symbol::from(START_PROP), Value::String(start.to_string()));
    props
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = amqp_url();
    let channel = fresh_channel();
    let addr = format!("events-store/{channel}");
    println!("Broker:  {url}");
    println!("Address: {addr}  (KubeMQ pattern=events-store, channel={channel})\n");

    let mut connection = Connection::open("amqp10-examples-startpos", url.as_str()).await?;
    let mut session = Session::begin(&mut connection).await?;

    // =========================================================================
    // 0. SEED — publish 6 events into the persisted events-store stream. They are
    //    stored at 1-based sequences 1..6 (per-channel, monotonic).
    // =========================================================================
    let mut sender = Sender::builder()
        .name("start-positions-seeder")
        .target(addr.as_str())
        .sender_settle_mode(SenderSettleMode::Unsettled)
        .attach(&mut session)
        .await?;
    for i in 0..SEED_COUNT {
        let body = format!("es-{i:03}");
        let outcome = sender.send(body.clone()).await?;
        if !outcome.is_accepted() {
            return Err(format!("seed publish {body}: unexpected outcome {outcome:?}").into());
        }
    }
    sender.close().await?;
    println!(
        "[seed] Published {SEED_COUNT} events (stored at 1-based sequences 1..{SEED_COUNT})\n"
    );

    // =========================================================================
    // 1. start=first -> FULL REPLAY (all 6 events from the beginning).
    // =========================================================================
    let got = read_from(
        &mut session,
        addr.as_str(),
        "first",
        SEED_COUNT,
        Duration::from_secs(15),
    )
    .await?;
    expect_exactly(
        &got,
        &["es-000", "es-001", "es-002", "es-003", "es-004", "es-005"],
        "first",
    )?;
    println!("[start=first]           replayed full history: {got:?}");

    // =========================================================================
    // 2. start=sequence:4 -> from the 4th stored event onward. Sequences are
    //    1-BASED (the connector passes the value straight to NATS streaming's
    //    StartAtSequence; sequence 1 = the first event), so the 4th stored event is
    //    es-003, delivering es-003, es-004, es-005.
    // =========================================================================
    let got = read_from(
        &mut session,
        addr.as_str(),
        "sequence:4",
        SEED_COUNT,
        Duration::from_secs(15),
    )
    .await?;
    expect_exactly(&got, &["es-003", "es-004", "es-005"], "sequence:4")?;
    println!("[start=sequence:4]      from the 4th stored event (1-based): {got:?}");

    // =========================================================================
    // 3. start=time-delta:3600 -> everything from the last hour (all 6, since the
    //    seed was published seconds ago). time-delta is SECONDS verbatim.
    // =========================================================================
    let got = read_from(
        &mut session,
        addr.as_str(),
        "time-delta:3600",
        SEED_COUNT,
        Duration::from_secs(15),
    )
    .await?;
    expect_exactly(
        &got,
        &["es-000", "es-001", "es-002", "es-003", "es-004", "es-005"],
        "time-delta:3600",
    )?;
    println!("[start=time-delta:3600] last hour (all 6): {got:?}");

    // (You can also start at an absolute instant, e.g. the RFC3339 form
    //   "time:2026-06-15T00:00:00Z"  or unix-seconds  "time:1623578400".
    //  Both forms resolve to the same instant; the broker stores the cursor as
    //  nanoseconds.)

    // =========================================================================
    // 4. start=new-only -> NONE of the 6 existing events; only what is published
    //    AFTER this attach. We attach, publish one more event, and prove only that
    //    one is delivered.
    // =========================================================================
    demo_new_only(&mut session, addr.as_str()).await?;

    // =========================================================================
    // 5. GOTCHA — a malformed start value is rejected at ATTACH with
    //    amqp:invalid-field. The connector tears the bad attach (and the session)
    //    down, so each malformed demo uses its OWN short-lived connection.
    // =========================================================================
    println!();
    demo_malformed(url.as_str(), addr.as_str(), "sequence:abc").await?;
    demo_malformed(url.as_str(), addr.as_str(), "whenever").await?;

    session.end().await?;
    connection.close().await?;

    println!("\nDone.");
    Ok(())
}

/// Open a fresh (non-durable) receiver at the given start position and drain up to
/// `max` events within `window`, returning their bodies in order.
async fn read_from(
    session: &mut fe2o3_amqp::session::SessionHandle<()>,
    addr: &str,
    start: &str,
    max: usize,
    window: Duration,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut receiver = Receiver::builder()
        .name(format!("start-positions-{start}").replace(':', "-"))
        .source(start_source(addr))
        .properties(start_props(start))
        .credit_mode(CreditMode::Auto(STANDING_CREDIT))
        .attach(session)
        .await?;

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
    receiver.close().await?;
    Ok(out)
}

/// Attach a new-only receiver, then publish one event and prove ONLY the
/// post-attach event is delivered (the 6 existing events are skipped).
async fn demo_new_only(
    session: &mut fe2o3_amqp::session::SessionHandle<()>,
    addr: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut receiver = Receiver::builder()
        .name("start-positions-new-only")
        .source(start_source(addr))
        .properties(start_props("new-only"))
        .credit_mode(CreditMode::Auto(STANDING_CREDIT))
        .attach(session)
        .await?;
    tokio::time::sleep(Duration::from_millis(750)).await; // let the new-only cursor settle

    // Publish one fresh event AFTER the new-only attach.
    let mut sender = Sender::builder()
        .name("start-positions-new-only-sender")
        .target(addr)
        .sender_settle_mode(SenderSettleMode::Unsettled)
        .attach(session)
        .await?;
    const FRESH: &str = "es-new-after-attach";
    let outcome = sender.send(FRESH).await?;
    if !outcome.is_accepted() {
        return Err(format!("new-only publish: unexpected outcome {outcome:?}").into());
    }
    sender.close().await?;

    // Only the post-attach event must arrive.
    let delivery =
        match tokio::time::timeout(Duration::from_secs(15), receiver.recv::<Body<Value>>()).await {
            Ok(Ok(d)) => d,
            _ => return Err("new-only: expected the post-attach event, but none arrived".into()),
        };
    receiver.accept(&delivery).await?;
    let got = body_string(&delivery.message().body);
    if got != FRESH {
        return Err(format!("new-only: expected {FRESH:?} (post-attach), but got {got:?} (an existing event leaked)").into());
    }

    // Nothing else (the 6 existing events must NOT be delivered).
    if let Ok(Ok(extra)) =
        tokio::time::timeout(Duration::from_secs(2), receiver.recv::<Body<Value>>()).await
    {
        return Err(format!(
            "new-only: an existing event {:?} leaked (new-only must skip history)",
            body_string(&extra.message().body)
        )
        .into());
    }
    receiver.close().await?;
    println!("[start=new-only]        skipped all {SEED_COUNT} existing events; delivered only the post-attach event: [{FRESH}]");
    Ok(())
}

/// Prove a bad start value is rejected at ATTACH with amqp:invalid-field (the
/// receiver never attaches). The connector tears the link/session down on the bad
/// attach, so we use a dedicated short-lived connection per demo.
async fn demo_malformed(
    url: &str,
    addr: &str,
    bad_start: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut connection = Connection::open("amqp10-examples-startpos-bad", url).await?;
    let mut session = Session::begin(&mut connection).await?;
    let attach = Receiver::builder()
        .name("start-positions-malformed")
        .source(start_source(addr))
        .properties(start_props(bad_start))
        .credit_mode(CreditMode::Auto(STANDING_CREDIT))
        .attach(&mut session)
        .await;
    match attach {
        Ok(r) => {
            let _ = r.close().await;
            return Err(format!(
                "expected start={bad_start:?} to be rejected, but the attach succeeded"
            )
            .into());
        }
        Err(e) => {
            println!("[gotcha] start={bad_start:?} correctly REJECTED at ATTACH: {e:?}");
        }
    }
    // The connector tore the link (and possibly the session) down — best-effort
    // teardown; the rejection above is the demonstration.
    let _ = session.end().await;
    let _ = connection.close().await;
    Ok(())
}

/// Fail unless `got` contains exactly the `want` set.
fn expect_exactly(
    got: &[String],
    want: &[&str],
    label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if got.len() != want.len() {
        return Err(format!(
            "[start={label}] expected {} events {want:?}, got {}: {got:?}",
            want.len(),
            got.len()
        )
        .into());
    }
    let set: HashSet<&str> = got.iter().map(String::as_str).collect();
    for w in want {
        if !set.contains(w) {
            return Err(
                format!("[start={label}] missing expected event {w:?} (got {got:?})").into(),
            );
        }
    }
    Ok(())
}

// Expected output (the channel suffix is a timestamp, so it varies per run):
//
// Broker:  amqp://localhost:5672
// Address: events-store/amqp10.examples.startpos.<ts>  (KubeMQ pattern=events-store, channel=amqp10.examples.startpos.<ts>)
//
// [seed] Published 6 events (stored at 1-based sequences 1..6)
//
// [start=first]           replayed full history: ["es-000", "es-001", "es-002", "es-003", "es-004", "es-005"]
// [start=sequence:4]      from the 4th stored event (1-based): ["es-003", "es-004", "es-005"]
// [start=time-delta:3600] last hour (all 6): ["es-000", "es-001", "es-002", "es-003", "es-004", "es-005"]
// [start=new-only]        skipped all 6 existing events; delivered only the post-attach event: [es-new-after-attach]
//
// [gotcha] start="sequence:abc" correctly REJECTED at ATTACH: ...
// [gotcha] start="whenever" correctly REJECTED at ATTACH: ...
//
// Done.
//
// The connector DETACHes the bad attach with amqp:invalid-field (description e.g.
// "invalid start sequence: abc" / "unknown start position: whenever"). fe2o3-amqp
// surfaces the connector tearing the link down as an attach error; either way the
// receiver never attaches. (The error text varies with timing; the invariant is
// that the attach returns Err.)
