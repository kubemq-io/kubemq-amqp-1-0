//! Example: events/selector (master-table variant #6)
//!
//! JMS / SQL-92 message selectors over KubeMQ **Events** with the native
//! `fe2o3-amqp` client (NO KubeMQ SDK).
//!
//! A receiver attaches to events/<ch> carrying a selector source-filter under the
//! OASIS-standard key "apache.org:selector-filter:string". The connector evaluates
//! the selector against each event's APPLICATION PROPERTIES and delivers ONLY the
//! matching events; non-matching events are silently withheld (copy semantics —
//! they stay available to OTHER subscribers, they are not consumed/discarded).
//!
//! The selector here is:  color = 'red' AND size > 2
//!
//! We publish 5 events and assert exactly 2 are delivered:
//!
//!   match-1      {color:red,  size:5}  delivered
//!   miss-blue    {color:blue, size:9}  color != red
//!   miss-small   {color:red,  size:1}  size not > 2
//!   match-2      {color:red,  size:3}  delivered
//!   miss-nocolor {           size:8}   color IS NULL  (3-valued logic: UNKNOWN => withheld)
//!
//! THREE-VALUED LOGIC: a property that is absent evaluates to NULL, so the
//! predicate is UNKNOWN (not true) and the event is NOT delivered — this is why
//! miss-nocolor is withheld even though it has no color to disqualify it.
//!
//! GOTCHA: a selector is honoured ONLY on events/ and events-store/ consume links.
//! Requesting one on a queues/ source is rejected at ATTACH with
//! amqp:not-implemented — see the README. This program demonstrates that rejection
//! at the end.
//!
//! NOTE on the fe2o3-amqp filter encoding (the churn point flagged in the plan):
//! fe2o3-amqp-types 0.14's `FilterSet = OrderedMap<Symbol, Value>`. The JMS selector
//! filter is a DESCRIBED value (descriptor symbol "apache.org:selector-filter:string",
//! value = the selector string), added via
//! `SourceBuilder::add_to_filter_using_legacy_format(key, Value::Described(...))`.
//! This encoding was dial-tested against the live connector.
//!
//! Grounded in connector test TestEventsSelector
//! (connectors/amqp10/integration_pubsub_test.go) and the selector-on-queues
//! rejection in connectors/amqp10/link.go (applySourceSelector).
//!
//! Run:
//!
//!   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//!   cargo run -p selector

use std::collections::HashSet;
use std::time::Duration;

use fe2o3_amqp::link::delivery::Delivery;
use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use fe2o3_amqp_types::definitions::SenderSettleMode;
use fe2o3_amqp_types::messaging::{ApplicationProperties, Body, Message, Source};
use fe2o3_amqp_types::primitives::{Symbol, Value};
use serde_amqp::described::Described;
use serde_amqp::descriptor::Descriptor;

const CHANNEL: &str = "amqp10.examples.selector";

/// A standard SQL-92 / JMS message selector evaluated against each event's
/// application properties.
const SELECTOR: &str = "color = 'red' AND size > 2";

/// OASIS standard filter key + descriptor for a JMS/SQL-92 string selector.
const SELECTOR_FILTER_KEY: &str = "apache.org:selector-filter:string";

/// Granted up front so the subscriber is never at 0 credit when a matching event
/// arrives (events are at-most-once; 0-credit => silent drop).
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

/// Build a `Source` for `addr` carrying the JMS/SQL-92 selector as a described
/// filter value under the OASIS key.
fn selector_source(addr: &str, selector: &str) -> Source {
    let filter_value = Value::Described(Box::new(Described {
        descriptor: Descriptor::Name(Symbol::from(SELECTOR_FILTER_KEY)),
        value: Value::String(selector.to_string()),
    }));
    Source::builder()
        .address(addr)
        .add_to_filter_using_legacy_format(Symbol::from(SELECTOR_FILTER_KEY), filter_value)
        .build()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = amqp_url();
    let addr = format!("events/{CHANNEL}");
    println!("Broker:   {url}");
    println!("Address:  {addr}  (KubeMQ pattern=events, channel={CHANNEL})");
    println!("Selector: {SELECTOR}\n");

    let mut connection = Connection::open("amqp10-examples-selector", url.as_str()).await?;
    let mut session = Session::begin(&mut connection).await?;

    // =========================================================================
    // 1. SUBSCRIBE FIRST with the selector filter. A successful attach means the
    //    connector accepted the filter; a parse error or unsupported pattern would
    //    have DETACHed the link. Events have no replay, so we subscribe before
    //    publishing.
    // =========================================================================
    let mut receiver = Receiver::builder()
        .name("selector-receiver")
        .source(selector_source(addr.as_str(), SELECTOR))
        .credit_mode(CreditMode::Auto(STANDING_CREDIT))
        .attach(&mut session)
        .await?;
    println!(
        "[recv] Subscribed to {addr} with selector filter (standing credit {STANDING_CREDIT})"
    );

    tokio::time::sleep(Duration::from_millis(750)).await;
    println!("[recv] Subscription pump settled (waited 750ms before publishing)");

    // =========================================================================
    // 2. PUBLISH 5 events with application properties (pre-settled). The connector
    //    evaluates the selector against each event's application properties.
    // =========================================================================
    let mut sender = Sender::builder()
        .name("selector-sender")
        .target(addr.as_str())
        .sender_settle_mode(SenderSettleMode::Settled)
        .attach(&mut session)
        .await?;

    struct Event {
        body: &'static str,
        color: Option<&'static str>,
        size: i64,
        matches: bool,
        why: &'static str,
    }
    let events = [
        Event {
            body: "match-1",
            color: Some("red"),
            size: 5,
            matches: true,
            why: "color=red AND size>2",
        },
        Event {
            body: "miss-blue",
            color: Some("blue"),
            size: 9,
            matches: false,
            why: "color!=red",
        },
        Event {
            body: "miss-small",
            color: Some("red"),
            size: 1,
            matches: false,
            why: "size not >2",
        },
        Event {
            body: "match-2",
            color: Some("red"),
            size: 3,
            matches: true,
            why: "color=red AND size>2",
        },
        Event {
            body: "miss-nocolor",
            color: None,
            size: 8,
            matches: false,
            why: "color IS NULL => UNKNOWN (3-valued)",
        },
    ];

    let mut want_matches = 0usize;
    for e in &events {
        let mut props = ApplicationProperties::builder();
        if let Some(c) = e.color {
            props = props.insert("color", c);
        }
        props = props.insert("size", e.size);
        let msg = Message::builder()
            .application_properties(props.build())
            .data(e.body.as_bytes().to_vec())
            .build();
        sender.send(msg).await?;
        let verdict = if e.matches {
            want_matches += 1;
            "should MATCH"
        } else {
            "should be FILTERED OUT"
        };
        let props_desc = match e.color {
            Some(c) => format!("color={c} size={}", e.size),
            None => format!("size={}", e.size),
        };
        println!(
            "[send] {:<13} {:<22} -> {verdict} ({})",
            e.body, props_desc, e.why
        );
    }
    sender.close().await?;

    // =========================================================================
    // 3. RECEIVE only the matching events. Drain exactly want_matches; then prove
    //    nothing else arrives (the non-matching events were silently withheld).
    // =========================================================================
    let mut got: HashSet<String> = HashSet::with_capacity(want_matches);
    while got.len() < want_matches {
        let delivery: Delivery<Body<Value>> = receiver.recv().await?;
        let _ = receiver.accept(&delivery).await; // no-op for pre-settled fan-out
        let body = body_string(delivery.message());
        println!("[recv] delivered: {body}");
        got.insert(body);
    }

    // No further delivery: the non-matching events must NOT arrive.
    match tokio::time::timeout(Duration::from_secs(2), receiver.recv::<Body<Value>>()).await {
        Ok(Ok(extra)) => {
            return Err(format!(
                "selector leak: an extra event {:?} was delivered (should have been filtered)",
                body_string(extra.message())
            )
            .into());
        }
        _ => println!(
            "[recv] Received exactly {} matching event(s); {} non-matching event(s) were silently withheld",
            got.len(),
            events.len() - want_matches
        ),
    }

    receiver.close().await?;

    // =========================================================================
    // 4. GOTCHA demo — a selector on a queues/ source is rejected at ATTACH.
    //    Selectors are honoured ONLY on events/ and events-store/ consume links;
    //    on queues/ (move-only) the connector DETACHes with amqp:not-implemented.
    // =========================================================================
    println!();
    let queue_addr = format!("queues/{CHANNEL}.q");
    let queue_attach = Receiver::builder()
        .name("selector-queue-receiver")
        .source(selector_source(queue_addr.as_str(), SELECTOR))
        .credit_mode(CreditMode::Auto(10))
        .attach(&mut session)
        .await;
    match queue_attach {
        Ok(r) => {
            let _ = r.close().await;
            return Err(format!(
                "expected the selector on {queue_addr} to be rejected, but the attach succeeded"
            )
            .into());
        }
        Err(e) => {
            println!("[gotcha] Selector on {queue_addr} correctly REJECTED at ATTACH:");
            println!("         {e:?}");
            println!("         (selectors are supported only on events/ and events-store/ — queues/ is move-only)");
        }
    }

    // The connector tears the link (and the session) down on the rejected attach,
    // so the session may already be unusable here — tear down best-effort and
    // ignore any resulting errors; the gotcha above is the demonstration.
    let _ = session.end().await;
    let _ = connection.close().await;

    println!("\nDone.");
    Ok(())
}

// Expected output:
//
// Broker:   amqp://localhost:5672
// Address:  events/amqp10.examples.selector  (KubeMQ pattern=events, channel=amqp10.examples.selector)
// Selector: color = 'red' AND size > 2
//
// [recv] Subscribed to events/amqp10.examples.selector with selector filter (standing credit 100)
// [recv] Subscription pump settled (waited 750ms before publishing)
// [send] match-1       color=red size=5       -> should MATCH (color=red AND size>2)
// [send] miss-blue     color=blue size=9      -> should be FILTERED OUT (color!=red)
// [send] miss-small    color=red size=1       -> should be FILTERED OUT (size not >2)
// [send] match-2       color=red size=3       -> should MATCH (color=red AND size>2)
// [send] miss-nocolor  size=8                 -> should be FILTERED OUT (color IS NULL => UNKNOWN (3-valued))
// [recv] delivered: match-1
// [recv] delivered: match-2
// [recv] Received exactly 2 matching event(s); 3 non-matching event(s) were silently withheld
//
// [gotcha] Selector on queues/amqp10.examples.selector.q correctly REJECTED at ATTACH:
//          IllegalSessionState
//          (selectors are supported only on events/ and events-store/ — queues/ is move-only)
//
// Done.
//
// The connector DETACHes the bad attach with amqp:not-implemented ("selector filter
// not supported on this address"). fe2o3-amqp surfaces the connector tearing the
// link/session down as `IllegalSessionState` on the attach call; either way the
// attach fails and the selector link never establishes. (The error text varies with
// timing; the invariant is that NewReceiver returns Err.)
