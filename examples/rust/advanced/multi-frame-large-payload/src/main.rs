//! Example: advanced/multi-frame-large-payload (master-table variant #11)
//!
//! A single AMQP 1.0 message whose body is larger than the connection's
//! max-frame-size is fragmented across multiple TRANSFER frames (More:true …
//! More:false) by the sender and reassembled bit-exact by the receiver — all
//! transparently, with NO application-level chunking. This example drives that path
//! against the KubeMQ AMQP 1.0 connector using the native `fe2o3-amqp` client (NO
//! KubeMQ SDK).
//!
//! Flow:
//!   - Open with `Connection::builder().max_frame_size(4096)` on BOTH the producer
//!     and consumer connections — a deliberately tiny 4 KiB frame so a ~1 MB body
//!     forces heavy fragmentation in both directions.
//!   - Sender → "queues/<ch>" (unsettled): one `send` carries a ~1 MB Data body.
//!     fe2o3-amqp splits it across many transfer frames; the connector reassembles
//!     it and stores a single message.
//!   - Receiver ← "queues/<ch>" Credit:1: one `recv` yields the full body. The
//!     example verifies the received length AND a CRC32 of the bytes match the
//!     original — proving a bit-exact round-trip across the fragment boundary.
//!
//! Grounded in connector test TestQueueMultiFrameLargePayload
//! (connectors/amqp10/integration_test.go).
//!
//! Run:
//!
//!   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//!   cargo run -p multi-frame-large-payload

use std::time::Duration;

use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use fe2o3_amqp_types::definitions::SenderSettleMode;
use fe2o3_amqp_types::messaging::{Body, Message};
use fe2o3_amqp_types::primitives::Value;

/// The KubeMQ queue channel; the link address is "queues/" + channel (explicit
/// prefix — never rely on DefaultPattern).
const CHANNEL: &str = "amqp10.examples.multiframe";

/// ~1 MB — comfortably larger than `MAX_FRAME_SIZE` so the body must span many
/// transfer frames (More:true … More:false).
const PAYLOAD_SIZE: usize = 1024 * 1024;

/// A deliberately tiny 4 KiB max-frame-size so the ~1 MB body fragments across ~256
/// frames in each direction.
const MAX_FRAME_SIZE: u32 = 4096;

fn amqp_url() -> String {
    std::env::var("KUBEMQ_AMQP_URL").unwrap_or_else(|_| "amqp://localhost:5672".to_string())
}

/// Concatenate every `Data` body section into a byte vector.
fn body_bytes(body: &Body<Value>) -> Vec<u8> {
    match body {
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
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = amqp_url();
    let addr = format!("queues/{CHANNEL}");
    println!("Broker:       {url}");
    println!("Address:      {addr}  (KubeMQ pattern=queues, channel={CHANNEL})");
    println!("MaxFrameSize: {MAX_FRAME_SIZE} bytes");
    println!(
        "Payload:      {PAYLOAD_SIZE} bytes (~{} KiB)\n",
        PAYLOAD_SIZE / 1024
    );

    // Build a deterministic, non-trivial payload and remember its CRC + length so we
    // can prove a bit-exact round-trip after reassembly.
    let payload: Vec<u8> = (0..PAYLOAD_SIZE).map(|i| (i % 251) as u8).collect(); // 251 prime → no short period
    let want_len = payload.len();
    let want_crc = crc32fast::hash(&payload);
    println!("[prep] Built payload: len={want_len} crc32=0x{want_crc:08x}");

    // =========================================================================
    // 1. PRODUCER connection — OPEN with a tiny max-frame-size. The connector
    //    advertises its own max-frame-size in the OPEN reply; fe2o3-amqp uses the
    //    smaller of the two when fragmenting transfers.
    // =========================================================================
    let mut prod_conn = Connection::builder()
        .container_id("amqp10-examples-multiframe-producer")
        .max_frame_size(MAX_FRAME_SIZE)
        .open(url.as_str())
        .await?;
    let mut prod_session = Session::begin(&mut prod_conn).await?;

    // ATTACH a sender and send the whole body in ONE `send`. fe2o3-amqp transparently
    // splits it across many transfer frames (More:true … final More:false). The
    // connector reassembles them into a single stored message.
    let mut sender = Sender::builder()
        .name("multi-frame-sender")
        .target(addr.as_str())
        .sender_settle_mode(SenderSettleMode::Unsettled)
        .attach(&mut prod_session)
        .await?;
    let msg = Message::builder().data(payload.clone()).build();
    let outcome = sender.send(msg).await?;
    if !outcome.is_accepted() {
        return Err(format!("multi-frame send: unexpected outcome {outcome:?}").into());
    }
    sender.close().await?;
    println!(
        "[send] Sent the {want_len}-byte body in ONE send (fragmented across ~{} frames, accepted)",
        (want_len / MAX_FRAME_SIZE as usize) + 1
    );

    // =========================================================================
    // 2. CONSUMER connection — same tiny max-frame-size so reassembly is exercised
    //    on the receive path too. One `recv` yields the FULL reassembled body.
    // =========================================================================
    let mut cons_conn = Connection::builder()
        .container_id("amqp10-examples-multiframe-consumer")
        .max_frame_size(MAX_FRAME_SIZE)
        .open(url.as_str())
        .await?;
    let mut cons_session = Session::begin(&mut cons_conn).await?;
    let mut receiver = Receiver::builder()
        .name("multi-frame-receiver")
        .source(addr.as_str())
        .credit_mode(CreditMode::Auto(1))
        .attach(&mut cons_session)
        .await?;

    let delivery =
        match tokio::time::timeout(Duration::from_secs(60), receiver.recv::<Body<Value>>()).await {
            Ok(Ok(d)) => d,
            _ => return Err("multi-frame receive: timed out".into()),
        };
    receiver.accept(&delivery).await?;

    let got = body_bytes(&delivery.message().body);
    let got_len = got.len();
    let got_crc = crc32fast::hash(&got);
    println!("[recv] Reassembled body: len={got_len} crc32=0x{got_crc:08x}");

    // =========================================================================
    // 3. Verify the round-trip is bit-exact: length AND CRC32 must match.
    // =========================================================================
    if got_len != want_len {
        return Err(format!("length mismatch: sent {want_len}, received {got_len}").into());
    }
    if got_crc != want_crc {
        return Err(
            format!("CRC mismatch: sent 0x{want_crc:08x}, received 0x{got_crc:08x}").into(),
        );
    }
    println!("[verify] Length and CRC32 match — multi-frame body round-tripped bit-exact");

    receiver.close().await?;
    cons_session.end().await?;
    cons_conn.close().await?;
    prod_session.end().await?;
    prod_conn.close().await?;

    println!("\nDone.");
    Ok(())
}

// Expected output:
//
// Broker:       amqp://localhost:5672
// Address:      queues/amqp10.examples.multiframe  (KubeMQ pattern=queues, channel=amqp10.examples.multiframe)
// MaxFrameSize: 4096 bytes
// Payload:      1048576 bytes (~1024 KiB)
//
// [prep] Built payload: len=1048576 crc32=0x........
// [send] Sent the 1048576-byte body in ONE send (fragmented across ~257 frames, accepted)
// [recv] Reassembled body: len=1048576 crc32=0x........
// [verify] Length and CRC32 match — multi-frame body round-tripped bit-exact
//
// Done.
//
// NOTE: the connector caps a single message at 100 MiB. A body over the cap is
// refused with amqp:link:message-size-exceeded — there is no application chunking to
// work around it; split very large payloads into multiple messages instead.
