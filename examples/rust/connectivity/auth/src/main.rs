//! Example: connectivity/auth (master-table variant #13)
//!
//! The ONE runnable authentication variant. It connects to the KubeMQ AMQP 1.0
//! connector with SASL PLAIN — the username is AUDIT-ONLY and the password is a
//! KubeMQ JWT — then runs a queues/<ch> round-trip. Driven with the native
//! `fe2o3-amqp` client (NO KubeMQ SDK).
//!
//! Identity precedence (connector contract):
//!   - With authentication ENABLED, the JWT in the SASL PLAIN *password* must
//!     validate; the ClientID/identity is derived from the verified token. The SASL
//!     *username* is recorded for audit (auth.success / auth.failure) only.
//!   - With authentication DISABLED (the stock dev-broker default), the SASL PLAIN
//!     *username* becomes the ClientID and any password is accepted; with ANONYMOUS,
//!     a default identity is used.
//!
//! CLONE-AND-RUN behavior: a stock dev broker has authentication OFF and accepts
//! ANONYMOUS, so this example reads the credentials from the environment and falls
//! back to ANONYMOUS when they are unset — it runs cleanly either way.
//!
//!   KUBEMQ_AMQP_USER  — SASL PLAIN username (audit identity; defaults to a label)
//!   KUBEMQ_AMQP_JWT   — SASL PLAIN password = a KubeMQ JWT (required to use PLAIN)
//!
//! If KUBEMQ_AMQP_JWT is set, the example dials SASL PLAIN; otherwise it dials
//! ANONYMOUS and prints a clear note.
//!
//! Grounded in connector tests TestAuthorizationReadDenied and
//! TestAuthenticationBadCredential (connectors/amqp10/integration_test.go).
//!
//! Run (ANONYMOUS, stock dev broker):
//!
//!   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//!   cargo run -p auth
//!
//! Run (SASL PLAIN with a KubeMQ JWT, auth-enabled broker):
//!
//!   export KUBEMQ_AMQP_USER=my-service
//!   export KUBEMQ_AMQP_JWT=<a-kubemq-jwt>
//!   cargo run -p auth

use std::time::Duration;

use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::sasl_profile::SaslProfile;
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use fe2o3_amqp_types::definitions::SenderSettleMode;
use fe2o3_amqp_types::messaging::{Body, Message};
use fe2o3_amqp_types::primitives::Value;

/// The KubeMQ queue channel; the link address is "queues/" + channel (explicit
/// prefix — never rely on DefaultPattern).
const CHANNEL: &str = "amqp10.examples.auth";

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = amqp_url();
    let addr = format!("queues/{CHANNEL}");
    println!("Broker:  {url}");
    println!("Address: {addr}  (KubeMQ pattern=queues, channel={CHANNEL})");

    // Choose the SASL mechanism from the environment so the example clone-and-runs on
    // a stock dev broker (auth OFF, ANONYMOUS) yet also demonstrates SASL PLAIN with a
    // KubeMQ JWT when credentials are provided.
    let user = std::env::var("KUBEMQ_AMQP_USER").unwrap_or_default();
    let jwt = std::env::var("KUBEMQ_AMQP_JWT").unwrap_or_default();

    let sasl = if !jwt.is_empty() {
        // SASL PLAIN: username is AUDIT-ONLY; password is the KubeMQ JWT.
        let username = if user.is_empty() {
            "amqp10-example".to_string()
        } else {
            user
        };
        println!(
            "Auth:    SASL PLAIN  (username={username:?} audit-only; password=<KubeMQ JWT>)\n"
        );
        SaslProfile::Plain {
            username,
            password: jwt,
        }
    } else {
        // Stock dev-broker default: ANONYMOUS.
        println!(
            "Auth:    ANONYMOUS  (KUBEMQ_AMQP_JWT unset — falling back to the dev-broker default)"
        );
        println!("         Set KUBEMQ_AMQP_USER + KUBEMQ_AMQP_JWT to dial SASL PLAIN with a KubeMQ JWT.\n");
        SaslProfile::Anonymous
    };

    // =========================================================================
    // 1. OPEN — the SASL handshake happens here. With auth ENABLED, a JWT that fails
    //    validation makes the open fail (amqp:unauthorized-access at the SASL layer —
    //    see TestAuthenticationBadCredential). With auth DISABLED, any credential is
    //    accepted.
    // =========================================================================
    let mut connection = Connection::builder()
        .container_id("amqp10-examples-auth")
        .sasl_profile(sasl)
        .open(url.as_str())
        .await?;
    println!("[open] Connected — SASL handshake accepted");

    let mut session = Session::begin(&mut connection).await?;

    // =========================================================================
    // 2. ATTACH + SEND — the WRITE authorization check runs at sender attach / send.
    //    With authorization ENABLED, an identity without a WRITE grant on this channel
    //    is refused with amqp:unauthorized-access (see TestAuthorizationReadDenied for
    //    the READ-attach counterpart). The sender pins an explicit settle-mode (the
    //    connector rejects the AMQP default `mixed`).
    // =========================================================================
    let mut sender = Sender::builder()
        .name("auth-sender")
        .target(addr.as_str())
        .sender_settle_mode(SenderSettleMode::Unsettled)
        .attach(&mut session)
        .await?;
    let outcome = sender
        .send(Message::builder().data(b"auth-round-trip".to_vec()).build())
        .await?;
    if !outcome.is_accepted() {
        return Err(format!("send: unexpected outcome {outcome:?}").into());
    }
    sender.close().await?;
    println!("[send] Produced 1 message to {addr} (accepted)");

    // =========================================================================
    // 3. ATTACH + RECEIVE — the READ authorization check runs at receiver attach. A
    //    denied identity's receiver attach is refused with amqp:unauthorized-access
    //    (TestAuthorizationReadDenied).
    // =========================================================================
    let mut receiver = Receiver::builder()
        .name("auth-receiver")
        .source(addr.as_str())
        .credit_mode(CreditMode::Auto(1))
        .attach(&mut session)
        .await?;
    let delivery =
        match tokio::time::timeout(Duration::from_secs(30), receiver.recv::<Body<Value>>()).await {
            Ok(Ok(d)) => d,
            _ => return Err("receive: timed out".into()),
        };
    receiver.accept(&delivery).await?;
    println!(
        "[recv] Consumed and accepted 1 message: {:?}",
        body_string(&delivery.message().body)
    );

    receiver.close().await?;
    session.end().await?;
    connection.close().await?;

    println!("\nDone.");
    Ok(())
}

// Expected output (ANONYMOUS, stock dev broker — no env set):
//
// Broker:  amqp://localhost:5672
// Address: queues/amqp10.examples.auth  (KubeMQ pattern=queues, channel=amqp10.examples.auth)
// Auth:    ANONYMOUS  (KUBEMQ_AMQP_JWT unset — falling back to the dev-broker default)
//          Set KUBEMQ_AMQP_USER + KUBEMQ_AMQP_JWT to dial SASL PLAIN with a KubeMQ JWT.
//
// [open] Connected — SASL handshake accepted
// [send] Produced 1 message to queues/amqp10.examples.auth (accepted)
// [recv] Consumed and accepted 1 message: "auth-round-trip"
//
// Done.
//
// Expected output (SASL PLAIN, KUBEMQ_AMQP_USER + KUBEMQ_AMQP_JWT set):
//
// Broker:  amqp://localhost:5672
// Address: queues/amqp10.examples.auth  (KubeMQ pattern=queues, channel=amqp10.examples.auth)
// Auth:    SASL PLAIN  (username="my-service" audit-only; password=<KubeMQ JWT>)
//
// [open] Connected — SASL handshake accepted
// [send] Produced 1 message to queues/amqp10.examples.auth (accepted)
// [recv] Consumed and accepted 1 message: "auth-round-trip"
//
// Done.
