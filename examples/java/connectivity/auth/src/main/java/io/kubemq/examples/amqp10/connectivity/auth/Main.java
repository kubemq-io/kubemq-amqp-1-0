// Example: connectivity/auth (master-table variant #13)
//
// The ONE runnable authentication variant. It connects to the KubeMQ AMQP 1.0
// connector with SASL PLAIN — the username is AUDIT-ONLY and the password is a
// KubeMQ JWT — then runs a queues/<ch> round-trip. Driven with Apache Qpid JMS
// (javax.jms) — NO KubeMQ SDK.
//
// Identity precedence (connector contract):
//   - With authentication ENABLED, the JWT in the SASL PLAIN *password* must
//     validate; the ClientID/identity is derived from the verified token. The
//     SASL *username* is recorded for audit (auth.success / auth.failure) only.
//   - With authentication DISABLED (the stock dev-broker default), the SASL
//     PLAIN *username* becomes the ClientID and any password is accepted; with
//     ANONYMOUS, a default identity is used.
//
// JMS-over-AMQP-1.0 mapping:
//   - SASL PLAIN is selected by passing a username/password to
//     factory.createConnection(user, jwt). To make the demonstration deterministic
//     the URI also pins amqp.saslMechanisms=PLAIN (or ANONYMOUS in the fallback).
//   - The username/password become the SASL PLAIN authcid/password; the connector
//     treats the password as the KubeMQ JWT when auth is enabled.
//
// CLONE-AND-RUN behavior: a stock dev broker has authentication OFF and accepts
// ANONYMOUS, so this example reads the credentials from the environment and
// falls back to ANONYMOUS when they are unset — it runs cleanly either way.
//
//   KUBEMQ_AMQP_USER  — SASL PLAIN username (audit identity; defaults to a label)
//   KUBEMQ_AMQP_JWT   — SASL PLAIN password = a KubeMQ JWT (required to use PLAIN)
//
// Grounded in connector tests TestAuthorizationReadDenied and
// TestAuthenticationBadCredential (connectors/amqp10/integration_test.go).
//
// Run (ANONYMOUS, stock dev broker):
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   mvn -pl connectivity/auth exec:java
//
// Run (SASL PLAIN with a KubeMQ JWT, auth-enabled broker):
//   export KUBEMQ_AMQP_USER=my-service
//   export KUBEMQ_AMQP_JWT=<a-kubemq-jwt>
//   mvn -pl connectivity/auth exec:java
package io.kubemq.examples.amqp10.connectivity.auth;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.jms.JmsConnectionFactory;

public final class Main {

    // The KubeMQ queue channel; the JMS Queue name is the explicit connector
    // address "queues/" + channel (never rely on a default pattern).
    private static final String CHANNEL = "amqp10.examples.auth";

    private Main() {
    }

    static String amqpUrl() {
        String v = System.getenv("KUBEMQ_AMQP_URL");
        return (v != null && !v.isEmpty()) ? v : "amqp://localhost:5672";
    }

    public static void main(String[] args) throws JMSException {
        String baseUrl = amqpUrl();
        String address = "queues/" + CHANNEL;
        System.out.printf("Broker:  %s%n", baseUrl);
        System.out.printf("Address: %s  (KubeMQ pattern=queues, channel=%s)%n", address, CHANNEL);

        // Choose the SASL mechanism from the environment so the example clone-and-
        // runs on a stock dev broker (auth OFF, ANONYMOUS) yet also demonstrates
        // SASL PLAIN with a KubeMQ JWT when credentials are provided.
        String user = System.getenv("KUBEMQ_AMQP_USER");
        String jwt = System.getenv("KUBEMQ_AMQP_JWT");

        boolean usePlain = jwt != null && !jwt.isEmpty();
        if (usePlain && (user == null || user.isEmpty())) {
            user = "amqp10-example"; // audit-only label; identity comes from the JWT
        }

        // Pin the SASL mechanism on the URI so the negotiation is deterministic for
        // the demo: PLAIN when a JWT is supplied, ANONYMOUS otherwise.
        String mechanism = usePlain ? "PLAIN" : "ANONYMOUS";
        String url = withSaslMechanism(baseUrl, mechanism);

        if (usePlain) {
            System.out.printf("Auth:    SASL PLAIN  (username=\"%s\" audit-only; password=<KubeMQ JWT>)%n%n", user);
        } else {
            System.out.println("Auth:    ANONYMOUS  (KUBEMQ_AMQP_JWT unset — falling back to the dev-broker default)");
            System.out.println("         Set KUBEMQ_AMQP_USER + KUBEMQ_AMQP_JWT to dial SASL PLAIN with a KubeMQ JWT.\n");
        }

        JmsConnectionFactory factory = new JmsConnectionFactory(url);

        // =====================================================================
        // 1. OPEN — the SASL handshake happens here. With auth ENABLED, a JWT that
        //    fails validation makes createConnection fail (amqp:unauthorized-access
        //    at the SASL layer — see TestAuthenticationBadCredential). With auth
        //    DISABLED, any credential is accepted.
        //
        //    SASL PLAIN: the username/password supplied to createConnection are the
        //    PLAIN authcid/password (password = the KubeMQ JWT). ANONYMOUS uses the
        //    no-arg createConnection.
        // =====================================================================
        try (Connection connection = usePlain
                ? factory.createConnection(user, jwt)
                : factory.createConnection()) {
            connection.start();
            System.out.println("[open] Connected — SASL handshake accepted");

            try (Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)) {
                Queue queue = session.createQueue(address);

                // =============================================================
                // 2. ATTACH + SEND — the WRITE authorization check runs at sender
                //    attach / send. With authorization ENABLED, an identity without
                //    a WRITE grant on this channel is refused with
                //    amqp:unauthorized-access (TestAuthorizationReadDenied is the
                //    READ-attach counterpart).
                // =============================================================
                String body = "auth-round-trip";
                try (MessageProducer producer = session.createProducer(queue)) {
                    producer.send(session.createTextMessage(body));
                }
                System.out.printf("[send] Produced 1 message to %s (accepted)%n", address);

                // =============================================================
                // 3. ATTACH + RECEIVE — the READ authorization check runs at
                //    receiver attach. A denied identity's receiver attach is refused
                //    with amqp:unauthorized-access (TestAuthorizationReadDenied).
                // =============================================================
                try (MessageConsumer consumer = session.createConsumer(queue)) {
                    Message msg = consumer.receive(30_000);
                    if (msg == null) {
                        throw new IllegalStateException("timed out waiting for the round-trip message");
                    }
                    // The connector may deliver the body as an AMQP Data (bytes)
                    // section, so Qpid JMS could surface a BytesMessage here.
                    // getBody(String.class) decodes either type as UTF-8.
                    String got = msg.getBody(String.class);
                    msg.acknowledge(); // accept ⇒ AckRange (removed from the queue)
                    System.out.printf("[recv] Consumed and accepted 1 message: \"%s\"%n", got);
                }
            }
        }

        System.out.println("\nDone.");
    }

    // withSaslMechanism appends the Qpid JMS amqp.saslMechanisms option to the
    // broker URL, preserving any existing query string.
    private static String withSaslMechanism(String url, String mechanism) {
        String sep = url.contains("?") ? "&" : "?";
        return url + sep + "amqp.saslMechanisms=" + mechanism;
    }
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
