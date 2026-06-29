// Example: queues/settlement-modes (master-table variant #3)
//
// The two producer reliability tiers, side by side, on a KubeMQ Queue through
// Apache Qpid JMS (javax.jms) — NO KubeMQ SDK:
//
//   - PRE-SETTLED producer (at-MOST-once). Qpid JMS sets snd-settle-mode=settled
//     when its pre-settle policy is on, so each TRANSFER is marked settled by the
//     client and producer.send returns WITHOUT waiting for a server DISPOSITION.
//     Fast, fire-and-forget — if the broker drops the transfer the producer never
//     learns; no redelivery, no delivery confirmation. We enable it with the
//     connection-factory URI option:
//         jms.presettlePolicy.presettleProducers=true
//   - UNSETTLED producer (default, at-LEAST-once). Each send blocks until the
//     connector returns an `accepted` DISPOSITION confirming the broker stored
//     the message. This is the variant #1 contract.
//
// On the consume side the connector supports only rcv-settle-mode=first (the JMS
// default). Requesting rcv-settle-mode=second is rejected with a DETACH carrying
// amqp:not-implemented (gotcha #7 — see README); Qpid JMS never requests `second`.
//
// Grounded in connector test TestQueuePreSettled
// (connectors/amqp10/integration_test.go).
//
// Run:
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   mvn -pl queues/settlement-modes exec:java
package io.kubemq.examples.amqp10.queues.settlementmodes;

import java.util.HashSet;
import java.util.Set;

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

    private static final String CHANNEL = "amqp10.examples.settlement";
    private static final int PER_SENDER = 10;

    private Main() {
    }

    static String amqpUrl() {
        String v = System.getenv("KUBEMQ_AMQP_URL");
        return (v != null && !v.isEmpty()) ? v : "amqp://localhost:5672";
    }

    // withQueryParam appends a Qpid JMS URI option to the broker URL, preserving
    // any options already present.
    private static String withQueryParam(String url, String kv) {
        return url.contains("?") ? url + "&" + kv : url + "?" + kv;
    }

    public static void main(String[] args) throws JMSException {
        String url = amqpUrl();
        String address = "queues/" + CHANNEL;
        System.out.printf("Broker:  %s%n", url);
        System.out.printf("Address: %s%n%n", address);

        // =====================================================================
        // 1. PRE-SETTLED producer (at-most-once). A connection whose pre-settle
        //    policy presettles producers makes Qpid JMS negotiate
        //    snd-settle-mode=settled — send does NOT await a DISPOSITION.
        // =====================================================================
        JmsConnectionFactory presettledFactory =
                new JmsConnectionFactory(withQueryParam(url, "jms.presettlePolicy.presettleProducers=true"));
        try (Connection connection = presettledFactory.createConnection()) {
            connection.start();
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                Queue queue = session.createQueue(address);
                try (MessageProducer producer = session.createProducer(queue)) {
                    for (int i = 0; i < PER_SENDER; i++) {
                        producer.send(session.createTextMessage(String.format("presettled-%02d", i)));
                    }
                }
            }
        }
        System.out.printf(
                "[send] Pre-settled (at-most-once): produced %d messages — NO DISPOSITION awaited%n", PER_SENDER);

        // =====================================================================
        // 2. UNSETTLED producer (at-least-once, the default). Each send blocks
        //    until the connector returns an `accepted` DISPOSITION.
        // =====================================================================
        JmsConnectionFactory factory = new JmsConnectionFactory(url);
        try (Connection connection = factory.createConnection()) {
            connection.start();
            try (Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)) {
                Queue queue = session.createQueue(address);
                try (MessageProducer producer = session.createProducer(queue)) {
                    for (int i = 0; i < PER_SENDER; i++) {
                        producer.send(session.createTextMessage(String.format("unsettled-%02d", i)));
                    }
                }
                System.out.printf(
                        "[send] Unsettled (at-least-once): produced %d messages — each accepted DISPOSITION%n",
                        PER_SENDER);

                // =============================================================
                // 3. Consume with the default rcv-settle-mode=first (the only
                //    mode the connector supports). Accept each message to drain.
                // =============================================================
                int total = 2 * PER_SENDER;
                int presettledSeen = 0;
                int unsettledSeen = 0;
                Set<String> seen = new HashSet<>();
                try (MessageConsumer consumer = session.createConsumer(queue)) {
                    while (seen.size() < total) {
                        Message msg = consumer.receive(30_000);
                        if (msg == null) {
                            throw new IllegalStateException(
                                    "timed out before draining (" + seen.size() + "/" + total + ")");
                        }
                        // The connector may deliver the body as an AMQP Data (bytes)
                        // section, so Qpid JMS could surface a BytesMessage here.
                        // getBody(String.class) decodes either type as UTF-8.
                        String body = msg.getBody(String.class);
                        msg.acknowledge();
                        if (seen.add(body)) {
                            if (body.startsWith("presettled")) {
                                presettledSeen++;
                            } else {
                                unsettledSeen++;
                            }
                        }
                    }
                    System.out.printf(
                            "[recv] Drained %d total — %d pre-settled + %d unsettled (rcv-settle-mode=first)%n",
                            seen.size(), presettledSeen, unsettledSeen);

                    Message extra = consumer.receive(2_000);
                    if (extra != null) {
                        throw new IllegalStateException("expected an empty queue, but received another message");
                    }
                    System.out.println("[recv] Queue drained to empty (no further messages)");
                }
            }
        }

        System.out.println("\nDone.");
    }
}

// Expected output:
//
// Broker:  amqp://localhost:5672
// Address: queues/amqp10.examples.settlement
//
// [send] Pre-settled (at-most-once): produced 10 messages — NO DISPOSITION awaited
// [send] Unsettled (at-least-once): produced 10 messages — each accepted DISPOSITION
// [recv] Drained 20 total — 10 pre-settled + 10 unsettled (rcv-settle-mode=first)
// [recv] Queue drained to empty (no further messages)
//
// Done.
//
// (On a healthy broker pre-settled messages also drain — the difference is the
// PRODUCER guarantee, not the happy-path result: a pre-settled send returns before
// any broker confirmation, so a drop on the way in is invisible to the producer.)
