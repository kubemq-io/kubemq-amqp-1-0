// Example: queues/basic-send-receive (master-table variant #1)
//
// At-least-once produce + credit-based consume against the KubeMQ AMQP 1.0
// connector using Apache Qpid JMS (javax.jms) — NO KubeMQ SDK.
//
// JMS-over-AMQP-1.0 mapping:
//   - The JMS destination name IS the connector address: a JMS Queue named
//     "queues/<ch>" attaches a link whose terminus address is "queues/<ch>",
//     which the connector resolves to KubeMQ pattern=queues, channel=<ch>.
//   - A MessageProducer maps to a server-receiver link (the client produces);
//     the server grants credit and DISPOSITIONs each delivery `accepted`.
//   - A Session.CLIENT_ACKNOWLEDGE MessageConsumer maps to a server-sender link
//     (the client consumes). The client grants link credit (Qpid JMS prefetch)
//     and message.acknowledge() settles `accepted` ⇒ the connector AckRanges the
//     delivery and removes it from the queue (at-least-once).
//
// Grounded in connector test TestQueueProduceConsumeAtLeastOnce
// (connectors/amqp10/integration_test.go).
//
// Run:
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   mvn -pl queues/basic-send-receive exec:java
package io.kubemq.examples.amqp10.queues.basicsendreceive;

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

    // The KubeMQ queue channel; the JMS Queue name is the explicit connector
    // address "queues/" + channel (never rely on DefaultPattern).
    private static final String CHANNEL = "amqp10.examples.basic";
    private static final int TOTAL = 10;

    private Main() {
    }

    static String amqpUrl() {
        String v = System.getenv("KUBEMQ_AMQP_URL");
        return (v != null && !v.isEmpty()) ? v : "amqp://localhost:5672";
    }

    public static void main(String[] args) throws JMSException {
        String url = amqpUrl();
        String address = "queues/" + CHANNEL;
        System.out.printf("Broker:  %s%n", url);
        System.out.printf("Address: %s  (KubeMQ pattern=queues, channel=%s)%n%n", address, CHANNEL);

        // OPEN: SASL ANONYMOUS by default (no userinfo in the URL). Qpid JMS sends
        // a non-empty container-id automatically.
        JmsConnectionFactory factory = new JmsConnectionFactory(url);

        // try-with-resources closes the connection (and its sessions/links) on exit.
        try (Connection connection = factory.createConnection()) {
            connection.start();

            // =================================================================
            // 1. Produce — a MessageProducer on the "queues/<ch>" Queue is a
            //    server-receiver link. The server grants credit on attach; each
            //    producer.send blocks until the server's accepted DISPOSITION
            //    confirms the broker stored the message (unsettled, at-least-once).
            // =================================================================
            try (Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)) {
                Queue queue = session.createQueue(address);
                try (MessageProducer producer = session.createProducer(queue)) {
                    for (int i = 0; i < TOTAL; i++) {
                        TextMessage msg = session.createTextMessage(String.format("msg-%03d", i));
                        producer.send(msg);
                    }
                }
                System.out.printf("[send] Produced %d messages to %s (accepted DISPOSITION each)%n", TOTAL, address);

                // =============================================================
                // 2. Consume — a CLIENT_ACKNOWLEDGE MessageConsumer is a
                //    server-sender link. Qpid JMS grants link credit (prefetch);
                //    message.acknowledge() settles `accepted` ⇒ the connector
                //    AckRanges the delivery and removes it from the queue.
                // =============================================================
                try (MessageConsumer consumer = session.createConsumer(queue)) {
                    Set<String> seen = new HashSet<>();
                    while (seen.size() < TOTAL) {
                        Message msg = consumer.receive(30_000);
                        if (msg == null) {
                            throw new IllegalStateException(
                                    "timed out before draining the queue (" + seen.size() + "/" + TOTAL + ")");
                        }
                        // The connector may deliver the body as an AMQP Data (bytes)
                        // section, so Qpid JMS could surface a BytesMessage here.
                        // getBody(String.class) decodes either type as UTF-8.
                        String body = msg.getBody(String.class);
                        msg.acknowledge(); // accept ⇒ AckRange (removed from the queue)
                        seen.add(body);
                    }
                    System.out.printf("[recv] Consumed and accepted %d messages (no loss)%n", seen.size());

                    // =========================================================
                    // 3. Assert the queue is empty — a further receive times out.
                    // =========================================================
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
// Address: queues/amqp10.examples.basic  (KubeMQ pattern=queues, channel=amqp10.examples.basic)
//
// [send] Produced 10 messages to queues/amqp10.examples.basic (accepted DISPOSITION each)
// [recv] Consumed and accepted 10 messages (no loss)
// [recv] Queue drained to empty (no further messages)
//
// Done.
