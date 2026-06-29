// Example: events/basic-pubsub (master-table variant #4)
//
// Fan-out, at-most-once pub/sub over KubeMQ Events through Apache Qpid JMS
// (javax.jms) — NO KubeMQ SDK.
//
// JMS-over-AMQP-1.0 mapping:
//   - A JMS Topic named "events/<ch>" resolves to KubeMQ pattern=events,
//     channel=<ch>. A producer to it is a server-receiver link; a consumer is a
//     server-sender link.
//   - Events are a fire-hose: deliveries are pre-settled (no DISPOSITION feedback),
//     there is NO replay, and an event that arrives at a subscriber with ZERO
//     link credit is SILENTLY DROPPED (counted by the server metric
//     kubemq_amqp10_events_dropped_no_credit_total). Two rules follow:
//       * SUBSCRIBE BEFORE PUBLISH. The attach reply confirms the link, not that
//         the connector's subscription pump is live; a publish that races the
//         subscription is lost (no replay). We sleep ~750ms after creating the
//         consumer before producing.
//       * KEEP STANDING CREDIT. Qpid JMS grants a prefetch window (default 1000)
//         on the consumer link and replenishes it as messages settle, so the
//         subscriber is never at 0 credit when an event arrives.
//
// Grounded in connector test TestEventsPubSubGroupFanout (lone-subscriber leg)
// (connectors/amqp10/integration_pubsub_test.go).
//
// Run:
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   mvn -pl events/basic-pubsub exec:java
package io.kubemq.examples.amqp10.events.basicpubsub;

import java.util.HashSet;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.qpid.jms.JmsConnectionFactory;

public final class Main {

    private static final String CHANNEL = "amqp10.examples.pubsub";
    private static final int TOTAL = 20;

    private Main() {
    }

    static String amqpUrl() {
        String v = System.getenv("KUBEMQ_AMQP_URL");
        return (v != null && !v.isEmpty()) ? v : "amqp://localhost:5672";
    }

    public static void main(String[] args) throws JMSException, InterruptedException {
        String url = amqpUrl();
        String address = "events/" + CHANNEL;
        System.out.printf("Broker:  %s%n", url);
        System.out.printf("Address: %s  (KubeMQ pattern=events, channel=%s)%n%n", address, CHANNEL);

        JmsConnectionFactory factory = new JmsConnectionFactory(url);

        try (Connection connection = factory.createConnection()) {
            connection.start();
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                Topic topic = session.createTopic(address);

                // =============================================================
                // 1. SUBSCRIBE FIRST. Create the consumer (with its standing
                //    prefetch credit) BEFORE any publish. Events have no replay —
                //    a publish that beats the subscription is lost forever.
                // =============================================================
                try (MessageConsumer consumer = session.createConsumer(topic)) {
                    System.out.printf("[recv] Subscribed to %s (Qpid JMS standing prefetch credit)%n", address);

                    // The attach reply confirms the link, not that the connector's
                    // subscription pump has run its SubscribeEvents yet. Wait for it
                    // to go live before publishing, or the first events race the
                    // subscription and are dropped (no replay).
                    Thread.sleep(750);
                    System.out.println("[recv] Subscription pump settled (waited 750ms before publishing)");

                    // =========================================================
                    // 2. PUBLISH pre-settled. NON_PERSISTENT events are at-most-
                    //    once; the connector sends them pre-settled, so there is no
                    //    DISPOSITION to await and no produce confirmation.
                    // =========================================================
                    try (MessageProducer producer = session.createProducer(topic)) {
                        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                        for (int i = 0; i < TOTAL; i++) {
                            producer.send(session.createTextMessage(String.format("event-%03d", i)));
                        }
                    }
                    System.out.printf("[send] Published %d events (fire-and-forget)%n", TOTAL);

                    // =========================================================
                    // 3. RECEIVE. With standing credit the subscriber drains
                    //    every event on the happy path.
                    // =========================================================
                    Set<String> seen = new HashSet<>();
                    while (seen.size() < TOTAL) {
                        Message msg = consumer.receive(30_000);
                        if (msg == null) {
                            throw new IllegalStateException(
                                    "timed out before receiving all events (" + seen.size() + "/" + TOTAL + ")");
                        }
                        // The connector re-emits the event body as an AMQP Data
                        // (bytes) section, so Qpid JMS may deliver a BytesMessage
                        // here. getBody(String.class) decodes either type as UTF-8.
                        seen.add(msg.getBody(String.class));
                    }
                    System.out.printf(
                            "[recv] Received all %d events (continuous credit ⇒ no 0-credit drop)%n", seen.size());
                }
            }
        }

        System.out.println("\nDone.");
    }
}

// Expected output:
//
// Broker:  amqp://localhost:5672
// Address: events/amqp10.examples.pubsub  (KubeMQ pattern=events, channel=amqp10.examples.pubsub)
//
// [recv] Subscribed to events/amqp10.examples.pubsub (Qpid JMS standing prefetch credit)
// [recv] Subscription pump settled (waited 750ms before publishing)
// [send] Published 20 events (fire-and-forget)
// [recv] Received all 20 events (continuous credit ⇒ no 0-credit drop)
//
// Done.
//
// (Events are at-most-once with no replay: if the subscriber were at 0 credit when
// an event arrived, that event would be SILENTLY DROPPED and counted on the server
// metric kubemq_amqp10_events_dropped_no_credit_total — never surfaced as a client
// error. Standing prefetch + subscribe-before-publish avoid both losses.)
