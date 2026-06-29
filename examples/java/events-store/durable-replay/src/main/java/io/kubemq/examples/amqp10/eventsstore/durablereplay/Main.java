// Example: events-store/durable-replay (master-table variant #7)
//
// Durable subscriptions with resume over KubeMQ Events Store through Apache Qpid
// JMS (javax.jms) — NO KubeMQ SDK.
//
// JMS-over-AMQP-1.0 mapping:
//   - A JMS durable subscriber — connection.setClientID(id) +
//     session.createDurableConsumer(topic, subName) — attaches with
//     terminus-expiry-policy=never, which the connector maps onto a durable Events
//     Store subscription. The durable identity is the pair:
//
//         (JMS clientID, subscription name) → (container-id, link name) → STAN durable
//
//   - On consumer.close() the durable subscription is RETAINED (only the link
//     detaches). Re-attaching with the SAME (clientID, subName) RESUMES the
//     subscription and delivers every event published while the subscriber was
//     away — no loss, no replay of already-consumed events.
//   - session.unsubscribe(subName) permanently removes the durable subscription
//     (the connector deletes the underlying durable cleanly). This example calls it
//     at the very end as a teardown step so it leaves no orphan subscription behind.
//     NOTE: durable unsubscribe requires a connector build that includes the AMQP
//     1.0 JMS-compat fixes; older connectors reject the null-source ATTACH for
//     durable deletion with amqp:not-found. See the README "Gotcha".
//
// Flow:
//   1. clientID + createDurableConsumer("durable-sub"). Publish 3 events; receive
//      all 3.
//   2. consumer.close() (detach but KEEP the durable) + close the connection.
//   3. Publish 5 MORE events while the durable subscriber is away.
//   4. Re-connect with the SAME clientID; re-create the durable consumer. It
//      RESUMES and delivers the 5 missed events.
//   5. session.unsubscribe("durable-sub") — clean teardown of the durable.
//
// Grounded in connector test TestEventsStoreDurableReplay
// (connectors/amqp10/integration_pubsub_test.go) and the server-side Qpid JMS
// conformance case DurableSubscriberConformanceTest.durableSubscriberReplay.
//
// Run:
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   mvn -pl events-store/durable-replay exec:java
package io.kubemq.examples.amqp10.eventsstore.durablereplay;

import java.util.HashSet;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.qpid.jms.JmsConnectionFactory;

public final class Main {

    private static final String CHANNEL = "amqp10.examples.durable";

    // The durable identity = (JMS clientID, subscription name). Both MUST be stable
    // across reconnects for the subscription to resume.
    private static final String CLIENT_ID = "amqp10-examples-durable-container";
    private static final String SUB_NAME = "durable-sub";

    private Main() {
    }

    static String amqpUrl() {
        String v = System.getenv("KUBEMQ_AMQP_URL");
        return (v != null && !v.isEmpty()) ? v : "amqp://localhost:5672";
    }

    public static void main(String[] args) throws JMSException, InterruptedException {
        String url = amqpUrl();
        String address = "events-store/" + CHANNEL;
        System.out.printf("Broker:     %s%n", url);
        System.out.printf("Address:    %s  (KubeMQ pattern=events-store, channel=%s)%n", address, CHANNEL);
        System.out.printf("Durable id: clientID=\"%s\"  sub-name=\"%s\"%n%n", CLIENT_ID, SUB_NAME);

        JmsConnectionFactory factory = new JmsConnectionFactory(url);

        // PRODUCER — a separate connection that publishes throughout the demo
        // (it does not need a stable clientID).
        try (Connection prodConn = factory.createConnection();
                Session prodSession = prodConn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            prodConn.start();
            Topic topic = prodSession.createTopic(address);
            try (MessageProducer producer = prodSession.createProducer(topic)) {

                // 1. DURABLE SUBSCRIBE (first attach). Stable clientID + sub-name make
                //    this subscription durable and resumable.
                try (Connection durConn = factory.createConnection()) {
                    durConn.setClientID(CLIENT_ID);
                    durConn.start();
                    try (Session durSession = durConn.createSession(false, Session.CLIENT_ACKNOWLEDGE)) {
                        Topic durTopic = durSession.createTopic(address);
                        MessageConsumer durable = durSession.createDurableConsumer(durTopic, SUB_NAME);
                        System.out.printf(
                                "[recv] Durable receiver attached (first attach): clientID=\"%s\" sub=\"%s\" expiry=never%n",
                                CLIENT_ID, SUB_NAME);
                        Thread.sleep(750); // let the subscription pump go live before producing

                        publish(prodSession, producer, 0, 3); // 3 events while live
                        Set<String> first = drain(durable, 3, 30_000);
                        if (first.size() != 3) {
                            throw new IllegalStateException(
                                    "durable subscriber expected the first 3 events, got " + first);
                        }
                        System.out.printf("[recv] First attach received %d events: %s%n%n", first.size(), first);

                        // consumer.close() detaches the link but KEEPS the durable.
                        durable.close();
                    }
                }
                System.out.println("[conn] Durable subscriber DISCONNECTED (cursor preserved by the connector)");
                Thread.sleep(1_000); // let the detach settle

                // 3. PUBLISH WHILE AWAY. 5 more events arrive at the persisted stream.
                publish(prodSession, producer, 3, 8);
                System.out.println("[send] Published 5 more events WHILE the durable subscriber was away");

                // 4. RE-ATTACH with the SAME durable identity. The subscription
                //    RESUMES and delivers exactly the 5 events published while away.
                try (Connection durConn2 = factory.createConnection()) {
                    durConn2.setClientID(CLIENT_ID);
                    durConn2.start();
                    try (Session durSession2 = durConn2.createSession(false, Session.CLIENT_ACKNOWLEDGE)) {
                        Topic durTopic2 = durSession2.createTopic(address);
                        MessageConsumer durable2 = durSession2.createDurableConsumer(durTopic2, SUB_NAME);
                        System.out.printf(
                                "[recv] Durable receiver attached (re-attach): clientID=\"%s\" sub=\"%s\" expiry=never%n",
                                CLIENT_ID, SUB_NAME);

                        Set<String> resumed = drain(durable2, 5, 30_000);
                        for (int i = 3; i < 8; i++) {
                            String body = String.format("es-%03d", i);
                            if (!resumed.contains(body)) {
                                throw new IllegalStateException("durable resume missing event " + body + " (got "
                                        + resumed + ")");
                            }
                        }
                        System.out.printf(
                                "[recv] Re-attach RESUMED and received the %d events published while away: %s%n",
                                resumed.size(), resumed);
                        System.out.println("[recv] No loss, no re-delivery of the already-consumed first 3 — "
                                + "the durable cursor resumed exactly");

                        // 5. CLEAN TEARDOWN. Close the active consumer, then
                        //    session.unsubscribe(SUB_NAME) permanently removes the durable
                        //    subscription (the connector deletes the underlying durable
                        //    cleanly via the null-source ATTACH for durable deletion). JMS
                        //    requires the durable's consumer to be closed before
                        //    unsubscribe. NOTE: this requires a connector build with the
                        //    AMQP 1.0 JMS-compat fixes — older connectors reject the
                        //    deletion ATTACH with amqp:not-found. See the README "Gotcha".
                        durable2.close();
                        durSession2.unsubscribe(SUB_NAME);
                        System.out.printf(
                                "[recv] Durable subscription \"%s\" unsubscribed (removed cleanly — no orphan left behind)%n",
                                SUB_NAME);
                    }
                }
            }
        }

        System.out.println("\nDone.");
    }

    // publish sends events es-<lo>..es-<hi-1> (unsettled — events-store persists
    // each accepted transfer).
    private static void publish(Session session, MessageProducer producer, int lo, int hi) throws JMSException {
        for (int i = lo; i < hi; i++) {
            producer.send(session.createTextMessage(String.format("es-%03d", i)));
        }
    }

    // drain receives up to max events within timeoutMillis, returning their bodies.
    private static Set<String> drain(MessageConsumer consumer, int max, long timeoutMillis) throws JMSException {
        Set<String> out = new HashSet<>();
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (out.size() < max) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }
            Message msg = consumer.receive(remaining);
            if (msg == null) {
                break;
            }
            msg.acknowledge();
            // The connector re-emits the event body as an AMQP Data (bytes)
            // section, so Qpid JMS may deliver a BytesMessage here.
            // getBody(String.class) decodes either type as UTF-8.
            out.add(msg.getBody(String.class));
        }
        return out;
    }
}

// Expected output:
//
// Broker:     amqp://localhost:5672
// Address:    events-store/amqp10.examples.durable  (KubeMQ pattern=events-store, channel=amqp10.examples.durable)
// Durable id: clientID="amqp10-examples-durable-container"  sub-name="durable-sub"
//
// [recv] Durable receiver attached (first attach): clientID="amqp10-examples-durable-container" sub="durable-sub" expiry=never
// [recv] First attach received 3 events: [es-000, es-001, es-002]
//
// [conn] Durable subscriber DISCONNECTED (cursor preserved by the connector)
// [send] Published 5 more events WHILE the durable subscriber was away
// [recv] Durable receiver attached (re-attach): clientID="amqp10-examples-durable-container" sub="durable-sub" expiry=never
// [recv] Re-attach RESUMED and received the 5 events published while away: [es-003, es-004, es-005, es-006, es-007]
// [recv] No loss, no re-delivery of the already-consumed first 3 — the durable cursor resumed exactly
// [recv] Durable subscription "durable-sub" unsubscribed (removed cleanly — no orphan left behind)
//
// Done.
//
// NOTE: durable subscriptions are NODE-LOCAL (see README gotcha). In a cluster the
// durable cursor lives on the node that owned the original attach; reconnect to the
// SAME node (or run a single-node dev broker, as here) to resume.
