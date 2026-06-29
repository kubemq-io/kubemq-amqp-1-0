// Example: queues/ack-release-redelivery (master-table variant #2)
//
// Accept vs release/redelivery on a KubeMQ Queue, driven through Apache Qpid JMS
// (javax.jms) — NO KubeMQ SDK.
//
// JMS-over-AMQP-1.0 mapping for the queue settlement outcomes:
//   - accept  → message.acknowledge() on a CLIENT_ACKNOWLEDGE session settles the
//     delivery `accepted` ⇒ the connector AckRanges it (removed from the queue).
//   - release → Session.recover() abandons every delivery that has NOT yet been
//     acknowledged on the session. Qpid JMS settles those deliveries `released`
//     (modified/released disposition) ⇒ the connector NAckRanges them: they are
//     REQUEUED to the tail and REDELIVERED with JMSRedelivered=true, a grown
//     header delivery-count (JMSXDeliveryCount >= 2), and first-acquirer=false.
//     Each release also increments the broker receive-count toward
//     MaxReceiveQueue (the gotcha below).
//
// The connector maps the KubeMQ broker receive-count onto the AMQP header:
//   header.delivery-count = ReceiveCount-1, first-acquirer = (ReceiveCount==1).
// Qpid JMS surfaces these as the JMSXDeliveryCount property (1-based: first
// delivery = 1) and the JMSRedelivered flag.
//
// Grounded in connector tests TestQueueReleasedRedelivery and
// TestQueueRejectedDiscard (connectors/amqp10/integration_test.go). The reject ⇒
// discard outcome (no requeue) has no first-class Qpid JMS verb on a plain
// CLIENT_ACKNOWLEDGE session — it is the broker MaxReceiveQueue poison policy,
// described in the README; this example demonstrates accept + release/redelivery.
//
// Run:
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   mvn -pl queues/ack-release-redelivery exec:java
package io.kubemq.examples.amqp10.queues.ackreleaseredelivery;

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

    private static final String CHANNEL = "amqp10.examples.ack";

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
        System.out.printf("Address: %s%n%n", address);

        JmsConnectionFactory factory = new JmsConnectionFactory(url);

        try (Connection connection = factory.createConnection()) {
            connection.start();
            try (Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)) {
                Queue queue = session.createQueue(address);

                // Produce two messages: one we accept on first sight, one we
                // release (recover) so it is requeued and redelivered.
                try (MessageProducer producer = session.createProducer(queue)) {
                    producer.send(session.createTextMessage("accept-me"));
                    producer.send(session.createTextMessage("release-me"));
                }
                System.out.println("[send] Produced: accept-me, release-me");

                try (MessageConsumer consumer = session.createConsumer(queue)) {
                    // ---------------------------------------------------------
                    // First pass: accept "accept-me" (it is removed); leave
                    // "release-me" UN-acknowledged, then Session.recover() to
                    // release it back to the queue tail (NAckRange ⇒ redelivery).
                    // ---------------------------------------------------------
                    boolean acceptedAcceptMe = false;
                    boolean releasedReleaseMe = false;
                    while (!acceptedAcceptMe || !releasedReleaseMe) {
                        Message msg = consumer.receive(30_000);
                        if (msg == null) {
                            throw new IllegalStateException("timed out waiting for the first pass");
                        }
                        // The connector may deliver the body as an AMQP Data (bytes)
                        // section, so Qpid JMS could surface a BytesMessage here.
                        // getBody(String.class) decodes either type as UTF-8.
                        String body = msg.getBody(String.class);
                        int deliveryCount = msg.getIntProperty("JMSXDeliveryCount");
                        if ("accept-me".equals(body)) {
                            msg.acknowledge(); // accept ⇒ AckRange (removed)
                            acceptedAcceptMe = true;
                            System.out.printf(
                                    "[recv] %-12s JMSXDeliveryCount=%d redelivered=%b  -> ACCEPTED (removed)%n",
                                    body, deliveryCount, msg.getJMSRedelivered());
                        } else if ("release-me".equals(body) && !releasedReleaseMe) {
                            System.out.printf(
                                    "[recv] %-12s JMSXDeliveryCount=%d redelivered=%b  -> RELEASING (recover ⇒ requeue)%n",
                                    body, deliveryCount, msg.getJMSRedelivered());
                            // recover() releases every un-acknowledged delivery on
                            // this session; the broker requeues + redelivers them.
                            session.recover();
                            releasedReleaseMe = true;
                        }
                    }

                    // ---------------------------------------------------------
                    // Redelivery: the released "release-me" comes back with a grown
                    // delivery-count, JMSRedelivered=true, first-acquirer=false.
                    // Accept it now to drain the queue.
                    // ---------------------------------------------------------
                    Message redelivered = consumer.receive(30_000);
                    if (redelivered == null) {
                        throw new IllegalStateException("released message was not redelivered");
                    }
                    // The connector may deliver the body as an AMQP Data (bytes)
                    // section, so Qpid JMS could surface a BytesMessage here.
                    // getBody(String.class) decodes either type as UTF-8.
                    String body = redelivered.getBody(String.class);
                    int deliveryCount = redelivered.getIntProperty("JMSXDeliveryCount");
                    if (!"release-me".equals(body) || !redelivered.getJMSRedelivered() || deliveryCount < 2) {
                        throw new IllegalStateException(String.format(
                                "expected redelivered 'release-me' with JMSRedelivered=true and JMSXDeliveryCount>=2, "
                                        + "got body=%s redelivered=%b deliveryCount=%d",
                                body, redelivered.getJMSRedelivered(), deliveryCount));
                    }
                    redelivered.acknowledge();
                    System.out.printf(
                            "[recv] %-12s JMSXDeliveryCount=%d redelivered=%b  -> REDELIVERED, then ACCEPTED%n",
                            body, deliveryCount, redelivered.getJMSRedelivered());

                    // The queue is now empty.
                    Message extra = consumer.receive(2_000);
                    if (extra != null) {
                        throw new IllegalStateException("expected an empty queue after draining");
                    }
                    System.out.println("[recv] Queue drained to empty (released message resumed exactly once)");
                }
            }
        }

        System.out.println("\nDone.");
    }
}

// Expected output:
//
// Broker:  amqp://localhost:5672
// Address: queues/amqp10.examples.ack
//
// [send] Produced: accept-me, release-me
// [recv] accept-me    JMSXDeliveryCount=1 redelivered=false  -> ACCEPTED (removed)
// [recv] release-me   JMSXDeliveryCount=1 redelivered=false  -> RELEASING (recover ⇒ requeue)
// [recv] release-me   JMSXDeliveryCount=2 redelivered=true   -> REDELIVERED, then ACCEPTED
// [recv] Queue drained to empty (released message resumed exactly once)
//
// Done.
//
// (Delivery order between accept-me and release-me on the first pass can vary;
// the redelivered "release-me" always carries JMSRedelivered=true and
// JMSXDeliveryCount>=2. reject ⇒ discard has no first-class Qpid JMS verb here —
// poison handling is the broker MaxReceiveQueue policy; see the README.)
