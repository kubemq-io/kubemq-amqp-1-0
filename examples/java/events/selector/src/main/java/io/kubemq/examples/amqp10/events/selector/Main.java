// Example: events/selector (master-table variant #6)
//
// JMS / SQL-92 message selectors over KubeMQ Events through Apache Qpid JMS
// (javax.jms) — NO KubeMQ SDK.
//
// JMS-over-AMQP-1.0 mapping:
//   - session.createConsumer(topic, selector) is the JMS-NATIVE selector surface.
//     Qpid JMS encodes the selector string on the consumer's ATTACH source filter
//     under the OASIS-standard key "apache.org:selector-filter:string" (the same
//     filter other native clients build by hand). The connector evaluates it
//     against each event's APPLICATION PROPERTIES and delivers ONLY the matching
//     events; non-matching events are silently withheld (copy semantics — they
//     stay available to OTHER subscribers, they are not consumed/discarded).
//   - Message properties (setStringProperty / setIntProperty) map to AMQP
//     application-properties, which is what the selector resolves against.
//
// The selector here is:  color = 'red' AND size > 2
//
// We publish 5 events and assert exactly 2 are delivered:
//   match-1      {color:red,  size:5}  delivered
//   miss-blue    {color:blue, size:9}  color != red
//   miss-small   {color:red,  size:1}  size not > 2
//   match-2      {color:red,  size:3}  delivered
//   miss-nocolor {           size:8}   color IS NULL ⇒ UNKNOWN (3-valued) ⇒ withheld
//
// THREE-VALUED LOGIC: an absent property evaluates to NULL, so the predicate is
// UNKNOWN (not true) and the event is NOT delivered — this is why miss-nocolor is
// withheld even though it has no color to disqualify it.
//
// We consume by EXPECTED COUNT (the 2 known matches) first, then issue a trailing
// receive(timeout) to confirm "no more arrive". With a connector that includes the
// AMQP 1.0 JMS-compat fixes, a drain=true FLOW on a pub-sub link now completes
// promptly, so a timed receive on an exhausted consumer returns null in a timely
// fashion (it is a reliable end-of-stream signal). See the README "Gotcha".
//
// SELECTOR-ON-QUEUES: a selector is honoured ONLY on events/ and events-store/
// consume links; queues/ is move-only. We DO exercise a queues+selector ATTACH here
// to prove the rejection is clean: with the JMS-compat fixes the connector replies
// ATTACH + a prompt DETACH carrying amqp:not-implemented, which Qpid JMS surfaces as
// a JMSException on createConsumer ("selector filter not supported on this address")
// — NO hang. (Older connectors left the ATTACH unanswered and Qpid JMS would block
// until its request timeout. See the README "Gotcha".)
//
// Grounded in connector test TestEventsSelector
// (connectors/amqp10/integration_pubsub_test.go).
//
// Run:
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   mvn -pl events/selector exec:java
package io.kubemq.examples.amqp10.events.selector;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.qpid.jms.JmsConnectionFactory;

public final class Main {

    private static final String CHANNEL = "amqp10.examples.selector";
    private static final String SELECTOR = "color = 'red' AND size > 2";

    private Main() {
    }

    static String amqpUrl() {
        String v = System.getenv("KUBEMQ_AMQP_URL");
        return (v != null && !v.isEmpty()) ? v : "amqp://localhost:5672";
    }

    public static void main(String[] args) throws JMSException, InterruptedException {
        String url = amqpUrl();
        String address = "events/" + CHANNEL;
        System.out.printf("Broker:   %s%n", url);
        System.out.printf("Address:  %s  (KubeMQ pattern=events, channel=%s)%n", address, CHANNEL);
        System.out.printf("Selector: %s%n%n", SELECTOR);

        JmsConnectionFactory factory = new JmsConnectionFactory(url);

        try (Connection connection = factory.createConnection()) {
            connection.start();
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                Topic topic = session.createTopic(address);

                // SUBSCRIBE FIRST with the JMS selector. A successful createConsumer
                // means the connector accepted (and echoed) the selector filter — a
                // parse error or unsupported pattern would DETACH the link.
                try (MessageConsumer consumer = session.createConsumer(topic, SELECTOR)) {
                    System.out.printf("[recv] Subscribed to %s with selector%n", address);
                    Thread.sleep(750); // let the subscription pump go live (no replay)
                    System.out.println("[recv] Subscription pump settled (waited 750ms before publishing)");

                    // PUBLISH 5 events with application properties.
                    int wantMatches = 0;
                    try (MessageProducer producer = session.createProducer(topic)) {
                        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                        wantMatches += publish(session, producer, "match-1", "red", 5, true, "color=red AND size>2");
                        wantMatches += publish(session, producer, "miss-blue", "blue", 9, false, "color!=red");
                        wantMatches += publish(session, producer, "miss-small", "red", 1, false, "size not > 2");
                        wantMatches += publish(session, producer, "match-2", "red", 3, true, "color=red AND size>2");
                        wantMatches += publish(session, producer, "miss-nocolor", null, 8, false,
                                "color IS NULL ⇒ UNKNOWN (3-valued)");
                    }

                    // RECEIVE the known number of matching events.
                    Map<String, Boolean> got = new LinkedHashMap<>();
                    while (got.size() < wantMatches) {
                        Message msg = consumer.receive(15_000);
                        if (msg == null) {
                            throw new IllegalStateException(
                                    "timed out before receiving all matches (" + got.size() + "/" + wantMatches + ")");
                        }
                        // The connector re-emits the event body as an AMQP Data
                        // (bytes) section, so Qpid JMS delivers a BytesMessage here even
                        // though the sender built a TextMessage. getBody(String.class)
                        // decodes either type as UTF-8 (matches the Go/Rust variants,
                        // which read the Data body and decode it as a string).
                        String body = msg.getBody(String.class);
                        System.out.printf("[recv] delivered: %s%n", body);
                        got.put(body, true);
                    }
                    System.out.printf(
                            "[recv] Received exactly %d matching event(s); %d non-matching event(s) were silently withheld%n",
                            got.size(), 5 - wantMatches);

                    // END-OF-STREAM via a trailing timed receive. With the JMS-compat
                    // connector fixes a drain=true FLOW on a pub-sub link completes
                    // promptly, so this receive(timeout) returns null in a timely
                    // fashion once the matches are exhausted — a reliable confirmation
                    // that no further (non-matching) events leak through. (Older
                    // connectors ignored drain=true FLOW; see the README "Gotcha".)
                    Message tail = consumer.receive(3_000);
                    if (tail != null) {
                        throw new IllegalStateException(
                                "unexpected extra delivery after the matches: " + tail.getBody(String.class));
                    }
                    System.out.println(
                            "[recv] Trailing receive(3s) returned null — drain confirms no non-matching events leaked");
                }

                // SELECTOR-ON-QUEUES rejection — selectors are honoured ONLY on
                // events/ and events-store/ consume links; a selector on a queues/
                // source is not supported (queues are move-only). With the JMS-compat
                // connector fixes this no longer hangs: the connector replies ATTACH +
                // a prompt DETACH carrying amqp:not-implemented, which Qpid JMS surfaces
                // as a JMSException on createConsumer. We exercise it here to prove the
                // rejection is clean and prompt.
                String queuesAddress = "queues/" + CHANNEL;
                Queue queue = session.createQueue(queuesAddress);
                System.out.printf("%n[probe] Attempting createConsumer on %s WITH a selector (expected: rejection)%n",
                        queuesAddress);
                try {
                    session.createConsumer(queue, SELECTOR);
                    throw new IllegalStateException(
                            "expected a selector-on-queues rejection, but createConsumer succeeded");
                } catch (JMSException expected) {
                    System.out.printf("[probe] Rejected promptly (no hang): %s%n", expected.getMessage());
                    System.out.println(
                            "[probe] queues/ is move-only — a selector is not supported there (amqp:not-implemented)");
                }
            }
        }

        System.out.println("\nDone.");
    }

    // publish sends one event with color (nullable) + size application properties,
    // returns 1 if it should match the selector (for the expected-match count).
    private static int publish(Session session, MessageProducer producer, String body, String color, int size,
            boolean match, String why) throws JMSException {
        TextMessage msg = session.createTextMessage(body);
        if (color != null) {
            msg.setStringProperty("color", color);
        }
        msg.setIntProperty("size", size);
        producer.send(msg);
        String verdict = match ? "should MATCH" : "should be FILTERED OUT";
        System.out.printf("[send] %-13s color=%-5s size=%d → %s (%s)%n",
                body, color == null ? "<null>" : color, size, verdict, why);
        return match ? 1 : 0;
    }
}

// Expected output:
//
// Broker:   amqp://localhost:5672
// Address:  events/amqp10.examples.selector  (KubeMQ pattern=events, channel=amqp10.examples.selector)
// Selector: color = 'red' AND size > 2
//
// [recv] Subscribed to events/amqp10.examples.selector with selector
// [recv] Subscription pump settled (waited 750ms before publishing)
// [send] match-1       color=red   size=5 → should MATCH (color=red AND size>2)
// [send] miss-blue     color=blue  size=9 → should be FILTERED OUT (color!=red)
// [send] miss-small    color=red   size=1 → should be FILTERED OUT (size not > 2)
// [send] match-2       color=red   size=3 → should MATCH (color=red AND size>2)
// [send] miss-nocolor  color=<null> size=8 → should be FILTERED OUT (color IS NULL ⇒ UNKNOWN (3-valued))
// [recv] delivered: match-1
// [recv] delivered: match-2
// [recv] Received exactly 2 matching event(s); 3 non-matching event(s) were silently withheld
// [recv] Trailing receive(3s) returned null — drain confirms no non-matching events leaked
//
// [probe] Attempting createConsumer on queues/amqp10.examples.selector WITH a selector (expected: rejection)
// [probe] Rejected promptly (no hang): selector filter not supported on this address [condition = amqp:not-implemented]
// [probe] queues/ is move-only — a selector is not supported there (amqp:not-implemented)
//
// Done.
