// Example: commands/request-reply-dynamic-node (master-table variant #9)
//
// JMS request/reply over KubeMQ **Commands** (RPC) with Apache Qpid JMS
// (javax.jms) — NO KubeMQ SDK, NO gRPC. The whole round-trip stays in-protocol
// over a single broker connection per role.
//
// JMS-over-AMQP-1.0 mapping (the Qpid-JMS-native RPC surface):
//   - The dynamic reply node is a JMS TEMPORARY QUEUE — session.createTemporary-
//     Queue(). Qpid JMS attaches a receiver on a server-assigned, connection-owned
//     transient node (the JMS analogue of go-amqp's DynamicAddress:true reply
//     node). The requester sets message.setJMSReplyTo(tempQueue) +
//     setJMSCorrelationID(id) and sends to the commands/<ch> Queue. The connector
//     verifies the reply-to names a node THIS connection owns (snooping guard:
//     amqp:not-allowed otherwise) and routes the command to SendCommand. The
//     broker Response is delivered out-of-band onto the temporary queue; the
//     requester correlates it by JMSCorrelationID.
//
//   - The responder consumes commands/<ch> and replies with a producer addressed
//     to the request's getJMSReplyTo() (the connector stamps the reply-to as
//     "/responses/<RequestID>"), echoing the JMSCorrelationID. A command reply
//     ALSO carries x-opt-kubemq-executed (boolean) + x-opt-kubemq-error (string)
//     AMQP application-properties.
//
// PROPERTY-NAME VALIDATION: JMS application-property names must be valid Java
//   identifiers, so by default Qpid JMS REJECTS hyphenated names like
//   "x-opt-kubemq-executed" in setStringProperty/setBooleanProperty. The
//   JMS-native escape hatch is the connection-factory option
//   jms.validatePropertyNames=false (URI) / setValidatePropertyNames(false),
//   which lets the example set/read the connector's hyphenated command-outcome
//   application-properties directly — without it the executed/error envelope is
//   unreachable from the JMS API.
//
// Commands vs Queries (the #9 vs #10 contrast): a command that FAILS still
// produces a reply (executed=false + error text) so the requester is NEVER left
// waiting. This example demonstrates BOTH: a successful command (executed=true)
// and a failed command (executed=false) — both round-trip, neither hangs.
//
// Grounded in connector tests TestRPCRequesterResponderViaAMQP10 (commands leg)
// and TestRPCInteropAMQP10RequesterGRPCResponder (the executed/error app-props)
// (connectors/amqp10/integration_test.go).
//
// Run:
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   mvn -pl commands/request-reply-dynamic-node exec:java
package io.kubemq.examples.amqp10.commands.requestreplydynamicnode;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;

import org.apache.qpid.jms.JmsConnectionFactory;

public final class Main {

    // The KubeMQ commands channel; the JMS Queue name is the explicit connector
    // address "commands/" + channel (never rely on a default pattern).
    private static final String CHANNEL = "amqp10.examples.commands";

    // Command-reply application-properties carrying the execution outcome. These
    // are HYPHENATED, so the connections set jms.validatePropertyNames=false.
    private static final String PROP_EXECUTED = "x-opt-kubemq-executed";
    private static final String PROP_ERROR = "x-opt-kubemq-error";

    private Main() {
    }

    static String amqpUrl() {
        String v = System.getenv("KUBEMQ_AMQP_URL");
        return (v != null && !v.isEmpty()) ? v : "amqp://localhost:5672";
    }

    public static void main(String[] args) throws JMSException, InterruptedException {
        String url = amqpUrl();
        String address = "commands/" + CHANNEL;
        System.out.printf("Broker:  %s%n", url);
        System.out.printf("Address: %s  (KubeMQ pattern=commands, channel=%s)%n%n", address, CHANNEL);

        // jms.validatePropertyNames=false lets the responder/requester set and read
        // the hyphenated x-opt-kubemq-executed / x-opt-kubemq-error app-properties
        // (otherwise the JMS layer rejects the property name).
        JmsConnectionFactory factory = new JmsConnectionFactory(url);
        factory.setValidatePropertyNames(false);

        // The responder runs on its own thread + connection so this one program is
        // runnable standalone. It signals readiness so the first command does not
        // race the responder's attach.
        Responder responder = new Responder(factory, address);
        Thread responderThread = new Thread(responder, "responder");
        responderThread.start();
        responder.awaitReady(15_000);

        runRequester(factory, address);

        responder.stop();
        responderThread.join(5_000);

        System.out.println("\nDone.");
    }

    // =========================================================================
    // REQUESTER — temporary-queue reply node + producer on commands/<ch>;
    // correlates replies by JMSCorrelationID.
    // =========================================================================
    private static void runRequester(JmsConnectionFactory factory, String address) throws JMSException {
        try (Connection connection = factory.createConnection()) {
            connection.start();
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                // DYNAMIC reply node: a JMS temporary queue is a server-assigned,
                // connection-owned transient node — the JMS analogue of a dynamic
                // reply address. Its consumer is the requester's private mailbox.
                TemporaryQueue replyNode = session.createTemporaryQueue();
                System.out.printf("[requester] Dynamic reply node (temp queue): %s%n", replyNode.getQueueName());

                Queue commands = session.createQueue(address);
                try (MessageConsumer replyConsumer = session.createConsumer(replyNode);
                        MessageProducer producer = session.createProducer(commands)) {

                    // 1. A SUCCESSFUL command: round-trips with executed=true.
                    doRequest(session, producer, replyConsumer, replyNode, "reboot-node-7", "corr-cmd-1");

                    // 2. A FAILED command ("fail"): the responder replies
                    //    executed=false + error text — the requester is NOT left
                    //    waiting (the key Commands contrast vs Queries, where a
                    //    failure delivers nothing and the requester times out).
                    doRequest(session, producer, replyConsumer, replyNode, "fail", "corr-cmd-2");
                }
            }
        }
    }

    // doRequest sends one command naming the temp-queue reply node + a correlation-
    // id, then awaits the correlated reply and prints the executed/error outcome.
    private static void doRequest(Session session, MessageProducer producer, MessageConsumer replyConsumer,
            TemporaryQueue replyNode, String body, String corr) throws JMSException {
        TextMessage req = session.createTextMessage(body);
        req.setJMSReplyTo(replyNode); // MUST name a node this connection owns (snooping guard)
        req.setJMSCorrelationID(corr);
        producer.send(req);
        System.out.printf("[requester] Sent command \"%s\" (reply-to=temp queue, correlation-id=%s)%n", body, corr);

        // A command always replies (success OR failure), so this never times out on
        // the happy path. Correlate on the value WE sent.
        Message reply = replyConsumer.receive(30_000);
        if (reply == null) {
            throw new IllegalStateException("timed out awaiting reply for \"" + body + "\"");
        }
        String gotCorr = reply.getJMSCorrelationID();
        if (!corr.equals(gotCorr)) {
            throw new IllegalStateException(
                    "correlation-id mismatch: want \"" + corr + "\" got \"" + gotCorr + "\"");
        }
        boolean executed = reply.getBooleanProperty(PROP_EXECUTED);
        String errText = reply.getStringProperty(PROP_ERROR);
        String replyBody = (reply instanceof TextMessage) ? ((TextMessage) reply).getText() : "";
        System.out.printf("[requester] Reply for \"%s\" (correlation-id=%s): executed=%b error=\"%s\" body=\"%s\"%n",
                body, gotCorr, executed, errText == null ? "" : errText, replyBody == null ? "" : replyBody);
    }

    // =========================================================================
    // RESPONDER — consumes commands/<ch>, replies to each request's JMSReplyTo
    // (the connector's /responses/<RequestID>) with executed/error app-properties.
    // =========================================================================
    private static final class Responder implements Runnable {
        private final JmsConnectionFactory factory;
        private final String address;
        private final Object readyLock = new Object();
        private volatile boolean ready;
        private volatile boolean running = true;

        Responder(JmsConnectionFactory factory, String address) {
            this.factory = factory;
            this.address = address;
        }

        void awaitReady(long timeoutMillis) throws InterruptedException {
            synchronized (readyLock) {
                long deadline = System.currentTimeMillis() + timeoutMillis;
                while (!ready) {
                    long remaining = deadline - System.currentTimeMillis();
                    if (remaining <= 0) {
                        throw new IllegalStateException("responder did not become ready");
                    }
                    readyLock.wait(remaining);
                }
            }
        }

        void stop() {
            running = false;
        }

        @Override
        public void run() {
            try (Connection connection = factory.createConnection()) {
                connection.start();
                try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                    Queue commands = session.createQueue(address);
                    // Reply producer with an UNIDENTIFIED destination (createProducer(null)):
                    // each reply is addressed at send time to the request's JMSReplyTo, so
                    // one producer serves every requester's temp queue.
                    try (MessageConsumer consumer = session.createConsumer(commands);
                            MessageProducer replyProducer = session.createProducer(null)) {

                        System.out.printf("[responder] Listening on %s (reply producer ready)%n", address);
                        signalReady();

                        while (running) {
                            Message req = consumer.receive(1_000);
                            if (req == null) {
                                continue;
                            }
                            handle(session, replyProducer, req);
                        }
                    }
                }
            } catch (JMSException e) {
                if (running) {
                    System.err.println("[responder] error: " + e.getMessage());
                }
            }
        }

        private void handle(Session session, MessageProducer replyProducer, Message req) throws JMSException {
            Destination replyTo = req.getJMSReplyTo();
            if (replyTo == null) {
                System.out.println("[responder] request with no reply-to; cannot reply");
                return;
            }
            String body = (req instanceof TextMessage) ? ((TextMessage) req).getText() : "";
            System.out.printf("[responder] Received command \"%s\" (correlation-id=%s)%n",
                    body, req.getJMSCorrelationID());

            // Business logic: a command body of "fail" is rejected (executed=false),
            // any other body succeeds (executed=true). BOTH paths reply — a command
            // failure must NOT leave the requester waiting (unlike a query, #10).
            //
            // NOTE: a COMMAND response carries the EXECUTED/ERROR outcome, not data.
            // The broker round-trips executed + error (and the echoed correlation-id)
            // but NOT a reply body — the requester observes an empty command body.
            // Use a QUERY (#10) when you need to return a value.
            boolean ok = !"fail".equals(body);
            String errText = ok ? "" : "command rejected by handler";

            TextMessage reply = session.createTextMessage("ack:" + body);
            reply.setJMSCorrelationID(req.getJMSCorrelationID()); // echo so the requester can match
            // A COMMAND reply carries the execution outcome as application-properties
            // (hyphenated names — enabled by jms.validatePropertyNames=false).
            reply.setBooleanProperty(PROP_EXECUTED, ok);
            reply.setStringProperty(PROP_ERROR, errText);

            replyProducer.send(replyTo, reply);
            System.out.printf("[responder] Replied to \"%s\" (executed=%b, error=\"%s\")%n", body, ok, errText);
        }

        private void signalReady() {
            synchronized (readyLock) {
                ready = true;
                readyLock.notifyAll();
            }
        }
    }
}

// Expected output (the [responder]/[requester] lines interleave — the two roles
// run concurrently):
//
// Broker:  amqp://localhost:5672
// Address: commands/amqp10.examples.commands  (KubeMQ pattern=commands, channel=amqp10.examples.commands)
//
// [responder] Listening on commands/amqp10.examples.commands (reply producer ready)
// [requester] Dynamic reply node (temp queue): <server-assigned temp queue name>
// [requester] Sent command "reboot-node-7" (reply-to=temp queue, correlation-id=corr-cmd-1)
// [responder] Received command "reboot-node-7" (correlation-id=<RequestID>)
// [responder] Replied to "reboot-node-7" (executed=true, error="")
// [requester] Reply for "reboot-node-7" (correlation-id=corr-cmd-1): executed=true error="" body=""
// [requester] Sent command "fail" (reply-to=temp queue, correlation-id=corr-cmd-2)
// [responder] Received command "fail" (correlation-id=<RequestID>)
// [responder] Replied to "fail" (executed=false, error="command rejected by handler")
// [requester] Reply for "fail" (correlation-id=corr-cmd-2): executed=false error="command rejected by handler" body=""
//
// Done.
//
// (The responder sees the connector-stamped RequestID as the delivered request's
// correlation-id, while the requester's reply correlation-id is its ORIGINAL
// corr-cmd-N — the connector echoes the requester's correlation-id back on the
// reply. A COMMAND response carries the executed/error outcome, NOT a body — the
// requester observes an empty command reply body; use a QUERY to return a value.)
