// Example: queries/request-reply (master-table variant #10)
//
// JMS request/reply over KubeMQ **Queries** (RPC) with Apache Qpid JMS
// (javax.jms) — NO KubeMQ SDK, NO gRPC. The whole round-trip stays in-protocol
// over a single broker connection per role.
//
// The reply path is IDENTICAL to commands (variant #9): the requester opens a
// JMS TEMPORARY QUEUE as its dynamic reply node (session.createTemporaryQueue()),
// sends to queries/<ch> with setJMSReplyTo(tempQueue) + setJMSCorrelationID(id);
// the responder consumes queries/<ch> and replies with a producer addressed to
// the request's getJMSReplyTo(), echoing the JMSCorrelationID.
//
// The CONTRAST with commands (the whole point of variant #10):
//
//   - A query reply carries ONLY the body + metadata — NO x-opt-kubemq-executed /
//     x-opt-kubemq-error application-properties. A query is a "fetch a value"
//     call; there is no executed/error envelope (so this example does NOT need
//     jms.validatePropertyNames=false at all).
//   - A FAILED query delivers NOTHING. The connector delivers no reply when a
//     query fails, times out, or the responder ignores it, so the requester
//     simply TIMES OUT. (A failed command, by contrast, always replies
//     executed=false so its requester is never left waiting.)
//
// This example demonstrates BOTH: a successful query (reply round-trips, body
// intact) and a query the responder ignores (no reply ⇒ the requester times out
// on a short demo deadline; in production the connector default is ~30s).
//
// Grounded in connector test TestRPCRequesterResponderViaAMQP10 (queries leg)
// (connectors/amqp10/integration_test.go).
//
// Run:
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   mvn -pl queries/request-reply exec:java
package io.kubemq.examples.amqp10.queries.requestreply;

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

    // The KubeMQ queries channel; the JMS Queue name is the explicit connector
    // address "queries/" + channel (never rely on a default pattern).
    private static final String CHANNEL = "amqp10.examples.queries";

    // Short per-request deadline so the "no reply" leg surfaces a timeout quickly.
    // The connector's own default RPC timeout is ~30s; in production set the
    // request's TTL to choose the per-request budget.
    private static final long DEMO_TIMEOUT_MILLIS = 5_000;

    private Main() {
    }

    static String amqpUrl() {
        String v = System.getenv("KUBEMQ_AMQP_URL");
        return (v != null && !v.isEmpty()) ? v : "amqp://localhost:5672";
    }

    public static void main(String[] args) throws JMSException, InterruptedException {
        String url = amqpUrl();
        String address = "queries/" + CHANNEL;
        System.out.printf("Broker:  %s%n", url);
        System.out.printf("Address: %s  (KubeMQ pattern=queries, channel=%s)%n%n", address, CHANNEL);

        JmsConnectionFactory factory = new JmsConnectionFactory(url);

        // The responder runs on its own thread + connection so this one program is
        // runnable standalone. It signals readiness so the first query does not race
        // the responder's attach.
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
    // REQUESTER — temporary-queue reply node + producer on queries/<ch>;
    // correlates replies by JMSCorrelationID.
    // =========================================================================
    private static void runRequester(JmsConnectionFactory factory, String address) throws JMSException {
        try (Connection connection = factory.createConnection()) {
            connection.start();
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                // DYNAMIC reply node: a JMS temporary queue is a server-assigned,
                // connection-owned transient node — the requester's reply mailbox.
                TemporaryQueue replyNode = session.createTemporaryQueue();
                System.out.printf("[requester] Dynamic reply node (temp queue): %s%n", replyNode.getQueueName());

                Queue queries = session.createQueue(address);
                try (MessageConsumer replyConsumer = session.createConsumer(replyNode);
                        MessageProducer producer = session.createProducer(queries)) {

                    // 1. A SUCCESSFUL query: round-trips, body intact, no
                    //    executed/error props.
                    doQuery(session, producer, replyConsumer, replyNode, "get-temp-sensor-3", "corr-qry-1");

                    // 2. A query the responder ignores: NOTHING is delivered, so the
                    //    requester TIMES OUT. This is the core Queries contrast — a
                    //    failed/unanswered query has no error envelope; the absence
                    //    of a reply IS the failure signal.
                    doQueryExpectTimeout(session, producer, replyConsumer, replyNode, "ignore", "corr-qry-2");
                }
            }
        }
    }

    // doQuery sends one query naming the temp-queue reply node + a correlation-id,
    // then awaits the correlated reply and prints the result body.
    private static void doQuery(Session session, MessageProducer producer, MessageConsumer replyConsumer,
            TemporaryQueue replyNode, String body, String corr) throws JMSException {
        sendQuery(session, producer, replyNode, body, corr);

        Message reply = replyConsumer.receive(DEMO_TIMEOUT_MILLIS);
        if (reply == null) {
            throw new IllegalStateException("timed out awaiting reply for \"" + body + "\"");
        }
        String gotCorr = reply.getJMSCorrelationID();
        if (!corr.equals(gotCorr)) {
            throw new IllegalStateException(
                    "correlation-id mismatch: want \"" + corr + "\" got \"" + gotCorr + "\"");
        }
        String replyBody = (reply instanceof TextMessage) ? ((TextMessage) reply).getText() : "";
        System.out.printf("[requester] Reply for \"%s\" (correlation-id=%s): body=\"%s\"%n", body, gotCorr, replyBody);
    }

    // doQueryExpectTimeout sends a query the responder will ignore and shows the
    // requester timing out (no reply is the failure signal for queries).
    private static void doQueryExpectTimeout(Session session, MessageProducer producer, MessageConsumer replyConsumer,
            TemporaryQueue replyNode, String body, String corr) throws JMSException {
        sendQuery(session, producer, replyNode, body, corr);

        Message reply = replyConsumer.receive(DEMO_TIMEOUT_MILLIS);
        if (reply != null) {
            throw new IllegalStateException("expected NO reply for \"" + body + "\", but one arrived");
        }
        // A null receive (deadline elapsed) here is the EXPECTED outcome for an
        // unanswered query.
        System.out.printf(
                "[requester] No reply for \"%s\" within %dms — query timed out (expected; failed queries deliver nothing)%n",
                body, DEMO_TIMEOUT_MILLIS);
    }

    // sendQuery sends one query message naming the temp-queue reply node +
    // correlation-id.
    private static void sendQuery(Session session, MessageProducer producer, TemporaryQueue replyNode, String body,
            String corr) throws JMSException {
        TextMessage req = session.createTextMessage(body);
        req.setJMSReplyTo(replyNode); // MUST name a node this connection owns (snooping guard)
        req.setJMSCorrelationID(corr);
        producer.send(req);
        System.out.printf("[requester] Sent query \"%s\" (reply-to=temp queue, correlation-id=%s)%n", body, corr);
    }

    // =========================================================================
    // RESPONDER — consumes queries/<ch>, replies with body + metadata only.
    // A query whose body is "ignore" gets NO reply (so its requester times out).
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
                    Queue queries = session.createQueue(address);
                    // Unidentified reply producer: each reply is addressed at send
                    // time to the request's JMSReplyTo, so one producer serves every
                    // requester's temp queue.
                    try (MessageConsumer consumer = session.createConsumer(queries);
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
            System.out.printf("[responder] Received query \"%s\" (correlation-id=%s)%n",
                    body, req.getJMSCorrelationID());

            // Business logic: a query body of "ignore" is dropped on the floor — the
            // responder sends NOTHING. The requester will time out. (A real responder
            // would only fail to reply on a crash / unreachable backend; "ignore"
            // makes the contrast deterministic for the demo.)
            if ("ignore".equals(body)) {
                System.out.printf("[responder] Ignoring \"%s\" — NO reply sent (requester will time out)%n", body);
                return;
            }

            // A QUERY reply carries ONLY the body + metadata — NO executed/error
            // application-properties (the Commands-vs-Queries contrast).
            TextMessage reply = session.createTextMessage("result:" + body);
            reply.setJMSCorrelationID(req.getJMSCorrelationID()); // echo so the requester can match
            replyProducer.send(replyTo, reply);
            System.out.printf("[responder] Replied to \"%s\" (body + metadata only, no executed/error props)%n", body);
        }

        private void signalReady() {
            synchronized (readyLock) {
                ready = true;
                readyLock.notifyAll();
            }
        }
    }
}

// Expected output (the [responder]/[requester] lines interleave; the second leg
// blocks for the 5s demo timeout before printing the timeout line):
//
// Broker:  amqp://localhost:5672
// Address: queries/amqp10.examples.queries  (KubeMQ pattern=queries, channel=amqp10.examples.queries)
//
// [responder] Listening on queries/amqp10.examples.queries (reply producer ready)
// [requester] Dynamic reply node (temp queue): <server-assigned temp queue name>
// [requester] Sent query "get-temp-sensor-3" (reply-to=temp queue, correlation-id=corr-qry-1)
// [responder] Received query "get-temp-sensor-3" (correlation-id=corr-qry-1)
// [responder] Replied to "get-temp-sensor-3" (body + metadata only, no executed/error props)
// [requester] Reply for "get-temp-sensor-3" (correlation-id=corr-qry-1): body="result:get-temp-sensor-3"
// [requester] Sent query "ignore" (reply-to=temp queue, correlation-id=corr-qry-2)
// [responder] Received query "ignore" (correlation-id=corr-qry-2)
// [responder] Ignoring "ignore" — NO reply sent (requester will time out)
// [requester] No reply for "ignore" within 5000ms — query timed out (expected; failed queries deliver nothing)
//
// Done.
//
// (Unlike a command — which always replies executed=false on failure so the
// requester is never left waiting — a query that fails/goes unanswered delivers
// NOTHING. The requester's timeout IS the failure signal. The connector's own
// default per-request timeout is ~30s.)
