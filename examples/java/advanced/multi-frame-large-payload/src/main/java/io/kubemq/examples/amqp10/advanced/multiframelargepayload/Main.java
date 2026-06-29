// Example: advanced/multi-frame-large-payload (master-table variant #11)
//
// A single AMQP 1.0 message whose body is larger than the connection's
// max-frame-size is fragmented across multiple TRANSFER frames (More:true …
// More:false) by the sender and reassembled bit-exact by the receiver — all
// transparently, with NO application-level chunking. This example drives that
// path against the KubeMQ AMQP 1.0 connector using Apache Qpid JMS (javax.jms) —
// NO KubeMQ SDK.
//
// JMS-over-AMQP-1.0 mapping:
//   - The connection's max-frame-size is tuned via the Qpid JMS URI option
//     amqp.maxFrameSize=4096 — a deliberately tiny 4 KiB frame so a ~1 MB body
//     forces heavy fragmentation in both directions. Both peers negotiate at OPEN;
//     the effective size is the minimum of the two.
//   - A BytesMessage carrying a ~1 MB Data body is sent in ONE producer.send to
//     the queues/<ch> Queue (unsettled). Qpid JMS splits it across many transfer
//     frames; the connector reassembles it and stores a single message.
//   - One consumer.receive yields the FULL reassembled body. The example verifies
//     the received length AND a CRC32 of the bytes match the original — proving a
//     bit-exact round-trip across the fragment boundary.
//
// Grounded in connector test TestQueueMultiFrameLargePayload
// (connectors/amqp10/integration_test.go).
//
// Run:
//   export KUBEMQ_AMQP_URL=amqp://localhost:5672
//   mvn -pl advanced/multi-frame-large-payload exec:java
package io.kubemq.examples.amqp10.advanced.multiframelargepayload;

import java.util.zip.CRC32;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.qpid.jms.JmsConnectionFactory;

public final class Main {

    // The KubeMQ queue channel; the JMS Queue name is the explicit connector
    // address "queues/" + channel (never rely on a default pattern).
    private static final String CHANNEL = "amqp10.examples.multiframe";

    // payloadSize is ~1 MB — comfortably larger than maxFrameSize so the body must
    // span many transfer frames (More:true … More:false).
    private static final int PAYLOAD_SIZE = 1 * 1024 * 1024;

    // maxFrameSize is a deliberately tiny 4 KiB so the ~1 MB body fragments across
    // ~256 frames in each direction.
    private static final int MAX_FRAME_SIZE = 4096;

    private Main() {
    }

    static String amqpUrl() {
        String v = System.getenv("KUBEMQ_AMQP_URL");
        return (v != null && !v.isEmpty()) ? v : "amqp://localhost:5672";
    }

    public static void main(String[] args) throws JMSException {
        String baseUrl = amqpUrl();
        // Tune the connection max-frame-size via the Qpid JMS URI option. The
        // connector advertises its own max-frame-size at OPEN; the effective value
        // is the minimum of the two.
        String tunedUrl = withMaxFrameSize(baseUrl, MAX_FRAME_SIZE);
        String address = "queues/" + CHANNEL;

        System.out.printf("Broker:        %s%n", baseUrl);
        System.out.printf("Address:       %s  (KubeMQ pattern=queues, channel=%s)%n", address, CHANNEL);
        System.out.printf("MaxFrameSize:  %d bytes%n", MAX_FRAME_SIZE);
        System.out.printf("Payload:       %d bytes (~%d KiB)%n%n", PAYLOAD_SIZE, PAYLOAD_SIZE / 1024);

        // Build a deterministic, non-trivial payload and remember its CRC + length
        // so we can prove a bit-exact round-trip after reassembly.
        byte[] payload = new byte[PAYLOAD_SIZE];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 251); // 251 is prime → no short repeating period
        }
        int wantLen = payload.length;
        long wantCrc = crc32(payload);
        System.out.printf("[prep] Built payload: len=%d crc32=0x%08x%n", wantLen, wantCrc);

        JmsConnectionFactory factory = new JmsConnectionFactory(tunedUrl);

        // =====================================================================
        // 1. PRODUCER connection — OPEN with the tiny max-frame-size. One
        //    producer.send carries the whole ~1 MB body; Qpid JMS transparently
        //    splits it across many transfer frames (More:true … final More:false).
        //    The connector reassembles them into a single stored message.
        // =====================================================================
        try (Connection prodConn = factory.createConnection();
                Session prodSession = prodConn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            prodConn.start();
            Queue queue = prodSession.createQueue(address);
            try (MessageProducer producer = prodSession.createProducer(queue)) {
                BytesMessage msg = prodSession.createBytesMessage();
                msg.writeBytes(payload);
                producer.send(msg);
            }
            System.out.printf("[send] Sent the %d-byte body in ONE send (fragmented across ~%d frames, accepted)%n",
                    wantLen, (wantLen / MAX_FRAME_SIZE) + 1);
        }

        // =====================================================================
        // 2. CONSUMER connection — same tiny max-frame-size so reassembly is
        //    exercised on the receive path too. One consumer.receive yields the
        //    FULL reassembled body.
        // =====================================================================
        try (Connection consConn = factory.createConnection();
                Session consSession = consConn.createSession(false, Session.CLIENT_ACKNOWLEDGE)) {
            consConn.start();
            Queue queue = consSession.createQueue(address);
            try (MessageConsumer consumer = consSession.createConsumer(queue)) {
                Message received = consumer.receive(60_000);
                if (received == null) {
                    throw new IllegalStateException("timed out waiting for the large message");
                }
                if (!(received instanceof BytesMessage)) {
                    throw new IllegalStateException("expected a BytesMessage, got " + received.getClass().getName());
                }
                BytesMessage bytes = (BytesMessage) received;
                int gotLen = (int) bytes.getBodyLength();
                byte[] got = new byte[gotLen];
                bytes.readBytes(got);
                received.acknowledge(); // accept ⇒ AckRange (removed from the queue)

                long gotCrc = crc32(got);
                System.out.printf("[recv] Reassembled body: len=%d crc32=0x%08x%n", gotLen, gotCrc);

                // =============================================================
                // 3. Verify the round-trip is bit-exact: length AND CRC32 must match.
                // =============================================================
                if (gotLen != wantLen) {
                    throw new IllegalStateException("length mismatch: sent " + wantLen + ", received " + gotLen);
                }
                if (gotCrc != wantCrc) {
                    throw new IllegalStateException(
                            String.format("CRC mismatch: sent 0x%08x, received 0x%08x", wantCrc, gotCrc));
                }
                System.out.println("[verify] Length and CRC32 match — multi-frame body round-tripped bit-exact");
            }
        }

        System.out.println("\nDone.");
    }

    // withMaxFrameSize appends the Qpid JMS amqp.maxFrameSize option to the broker
    // URL, preserving any existing query string.
    private static String withMaxFrameSize(String url, int frameSize) {
        String sep = url.contains("?") ? "&" : "?";
        return url + sep + "amqp.maxFrameSize=" + frameSize;
    }

    private static long crc32(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return crc.getValue();
    }
}

// Expected output:
//
// Broker:        amqp://localhost:5672
// Address:       queues/amqp10.examples.multiframe  (KubeMQ pattern=queues, channel=amqp10.examples.multiframe)
// MaxFrameSize:  4096 bytes
// Payload:       1048576 bytes (~1024 KiB)
//
// [prep] Built payload: len=1048576 crc32=0x........
// [send] Sent the 1048576-byte body in ONE send (fragmented across ~257 frames, accepted)
// [recv] Reassembled body: len=1048576 crc32=0x........
// [verify] Length and CRC32 match — multi-frame body round-tripped bit-exact
//
// Done.
