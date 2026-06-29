"""Example: queues/basic_send_receive (master-table variant #1).

At-least-once produce + credit-based consume against the KubeMQ AMQP 1.0
connector using the native ``python-qpid-proton`` blocking client (NO KubeMQ SDK).

Flow:
  * Sender -> "queues/<ch>" (unsettled): each ``send`` blocks until the server's
    receiver DISPOSITION (accepted) before returning -- the broker has stored the
    message.
  * Receiver <- "queues/<ch>" with ``credit=10``: ``receive`` + ``accept`` each ⇒
    the connector emits an AckRange and removes the message from the queue.
  * After draining, the queue is empty (a further ``receive`` times out).

Grounded in connector test TestQueueProduceConsumeAtLeastOnce
(connectors/amqp10/integration_test.go).

Run::

    export KUBEMQ_AMQP_URL=amqp://localhost:5672
    uv run python queues/basic_send_receive/main.py
"""

from __future__ import annotations

import os

from proton import Message
from proton.utils import BlockingConnection

# channel is the KubeMQ queue channel; the link address is "queues/" + channel
# (explicit prefix -- never rely on a default pattern).
CHANNEL = "amqp10.examples.basic"

TOTAL = 10


def amqp_url() -> str:
    return os.environ.get("KUBEMQ_AMQP_URL", "amqp://localhost:5672")


def main() -> None:
    addr = "queues/" + CHANNEL
    print(f"Broker:  {amqp_url()}")
    print(f"Address: {addr}  (KubeMQ pattern=queues, channel={CHANNEL})\n")

    # OPEN: connect (SASL ANONYMOUS by default -- no userinfo in the URL). proton
    # sends a non-empty container-id automatically.
    conn = BlockingConnection(amqp_url())
    try:
        # =====================================================================
        # 1. Produce -- ATTACH a sender (server-receiver link). The server grants
        #    credit on attach; each send is unsettled and blocks until the server
        #    DISPOSITION (accepted) confirms the broker stored the message.
        # =====================================================================
        sender = conn.create_sender(addr)
        for i in range(TOTAL):
            sender.send(Message(body=f"msg-{i:03d}"))
        sender.close()
        print(f"[send] Produced {TOTAL} messages to {addr} (accepted DISPOSITION each)")

        # =====================================================================
        # 2. Consume -- ATTACH a receiver (server-sender link). The CLIENT grants
        #    credit (credit=10). Receive each message and accept it ⇒ the
        #    connector AckRanges it (removed from the queue). proton replenishes
        #    credit as deliveries settle.
        # =====================================================================
        receiver = conn.create_receiver(addr, credit=10)
        seen: set[str] = set()
        while len(seen) < TOTAL:
            msg = receiver.receive(timeout=30.0)
            receiver.accept()
            seen.add(str(msg.body))
        print(f"[recv] Consumed and accepted {len(seen)} messages (no loss)")

        # =====================================================================
        # 3. Assert the queue is empty -- a further receive must time out.
        # =====================================================================
        try:
            receiver.receive(timeout=2.0)
        except Exception:  # noqa: BLE001 - the EXPECTED idle timeout on an empty queue
            pass
        else:
            raise SystemExit("expected an empty queue, but received another message")
        print("[recv] Queue drained to empty (no further messages)")

        receiver.close()
    finally:
        conn.close()

    print("\nDone.")


if __name__ == "__main__":
    main()

# Expected output:
#
# Broker:  amqp://localhost:5672
# Address: queues/amqp10.examples.basic  (KubeMQ pattern=queues, channel=amqp10.examples.basic)
#
# [send] Produced 10 messages to queues/amqp10.examples.basic (accepted DISPOSITION each)
# [recv] Consumed and accepted 10 messages (no loss)
# [recv] Queue drained to empty (no further messages)
#
# Done.
