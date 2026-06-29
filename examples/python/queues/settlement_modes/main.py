"""Example: queues/settlement_modes (master-table variant #3).

The two producer reliability tiers, side by side, against the KubeMQ AMQP 1.0
connector using the native ``python-qpid-proton`` blocking client (NO KubeMQ SDK):

  * PRE-SETTLED sender (``AtMostOnce`` -> snd-settle-mode=settled): at-MOST-once.
    proton marks every TRANSFER settled, so ``send`` returns WITHOUT waiting for a
    server DISPOSITION. Fast and fire-and-forget -- if the broker drops the
    transfer (oversize, no capacity), the producer never learns. There is no
    redelivery and no delivery confirmation.
  * UNSETTLED sender (default): at-LEAST-once. Each ``send`` blocks until the
    connector returns an ``accepted`` DISPOSITION, confirming the broker stored the
    message. This is the variant #1 contract.

On the consume side this example uses the default rcv-settle-mode=first (the ONLY
receiver settle-mode the connector supports): the server settles the delivery on
the first transfer. rcv-settle-mode=second is rejected by the connector with a
DETACH carrying amqp:not-implemented (gotcha #7 -- see the README).

Both senders' messages drain to the same consumer; the program proves no loss on
this happy path while explaining the reliability difference.

Grounded in connector test TestQueuePreSettled
(connectors/amqp10/integration_test.go).

Run::

    export KUBEMQ_AMQP_URL=amqp://localhost:5672
    uv run python queues/settlement_modes/main.py
"""

from __future__ import annotations

import os

from proton import Message
from proton.reactor import AtMostOnce
from proton.utils import BlockingConnection

CHANNEL = "amqp10.examples.settlement"

# We produce this many messages on each sender (pre-settled, then unsettled).
PER_SENDER = 10


def amqp_url() -> str:
    return os.environ.get("KUBEMQ_AMQP_URL", "amqp://localhost:5672")


def main() -> None:
    addr = "queues/" + CHANNEL
    print(f"Broker:  {amqp_url()}")
    print(f"Address: {addr}\n")

    conn = BlockingConnection(amqp_url())
    try:
        # =====================================================================
        # 1. PRE-SETTLED sender (at-most-once). AtMostOnce sets snd-settle-mode=
        #    settled, so each send is marked settled locally and returns as soon
        #    as the frame is written -- NO server DISPOSITION is awaited. Fast,
        #    but no delivery confirmation and no redelivery.
        # =====================================================================
        settled_sender = conn.create_sender(addr, options=AtMostOnce())
        for i in range(PER_SENDER):
            settled_sender.send(Message(body=f"presettled-{i:02d}"))
        settled_sender.close()
        print(f"[send] Pre-settled (at-most-once): produced {PER_SENDER} messages -- NO DISPOSITION awaited")

        # =====================================================================
        # 2. UNSETTLED sender (at-least-once -- the default). Each send blocks
        #    until the connector returns an ``accepted`` DISPOSITION confirming the
        #    broker stored the message. This is the variant #1 reliability contract.
        # =====================================================================
        unsettled_sender = conn.create_sender(addr)
        for i in range(PER_SENDER):
            unsettled_sender.send(Message(body=f"unsettled-{i:02d}"))
        unsettled_sender.close()
        print(f"[send] Unsettled (at-least-once): produced {PER_SENDER} messages -- each accepted DISPOSITION")

        # =====================================================================
        # 3. Consume with the default rcv-settle-mode=first (the only mode the
        #    connector supports; rcv-settle-mode=second ⇒ DETACH amqp:not-implemented,
        #    see the README gotcha). Accept each message to drain the queue.
        # =====================================================================
        receiver = conn.create_receiver(addr, credit=20)
        total = 2 * PER_SENDER
        presettled_seen = 0
        unsettled_seen = 0
        seen: set[str] = set()
        while len(seen) < total:
            msg = receiver.receive(timeout=30.0)
            receiver.accept()
            body = str(msg.body)
            if body not in seen:
                seen.add(body)
                if body.startswith("presettled"):
                    presettled_seen += 1
                else:
                    unsettled_seen += 1
        print(
            f"[recv] Drained {len(seen)} total -- {presettled_seen} pre-settled + "
            f"{unsettled_seen} unsettled (rcv-settle-mode=first)"
        )

        # =====================================================================
        # 4. Assert the queue is empty -- a further receive must time out.
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
# Address: queues/amqp10.examples.settlement
#
# [send] Pre-settled (at-most-once): produced 10 messages -- NO DISPOSITION awaited
# [send] Unsettled (at-least-once): produced 10 messages -- each accepted DISPOSITION
# [recv] Drained 20 total -- 10 pre-settled + 10 unsettled (rcv-settle-mode=first)
# [recv] Queue drained to empty (no further messages)
#
# Done.
#
# (On a healthy broker pre-settled messages also drain -- the difference is the
# PRODUCER guarantee, not the happy-path result: a pre-settled send returns before
# any broker confirmation, so a drop on the way in is invisible to the producer.
# Unsettled sends block until the broker confirms storage.)
