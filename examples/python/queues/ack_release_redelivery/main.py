"""Example: queues/ack_release_redelivery (master-table variant #2).

The three queue settlement outcomes, side by side, against the KubeMQ AMQP 1.0
connector using the native ``python-qpid-proton`` blocking client (NO KubeMQ SDK):

  * release (``receiver.release()``) ⇒ NAckRange: the message is requeued to the
    tail and REDELIVERED with a grown delivery-count (``msg.delivery_count >= 1``)
    and ``first_acquirer=False``. Each release also increments the broker
    receive-count toward MaxReceiveQueue (see the gotcha in the README).
  * reject  (``receiver.reject()``)  ⇒ AckRange/discard: the message is removed and
    NOT redelivered to this receiver (poison handling is a broker MaxReceiveQueue
    policy -- there is no connector DLX).
  * accept  (``receiver.accept()``)  ⇒ AckRange: the message is removed (success).

The connector maps the KubeMQ broker receive-count onto the AMQP header fields:
``delivery-count = ReceiveCount - 1`` and ``first-acquirer = (ReceiveCount == 1)``.

Grounded in connector tests TestQueueReleasedRedelivery and
TestQueueRejectedDiscard (connectors/amqp10/integration_test.go).

Run::

    export KUBEMQ_AMQP_URL=amqp://localhost:5672
    uv run python queues/ack_release_redelivery/main.py
"""

from __future__ import annotations

import os

from proton import Message
from proton.utils import BlockingConnection

CHANNEL = "amqp10.examples.ack"


def amqp_url() -> str:
    return os.environ.get("KUBEMQ_AMQP_URL", "amqp://localhost:5672")


def delivery_info(msg: Message) -> tuple[int, bool]:
    """Read the AMQP header delivery-count + first-acquirer the connector stamps."""
    return int(msg.delivery_count or 0), bool(msg.first_acquirer)


def main() -> None:
    addr = "queues/" + CHANNEL
    print(f"Broker:  {amqp_url()}")
    print(f"Address: {addr}\n")

    conn = BlockingConnection(amqp_url())
    try:
        # Produce three distinct messages: one we release, one we reject, one we accept.
        sender = conn.create_sender(addr)
        for body in ("release-me", "reject-me", "accept-me"):
            sender.send(Message(body=body))
        sender.close()
        print("[send] Produced: release-me, reject-me, accept-me")

        receiver = conn.create_receiver(addr, credit=10)

        # Track which terminal outcome we still owe each body. A released message is
        # redelivered, so "release-me" appears twice (released, then accepted).
        remaining = {"release-me", "reject-me", "accept-me"}
        released_once = False

        while remaining:
            msg = receiver.receive(timeout=30.0)
            body = str(msg.body)
            dc, first = delivery_info(msg)

            if body == "release-me" and not released_once:
                # First sight: RELEASE it back to the queue tail (NAckRange).
                # delivered=True marks the delivery as failed so the broker grows
                # the delivery-count on redelivery.
                receiver.release(delivered=True)
                released_once = True
                print(f"[recv] {body:<12} delivery-count={dc} first-acquirer={first}  -> RELEASED (requeued)")

            elif body == "release-me":
                # Redelivery: grown delivery-count, no longer first-acquirer. Accept now.
                if dc < 1 or first:
                    raise SystemExit(
                        f"expected redelivered copy to have delivery-count>=1 and "
                        f"first-acquirer=False, got dc={dc} first={first}"
                    )
                receiver.accept()
                print(f"[recv] {body:<12} delivery-count={dc} first-acquirer={first} -> REDELIVERED, then ACCEPTED")
                remaining.discard(body)

            elif body == "reject-me":
                # REJECT it (AckRange/discard). It will NOT be redelivered here.
                receiver.reject()
                print(
                    f"[recv] {body:<12} delivery-count={dc} first-acquirer={first}  -> REJECTED (discarded, no requeue)"
                )
                remaining.discard(body)

            else:  # "accept-me"
                receiver.accept()
                print(f"[recv] {body:<12} delivery-count={dc} first-acquirer={first}  -> ACCEPTED (removed)")
                remaining.discard(body)

        # The rejected body must NOT come back to this receiver.
        try:
            receiver.receive(timeout=2.0)
        except Exception:  # noqa: BLE001 - the EXPECTED idle timeout (nothing redelivered)
            pass
        else:
            raise SystemExit("rejected message was unexpectedly redelivered")
        print("[recv] Rejected message was not redelivered (discarded)")

        receiver.close()
    finally:
        conn.close()

    print("\nDone.")


if __name__ == "__main__":
    main()

# Expected output:
#
# Broker:  amqp://localhost:5672
# Address: queues/amqp10.examples.ack
#
# [send] Produced: release-me, reject-me, accept-me
# [recv] release-me   delivery-count=0 first-acquirer=True  -> RELEASED (requeued)
# [recv] reject-me    delivery-count=0 first-acquirer=True  -> REJECTED (discarded, no requeue)
# [recv] accept-me    delivery-count=0 first-acquirer=True  -> ACCEPTED (removed)
# [recv] release-me   delivery-count=1 first-acquirer=False -> REDELIVERED, then ACCEPTED
# [recv] Rejected message was not redelivered (discarded)
#
# Done.
#
# (Delivery order between the original and the redelivered copy can vary; the
# redelivered "release-me" always carries delivery-count>=1 / first-acquirer=False.
# The connector maps the broker receive-count onto the header: first-acquirer=True
# and delivery-count=0 on the FIRST delivery (ReceiveCount==1), then first-acquirer=
# False and delivery-count>=1 on each redelivery -- the reliable redelivery signal.)
