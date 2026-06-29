"""Example: events/basic_pubsub (master-table variant #4).

Fan-out, at-most-once pub/sub over KubeMQ **Events** with the native
``python-qpid-proton`` blocking client (NO KubeMQ SDK).

Events are a fire-hose: deliveries are pre-settled (no DISPOSITION feedback),
there is NO replay, and a message that arrives at a subscriber with zero credit is
SILENTLY DROPPED (counted by the server metric
kubemq_amqp10_events_dropped_no_credit_total). Two rules follow:

  * SUBSCRIBE BEFORE PUBLISH. The attach reply only confirms the link, not that the
    connector's subscription pump is live. A publish that races the subscription is
    lost (no replay). This example sleeps ~750ms after attach before producing.
  * GRANT STANDING CREDIT. The receiver attaches with a large standing credit (and
    proton replenishes as messages settle) so the subscriber is never at 0 credit
    when an event arrives.

The sender publishes pre-settled (``AtMostOnce``) to events/<ch>
(fire-and-forget); the receiver drains every event on the happy path.

Grounded in connector test TestEventsPubSubGroupFanout (the lone-subscriber
fan-out leg) (connectors/amqp10/integration_pubsub_test.go).

Run::

    export KUBEMQ_AMQP_URL=amqp://localhost:5672
    uv run python events/basic_pubsub/main.py
"""

from __future__ import annotations

import os
import time

from proton import Message
from proton.reactor import AtMostOnce
from proton.utils import BlockingConnection

CHANNEL = "amqp10.examples.pubsub"

TOTAL = 20

# standing_credit is granted up front so the subscriber is never at 0 credit when
# an event arrives. proton auto-replenishes as deliveries settle.
STANDING_CREDIT = 100


def amqp_url() -> str:
    return os.environ.get("KUBEMQ_AMQP_URL", "amqp://localhost:5672")


def accept_if_unsettled(receiver) -> None:  # noqa: ANN001 - proton BlockingReceiver
    """Accept the just-received delivery, but ONLY if it is unsettled.

    Events fan-out deliveries are pre-settled by the connector (at-most-once), so
    there is no delivery to settle -- calling ``receiver.accept()`` on a settled
    delivery raises ``IndexError`` (proton only tracks unsettled deliveries). This
    helper makes accept a true no-op on pre-settled pub/sub.
    """
    if receiver.fetcher.unsettled:
        receiver.accept()


def main() -> None:
    addr = "events/" + CHANNEL
    print(f"Broker:  {amqp_url()}")
    print(f"Address: {addr}  (KubeMQ pattern=events, channel={CHANNEL})\n")

    conn = BlockingConnection(amqp_url())
    try:
        # =====================================================================
        # 1. SUBSCRIBE FIRST. Attach the receiver with standing credit BEFORE any
        #    publish. Events have no replay -- a publish that beats the subscription
        #    is lost forever.
        # =====================================================================
        receiver = conn.create_receiver(addr, credit=STANDING_CREDIT)
        print(f"[recv] Subscribed to {addr} with standing credit {STANDING_CREDIT}")

        # The attach reply confirms the link, not that the connector's subscription
        # pump has run its SubscribeEvents yet. Wait for the pump to go live before
        # publishing, or the first events race the subscription and are dropped.
        time.sleep(0.75)
        print("[recv] Subscription pump settled (waited 750ms before publishing)")

        # =====================================================================
        # 2. PUBLISH pre-settled. AtMostOnce marks every TRANSFER settled
        #    (fire-and-forget) -- events are at-most-once, so there is no
        #    DISPOSITION to await and no produce confirmation.
        # =====================================================================
        sender = conn.create_sender(addr, options=AtMostOnce())
        for i in range(TOTAL):
            sender.send(Message(body=f"event-{i:03d}"))
        sender.close()
        print(f"[send] Published {TOTAL} events (pre-settled, fire-and-forget)")

        # =====================================================================
        # 3. RECEIVE. With standing credit the subscriber drains every event.
        #    accept is a no-op on pre-settled pub/sub deliveries but is harmless.
        # =====================================================================
        seen: set[str] = set()
        while len(seen) < TOTAL:
            msg = receiver.receive(timeout=30.0)
            accept_if_unsettled(receiver)  # no-op for pre-settled fan-out
            seen.add(str(msg.body))
        print(f"[recv] Received all {len(seen)} events (continuous credit ⇒ no 0-credit drop)")

        receiver.close()
    finally:
        conn.close()

    print("\nDone.")


if __name__ == "__main__":
    main()

# Expected output:
#
# Broker:  amqp://localhost:5672
# Address: events/amqp10.examples.pubsub  (KubeMQ pattern=events, channel=amqp10.examples.pubsub)
#
# [recv] Subscribed to events/amqp10.examples.pubsub with standing credit 100
# [recv] Subscription pump settled (waited 750ms before publishing)
# [send] Published 20 events (pre-settled, fire-and-forget)
# [recv] Received all 20 events (continuous credit ⇒ no 0-credit drop)
#
# Done.
#
# (Events are at-most-once with no replay: if the subscriber were at 0 credit when
# an event arrived, that event would be SILENTLY DROPPED and counted on the server
# metric kubemq_amqp10_events_dropped_no_credit_total -- never surfaced as a client
# error. Standing credit + subscribe-before-publish avoid both losses.)
