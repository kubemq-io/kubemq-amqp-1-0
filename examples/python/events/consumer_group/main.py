"""Example: events/consumer_group (master-table variant #5).

Consumer-group load-balancing over KubeMQ **Events** with the native
``python-qpid-proton`` blocking client (NO KubeMQ SDK).

The ``x-opt-kubemq-group`` receiver link property places a subscriber in a named
load-balancing group. Within ONE group, the connector round-robins the event
stream across the group's members (no duplication). A DISTINCT group is an
independent virtual-topic subscriber that gets the FULL stream.

This example opens:
  * g1a, g1b -- two receivers in group "g1" -> together they receive every event
    with NO body delivered to both (the group splits the stream).
  * g2       -- one receiver in group "g2" -> gets EVERY event (independent).

The blocking client is single-threaded per connection, so each subscriber runs on
its OWN connection in its OWN thread (one session/receiver per thread).

Grounded in connector test TestEventsPubSubGroupFanout
(connectors/amqp10/integration_pubsub_test.go).

Run::

    export KUBEMQ_AMQP_URL=amqp://localhost:5672
    uv run python events/consumer_group/main.py
"""

from __future__ import annotations

import os
import threading
import time

from proton import Message, symbol
from proton.reactor import AtMostOnce, ReceiverOption
from proton.utils import BlockingConnection

CHANNEL = "amqp10.examples.consumergroup"

TOTAL = 30

GROUP_PROP = "x-opt-kubemq-group"
STANDING_CREDIT = 100

# Each subscriber drains its window within this many seconds.
WINDOW = 20.0


def amqp_url() -> str:
    return os.environ.get("KUBEMQ_AMQP_URL", "amqp://localhost:5672")


def accept_if_unsettled(receiver) -> None:  # noqa: ANN001 - proton BlockingReceiver
    """Accept the just-received delivery, but ONLY if it is unsettled.

    Events fan-out deliveries are pre-settled by the connector (at-most-once), so
    there is no delivery to settle -- calling ``receiver.accept()`` on a settled
    delivery raises ``IndexError`` (proton only tracks unsettled deliveries).
    """
    if receiver.fetcher.unsettled:
        receiver.accept()


class ConsumerGroup(ReceiverOption):
    """Receiver option that stamps the ``x-opt-kubemq-group`` ATTACH link property.

    The connector reads this from the ATTACH-frame *link* properties
    (connectors/amqp10/link.go ``applyPubSubProperties``).
    """

    def __init__(self, group: str) -> None:
        self.group = group

    def apply(self, receiver) -> None:  # noqa: ANN001 - proton Receiver
        receiver.properties = {symbol(GROUP_PROP): self.group}


class Subscriber:
    """One group receiver on its own connection + thread."""

    def __init__(self, label: str, group: str) -> None:
        self.label = label
        self.group = group
        self.got: set[str] = set()

    def run(self, addr: str, started: threading.Event) -> None:
        conn = BlockingConnection(amqp_url())
        try:
            receiver = conn.create_receiver(addr, credit=STANDING_CREDIT, options=ConsumerGroup(self.group))
            started.set()  # this subscriber's link is attached
            deadline = time.monotonic() + WINDOW
            while len(self.got) < TOTAL and time.monotonic() < deadline:
                try:
                    msg = receiver.receive(timeout=max(0.0, deadline - time.monotonic()))
                except Exception:  # noqa: BLE001 - window elapsed / no more messages
                    break
                accept_if_unsettled(receiver)  # no-op for pre-settled fan-out
                self.got.add(str(msg.body))
            receiver.close()
        finally:
            conn.close()


def main() -> None:
    addr = "events/" + CHANNEL
    print(f"Broker:  {amqp_url()}")
    print(f"Address: {addr}  (KubeMQ pattern=events, channel={CHANNEL})\n")

    # Three group subscribers, each on its own connection/thread.
    g1a = Subscriber("g1a", "g1")
    g1b = Subscriber("g1b", "g1")
    g2 = Subscriber("g2", "g2")
    subs = [g1a, g1b, g2]

    started = [threading.Event() for _ in subs]
    threads = [
        threading.Thread(target=s.run, args=(addr, ev), daemon=True) for s, ev in zip(subs, started, strict=True)
    ]
    for t in threads:
        t.start()

    # Wait until all three links are attached, then let the subscription pumps go
    # live (events have no replay -- a publish that races a subscription is lost).
    for ev in started:
        if not ev.wait(timeout=30.0):
            raise SystemExit("a subscriber did not attach in time")
    time.sleep(0.75)
    print("[recv] 3 subscribers attached: g1a+g1b (group g1), g2 (group g2)")

    # Publish on a dedicated connection. Pre-settled fire-and-forget.
    prod_conn = BlockingConnection(amqp_url())
    sender = prod_conn.create_sender(addr, options=AtMostOnce())
    for i in range(TOTAL):
        sender.send(Message(body=f"event-{i:03d}"))
    sender.close()
    prod_conn.close()
    print(f"[send] Published {TOTAL} events (pre-settled)")

    # Wait for the subscribers to drain their windows.
    for t in threads:
        t.join(timeout=WINDOW + 10.0)

    # --- Assert the consumer-group semantics ---------------------------------

    # g2 (a distinct group) receives EVERY event.
    if len(g2.got) != TOTAL:
        raise SystemExit(f"group g2 (independent) expected all {TOTAL} events, got {len(g2.got)}")
    print(f"[recv] g2 (group g2, independent): {len(g2.got)}/{TOTAL} events -- FULL stream")

    # g1a + g1b TOGETHER receive every event, with NO body delivered to both.
    dups = g1a.got & g1b.got
    if dups:
        raise SystemExit(f"group g1 load-balancing broken: {len(dups)} event(s) delivered to BOTH g1a and g1b")
    combined = g1a.got | g1b.got
    if len(combined) != TOTAL:
        raise SystemExit(f"group g1 members together expected all {TOTAL} events, got {len(combined)}")
    if not g1a.got or not g1b.got:
        raise SystemExit(f"group g1 not load-balanced: g1a={len(g1a.got)} g1b={len(g1b.got)} (one member got nothing)")
    print(f"[recv] g1a (group g1): {len(g1a.got)} events; g1b (group g1): {len(g1b.got)} events")
    print(f"[recv] g1a+g1b together: {len(combined)}/{TOTAL} events, 0 duplicates -- group SPLIT the stream")

    print("\nDone.")


if __name__ == "__main__":
    main()

# Expected output (the g1a/g1b split varies run to run; the totals are fixed):
#
# Broker:  amqp://localhost:5672
# Address: events/amqp10.examples.consumergroup  (KubeMQ pattern=events, channel=amqp10.examples.consumergroup)
#
# [recv] 3 subscribers attached: g1a+g1b (group g1), g2 (group g2)
# [send] Published 30 events (pre-settled)
# [recv] g2 (group g2, independent): 30/30 events -- FULL stream
# [recv] g1a (group g1): 16 events; g1b (group g1): 14 events
# [recv] g1a+g1b together: 30/30 events, 0 duplicates -- group SPLIT the stream
#
# Done.
