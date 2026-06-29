"""Example: events_store/durable_replay (master-table variant #7).

Durable subscriptions with resume over KubeMQ **Events Store** using the native
``python-qpid-proton`` blocking client (NO KubeMQ SDK).

Unlike Events (fire-and-forget, no replay), Events Store PERSISTS the stream and
lets a DURABLE subscriber resume where it left off. A durable subscription is
identified by the pair::

    (connection container-id, link name)

To make a subscriber durable and resumable:

  * dial with a STABLE container-id  -> ``Container(...).container_id = "..."``
  * attach with a STABLE link name   -> ``create_receiver(name="durable-sub")``
  * request a non-expiring source    -> ``source.expiry_policy = Terminus.EXPIRE_NEVER``
  * set the start position once       -> link property ``x-opt-kubemq-start: new-only``

On a clean disconnect the connector preserves the durable position. Re-attaching
with the SAME (container-id, link name) RESUMES the subscription and delivers
every event published while the subscriber was away -- no loss, no replay of
already-consumed events.

Flow:
  1. Dial with a stable container-id; attach durable receiver "durable-sub"
     (start new-only). Publish 3 events; receive all 3.
  2. Disconnect (close the connection).
  3. Publish 5 MORE events while the durable subscriber is away.
  4. Re-dial with the SAME container-id; re-attach "durable-sub". The
     subscription RESUMES and delivers the 5 missed events.

Grounded in connector test TestEventsStoreDurableReplay
(connectors/amqp10/integration_pubsub_test.go).

Run::

    export KUBEMQ_AMQP_URL=amqp://localhost:5672
    uv run python events_store/durable_replay/main.py
"""

from __future__ import annotations

import os
import time

from proton import Message, Terminus, symbol
from proton.reactor import Container, ReceiverOption
from proton.utils import BlockingConnection

CHANNEL = "amqp10.examples.durable"

# The durable identity = (container-id, link-name). Both MUST be stable across
# reconnects for the subscription to resume.
CONTAINER_ID = "amqp10-examples-durable-container"
LINK_NAME = "durable-sub"

START_PROP = "x-opt-kubemq-start"
STANDING_CREDIT = 100


def amqp_url() -> str:
    return os.environ.get("KUBEMQ_AMQP_URL", "amqp://localhost:5672")


def accept_if_unsettled(receiver) -> None:  # noqa: ANN001 - proton BlockingReceiver
    """Accept the just-received delivery, but ONLY if it is unsettled.

    Events Store fan-out deliveries are pre-settled by the connector (like Events),
    so there is no delivery to settle -- calling ``receiver.accept()`` on a settled
    delivery raises ``IndexError`` (proton only tracks unsettled deliveries). This
    helper makes accept a true no-op on pre-settled deliveries, matching go-amqp's
    AcceptMessage which is harmless on settled deliveries.
    """
    if receiver.fetcher.unsettled:
        receiver.accept()


class DurableSource(ReceiverOption):
    """Receiver option that makes the source durable + non-expiring and stamps
    the ``x-opt-kubemq-start`` link property.

    The connector reads ``x-opt-kubemq-start`` from the ATTACH-frame *link*
    properties (connectors/amqp10/link.go ``applyPubSubProperties``) and treats a
    terminus expiry-policy of "never" as the durable signal
    (``Source.ExpiryPolicy == ExpiryNever``).
    """

    def __init__(self, start: str) -> None:
        self.start = start

    def apply(self, receiver) -> None:  # noqa: ANN001 - proton Receiver
        # Non-expiring durable source: half of the durable contract.
        receiver.source.durability = Terminus.DELIVERIES
        receiver.source.expiry_policy = Terminus.EXPIRE_NEVER
        # The start cursor is a LINK property (honoured on first attach).
        receiver.properties = {symbol(START_PROP): self.start}


def attach_durable(phase: str) -> tuple[BlockingConnection, object]:
    """Dial with the stable container-id and attach the durable receiver (stable
    link name + non-expiring source + start position). Returns the connection
    (close it to disconnect) and the blocking receiver.
    """
    container = Container()
    container.container_id = CONTAINER_ID  # half of the durable identity
    conn = BlockingConnection(amqp_url(), container=container)
    receiver = conn.create_receiver(
        "events-store/" + CHANNEL,
        credit=STANDING_CREDIT,
        name=LINK_NAME,  # stable link name = the other half of the identity
        options=DurableSource("new-only"),
    )
    print(f'[recv] Durable receiver attached ({phase}): container-id="{CONTAINER_ID}" name="{LINK_NAME}" expiry=never')
    # Let the connector's subscription pump go live before producing.
    time.sleep(0.75)
    return conn, receiver


def publish(sender, lo: int, hi: int) -> None:  # noqa: ANN001 - proton sender
    """Send events es-<lo>..es-<hi-1> (unsettled -- events-store persists each
    accepted transfer).
    """
    for i in range(lo, hi):
        sender.send(Message(body=f"es-{i:03d}"))


def drain(receiver, want: int, window: float) -> list[str]:  # noqa: ANN001
    """Receive up to ``want`` events within ``window`` seconds, returning bodies."""
    out: list[str] = []
    deadline = time.monotonic() + window
    while len(out) < want and time.monotonic() < deadline:
        try:
            msg = receiver.receive(timeout=max(0.0, deadline - time.monotonic()))
        except Exception:  # noqa: BLE001 - timeout surfaces as Timeout exception
            break
        accept_if_unsettled(receiver)  # no-op for pre-settled events-store deliveries
        out.append(str(msg.body))
    return out


def main() -> None:
    addr = "events-store/" + CHANNEL
    print(f"Broker:        {amqp_url()}")
    print(f"Address:       {addr}  (KubeMQ pattern=events-store, channel={CHANNEL})")
    print(f'Durable id:    container-id="{CONTAINER_ID}"  link-name="{LINK_NAME}"\n')

    # PRODUCER -- a separate, plain connection that publishes throughout the demo
    # (it does not need a stable id).
    prod_conn = BlockingConnection(amqp_url())
    sender = prod_conn.create_sender(addr)

    # 1. DURABLE SUBSCRIBE (first attach). Stable container-id + link name +
    #    non-expiring source make this subscription durable. start=new-only means
    #    "deliver events from now on" (this attach establishes the cursor).
    dur_conn, dur_rcv = attach_durable("first attach")
    publish(sender, 0, 3)  # 3 events while the durable subscriber is live

    first = drain(dur_rcv, 3, 30.0)
    if len(first) != 3:
        raise SystemExit(f"durable subscriber expected the first 3 events, got {first}")
    print(f"[recv] First attach received {len(first)} events: {first}\n")

    # 2. DISCONNECT. A clean close detaches the durable link; the connector
    #    preserves the durable cursor for this (container-id, link name).
    dur_conn.close()
    print("[conn] Durable subscriber DISCONNECTED (cursor preserved by the connector)")
    time.sleep(1.0)  # let the detach + unsubscribe settle

    # 3. PUBLISH WHILE AWAY. 5 more events arrive at the persisted stream while
    #    the durable subscriber is offline.
    publish(sender, 3, 8)
    print("[send] Published 5 more events WHILE the durable subscriber was away")

    # 4. RE-ATTACH with the SAME durable identity. The subscription RESUMES and
    #    delivers exactly the 5 events published while away.
    dur_conn2, dur_rcv2 = attach_durable("re-attach")
    resumed = drain(dur_rcv2, 5, 30.0)
    resumed_set = set(resumed)
    for i in range(3, 8):
        body = f"es-{i:03d}"
        if body not in resumed_set:
            raise SystemExit(f"durable resume missing event {body} (got {resumed})")
    print(f"[recv] Re-attach RESUMED and received the {len(resumed_set)} events published while away: {resumed}")
    print("[recv] No loss, no re-delivery of the already-consumed first 3 -- the durable cursor resumed exactly")

    dur_conn2.close()
    sender.close()
    prod_conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()

# Expected output:
#
# Broker:        amqp://localhost:5672
# Address:       events-store/amqp10.examples.durable  (KubeMQ pattern=events-store, channel=amqp10.examples.durable)
# Durable id:    container-id="amqp10-examples-durable-container"  link-name="durable-sub"
#
# [recv] Durable receiver attached (first attach): container-id="amqp10-examples-durable-container" name="durable-sub" expiry=never
# [recv] First attach received 3 events: ['es-000', 'es-001', 'es-002']
#
# [conn] Durable subscriber DISCONNECTED (cursor preserved by the connector)
# [send] Published 5 more events WHILE the durable subscriber was away
# [recv] Durable receiver attached (re-attach): container-id="amqp10-examples-durable-container" name="durable-sub" expiry=never
# [recv] Re-attach RESUMED and received the 5 events published while away: ['es-003', 'es-004', 'es-005', 'es-006', 'es-007']
# [recv] No loss, no re-delivery of the already-consumed first 3 -- the durable cursor resumed exactly
#
# Done.
#
# NOTE: durable subscriptions are NODE-LOCAL (see README gotcha). In a cluster the
# durable cursor lives on the node that owned the original attach; reconnect to the
# SAME node (or run a single-node dev broker, as here) to resume.
