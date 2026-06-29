"""Example: advanced/anonymous_terminus (master-table variant #12).

An ANONYMOUS sender (a link attached with a NULL target -- ``create_sender(None)``)
carries no fixed channel. Instead, EACH message selects its own destination via
its ``Message.address`` (the AMQP ``properties.to`` field), and the KubeMQ
connector routes it per-message to the right pattern/channel. One link, many
destinations. Driven with the native ``python-qpid-proton`` blocking client (NO
KubeMQ SDK).

Flow:
  * ATTACH an anonymous sender: ``create_sender(None)`` -> null target.
  * Send #1: ``Message(address="queues/<ch>")`` routes to a queue.
  * Send #2: ``Message(address="events/<ch>")`` routes to an events topic (a
    subscriber is attached BEFORE the send -- events are fire-and-forget).
  * The queue message is then consumed back to prove it landed correctly.
  * (Demonstrated as expected errors) a BAD ``to`` (unknown prefix) and a MISSING
    ``to`` are both rejected by the connector: the send raises an error carrying
    amqp:precondition-failed.

Per-message authorization: each anonymous send is authorized for WRITE on the
resolved (pattern, channel) via the connector's Casbin policy -- there is no
per-link grant for an anonymous terminus.

Grounded in connector test TestAnonymousTerminusRouting
(connectors/amqp10/integration_test.go).

Run::

    export KUBEMQ_AMQP_URL=amqp://localhost:5672
    uv run python advanced/anonymous_terminus/main.py
"""

from __future__ import annotations

import os
import time

from proton import Delivery, Message
from proton.utils import BlockingConnection

# Explicit <pattern>/<channel> destinations selected per-message via Message.address.
QUEUE_CHANNEL = "amqp10.examples.anon.q"
EVENTS_CHANNEL = "amqp10.examples.anon.e"


def amqp_url() -> str:
    return os.environ.get("KUBEMQ_AMQP_URL", "amqp://localhost:5672")


def accept_if_unsettled(receiver) -> None:  # noqa: ANN001 - proton BlockingReceiver
    """Accept the just-received delivery, but ONLY if it is unsettled.

    Events deliveries are pre-settled by the connector (at-most-once), so there is
    no delivery to settle -- calling ``receiver.accept()`` on a settled delivery
    raises ``IndexError`` (proton only tracks unsettled deliveries). The queue
    delivery IS unsettled, so this accepts it; the event delivery is a no-op.
    """
    if receiver.fetcher.unsettled:
        receiver.accept()


def send_to(sender, address: str | None, body: str) -> None:  # noqa: ANN001 - proton sender
    """Issue one send routed by Message.address, raising on a connector rejection.

    A connector rejection arrives as a REJECTED disposition carrying the
    ``amqp:precondition-failed`` condition. We pass ``error_states=[]`` so proton
    does NOT raise its own bare ``SendException`` (whose ``str()`` is just the
    numeric delivery state, e.g. "37", and which leaves the rejected delivery
    UNSETTLED -- corrupting this session's delivery-id sequencing for the later
    consume). Instead we settle the delivery, then inspect ``remote_state`` and
    raise a clear error carrying the connector's rich condition text.
    """
    msg = Message(body=body)
    if address is not None:
        msg.address = address  # the AMQP properties.to the connector routes on
    delivery = sender.send(msg, error_states=[])
    if delivery.remote_state in (Delivery.REJECTED, Delivery.RELEASED):
        cond = delivery.remote.condition
        raise RuntimeError(str(cond) if cond is not None else f"send refused (state={delivery.remote_state})")


def main() -> None:
    queue_to = "queues/" + QUEUE_CHANNEL
    events_to = "events/" + EVENTS_CHANNEL
    print(f"Broker: {amqp_url()}")
    print("Anonymous sender (null target) -- routes per-message via Message.address (properties.to)")
    print(f"  msg #1 to: {queue_to}")
    print(f"  msg #2 to: {events_to}\n")

    # proton's BlockingConnection multiplexes ALL links onto ONE session, so an
    # inbound event delivery and the anonymous sender's mixed accept/reject
    # dispositions would collide on that session's delivery-id sequence
    # (amqp:session:invalid-field). Give each role its OWN connection (= its own
    # session, hence its own delivery-id space) -- the "one session per role"
    # pattern the RPC and consumer-group variants also use.

    # A consumer for the EVENTS channel must be subscribed BEFORE we publish to it
    # -- events are fire-and-forget (no replay). The queue message, by contrast, is
    # durable, so we consume it after sending, on its own connection.
    sub_conn = BlockingConnection(amqp_url())
    send_conn = BlockingConnection(amqp_url())
    try:
        event_rcv = sub_conn.create_receiver(events_to, credit=5)

        # 1. ATTACH an anonymous sender. The None target attaches a link with a NULL
        #    target -- there is no bound channel. Every message routes by its own
        #    Message.address.
        anon = send_conn.create_sender(None)
        print("[attach] Anonymous sender attached (null target)")
        time.sleep(0.5)  # let the fresh events subscription register before the publish

        # 2. Send #1 -- route to a QUEUE via Message.address. The connector resolves
        #    "queues/<ch>", authorizes WRITE for this connection, and stores it.
        send_to(anon, queue_to, "to-queue")
        print(f"[send] msg #1 routed to {queue_to} (accepted)")

        # 3. Send #2 -- route to an EVENTS topic via Message.address. Same anonymous
        #    link, a DIFFERENT pattern. The subscriber attached above receives it.
        send_to(anon, events_to, "to-events")
        print(f"[send] msg #2 routed to {events_to} (accepted)")

        # 4. Negative cases (expected errors) -- the connector rejects a bad/missing
        #    `to` with amqp:precondition-failed, surfaced to the client as a send error.
        bad_to = "bogus/prefix/x"
        try:
            send_to(anon, bad_to, "nowhere")
            raise SystemExit("expected a bad `to` to be rejected, but the send succeeded")
        except SystemExit:
            raise
        except Exception as exc:  # noqa: BLE001 - amqp:precondition-failed
            print(f"[send] msg with bad `to`={bad_to!r} rejected as expected: {exc}")

        try:
            send_to(anon, None, "orphan")  # NO address at all
            raise SystemExit("expected a missing `to` to be rejected, but the send succeeded")
        except SystemExit:
            raise
        except Exception as exc:  # noqa: BLE001 - amqp:precondition-failed
            print(f"[send] msg with NO `to` rejected as expected: {exc}")

        anon.close()

        # 5. Verify routing -- consume the queue message back (its own connection),
        #    and receive the event (on the pre-attached subscriber connection).
        cons_conn = BlockingConnection(amqp_url())
        try:
            q_rcv = cons_conn.create_receiver(queue_to, credit=1)
            q_got = q_rcv.receive(timeout=30.0)
            accept_if_unsettled(q_rcv)  # queue delivery is unsettled -> accepted
            print(f"[recv] queue {queue_to} delivered: {str(q_got.body)!r}")
            q_rcv.close()
        finally:
            cons_conn.close()

        e_got = event_rcv.receive(timeout=30.0)
        accept_if_unsettled(event_rcv)  # event delivery is pre-settled -> no-op
        print(f"[recv] events {events_to} delivered: {str(e_got.body)!r}")
        event_rcv.close()
    finally:
        send_conn.close()
        sub_conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()

# Expected output:
#
# Broker: amqp://localhost:5672
# Anonymous sender (null target) -- routes per-message via Message.address (properties.to)
#   msg #1 to: queues/amqp10.examples.anon.q
#   msg #2 to: events/amqp10.examples.anon.e
#
# [attach] Anonymous sender attached (null target)
# [send] msg #1 routed to queues/amqp10.examples.anon.q (accepted)
# [send] msg #2 routed to events/amqp10.examples.anon.e (accepted)
# [send] msg with bad `to`='bogus/prefix/x' rejected as expected: <amqp:precondition-failed ...>
# [send] msg with NO `to` rejected as expected: <amqp:precondition-failed ...>
# [recv] queue queues/amqp10.examples.anon.q delivered: 'to-queue'
# [recv] events events/amqp10.examples.anon.e delivered: 'to-events'
#
# Done.
