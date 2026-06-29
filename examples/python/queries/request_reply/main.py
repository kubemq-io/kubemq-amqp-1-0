"""Example: queries/request_reply (master-table variant #10).

Native AMQP 1.0 request/reply over KubeMQ **Queries** (RPC) with the native
``python-qpid-proton`` blocking client -- NO kubemq SDK, NO gRPC. The whole
round-trip stays in-protocol over a single broker connection per role.

The reply path is IDENTICAL to commands (variant #9): the requester opens a
DYNAMIC reply node (``create_receiver(None, dynamic=True)`` -> read
``link.remote_source.address``), sends to queries/<ch> with ``Message.reply_to``
= that node + a ``correlation_id``; the responder receives on queries/<ch> and
replies via an ANONYMOUS sender (``create_sender(None)``) with ``Message.address``
= the request's reply-to + the echoed correlation_id.

The CONTRAST with commands (the whole point of variant #10):

  * A query reply carries ONLY the body + metadata -- NO x-opt-kubemq-executed /
    x-opt-kubemq-error application-properties. A query is a "fetch a value" call;
    there is no executed/error envelope.
  * A FAILED query delivers NOTHING. The connector's runRequest delivers no reply
    when a query fails or times out (MQTT-bridge parity), so the requester simply
    TIMES OUT. (A failed command, by contrast, always replies executed=false so
    its requester is never left waiting.)

This example demonstrates BOTH: a successful query (reply round-trips, body
intact) and a query the responder ignores (no reply => the requester times out on
a short demo deadline; in production the connector default is ~30s).

Grounded in connector test TestRPCRequesterResponderViaAMQP10 (queries leg)
(connectors/amqp10/integration_test.go).

Run::

    export KUBEMQ_AMQP_URL=amqp://localhost:5672
    uv run python queries/request_reply/main.py
"""

from __future__ import annotations

import os
import threading

from proton import Message
from proton.utils import BlockingConnection

# channel is the KubeMQ queries channel; the link address is "queries/" + channel
# (explicit prefix -- never rely on a default pattern).
CHANNEL = "amqp10.examples.queries"

# A short per-request deadline so the "no reply" leg surfaces a timeout quickly.
# The connector's own default RPC timeout is ~30s; in production set the request
# message ttl to choose the per-request budget.
DEMO_TIMEOUT = 5.0


def amqp_url() -> str:
    return os.environ.get("KUBEMQ_AMQP_URL", "amqp://localhost:5672")


def run_responder(addr: str, ready: threading.Event, stop: threading.Event) -> None:
    """Receive queries on queries/<ch> and reply via an anonymous sender. A query
    whose body is "ignore" gets NO reply (so its requester times out).
    """
    conn = BlockingConnection(amqp_url())
    try:
        rcv = conn.create_receiver(addr, credit=10)
        snd = conn.create_sender(None)  # anonymous reply sender (null target)

        print(f"[responder] Listening on {addr} (anonymous reply sender ready)")
        ready.set()

        while not stop.is_set():
            try:
                req = rcv.receive(timeout=1.0)
            except Exception:  # noqa: BLE001 - idle timeout; loop to re-check stop
                continue
            rcv.accept()

            if not req.reply_to:
                print("[responder] request with no reply-to; cannot reply")
                continue
            body = str(req.body)
            print(f"[responder] Received query {body!r} (correlation-id={req.correlation_id})")

            # Business logic: a query body of "ignore" is dropped on the floor --
            # the responder sends NOTHING. The requester will time out. (A real
            # responder would only fail to reply on a crash / unreachable backend;
            # "ignore" makes the contrast deterministic for the demo.)
            if body == "ignore":
                print(f"[responder] Ignoring {body!r} -- NO reply sent (requester will time out)")
                continue

            # A QUERY reply carries ONLY the body + metadata -- NO executed/error
            # application-properties (the Commands-vs-Queries contrast).
            reply = Message(body="result:" + body)
            reply.address = req.reply_to
            reply.correlation_id = req.correlation_id if req.correlation_id else req.id

            snd.send(reply)
            print(f"[responder] Replied to {body!r} (body + metadata only, no executed/error props)")
    finally:
        conn.close()


def do_query(snd, reply_rcv, reply_node: str, body: str, corr: str) -> None:  # noqa: ANN001
    """Send one query and await the correlated reply; print the result body."""
    send_query(snd, reply_node, body, corr)
    reply = reply_rcv.receive(timeout=DEMO_TIMEOUT)
    reply_rcv.accept()
    got_corr = reply.correlation_id
    if str(got_corr) != corr:
        raise SystemExit(f"[requester] correlation-id mismatch: want {corr!r} got {got_corr!r}")
    print(f"[requester] Reply for {body!r} (correlation-id={got_corr}): body={str(reply.body)!r}")


def do_query_expect_timeout(snd, reply_rcv, reply_node: str, body: str, corr: str) -> None:  # noqa: ANN001
    """Send a query the responder will ignore and show the requester timing out
    (no reply is the failure signal for queries).
    """
    send_query(snd, reply_node, body, corr)
    try:
        reply_rcv.receive(timeout=DEMO_TIMEOUT)
    except Exception:  # noqa: BLE001 - the EXPECTED outcome for an unanswered query
        print(
            f"[requester] No reply for {body!r} within {DEMO_TIMEOUT}s -- query timed out "
            "(expected; failed queries deliver nothing)"
        )
        return
    raise SystemExit(f"[requester] expected NO reply for {body!r}, but one arrived")


def send_query(snd, reply_node: str, body: str, corr: str) -> None:  # noqa: ANN001
    """Send one query naming the dynamic reply node + correlation-id."""
    req = Message(body=body)
    req.reply_to = reply_node  # MUST name a node this connection owns (snooping guard)
    req.correlation_id = corr
    snd.send(req)
    print(f"[requester] Sent query {body!r} (reply-to=dynamic node, correlation-id={corr})")


def run_requester(addr: str) -> None:
    """Open a dynamic reply node + a sender on queries/<ch> and correlate replies."""
    conn = BlockingConnection(amqp_url())
    try:
        reply_rcv = conn.create_receiver(None, dynamic=True, credit=5)
        reply_node = reply_rcv.link.remote_source.address
        if not reply_node:
            raise SystemExit("[requester] server did not assign a dynamic reply-node address")
        print(f"[requester] Dynamic reply node: {reply_node}")

        snd = conn.create_sender(addr)

        # 1. A SUCCESSFUL query: round-trips, body intact, no executed/error props.
        do_query(snd, reply_rcv, reply_node, "get-temp-sensor-3", "corr-qry-1")

        # 2. A query the responder ignores: NOTHING is delivered, so the requester
        #    TIMES OUT. This is the core Queries contrast -- a failed/unanswered
        #    query has no error envelope; the absence of a reply IS the signal.
        do_query_expect_timeout(snd, reply_rcv, reply_node, "ignore", "corr-qry-2")
    finally:
        conn.close()


def main() -> None:
    addr = "queries/" + CHANNEL
    print(f"Broker:  {amqp_url()}")
    print(f"Address: {addr}  (KubeMQ pattern=queries, channel={CHANNEL})\n")

    ready = threading.Event()
    stop = threading.Event()
    responder = threading.Thread(target=run_responder, args=(addr, ready, stop), daemon=True)
    responder.start()

    if not ready.wait(timeout=30.0):
        raise SystemExit("responder did not become ready")

    try:
        run_requester(addr)
    finally:
        stop.set()
        responder.join(timeout=10.0)

    print("\nDone.")


if __name__ == "__main__":
    main()

# Expected output:
#
# Broker:  amqp://localhost:5672
# Address: queries/amqp10.examples.queries  (KubeMQ pattern=queries, channel=amqp10.examples.queries)
#
# [responder] Listening on queries/amqp10.examples.queries (anonymous reply sender ready)
# [requester] Dynamic reply node: _amqp10.tmp.<connID>.<uuid>
# [requester] Sent query 'get-temp-sensor-3' (reply-to=dynamic node, correlation-id=corr-qry-1)
# [responder] Received query 'get-temp-sensor-3' (correlation-id=corr-qry-1)
# [responder] Replied to 'get-temp-sensor-3' (body + metadata only, no executed/error props)
# [requester] Reply for 'get-temp-sensor-3' (correlation-id=corr-qry-1): body='result:get-temp-sensor-3'
# [requester] Sent query 'ignore' (reply-to=dynamic node, correlation-id=corr-qry-2)
# [responder] Received query 'ignore' (correlation-id=corr-qry-2)
# [responder] Ignoring 'ignore' -- NO reply sent (requester will time out)
# [requester] No reply for 'ignore' within 5.0s -- query timed out (expected; failed queries deliver nothing)
#
# Done.
#
# (Unlike a command -- which always replies executed=false on failure so the
# requester is never left waiting -- a query that fails/goes unanswered delivers
# NOTHING. The requester's timeout IS the failure signal. The connector's own
# default per-request timeout is ~30s; set the request message ttl to choose it.)
