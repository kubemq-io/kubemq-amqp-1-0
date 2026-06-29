"""Example: commands/request_reply_dynamic_node (master-table variant #9).

Native AMQP 1.0 request/reply over KubeMQ **Commands** (RPC) with the native
``python-qpid-proton`` blocking client -- NO kubemq SDK, NO gRPC. The whole
round-trip stays in-protocol over a single broker connection per role.

The mechanism (spec section 2.4/6.5; connector connectors/amqp10/rpc.go + dynamic.go):

  * REQUESTER opens a DYNAMIC reply node: ``create_receiver(None, dynamic=True)``
    -- the server creates a transient node and echoes its address back, read via
    ``reply_rcv.link.remote_source.address`` (a "_amqp10.tmp.<connID>.<uuid>"
    token). The requester sends the command to commands/<ch> carrying
    ``Message.reply_to`` = that node + ``Message.correlation_id``. The connector
    verifies the reply-to names a node THIS connection owns (snooping guard: a
    reply-to that does not resolve to a connection-owned node is refused with
    amqp:not-allowed) and routes the request to SendCommand. The broker Response
    is delivered out-of-band onto the dynamic node; the requester correlates it by
    correlation-id (the connector falls back to message-id when absent).

  * RESPONDER receives requests on commands/<ch> (a server-sender link pumped
    under credit) and replies via an ANONYMOUS sender -- ``create_sender(None)``
    (null target) -- setting ``Message.address`` = the request's reply_to (the
    connector stamps that as "/responses/<RequestID>") + the echoed
    correlation_id. A command reply ALSO carries application properties:
    x-opt-kubemq-executed (bool) + x-opt-kubemq-error (string).

Commands vs Queries (the #9 vs #10 contrast): a command that FAILS still produces
a reply (executed=false + error text) so the requester is NEVER left waiting. This
example demonstrates BOTH: a successful command (executed=true) and a failed
command (executed=false) -- both round-trip, neither hangs.

Grounded in connector tests TestRPCRequesterResponderViaAMQP10 (commands leg) and
TestRPCInteropAMQP10RequesterGRPCResponder (the executed/error app-props)
(connectors/amqp10/integration_test.go).

Run::

    export KUBEMQ_AMQP_URL=amqp://localhost:5672
    uv run python commands/request_reply_dynamic_node/main.py
"""

from __future__ import annotations

import os
import threading

from proton import Message
from proton.utils import BlockingConnection

# channel is the KubeMQ commands channel; the link address is "commands/" + channel
# (explicit prefix -- never rely on a default pattern).
CHANNEL = "amqp10.examples.commands"

EXECUTED_PROP = "x-opt-kubemq-executed"
ERROR_PROP = "x-opt-kubemq-error"


def amqp_url() -> str:
    return os.environ.get("KUBEMQ_AMQP_URL", "amqp://localhost:5672")


def body_text(body: object) -> str:
    """Render a message body as text for display.

    A COMMAND reply carries the executed/error outcome, NOT data, so the connector
    returns an EMPTY body -- proton surfaces it as an empty ``memoryview`` (binary
    section), whose ``str()`` is the unhelpful ``<memory at 0x...>`` repr. Decode
    bytes/memoryview to text so an empty command body prints as ``''``.
    """
    if isinstance(body, (bytes, bytearray, memoryview)):
        return bytes(body).decode("utf-8", "replace")
    return str(body) if body is not None else ""


def run_responder(addr: str, ready: threading.Event, stop: threading.Event) -> None:
    """Receive commands on commands/<ch> and reply via an anonymous sender.

    The blocking client is single-threaded per connection, so the responder runs
    on its OWN connection in its OWN thread -- honouring "one session/sender per
    thread".
    """
    conn = BlockingConnection(amqp_url())
    try:
        # ATTACH a receiver on commands/<ch> (a server-sender link -- the client
        # consumes requests). Credit makes the connector pump requests.
        rcv = conn.create_receiver(addr, credit=10)
        # ATTACH an ANONYMOUS sender (null target). Each reply sets Message.address
        # to the request's reply-to so it routes back to the dynamic node.
        snd = conn.create_sender(None)

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
            print(f"[responder] Received command {body!r} (correlation-id={req.correlation_id})")

            # Business logic: a command body of "fail" is rejected (executed=false);
            # any other body succeeds (executed=true). BOTH paths reply -- a command
            # failure must NOT leave the requester waiting (unlike a query, #10).
            #
            # NOTE: a COMMAND response carries the EXECUTED/ERROR outcome, not data.
            # The reply body below is sent for completeness but the requester
            # observes an empty command body. Use a QUERY (#10) to return a value.
            ok = body != "fail"
            err_text = "" if ok else "command rejected by handler"

            reply = Message(body="ack:" + body)
            reply.address = req.reply_to
            # Echo the correlation-id (fall back to message-id, the connector
            # convention) so the requester can match the reply to its request.
            reply.correlation_id = req.correlation_id if req.correlation_id else req.id
            # A COMMAND reply carries the execution outcome as application-properties.
            reply.properties = {EXECUTED_PROP: ok, ERROR_PROP: err_text}

            snd.send(reply)
            print(f"[responder] Replied to {body!r} (executed={ok}, error={err_text!r})")
    finally:
        conn.close()


def do_request(snd, reply_rcv, reply_node: str, body: str, corr: str) -> None:  # noqa: ANN001
    """Send one command naming the dynamic reply node + a correlation-id, then
    await the correlated reply and print the executed/error outcome.
    """
    req = Message(body=body)
    req.reply_to = reply_node  # MUST name a node this connection owns (snooping guard)
    req.correlation_id = corr
    snd.send(req)
    print(f"[requester] Sent command {body!r} (reply-to=dynamic node, correlation-id={corr})")

    # Await the correlated reply on the dynamic node. A command always replies
    # (success OR failure), so this never times out on the happy path.
    reply = reply_rcv.receive(timeout=30.0)
    reply_rcv.accept()

    got_corr = reply.correlation_id
    if str(got_corr) != corr:
        raise SystemExit(f"[requester] correlation-id mismatch: want {corr!r} got {got_corr!r}")
    props = reply.properties or {}
    executed = bool(props.get(EXECUTED_PROP, False))
    err_text = str(props.get(ERROR_PROP, ""))
    print(
        f"[requester] Reply for {body!r} (correlation-id={got_corr}): "
        f"executed={executed} error={err_text!r} body={body_text(reply.body)!r}"
    )


def run_requester(addr: str) -> None:
    """Open a dynamic reply node + a sender on commands/<ch> and correlate replies."""
    conn = BlockingConnection(amqp_url())
    try:
        # ATTACH a DYNAMIC reply node: dynamic=True asks the server to create a
        # transient node and echo its address (read via remote_source.address).
        reply_rcv = conn.create_receiver(None, dynamic=True, credit=5)
        reply_node = reply_rcv.link.remote_source.address
        if not reply_node:
            raise SystemExit("[requester] server did not assign a dynamic reply-node address")
        print(f"[requester] Dynamic reply node: {reply_node}")

        # ATTACH a sender on commands/<ch> (a server-receiver link -- the client
        # produces requests). The server grants credit on attach.
        snd = conn.create_sender(addr)

        # 1. A SUCCESSFUL command: round-trips with executed=true.
        do_request(snd, reply_rcv, reply_node, "reboot-node-7", "corr-cmd-1")

        # 2. A FAILED command ("fail"): the responder replies executed=false + an
        #    error text -- the requester is NOT left waiting (the key Commands
        #    contrast vs Queries, where a failure delivers nothing).
        do_request(snd, reply_rcv, reply_node, "fail", "corr-cmd-2")
    finally:
        conn.close()


def main() -> None:
    addr = "commands/" + CHANNEL
    print(f"Broker:  {amqp_url()}")
    print(f"Address: {addr}  (KubeMQ pattern=commands, channel={CHANNEL})\n")

    ready = threading.Event()
    stop = threading.Event()
    responder = threading.Thread(target=run_responder, args=(addr, ready, stop), daemon=True)
    responder.start()

    # Wait for the responder's subscription to go live before sending requests.
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
# Address: commands/amqp10.examples.commands  (KubeMQ pattern=commands, channel=amqp10.examples.commands)
#
# [responder] Listening on commands/amqp10.examples.commands (anonymous reply sender ready)
# [requester] Dynamic reply node: _amqp10.tmp.<connID>.<uuid>
# [requester] Sent command 'reboot-node-7' (reply-to=dynamic node, correlation-id=corr-cmd-1)
# [responder] Received command 'reboot-node-7' (correlation-id=<RequestID>)
# [responder] Replied to 'reboot-node-7' (executed=True, error='')
# [requester] Reply for 'reboot-node-7' (correlation-id=corr-cmd-1): executed=True error='' body=''
# [requester] Sent command 'fail' (reply-to=dynamic node, correlation-id=corr-cmd-2)
# [responder] Received command 'fail' (correlation-id=<RequestID>)
# [responder] Replied to 'fail' (executed=False, error='command rejected by handler')
# [requester] Reply for 'fail' (correlation-id=corr-cmd-2): executed=False error='command rejected by handler' body=''
#
# Done.
#
# (The responder sees the connector-stamped RequestID as the delivered request's
# correlation-id, while the requester's reply correlation-id is its ORIGINAL
# corr-cmd-N -- the connector echoes the requester's correlation-id back on the
# reply. A COMMAND response carries the executed/error outcome, NOT a body -- the
# requester observes an empty command reply body; use a QUERY to return a value.)
#
# (A failed command still delivers a reply -- executed=false + error text -- so the
# requester is NEVER left waiting. Contrast queries/request_reply, where a failed
# query delivers nothing and the requester simply times out.)
