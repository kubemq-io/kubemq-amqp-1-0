"""Example: events_store/start_positions (master-table variant #8).

The ``x-opt-kubemq-start`` link property over KubeMQ **Events Store** using the
native ``python-qpid-proton`` blocking client (NO KubeMQ SDK).

Events Store persists the stream, so a (non-durable) subscriber can choose WHERE
in the history to start consuming via the ``x-opt-kubemq-start`` receiver link
property. The full grammar (parsed by the connector's ``parseEventsStoreStart``,
connectors/amqp10/link.go):

    (absent) / "new-only"  -> deliver only events published AFTER attach
    "first"                -> replay the ENTIRE history from the beginning
    "last"                 -> start at the last stored event
    "sequence:<n>"         -> start at store sequence n (1-BASED; sequence 1 = the
                              first stored event -- the connector passes n straight
                              to NATS streaming's StartAtSequence)
    "time:<RFC3339|secs>"  -> start at a wall-clock instant (RFC3339 or unix-seconds)
    "time-delta:<secs>"    -> start <secs> seconds ago (relative to now)

IMPORTANT -- time encoding: the client sends a ``time:`` value as RFC3339 OR as
unix SECONDS; the connector parses BOTH to the same instant and the broker stores
the cursor as unix NANOSECONDS. ``time-delta:`` is seconds verbatim. A malformed
value (e.g. "sequence:abc", "whenever") is rejected at ATTACH with
amqp:invalid-field. There is NO native "last N by count" form -- to read the
tail, compute a sequence or a time window.

This program seeds 6 events, then demonstrates four start positions on fresh
(non-durable) receivers against the SAME persisted stream:

    first              -> all 6 (full replay)
    sequence:4         -> from the 4th stored event onward (1-based => es-003,004,005)
    time-delta:3600    -> all 6 (all were published within the last hour)
    new-only           -> none of the existing 6; only events published after attach

Grounded in connector tests TestEventsStoreDurableReplay (the start:first leg)
and TestParseEventsStoreStart (connectors/amqp10/link_pubsub_test.go), and the
grammar in connectors/amqp10/link.go (parseEventsStoreStart).

Run::

    export KUBEMQ_AMQP_URL=amqp://localhost:5672
    uv run python events_store/start_positions/main.py
"""

from __future__ import annotations

import os
import time

from proton import Message, symbol
from proton.reactor import ReceiverOption
from proton.utils import BlockingConnection

# A fresh channel per run keeps the sequence numbers deterministic (this demo
# reads by absolute sequence, which is per-channel and monotonic).
CHANNEL = f"amqp10.examples.startpos.{time.time_ns()}"

START_PROP = "x-opt-kubemq-start"
SEED_COUNT = 6
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


class StartPosition(ReceiverOption):
    """Receiver option that stamps the ``x-opt-kubemq-start`` ATTACH link property.

    The connector reads this from the ATTACH-frame *link* properties
    (connectors/amqp10/link.go ``applyPubSubProperties`` / ``linkPropString``).
    """

    def __init__(self, start: str) -> None:
        self.start = start

    def apply(self, receiver) -> None:  # noqa: ANN001 - proton Receiver
        receiver.properties = {symbol(START_PROP): self.start}


def read_from(addr: str, start: str, want: int, window: float) -> list[str]:
    """Open a fresh (non-durable) receiver at the given start position and drain
    up to ``want`` events within ``window`` seconds, returning bodies in order.

    Each probe runs on its OWN connection (= its own AMQP session, hence its own
    delivery-id space). Events-store deliveries are pre-settled and proton's
    BlockingConnection shares ONE session across receivers, so reusing a single
    connection across start-position probes corrupts the session's delivery-id
    sequencing on the next attach (amqp:session:invalid-field). A connection per
    probe mirrors the Go variant's "new session per receiver" and keeps each
    subscription independent.
    """
    conn = BlockingConnection(amqp_url())
    try:
        receiver = conn.create_receiver(addr, credit=STANDING_CREDIT, options=StartPosition(start))
        out: list[str] = []
        deadline = time.monotonic() + window
        while len(out) < want and time.monotonic() < deadline:
            try:
                msg = receiver.receive(timeout=max(0.0, deadline - time.monotonic()))
            except Exception:  # noqa: BLE001 - timeout surfaces as Timeout
                break
            accept_if_unsettled(receiver)  # no-op for pre-settled events-store deliveries
            out.append(str(msg.body))
        receiver.close()
        return out
    finally:
        conn.close()


def demo_new_only(addr: str) -> None:
    """Attach a new-only receiver, then publish one event and prove ONLY the
    post-attach event is delivered (the existing events are skipped).

    Runs on its own connection/session (see ``read_from`` -- independent
    delivery-id space).
    """
    conn = BlockingConnection(amqp_url())
    try:
        receiver = conn.create_receiver(addr, credit=STANDING_CREDIT, options=StartPosition("new-only"))
        time.sleep(0.75)  # let the new-only cursor settle before publishing

        sender = conn.create_sender(addr)
        fresh = "es-new-after-attach"
        sender.send(Message(body=fresh))
        sender.close()

        # Only the post-attach event must arrive.
        msg = receiver.receive(timeout=15.0)
        accept_if_unsettled(receiver)  # no-op for pre-settled events-store deliveries
        if str(msg.body) != fresh:
            raise SystemExit(
                f"[start=new-only] expected {fresh!r} (post-attach), but got "
                f"{str(msg.body)!r} (an existing event leaked)"
            )

        # Nothing else (the existing events must NOT be delivered).
        try:
            extra = receiver.receive(timeout=2.0)
            raise SystemExit(
                f"[start=new-only] an existing event {str(extra.body)!r} leaked (new-only must skip history)"
            )
        except SystemExit:
            raise
        except Exception:  # noqa: BLE001 - the expected timeout
            pass

        print(
            f"[start=new-only]       skipped all {SEED_COUNT} existing events; "
            f"delivered only the post-attach event: [{fresh}]"
        )
        receiver.close()
    finally:
        conn.close()


def demo_malformed(addr: str, bad_start: str) -> None:
    """Prove a bad start value is rejected at ATTACH with amqp:invalid-field (the
    receiver never attaches).

    Runs on its own connection: the connector DETACHes the bad link, which proton
    commonly surfaces as a connection-level disconnect, so an isolated connection
    keeps the rejection from disturbing the other probes.
    """
    conn = BlockingConnection(amqp_url())
    try:
        conn.create_receiver(addr, credit=STANDING_CREDIT, options=StartPosition(bad_start))
    except Exception as exc:  # noqa: BLE001 - the connector DETACHes the bad attach
        print(f"[gotcha] start={bad_start!r} correctly REJECTED at ATTACH: {exc}")
        return
    finally:
        conn.close()
    raise SystemExit(f"[malformed] expected {bad_start!r} to be rejected, but the attach succeeded")


def expect_exactly(got: list[str], want: list[str], label: str) -> None:
    if sorted(got) != sorted(want):
        raise SystemExit(f"[start={label}] expected {want}, got {got}")


def main() -> None:
    addr = "events-store/" + CHANNEL
    print(f"Broker:  {amqp_url()}")
    print(f"Address: {addr}  (KubeMQ pattern=events-store, channel={CHANNEL})\n")

    # 0. SEED -- publish 6 events into the persisted events-store stream on a
    #    dedicated producer connection, then close it. Each read probe below opens
    #    its OWN connection so the probes never share a session (see read_from).
    seed_conn = BlockingConnection(amqp_url())
    sender = seed_conn.create_sender(addr)
    for i in range(SEED_COUNT):
        sender.send(Message(body=f"es-{i:03d}"))
    sender.close()
    seed_conn.close()
    print(f"[seed] Published {SEED_COUNT} events (stored at 1-based sequences 1..{SEED_COUNT})\n")

    # 1. start=first -> FULL REPLAY (all 6 events from the beginning).
    got = read_from(addr, "first", SEED_COUNT, 15.0)
    expect_exactly(got, [f"es-{i:03d}" for i in range(6)], "first")
    print(f"[start=first]          replayed full history: {got}")

    # 2. start=sequence:4 -> from the 4th stored event onward. Sequences are
    #    1-BASED (the connector passes the value straight to NATS streaming's
    #    StartAtSequence; sequence 1 = the first event), so the 4th stored event
    #    is es-003, delivering es-003, es-004, es-005.
    got = read_from(addr, "sequence:4", SEED_COUNT, 15.0)
    expect_exactly(got, ["es-003", "es-004", "es-005"], "sequence:4")
    print(f"[start=sequence:4]     from the 4th stored event (1-based): {got}")

    # 3. start=time-delta:3600 -> everything from the last hour (all 6, since the
    #    seed was published seconds ago). time-delta is SECONDS verbatim.
    got = read_from(addr, "time-delta:3600", SEED_COUNT, 15.0)
    expect_exactly(got, [f"es-{i:03d}" for i in range(6)], "time-delta:3600")
    print(f"[start=time-delta:3600] last hour (all 6): {got}")

    # (You can also start at an absolute instant, e.g.
    #   StartPosition("time:" + datetime.now(timezone.utc).isoformat())
    # or with unix-seconds: StartPosition("time:1623578400"). Both forms resolve
    # to the same instant; the broker stores the cursor as nanoseconds.)

    # 4. start=new-only -> NONE of the 6 existing events; only what is published
    #    AFTER this attach.
    demo_new_only(addr)

    # 5. GOTCHA -- a malformed start value is rejected at ATTACH with
    #    amqp:invalid-field.
    print()
    demo_malformed(addr, "sequence:abc")
    demo_malformed(addr, "whenever")

    print("\nDone.")


if __name__ == "__main__":
    main()

# Expected output (the channel suffix is a timestamp, so it varies per run):
#
# Broker:  amqp://localhost:5672
# Address: events-store/amqp10.examples.startpos.<ts>  (KubeMQ pattern=events-store, channel=amqp10.examples.startpos.<ts>)
#
# [seed] Published 6 events (stored at 1-based sequences 1..6)
#
# [start=first]          replayed full history: ['es-000', 'es-001', 'es-002', 'es-003', 'es-004', 'es-005']
# [start=sequence:4]     from the 4th stored event (1-based): ['es-003', 'es-004', 'es-005']
# [start=time-delta:3600] last hour (all 6): ['es-000', 'es-001', 'es-002', 'es-003', 'es-004', 'es-005']
# [start=new-only]       skipped all 6 existing events; delivered only the post-attach event: [es-new-after-attach]
#
# [gotcha] start='sequence:abc' correctly REJECTED at ATTACH: <amqp:invalid-field detach>
# [gotcha] start='whenever' correctly REJECTED at ATTACH: <amqp:invalid-field detach>
#
# Done.
#
# The connector DETACHes the bad attach with amqp:invalid-field (description
# "invalid start sequence: abc" / "unknown start position: whenever"); proton's
# blocking create_receiver raises on the link error and the receiver never attaches.
