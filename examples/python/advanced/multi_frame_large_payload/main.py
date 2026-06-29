"""Example: advanced/multi_frame_large_payload (master-table variant #11).

A single AMQP 1.0 message whose body is larger than the connection's
max-frame-size is fragmented across multiple TRANSFER frames (more=true ...
more=false) by the sender and reassembled bit-exact by the receiver -- all
transparently, with NO application-level chunking. This example drives that path
against the KubeMQ AMQP 1.0 connector using the native ``python-qpid-proton``
blocking client (NO KubeMQ SDK).

Flow:
  * Dial with a deliberately tiny max-frame-size (4 KiB) on BOTH the producer and
    consumer connections so a ~1 MB body forces heavy fragmentation in both
    directions. (max_frame_size is a connect() kwarg forwarded by
    BlockingConnection -> Container.connect.)
  * Sender -> "queues/<ch>" (unsettled): one send carries a ~1 MB binary body.
    proton splits it across many transfer frames; the connector reassembles it and
    stores a single message.
  * Receiver <- "queues/<ch>" credit=1: one receive yields the full body. The
    example verifies the received length AND a CRC32 of the bytes match the
    original -- proving a bit-exact round-trip across the fragment boundary.

Grounded in connector test TestQueueMultiFrameLargePayload
(connectors/amqp10/integration_test.go).

Run::

    export KUBEMQ_AMQP_URL=amqp://localhost:5672
    uv run python advanced/multi_frame_large_payload/main.py
"""

from __future__ import annotations

import os
import zlib

from proton import Message
from proton.utils import BlockingConnection

# channel is the KubeMQ queue channel; the link address is "queues/" + channel
# (explicit prefix -- never rely on a default pattern).
CHANNEL = "amqp10.examples.multiframe"

# payloadSize is ~1 MB -- comfortably larger than maxFrameSize so the body must
# span many transfer frames (more=true ... more=false).
PAYLOAD_SIZE = 1 * 1024 * 1024
# maxFrameSize is a deliberately tiny 4 KiB so the ~1 MB body fragments across
# ~256 frames in each direction.
MAX_FRAME_SIZE = 4096


def amqp_url() -> str:
    return os.environ.get("KUBEMQ_AMQP_URL", "amqp://localhost:5672")


def main() -> None:
    addr = "queues/" + CHANNEL
    print(f"Broker:        {amqp_url()}")
    print(f"Address:       {addr}  (KubeMQ pattern=queues, channel={CHANNEL})")
    print(f"MaxFrameSize:  {MAX_FRAME_SIZE} bytes")
    print(f"Payload:       {PAYLOAD_SIZE} bytes (~{PAYLOAD_SIZE // 1024} KiB)\n")

    # Build a deterministic, non-trivial payload and remember its CRC + length so
    # we can prove a bit-exact round-trip after reassembly. (251 is prime -> no
    # short repeating period.)
    payload = bytes(i % 251 for i in range(PAYLOAD_SIZE))
    want_len = len(payload)
    want_crc = zlib.crc32(payload) & 0xFFFFFFFF
    print(f"[prep] Built payload: len={want_len} crc32=0x{want_crc:08x}")

    # 1. PRODUCER connection -- OPEN with a tiny max-frame-size. The connector
    #    advertises its own max-frame-size in the OPEN reply; proton uses the
    #    smaller of the two when fragmenting transfers.
    prod_conn = BlockingConnection(amqp_url(), max_frame_size=MAX_FRAME_SIZE)
    sender = prod_conn.create_sender(addr)
    # ONE send carries the whole body; proton transparently splits it across many
    # transfer frames (more=true ... final more=false). The connector reassembles
    # them into a single stored message.
    sender.send(Message(body=payload))
    sender.close()
    approx_frames = (want_len // MAX_FRAME_SIZE) + 1
    print(f"[send] Sent the {want_len}-byte body in ONE send (fragmented across ~{approx_frames} frames, accepted)")

    # 2. CONSUMER connection -- same tiny max-frame-size so reassembly is exercised
    #    on the receive path too. One receive yields the FULL reassembled body.
    cons_conn = BlockingConnection(amqp_url(), max_frame_size=MAX_FRAME_SIZE)
    receiver = cons_conn.create_receiver(addr, credit=1)
    msg = receiver.receive(timeout=60.0)
    receiver.accept()

    got = bytes(msg.body)
    got_len = len(got)
    got_crc = zlib.crc32(got) & 0xFFFFFFFF
    print(f"[recv] Reassembled body: len={got_len} crc32=0x{got_crc:08x}")

    # 3. Verify the round-trip is bit-exact: length AND CRC32 must match.
    if got_len != want_len:
        raise SystemExit(f"length mismatch: sent {want_len}, received {got_len}")
    if got_crc != want_crc:
        raise SystemExit(f"CRC mismatch: sent 0x{want_crc:08x}, received 0x{got_crc:08x}")
    print("[verify] Length and CRC32 match -- multi-frame body round-tripped bit-exact")

    receiver.close()
    cons_conn.close()
    prod_conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()

# Expected output:
#
# Broker:        amqp://localhost:5672
# Address:       queues/amqp10.examples.multiframe  (KubeMQ pattern=queues, channel=amqp10.examples.multiframe)
# MaxFrameSize:  4096 bytes
# Payload:       1048576 bytes (~1024 KiB)
#
# [prep] Built payload: len=1048576 crc32=0x........
# [send] Sent the 1048576-byte body in ONE send (fragmented across ~257 frames, accepted)
# [recv] Reassembled body: len=1048576 crc32=0x........
# [verify] Length and CRC32 match -- multi-frame body round-tripped bit-exact
#
# Done.
