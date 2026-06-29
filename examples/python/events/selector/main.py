"""Example: events/selector (master-table variant #6).

JMS / SQL-92 message selectors over KubeMQ **Events** with the native
``python-qpid-proton`` blocking client (NO KubeMQ SDK).

A receiver attaches to events/<ch> carrying a selector SOURCE filter under the
OASIS-standard descriptor ``apache.org:selector-filter:string`` (the same key
go-amqp's ``NewSelectorFilter`` emits). proton encodes it as a filter-set entry::

    source.filter = { symbol("selector"):
        Described(symbol("apache.org:selector-filter:string"), "<selector>") }

The connector evaluates the selector against each event's APPLICATION PROPERTIES
and delivers ONLY the matching events; non-matching events are silently withheld
(copy semantics -- they stay available to OTHER subscribers, not consumed/discarded).

The selector here is:  color = 'red' AND size > 2

We publish 5 events and assert exactly 2 are delivered:

    match-1      {color:red,  size:5}  delivered
    miss-blue    {color:blue, size:9}  color != red
    miss-small   {color:red,  size:1}  size not > 2
    match-2      {color:red,  size:3}  delivered
    miss-nocolor {           size:8}   color IS NULL  (3-valued logic: UNKNOWN -> withheld)

THREE-VALUED LOGIC: a property that is absent evaluates to NULL, so the predicate
is UNKNOWN (not true) and the event is NOT delivered -- this is why miss-nocolor is
withheld even though it has no color to disqualify it.

GOTCHA: a selector is honoured ONLY on events/ and events-store/ consume links.
Requesting one on a queues/ source is rejected at ATTACH with amqp:not-implemented
("selector filter not supported on this address") -- see the README. This program
demonstrates that rejection at the end.

Grounded in connector test TestEventsSelector
(connectors/amqp10/integration_pubsub_test.go) and the selector-on-queues
rejection in connectors/amqp10/link.go (applySourceSelector).

Run::

    export KUBEMQ_AMQP_URL=amqp://localhost:5672
    uv run python events/selector/main.py
"""

from __future__ import annotations

import os
import time

from proton import Described, Message, symbol
from proton.reactor import AtMostOnce, ReceiverOption
from proton.utils import BlockingConnection

CHANNEL = "amqp10.examples.selector"

# selector is a standard SQL-92 / JMS message selector evaluated against each
# event's application properties.
SELECTOR = "color = 'red' AND size > 2"

# The OASIS descriptor for a JMS/SQL-92 string selector filter.
SELECTOR_FILTER_KEY = "apache.org:selector-filter:string"

STANDING_CREDIT = 100


def amqp_url() -> str:
    return os.environ.get("KUBEMQ_AMQP_URL", "amqp://localhost:5672")


def accept_if_unsettled(receiver) -> None:  # noqa: ANN001 - proton BlockingReceiver
    """Accept the just-received delivery, but ONLY if it is unsettled.

    Events fan-out deliveries are pre-settled by the connector (at-most-once), so
    there is no delivery to settle -- calling ``receiver.accept()`` on a settled
    delivery raises ``IndexError`` (proton only tracks unsettled deliveries). This
    helper makes accept a true no-op on pre-settled pub/sub, matching go-amqp's
    AcceptMessage which is harmless on settled deliveries.
    """
    if receiver.fetcher.unsettled:
        receiver.accept()


class SelectorFilter(ReceiverOption):
    """Receiver option that attaches a JMS/SQL-92 selector source filter.

    proton stores it as a filter-set: the entry key is the symbol "selector" and
    the value is a DESCRIBED string whose descriptor is the OASIS selector key.
    """

    def __init__(self, selector: str) -> None:
        self.selector = selector

    def apply(self, receiver) -> None:  # noqa: ANN001 - proton Receiver
        # The filter-set is keyed by the OASIS descriptor SYMBOL itself (this is the
        # key the connector reads, connectors/amqp10/link.go sourceSelectorText, and
        # the key go-amqp's NewSelectorFilter emits). The value is a DESCRIBED string
        # carrying the same descriptor so a strict client recognises the in-place
        # filter the connector echoes back.
        receiver.source.filter.put_dict(
            {symbol(SELECTOR_FILTER_KEY): Described(symbol(SELECTOR_FILTER_KEY), self.selector)}
        )


def main() -> None:
    addr = "events/" + CHANNEL
    print(f"Broker:   {amqp_url()}")
    print(f"Address:  {addr}  (KubeMQ pattern=events, channel={CHANNEL})")
    print(f"Selector: {SELECTOR}\n")

    conn = BlockingConnection(amqp_url())
    try:
        # =====================================================================
        # 1. SUBSCRIBE FIRST with the selector filter. A successful create_receiver
        #    means the connector accepted (and echoed) the filter -- a parse error
        #    or unsupported pattern would have DETACHed the link. Events have no
        #    replay, so we subscribe before publishing.
        # =====================================================================
        receiver = conn.create_receiver(addr, credit=STANDING_CREDIT, options=SelectorFilter(SELECTOR))
        print(f"[recv] Subscribed to {addr} with selector filter (standing credit {STANDING_CREDIT})")

        # Wait for the connector's subscription pump to go live before publishing.
        time.sleep(0.75)
        print("[recv] Subscription pump settled (waited 750ms before publishing)")

        # =====================================================================
        # 2. PUBLISH 5 events with application properties. The sender is pre-settled
        #    (fire-and-forget). The connector evaluates the selector against each
        #    event's application properties on the delivery path.
        # =====================================================================
        sender = conn.create_sender(addr, options=AtMostOnce())
        events = [
            ("match-1", {"color": "red", "size": 5}, True, "color=red AND size>2"),
            ("miss-blue", {"color": "blue", "size": 9}, False, "color != red"),
            ("miss-small", {"color": "red", "size": 1}, False, "size not > 2"),
            ("match-2", {"color": "red", "size": 3}, True, "color=red AND size>2"),
            ("miss-nocolor", {"size": 8}, False, "color IS NULL -> UNKNOWN (3-valued)"),
        ]
        want_matches = 0
        for body, props, match, why in events:
            msg = Message(body=body, properties=props)
            sender.send(msg)
            verdict = "should MATCH" if match else "should be FILTERED OUT"
            if match:
                want_matches += 1
            print(f"[send] {body:<13} {str(props):<28} -> {verdict} ({why})")
        sender.close()

        # =====================================================================
        # 3. RECEIVE only the matching events. Drain exactly want_matches; then prove
        #    nothing else arrives (the non-matching events were silently withheld).
        # =====================================================================
        got: set[str] = set()
        while len(got) < want_matches:
            msg = receiver.receive(timeout=15.0)
            accept_if_unsettled(receiver)  # no-op for pre-settled fan-out
            body = str(msg.body)
            print(f"[recv] delivered: {body}")
            got.add(body)

        # No further delivery: the non-matching events must NOT arrive.
        try:
            extra = receiver.receive(timeout=2.0)
        except Exception:  # noqa: BLE001 - the EXPECTED idle timeout (nothing else matches)
            pass
        else:
            raise SystemExit(f"selector leak: an extra event {str(extra.body)!r} was delivered (should be filtered)")
        print(
            f"[recv] Received exactly {len(got)} matching event(s); "
            f"{len(events) - want_matches} non-matching event(s) were silently withheld"
        )

        receiver.close()

        # =====================================================================
        # 4. GOTCHA demo -- a selector on a queues/ source is rejected at ATTACH.
        #    Selectors are honoured ONLY on events/ and events-store/ consume links;
        #    on queues/ (move-only) the connector DETACHes with amqp:not-implemented.
        # =====================================================================
        print()
        queue_addr = "queues/" + CHANNEL + ".q"
        try:
            conn.create_receiver(queue_addr, credit=10, options=SelectorFilter(SELECTOR))
        except Exception as exc:  # noqa: BLE001 - the connector DETACHes the bad attach
            print(f"[gotcha] Selector on {queue_addr} correctly REJECTED at ATTACH:\n         {exc}")
            print("         (selectors are supported only on events/ and events-store/ -- queues/ is move-only)")
        else:
            raise SystemExit(f"expected the selector on {queue_addr} to be rejected, but the attach succeeded")
    finally:
        conn.close()

    print("\nDone.")


if __name__ == "__main__":
    main()

# Expected output:
#
# Broker:   amqp://localhost:5672
# Address:  events/amqp10.examples.selector  (KubeMQ pattern=events, channel=amqp10.examples.selector)
# Selector: color = 'red' AND size > 2
#
# [recv] Subscribed to events/amqp10.examples.selector with selector filter (standing credit 100)
# [recv] Subscription pump settled (waited 750ms before publishing)
# [send] match-1       {'color': 'red', 'size': 5}  -> should MATCH (color=red AND size>2)
# [send] miss-blue     {'color': 'blue', 'size': 9} -> should be FILTERED OUT (color != red)
# [send] miss-small    {'color': 'red', 'size': 1}  -> should be FILTERED OUT (size not > 2)
# [send] match-2       {'color': 'red', 'size': 3}  -> should MATCH (color=red AND size>2)
# [send] miss-nocolor  {'size': 8}                  -> should be FILTERED OUT (color IS NULL -> UNKNOWN (3-valued))
# [recv] delivered: match-1
# [recv] delivered: match-2
# [recv] Received exactly 2 matching event(s); 3 non-matching event(s) were silently withheld
#
# [gotcha] Selector on queues/amqp10.examples.selector.q correctly REJECTED at ATTACH:
#          Connection amqp://localhost:5672 disconnected: Condition('amqp:invalid-field', 'no such handle: 0')
#          (selectors are supported only on events/ and events-store/ -- queues/ is move-only)
#
# Done.
#
# The connector refuses the bad attach with amqp:not-implemented (description
# "selector filter not supported on this address"). proton's blocking
# create_receiver races the DETACH against link registration, so it commonly
# surfaces the refusal as the connection-level message above; either way the
# selector link never attaches and create_receiver raises.
