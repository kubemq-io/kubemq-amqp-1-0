// Example: events/selector (master-table variant #6)
//
// JMS / SQL-92 message selectors over KubeMQ **Events** with the native
// github.com/Azure/go-amqp client (NO KubeMQ SDK).
//
// A receiver attaches to events/<ch> carrying a selector source-filter
// (amqp.NewSelectorFilter), which go-amqp encodes under the OASIS-standard key
// "apache.org:selector-filter:string". The connector evaluates the selector
// against each event's APPLICATION PROPERTIES and delivers ONLY the matching
// events; non-matching events are silently withheld (copy semantics — they stay
// available to OTHER subscribers, they are not consumed/discarded).
//
// The selector here is:  color = 'red' AND size > 2
//
// We publish 5 events and assert exactly 2 are delivered:
//
//	match-1     {color:red,  size:5}  ✅ delivered
//	miss-blue   {color:blue, size:9}  ❌ color≠red
//	miss-small  {color:red,  size:1}  ❌ size≯2
//	match-2     {color:red,  size:3}  ✅ delivered
//	miss-nocolor{          size:8}    ❌ color IS NULL  (3-valued logic: UNKNOWN ⇒ withheld)
//
// THREE-VALUED LOGIC: a property that is absent evaluates to NULL, so the
// predicate is UNKNOWN (not true) and the event is NOT delivered — this is why
// miss-nocolor is withheld even though it has no color to disqualify it.
//
// GOTCHA: a selector is honoured ONLY on events/ and events-store/ consume
// links. Requesting one on a queues/ source is rejected at ATTACH with
// amqp:not-implemented ("selector filter not supported on this address") —
// see the README. This program demonstrates that rejection at the end.
//
// Grounded in connector test TestEventsSelector
// (connectors/amqp10/integration_pubsub_test.go) and the selector-on-queues
// rejection in connectors/amqp10/link.go (applySourceSelector).
//
// Run:
//
//	export KUBEMQ_AMQP_URL=amqp://localhost:5672
//	go run ./events/selector
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/Azure/go-amqp"
)

const channel = "amqp10.examples.selector"

// selector is a standard SQL-92 / JMS message selector evaluated against each
// event's application properties.
const selector = "color = 'red' AND size > 2"

// standingCredit is granted up front so the subscriber is never at 0 credit when
// a matching event arrives (events are at-most-once; 0-credit ⇒ silent drop).
const standingCredit = 100

func amqpURL() string {
	if v := os.Getenv("KUBEMQ_AMQP_URL"); v != "" {
		return v
	}
	return "amqp://localhost:5672"
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	addr := "events/" + channel
	fmt.Printf("Broker:   %s\n", amqpURL())
	fmt.Printf("Address:  %s  (KubeMQ pattern=events, channel=%s)\n", addr, channel)
	fmt.Printf("Selector: %s\n\n", selector)

	conn, err := amqp.Dial(ctx, amqpURL(), nil)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		log.Fatalf("new session: %v", err)
	}

	// =========================================================================
	// 1. SUBSCRIBE FIRST with the selector filter. go-amqp encodes the selector
	//    under the OASIS key "apache.org:selector-filter:string". A successful
	//    NewReceiver means the connector accepted (and echoed) the filter — a
	//    parse error or unsupported pattern would have DETACHed the link.
	//    Events have no replay, so we subscribe before publishing.
	// =========================================================================
	receiver, err := session.NewReceiver(ctx, addr, &amqp.ReceiverOptions{
		Credit:  standingCredit,
		Filters: []amqp.LinkFilter{amqp.NewSelectorFilter(selector)},
	})
	if err != nil {
		log.Fatalf("new receiver (selector): %v", err)
	}
	fmt.Printf("[recv] Subscribed to %s with selector filter (standing credit %d)\n", addr, standingCredit)

	// The attach reply confirms the link; wait for the connector's subscription
	// pump to go live before publishing (a publish that races the subscription is
	// lost — no replay on Events).
	time.Sleep(750 * time.Millisecond)
	fmt.Println("[recv] Subscription pump settled (waited 750ms before publishing)")

	// =========================================================================
	// 2. PUBLISH 5 events with application properties. The sender is pre-settled
	//    (fire-and-forget). The connector evaluates the selector against each
	//    event's application properties on the delivery path.
	// =========================================================================
	sender, err := session.NewSender(ctx, addr, &amqp.SenderOptions{
		SettlementMode: amqp.SenderSettleModeSettled.Ptr(),
	})
	if err != nil {
		log.Fatalf("new sender: %v", err)
	}

	type event struct {
		body  string
		props map[string]any
		match bool
		why   string
	}
	events := []event{
		{"match-1", map[string]any{"color": "red", "size": int64(5)}, true, "color=red AND size>2"},
		{"miss-blue", map[string]any{"color": "blue", "size": int64(9)}, false, "color≠red"},
		{"miss-small", map[string]any{"color": "red", "size": int64(1)}, false, "size≯2"},
		{"match-2", map[string]any{"color": "red", "size": int64(3)}, true, "color=red AND size>2"},
		{"miss-nocolor", map[string]any{"size": int64(8)}, false, "color IS NULL ⇒ UNKNOWN (3-valued)"},
	}

	wantMatches := 0
	for _, e := range events {
		msg := amqp.NewMessage([]byte(e.body))
		msg.ApplicationProperties = e.props
		sendCtx, sendCancel := context.WithTimeout(ctx, 15*time.Second)
		err := sender.Send(sendCtx, msg, nil)
		sendCancel()
		if err != nil {
			log.Fatalf("publish %s: %v", e.body, err)
		}
		verdict := "should be FILTERED OUT"
		if e.match {
			verdict = "should MATCH"
			wantMatches++
		}
		fmt.Printf("[send] %-13s %-28s → %s (%s)\n", e.body, fmt.Sprintf("%v", e.props), verdict, e.why)
	}
	_ = sender.Close(ctx)

	// =========================================================================
	// 3. RECEIVE only the matching events. Drain exactly wantMatches; then prove
	//    nothing else arrives (the non-matching events were silently withheld).
	// =========================================================================
	got := make(map[string]struct{}, wantMatches)
	for len(got) < wantMatches {
		rcvCtx, rcvCancel := context.WithTimeout(ctx, 15*time.Second)
		msg, err := receiver.Receive(rcvCtx, nil)
		rcvCancel()
		if err != nil {
			log.Fatalf("receive (%d/%d matching): %v", len(got), wantMatches, err)
		}
		_ = receiver.AcceptMessage(ctx, msg) // no-op for pre-settled fan-out
		body := string(msg.GetData())
		fmt.Printf("[recv] delivered: %s\n", body)
		got[body] = struct{}{}
	}

	// No further delivery: the non-matching events must NOT arrive.
	emptyCtx, emptyCancel := context.WithTimeout(ctx, 2*time.Second)
	extra, err := receiver.Receive(emptyCtx, nil)
	emptyCancel()
	if err == nil {
		log.Fatalf("selector leak: an extra event %q was delivered (should have been filtered)", string(extra.GetData()))
	}
	fmt.Printf("[recv] Received exactly %d matching event(s); %d non-matching event(s) were silently withheld\n",
		len(got), len(events)-wantMatches)

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = receiver.Close(closeCtx)
	closeCancel()

	// =========================================================================
	// 4. GOTCHA demo — a selector on a queues/ source is rejected at ATTACH.
	//    Selectors are honoured ONLY on events/ and events-store/ consume links;
	//    on queues/ (move-only) the connector DETACHes with amqp:not-implemented.
	// =========================================================================
	fmt.Println()
	queueAddr := "queues/" + channel + ".q"
	_, err = session.NewReceiver(ctx, queueAddr, &amqp.ReceiverOptions{
		Credit:  10,
		Filters: []amqp.LinkFilter{amqp.NewSelectorFilter(selector)},
	})
	if err == nil {
		log.Fatalf("expected the selector on %s to be rejected, but the attach succeeded", queueAddr)
	}
	fmt.Printf("[gotcha] Selector on %s correctly REJECTED at ATTACH:\n         %v\n", queueAddr, err)
	fmt.Println("         (selectors are supported only on events/ and events-store/ — queues/ is move-only)")

	fmt.Println("\nDone.")
}

// Expected output:
//
// Broker:   amqp://localhost:5672
// Address:  events/amqp10.examples.selector  (KubeMQ pattern=events, channel=amqp10.examples.selector)
// Selector: color = 'red' AND size > 2
//
// [recv] Subscribed to events/amqp10.examples.selector with selector filter (standing credit 100)
// [recv] Subscription pump settled (waited 750ms before publishing)
// [send] match-1       map[color:red size:5]        → should MATCH (color=red AND size>2)
// [send] miss-blue     map[color:blue size:9]       → should be FILTERED OUT (color≠red)
// [send] miss-small    map[color:red size:1]        → should be FILTERED OUT (size≯2)
// [send] match-2       map[color:red size:3]        → should MATCH (color=red AND size>2)
// [send] miss-nocolor  map[size:8]                  → should be FILTERED OUT (color IS NULL ⇒ UNKNOWN (3-valued))
// [recv] delivered: match-1
// [recv] delivered: match-2
// [recv] Received exactly 2 matching event(s); 3 non-matching event(s) were silently withheld
//
// [gotcha] Selector on queues/amqp10.examples.selector.q correctly REJECTED at ATTACH:
//          received detach frame with unknown link handle 0
//          (selectors are supported only on events/ and events-store/ — queues/ is move-only)
//
// Done.
//
// The connector DETACHes the bad attach with amqp:not-implemented (description
// "selector filter not supported on this address"). go-amqp v1.7.0 reports a
// detach that races link registration as the generic message above; either way
// NewReceiver returns an error and the selector link never attaches.
