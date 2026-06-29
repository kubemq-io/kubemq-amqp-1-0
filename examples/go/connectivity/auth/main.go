// Example: connectivity/auth (master-table variant #13)
//
// The ONE runnable authentication variant. It connects to the KubeMQ AMQP 1.0
// connector with SASL PLAIN — the username is AUDIT-ONLY and the password is a
// KubeMQ JWT — then runs a queues/<ch> round-trip. Driven with the native
// github.com/Azure/go-amqp client (NO KubeMQ SDK).
//
// Identity precedence (connector contract):
//   - With authentication ENABLED, the JWT in the SASL PLAIN *password* must
//     validate; the ClientID/identity is derived from the verified token. The
//     SASL *username* is recorded for audit (auth.success / auth.failure) only.
//   - With authentication DISABLED (the stock dev-broker default), the SASL
//     PLAIN *username* becomes the ClientID and any password is accepted; with
//     ANONYMOUS, a default identity is used.
//
// CLONE-AND-RUN behavior: a stock dev broker has authentication OFF and accepts
// ANONYMOUS, so this example reads the credentials from the environment and
// falls back to ANONYMOUS when they are unset — it runs cleanly either way.
//
//	KUBEMQ_AMQP_USER  — SASL PLAIN username (audit identity; defaults to a label)
//	KUBEMQ_AMQP_JWT   — SASL PLAIN password = a KubeMQ JWT (required to use PLAIN)
//
// If KUBEMQ_AMQP_JWT is set, the example dials SASL PLAIN; otherwise it dials
// ANONYMOUS and prints a clear note.
//
// Grounded in connector tests TestAuthorizationReadDenied and
// TestAuthenticationBadCredential (connectors/amqp10/integration_test.go).
//
// Run (ANONYMOUS, stock dev broker):
//
//	export KUBEMQ_AMQP_URL=amqp://localhost:5672
//	go run ./connectivity/auth
//
// Run (SASL PLAIN with a KubeMQ JWT, auth-enabled broker):
//
//	export KUBEMQ_AMQP_USER=my-service
//	export KUBEMQ_AMQP_JWT=<a-kubemq-jwt>
//	go run ./connectivity/auth
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/Azure/go-amqp"
)

// channel is the KubeMQ queue channel; the link address is "queues/" + channel
// (explicit prefix — never rely on DefaultPattern).
const channel = "amqp10.examples.auth"

func amqpURL() string {
	if v := os.Getenv("KUBEMQ_AMQP_URL"); v != "" {
		return v
	}
	return "amqp://localhost:5672"
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	addr := "queues/" + channel
	fmt.Printf("Broker:  %s\n", amqpURL())
	fmt.Printf("Address: %s  (KubeMQ pattern=queues, channel=%s)\n", addr, channel)

	// Choose the SASL mechanism from the environment so the example clone-and-runs
	// on a stock dev broker (auth OFF, ANONYMOUS) yet also demonstrates SASL PLAIN
	// with a KubeMQ JWT when credentials are provided.
	user := os.Getenv("KUBEMQ_AMQP_USER")
	jwt := os.Getenv("KUBEMQ_AMQP_JWT")

	var connOpts *amqp.ConnOptions
	if jwt != "" {
		if user == "" {
			user = "amqp10-example" // audit-only label; identity comes from the JWT
		}
		// SASL PLAIN: username is AUDIT-ONLY; password is the KubeMQ JWT.
		connOpts = &amqp.ConnOptions{SASLType: amqp.SASLTypePlain(user, jwt)}
		fmt.Printf("Auth:    SASL PLAIN  (username=%q audit-only; password=<KubeMQ JWT>)\n\n", user)
	} else {
		// Stock dev-broker default: ANONYMOUS. (Explicit here for clarity; passing
		// nil ConnOptions also negotiates ANONYMOUS.)
		connOpts = &amqp.ConnOptions{SASLType: amqp.SASLTypeAnonymous()}
		fmt.Printf("Auth:    ANONYMOUS  (KUBEMQ_AMQP_JWT unset — falling back to the dev-broker default)\n")
		fmt.Printf("         Set KUBEMQ_AMQP_USER + KUBEMQ_AMQP_JWT to dial SASL PLAIN with a KubeMQ JWT.\n\n")
	}

	// =========================================================================
	// 1. OPEN — the SASL handshake happens here. With auth ENABLED, a JWT that
	//    fails validation makes Dial fail (amqp:unauthorized-access at the SASL
	//    layer — see TestAuthenticationBadCredential). With auth DISABLED, any
	//    credential is accepted.
	// =========================================================================
	conn, err := amqp.Dial(ctx, amqpURL(), connOpts)
	if err != nil {
		log.Fatalf("dial (SASL handshake failed — bad/expired JWT? auth-disabled broker?): %v", err)
	}
	defer func() { _ = conn.Close() }()
	fmt.Printf("[open] Connected — SASL handshake accepted\n")

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		log.Fatalf("new session: %v", err)
	}

	// =========================================================================
	// 2. ATTACH + SEND — the WRITE authorization check runs at sender attach /
	//    send. With authorization ENABLED, an identity without a WRITE grant on
	//    this channel is refused with amqp:unauthorized-access (see
	//    TestAuthorizationReadDenied for the READ-attach counterpart).
	// =========================================================================
	sender, err := session.NewSender(ctx, addr, nil)
	if err != nil {
		log.Fatalf("new sender (authorization denied? amqp:unauthorized-access): %v", err)
	}
	body := "auth-round-trip"
	sendCtx, sendCancel := context.WithTimeout(ctx, 15*time.Second)
	err = sender.Send(sendCtx, amqp.NewMessage([]byte(body)), nil)
	sendCancel()
	if err != nil {
		log.Fatalf("send: %v", err)
	}
	_ = sender.Close(ctx)
	fmt.Printf("[send] Produced 1 message to %s (accepted)\n", addr)

	// =========================================================================
	// 3. ATTACH + RECEIVE — the READ authorization check runs at receiver attach.
	//    A denied identity's receiver attach is refused with
	//    amqp:unauthorized-access (TestAuthorizationReadDenied).
	// =========================================================================
	receiver, err := session.NewReceiver(ctx, addr, &amqp.ReceiverOptions{Credit: 1})
	if err != nil {
		log.Fatalf("new receiver (authorization denied? amqp:unauthorized-access): %v", err)
	}
	rcvCtx, rcvCancel := context.WithTimeout(ctx, 30*time.Second)
	msg, err := receiver.Receive(rcvCtx, nil)
	rcvCancel()
	if err != nil {
		log.Fatalf("receive: %v", err)
	}
	if err := receiver.AcceptMessage(ctx, msg); err != nil {
		log.Fatalf("accept: %v", err)
	}
	fmt.Printf("[recv] Consumed and accepted 1 message: %q\n", string(msg.GetData()))

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = receiver.Close(closeCtx)
	closeCancel()

	fmt.Println("\nDone.")
}

// Expected output (ANONYMOUS, stock dev broker — no env set):
//
// Broker:  amqp://localhost:5672
// Address: queues/amqp10.examples.auth  (KubeMQ pattern=queues, channel=amqp10.examples.auth)
// Auth:    ANONYMOUS  (KUBEMQ_AMQP_JWT unset — falling back to the dev-broker default)
//          Set KUBEMQ_AMQP_USER + KUBEMQ_AMQP_JWT to dial SASL PLAIN with a KubeMQ JWT.
//
// [open] Connected — SASL handshake accepted
// [send] Produced 1 message to queues/amqp10.examples.auth (accepted)
// [recv] Consumed and accepted 1 message: "auth-round-trip"
//
// Done.
//
// Expected output (SASL PLAIN, KUBEMQ_AMQP_USER + KUBEMQ_AMQP_JWT set):
//
// Broker:  amqp://localhost:5672
// Address: queues/amqp10.examples.auth  (KubeMQ pattern=queues, channel=amqp10.examples.auth)
// Auth:    SASL PLAIN  (username="my-service" audit-only; password=<KubeMQ JWT>)
//
// [open] Connected — SASL handshake accepted
// [send] Produced 1 message to queues/amqp10.examples.auth (accepted)
// [recv] Consumed and accepted 1 message: "auth-round-trip"
//
// Done.
