"""Example: connectivity/auth (master-table variant #13).

The ONE runnable authentication variant. It connects to the KubeMQ AMQP 1.0
connector with SASL PLAIN -- the username is AUDIT-ONLY and the password is a
KubeMQ JWT -- then runs a queues/<ch> round-trip. Driven with the native
``python-qpid-proton`` blocking client (NO KubeMQ SDK).

Identity precedence (connector contract):
  * With authentication ENABLED, the JWT in the SASL PLAIN *password* must
    validate; the ClientID/identity is derived from the verified token. The SASL
    *username* is recorded for audit (auth.success / auth.failure) only.
  * With authentication DISABLED (the stock dev-broker default), the SASL PLAIN
    *username* becomes the ClientID and any password is accepted; with ANONYMOUS,
    a default identity is used.

CLONE-AND-RUN behavior: a stock dev broker has authentication OFF and accepts
ANONYMOUS, so this example reads the credentials from the environment and falls
back to ANONYMOUS when they are unset -- it runs cleanly either way.

    KUBEMQ_AMQP_USER  -- SASL PLAIN username (audit identity; defaults to a label)
    KUBEMQ_AMQP_JWT   -- SASL PLAIN password = a KubeMQ JWT (required to use PLAIN)

If KUBEMQ_AMQP_JWT is set, the example dials SASL PLAIN; otherwise it dials
ANONYMOUS and prints a clear note.

Grounded in connector tests TestAuthorizationReadDenied and
TestAuthenticationBadCredential (connectors/amqp10/integration_test.go).

Run (ANONYMOUS, stock dev broker)::

    export KUBEMQ_AMQP_URL=amqp://localhost:5672
    uv run python connectivity/auth/main.py

Run (SASL PLAIN with a KubeMQ JWT, auth-enabled broker)::

    export KUBEMQ_AMQP_USER=my-service
    export KUBEMQ_AMQP_JWT=<a-kubemq-jwt>
    uv run python connectivity/auth/main.py
"""

from __future__ import annotations

import os

from proton import Message
from proton.utils import BlockingConnection

# channel is the KubeMQ queue channel; the link address is "queues/" + channel
# (explicit prefix -- never rely on a default pattern).
CHANNEL = "amqp10.examples.auth"


def amqp_url() -> str:
    return os.environ.get("KUBEMQ_AMQP_URL", "amqp://localhost:5672")


def main() -> None:
    addr = "queues/" + CHANNEL
    print(f"Broker:  {amqp_url()}")
    print(f"Address: {addr}  (KubeMQ pattern=queues, channel={CHANNEL})")

    # Choose the SASL mechanism from the environment so the example clone-and-runs
    # on a stock dev broker (auth OFF, ANONYMOUS) yet also demonstrates SASL PLAIN
    # with a KubeMQ JWT when credentials are provided.
    user = os.environ.get("KUBEMQ_AMQP_USER", "")
    jwt = os.environ.get("KUBEMQ_AMQP_JWT", "")

    if jwt:
        if not user:
            user = "amqp10-example"  # audit-only label; identity comes from the JWT
        # SASL PLAIN: username is AUDIT-ONLY; password is the KubeMQ JWT.
        # allow_insecure_mechs=True permits PLAIN over a plaintext amqp:// socket
        # (a stock dev broker); use amqps:// + TLS in production.
        conn_kwargs = {
            "user": user,
            "password": jwt,
            "allowed_mechs": "PLAIN",
            "allow_insecure_mechs": True,
        }
        print(f'Auth:    SASL PLAIN  (username="{user}" audit-only; password=<KubeMQ JWT>)\n')
    else:
        # Stock dev-broker default: ANONYMOUS.
        conn_kwargs = {"allowed_mechs": "ANONYMOUS"}
        print("Auth:    ANONYMOUS  (KUBEMQ_AMQP_JWT unset -- falling back to the dev-broker default)")
        print("         Set KUBEMQ_AMQP_USER + KUBEMQ_AMQP_JWT to dial SASL PLAIN with a KubeMQ JWT.\n")

    # 1. OPEN -- the SASL handshake happens here. With auth ENABLED, a JWT that
    #    fails validation makes the connect fail (amqp:unauthorized-access at the
    #    SASL layer -- see TestAuthenticationBadCredential). With auth DISABLED, any
    #    credential is accepted.
    conn = BlockingConnection(amqp_url(), **conn_kwargs)
    print("[open] Connected -- SASL handshake accepted")

    # 2. ATTACH + SEND -- the WRITE authorization check runs at sender attach /
    #    send. With authorization ENABLED, an identity without a WRITE grant on this
    #    channel is refused with amqp:unauthorized-access (see
    #    TestAuthorizationReadDenied for the READ-attach counterpart).
    sender = conn.create_sender(addr)
    body = "auth-round-trip"
    sender.send(Message(body=body))
    sender.close()
    print(f"[send] Produced 1 message to {addr} (accepted)")

    # 3. ATTACH + RECEIVE -- the READ authorization check runs at receiver attach.
    #    A denied identity's receiver attach is refused with
    #    amqp:unauthorized-access (TestAuthorizationReadDenied).
    receiver = conn.create_receiver(addr, credit=1)
    msg = receiver.receive(timeout=30.0)
    receiver.accept()
    print(f"[recv] Consumed and accepted 1 message: {str(msg.body)!r}")

    receiver.close()
    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()

# Expected output (ANONYMOUS, stock dev broker -- no env set):
#
# Broker:  amqp://localhost:5672
# Address: queues/amqp10.examples.auth  (KubeMQ pattern=queues, channel=amqp10.examples.auth)
# Auth:    ANONYMOUS  (KUBEMQ_AMQP_JWT unset -- falling back to the dev-broker default)
#          Set KUBEMQ_AMQP_USER + KUBEMQ_AMQP_JWT to dial SASL PLAIN with a KubeMQ JWT.
#
# [open] Connected -- SASL handshake accepted
# [send] Produced 1 message to queues/amqp10.examples.auth (accepted)
# [recv] Consumed and accepted 1 message: 'auth-round-trip'
#
# Done.
#
# Expected output (SASL PLAIN, KUBEMQ_AMQP_USER + KUBEMQ_AMQP_JWT set):
#
# Broker:  amqp://localhost:5672
# Address: queues/amqp10.examples.auth  (KubeMQ pattern=queues, channel=amqp10.examples.auth)
# Auth:    SASL PLAIN  (username="my-service" audit-only; password=<KubeMQ JWT>)
#
# [open] Connected -- SASL handshake accepted
# [send] Produced 1 message to queues/amqp10.examples.auth (accepted)
# [recv] Consumed and accepted 1 message: 'auth-round-trip'
#
# Done.
