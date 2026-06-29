# Python — Advanced / Anonymous Terminus

An **anonymous sender** (a link attached with a **null target** —
`create_sender(None)`) carries no fixed channel. Instead, **each message** selects
its own destination via `Message.address` (the AMQP `properties.to` field), and
the KubeMQ connector routes it **per-message** to the right pattern/channel. One
link, many destinations. Driven with the native `python-qpid-proton` blocking
client.

## Prerequisites

- Python 3.10+
- `python-qpid-proton` 0.40.0 (pinned in `examples/python/pyproject.toml`; the
  prebuilt `python-qpid-proton-wheel` installs without a C toolchain)
- A running KubeMQ broker with the AMQP 1.0 connector (enabled by default),
  reachable at `KUBEMQ_AMQP_URL` (default `amqp://localhost:5672`).

## How to Run

```bash
cd examples/python
uv sync
uv run python advanced/anonymous_terminus/main.py
```

Override the broker URL:

```bash
KUBEMQ_AMQP_URL=amqp://my-server:5672 uv run python advanced/anonymous_terminus/main.py
```

## Expected Output

```
Broker: amqp://localhost:5672
Anonymous sender (null target) -- routes per-message via Message.address (properties.to)
  msg #1 to: queues/amqp10.examples.anon.q
  msg #2 to: events/amqp10.examples.anon.e

[attach] Anonymous sender attached (null target)
[send] msg #1 routed to queues/amqp10.examples.anon.q (accepted)
[send] msg #2 routed to events/amqp10.examples.anon.e (accepted)
[send] msg with bad `to`='bogus/prefix/x' rejected as expected: <amqp:precondition-failed ...>
[send] msg with NO `to` rejected as expected: <amqp:precondition-failed ...>
[recv] queue queues/amqp10.examples.anon.q delivered: 'to-queue'
[recv] events events/amqp10.examples.anon.e delivered: 'to-events'

Done.
```

## What's Happening

1. **ATTACH an anonymous sender** — `create_sender(None)` attaches a link with a
   **null target** (no bound channel).
2. **Subscribe the events consumer first** — Events are fire-and-forget (no
   replay), so the `events/<ch>` receiver must attach **before** the publish; the
   queue message is durable, so it is consumed afterwards.
3. **Send #1 → a queue** — `Message(address="queues/<ch>")`. The connector reads
   `properties.to`, resolves `queues/<ch>`, authorizes WRITE for this connection
   (per-message Casbin check), and stores it.
4. **Send #2 → an events topic** — `Message(address="events/<ch>")` on the **same**
   anonymous link, a **different** pattern. The subscriber receives it.
5. **Negative cases** — a bad `to` (`bogus/prefix/x`) and a **missing** `to` are
   both rejected by the connector with **`amqp:precondition-failed`**, surfaced as
   a `send()` exception. The anonymous link stays usable afterwards.
6. **Verify routing** — the queue message is consumed back and the event is
   received, proving both landed at the right destination.

Frame-level: `ATTACH(target=null)` → `TRANSFER(properties.to=queues/…)` →
`DISPOSITION(accept)` → `TRANSFER(properties.to=events/…)` → … the bad/missing-`to`
transfers come back as **rejected/precondition-failed** dispositions.

## AMQP 1.0 specifics

| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |
|---|---|---|---|---|---|---|---|
| sender (client → KubeMQ) | **anonymous** (null target) | unsettled (default) | server-granted | `accepted` per valid `to`; `rejected`/`amqp:precondition-failed` for bad/missing `to` | per-message `to` (`Message.address`) | `Data` | routing decided per message; per-message WRITE authorization |
| receiver — queue (KubeMQ → client) | source `queues/<ch>` | `first` (default) | client-granted `credit=1` | `accepted` | none | `Data` | confirms the queue route |
| receiver — events (KubeMQ → client) | source `events/<ch>` | `first` (default) | client-granted `credit=5` | n/a (at-most-once) | none | `Data` | subscribe **before** publish |

## Gotchas

> **Anonymous routing is driven by a NULL target address, NOT by an
> `ANONYMOUS-RELAY` capability.** The connector advertises **no** offered/desired
> capabilities (`conn.go` `serverOpen` sets only container-id / max-frame-size /
> channel-max / idle-timeout), so clients must **not** depend on capability
> negotiation — attach a link with a null target and set `Message.address` per
> message. (This is exactly why the Qpid JMS variant of this example is N/A — JMS
> exposes no API to force a raw null-target link.)

- **A bad or missing `to` is a hard reject.** Unknown prefix or absent
  `Message.address` → `amqp:precondition-failed` on the send; the link survives.
- **Use explicit `<pattern>/<channel>` in `to`.** e.g. `queues/...`, `events/...`
  — never rely on a default pattern.

## Related Examples

- [commands/request_reply_dynamic_node](../../commands/request_reply_dynamic_node/) — the responder's reply leg is exactly this null-target sender pattern
- [queues/basic_send_receive](../../queues/basic_send_receive/) — a fixed-target queue sender (the non-anonymous baseline)
- [events/basic_pubsub](../../events/basic_pubsub/) — fixed-target events fan-out (subscribe-before-publish)

---

Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
JWT see `connectivity/auth` + `guides/authentication.md`.
