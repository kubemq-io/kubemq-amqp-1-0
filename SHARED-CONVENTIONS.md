# SHARED CONVENTIONS — kubemq-amqp-1-0

The conventions every example, every doc, and the burn-in harness in this repo
follow. They are the single source of truth for addressing, the connection
variable, auth defaults, the per-example README shape, and the settlement /
credit / channel-naming rules. On any conflict with prose elsewhere, **the merged
connector code wins** (`connectors/amqp10/`,
`connectors/amqpmux/`).

---

## 1. Connection

Every example reads a **single** environment variable:

```bash
# default: amqp://localhost:5672
export KUBEMQ_AMQP_URL="amqp://localhost:5672"
```

| Scheme | Transport | Endpoint |
|--------|-----------|----------|
| `amqp://` | Plain TCP (SASL ANONYMOUS / PLAIN) | `amqp://localhost:5672` |
| `amqps://` | TLS over TCP (**documentation-only**) | `amqps://localhost:5671` |

- Port **5672** (plain) and **5671** (TLS) are served by the shared `amqpmux`
 front door — they are **shared with the AMQP 0-9-1 connector**, which dedupes
 the bind by reading the 8-byte protocol header.
- The connector is **enabled by default** (`CONNECTORS_AMQP10_ENABLE=true`).
- The burn-in harness uses a different variable: **`KUBEMQ_BROKER_ADDRESS`**
 (default `localhost:5672`), not `KUBEMQ_AMQP_URL`.

## 2. Authentication — ANONYMOUS by default

- **Runnable default: SASL ANONYMOUS.** A `KUBEMQ_AMQP_URL` with no userinfo
 negotiates ANONYMOUS, so every example is clone-and-run on a stock dev broker
 with auth off. No example requires credentials except `connectivity/auth`.
- **Documented production contract: SASL PLAIN.** The username is informational /
 audit-only; the **password carries the KubeMQ JWT**. Form:
 `amqp://<user>:<JWT>@host:5672`.
- **EXTERNAL (mTLS)** maps the client-certificate CN to the ClientID; it is
 documentation-only (see `docs/guides/tls-and-mtls.md`).
- The connector advertises **no `ANONYMOUS-RELAY`** and no negotiated
 capabilities — clients must not depend on capability negotiation.

Every per-example README ends with the same banner:

> Runs ANONYMOUS by default on a stock dev broker; for SASL PLAIN with a KubeMQ
> JWT see `connectivity/auth` + `docs/guides/authentication.md`.

## 3. Addressing — always the explicit prefix

A node address is `[/]<pattern>/<channel>`. The leading **pattern** selects the
KubeMQ pattern; the remainder is the KubeMQ channel.

| Pattern prefix | KubeMQ pattern |
|----------------|----------------|
| `queues/` | Queues |
| `events/` | Events |
| `events-store/` | Events-Store |
| `commands/` | Commands (RPC) |
| `queries/` | Queries (RPC) |
| `responses/` | RPC reply path — **write-only**, connector-managed (`/responses/<RequestID>`) |

Rules every example obeys:

- **Always emit the explicit prefix.** Never rely on a bare address or the
 server's `DefaultPattern` (it is non-deterministic by design — a bare address
 resolves by JMS capability hint, else `DefaultPattern`). Bare addressing is
 documented only as a Qpid-JMS migration convenience.
- **Longest-prefix wins.** `events-store/` is matched before `events/`.
- **A leading slash is optional** — at most one is stripped before matching.
- **`/responses/<RequestID>`** is the connector-owned RPC reply node; a receiver
 attach on it is refused with `amqp:not-allowed`.

### Channel naming

- **Charset** (stricter than the array layer): non-empty, ≤255 chars, no trailing
 `.`, no whitespace, no `*` `>` `;` `:`. Any violation → `amqp:not-found`.
- **Convention in this repo:** example channels are named
 **`amqp10.examples.<short>`** (e.g. `queues/amqp10.examples.basic`,
 `events/amqp10.examples.pubsub`). The burn-in uses
 `<pattern>/amqp10.burnin.<worker>.<idx:04d>`.
- Use `.`-segmented channel names (KubeMQ's hierarchy separator); never use `/`
 inside a channel — `/` is the reserved pattern separator.

## 4. The five patterns at a glance

| Pattern | Produce (client → server target) | Consume (client ← server source) |
|---------|----------------------------------|----------------------------------|
| Queues | at-least-once enqueue; `accepted` DISPOSITION per send | credit-driven destructive consume; `accept`/`release`/`modify`/`reject` |
| Events | pre-settled fan-out (at-most-once) | standing-credit fan-out; **0-credit → silent drop** |
| Events-Store | persisted append | durable replay/resume; start positions via `x-opt-kubemq-start` |
| Commands | request + dynamic reply node | reply carries `x-opt-kubemq-executed` / `x-opt-kubemq-error` |
| Queries | request + dynamic reply node | reply = body + metadata only; failure ⇒ timeout |

## 5. Settlement & credit defaults

| Aspect | Default in examples | Notes |
|--------|---------------------|-------|
| Sender settle mode (queues) | **unsettled** (at-least-once) | the server DISPOSITIONs `accepted` per send |
| Sender settle mode (events / events-store) | **pre-settled** (at-most-once) | the connector sends events TRANSFERs pre-settled |
| Receiver settle mode | **`first`** | `rcv-settle-mode=second` is unsupported → `amqp:not-implemented` |
| Consumer credit | a **standing window**, replenished as messages settle | the server never delivers without credit |
| `accept` | removes the message (AckRange) | |
| `release` / `modify` | requeue (NAckRange); **increment the receive-count** | grows delivery-count toward the broker `MaxReceiveQueue` poison cap |
| `reject` | discards (AckRange, no requeue) | |
| Body sections | **`Data`** (default) or **`AmqpValue`**; empty body OK | `AmqpSequence` is rejected → `amqp:not-implemented` |
| Reliability primitive | **settlement** (accept/release/reject) | there are **no AMQP transactions** |

### The two data-loss footguns (first-class)

1. **Events drop silently at zero credit.** Events are pre-settled at-most-once;
 a message arriving at a consumer with no credit is **dropped and counted**
 (`kubemq_amqp10_events_dropped_no_credit_total`). Keep credit topped up and
 subscribe-before-publish.
2. **Events-Store stalls and loses its window if credit is not replenished.**
 When a durable consumer's unsettled buffer fills, the new message plus the
 buffered window is dropped and the link is DETACHed with
 `amqp:resource-limit-exceeded`.

## 6. The 13-variant master table

Sequence: queues → events → events-store → commands → queries → advanced →
connectivity. Every variant is traceable to a connector integration `Test*`.

| # | Group / Variant | Pattern | Canonical `Test*` |
|---|-----------------|---------|-------------------|
| 1 | `queues/basic-send-receive` | Queues | |
| 2 | `queues/ack-release-redelivery` | Queues | , |
| 3 | `queues/settlement-modes` | Queues | |
| 4 | `events/basic-pubsub` | Events | |
| 5 | `events/consumer-group` | Events | |
| 6 | `events/selector` | Events | |
| 7 | `events-store/durable-replay` | Events-Store | |
| 8 | `events-store/start-positions` | Events-Store | , |
| 9 | `commands/request-reply-dynamic-node` | Commands | , |
| 10 | `queries/request-reply` | Queries | |
| 11 | `advanced/multi-frame-large-payload` | Queues | |
| 12 | `advanced/anonymous-terminus` | routes by `to` | |
| 13 | `connectivity/auth` | connection | , |

Java is `N/A (justified)` for **#8** (Qpid JMS has no arbitrary-link-property
API for `x-opt-kubemq-start`) and **#12** (the connector advertises no
`ANONYMOUS-RELAY`, and Qpid JMS cannot force a raw null-target link). For both,
the folder + README exist with no program file.

## 7. The 11 gotchas (surface in docs AND example READMEs)

The "gotcha device" is mandatory: each gotcha appears in the relevant doc **and**
the relevant example READMEs. Footguns #1 and #2 are first-class.

| # | Gotcha | Surfaced in example READMEs |
|---|--------|------------------------------|
| 1 | Events drop silently at zero credit | every `events/*` |
| 2 | Events-Store stalled-credit window loss | `events-store/*`, burn-in `events_store` |
| 3 | `released`/`modified` increment the receive-count | `queues/ack-release-redelivery`, `queues/settlement-modes` |
| 4 | Selectors rejected on `queues/` | `events/selector`, `queues/*` (negative note) |
| 5 | `AmqpSequence` body rejected | every producer README |
| 6 | Durable subs + dynamic nodes are node-local | `events-store/durable-replay`, `commands/*`, `queries/*` |
| 7 | `rcv-settle-mode=second` unsupported | `queues/settlement-modes` |
| 8 | No AMQP transactions | `queues/basic-send-receive`, migration cheat-sheet |
| 9 | Channel charset stricter than array layer | every example (address table) |
| 10 | RPC `reply-to` must be connection-owned | `commands/*`, `queries/*` |
| 11 | No vhost (OPEN hostname ignored) | `connectivity/auth`, getting-started |

## 8. The per-example README template (8 sections)

Every variant ships a `README.md` with these 8 sections, H1 =
`{Language} — {Group} / {Variant}`:

1. **Prerequisites** — language runtime + pinned native AMQP 1.0 client/version +
 a running broker reachable at `KUBEMQ_AMQP_URL`.
2. **How to Run** — the exact per-language command (see §1 of
 [`examples/README.md`](examples/README.md)) with a `KUBEMQ_AMQP_URL` override
 line; ANONYMOUS by default.
3. **Expected Output** — a concrete sample success transcript.
4. **What's Happening** — prose walkthrough of the AMQP 1.0 flow
 (OPEN → BEGIN → ATTACH → TRANSFER / FLOW / DISPOSITION → DETACH / CLOSE).
5. **AMQP 1.0 specifics** — a table:
 `| Link role | Address (source/target) | Settlement mode | Credit / drain | Delivery-state outcomes | Link / app properties | Body section | Special handling |`.
6. **Gotchas / Related Examples** — the §7 gotcha(s) that apply, as an inline
 callout, plus cross-links to sibling variants.
7. **Auth banner** — the standard ANONYMOUS-default banner (see §2).

(The N/A Java folders carry a README that explains the client-library limitation
and points to the supported alternative — no program file.)

## 9. Per-language run / lint

| Language | Build | Run | Lint |
|----------|-------|-----|------|
| Go | `go build ./...` | `go run ./<group>/<variant>` | `gofumpt -w . && golangci-lint run ./...` |
| Python | `uv sync` | `uv run python <group>/<variant>/main.py` | `uv run ruff format . && uv run ruff check --fix .` |
| Java | `mvn compile` | `mvn -pl <group>/<variant> exec:java` | `mvn compile` |
| C# | `dotnet build` | `cd <group>/<variant> && dotnet run` | `dotnet format && dotnet build -warnaserror` |
| JS/TS | `npm install` | `npx tsx <group>/<variant>/index.ts` | `npx tsc --noEmit` |
| Rust | `cargo build --workspace` | `cargo run -p <variant-crate>` | `cargo fmt && cargo clippy --workspace -- -D warnings` |

---
