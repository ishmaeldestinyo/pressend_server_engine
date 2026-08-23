# Pressend Server Engine

The core backend powering Pressend — biometric-authenticated crypto and fiat cash-out infrastructure. Built in Rust for correctness and throughput at the point of transaction: a user should never wait, and a transaction should never duplicate.

---

## What this service does

Pressend lets users withdraw crypto or fiat as physical cash at accredited POS centres using only a palmprint or faceprint — no card, no phone. This engine is the system of record and orchestration layer behind that interaction: it authenticates the request, routes the swap/off-ramp, settles the payout, and keeps every downstream system (notifications, admin, ledger, VAS) in sync in real time.

---

## Architecture at a glance

```
src/
├── cron/                # Scheduled jobs (reconciliation, expiry, referral payouts)
├── middlewares/          # Request auth, rate limiting, request validation
├── modules/
│   ├── account/          # User identity, biometric enrollment & auth
│   ├── admin/             # Internal admin/ops tooling
│   ├── beneficiary/       # Linked beneficiaries & payout recipients
│   ├── legacy_plan/       # Legacy pricing/plan support
│   ├── push_notifications/# Device token management & push delivery
│   ├── transactions/      # Core swap, off-ramp & payout lifecycle
│   ├── vas/                # Value-added services (bill pay, airtime, etc.)
│   └── webhook/            # Inbound/outbound webhook handling
├── scripts/               # One-off and maintenance scripts
├── utils/                 # Shared helpers
├── ws/                     # WebSocket layer (live transaction/status updates)
├── config.rs              # Environment & runtime configuration
├── db.rs                   # Postgres connection pool & query layer
├── errors.rs               # Unified error types & API error mapping
├── kafka.rs                # Kafka producer/consumer setup
├── redis.rs                # Redis client (caching, rate limits, ephemeral state)
├── worker_events.rs        # Event definitions consumed by background workers
├── worker_handlers.rs      # Background worker logic (async settlement, retries)
├── lib.rs
└── main.rs                 # Service entrypoint
```

Supporting directories:

- `migrations/` — Postgres schema migrations
- `.sqlx/` — Compile-time verified SQL query metadata (via `sqlx`)
- `templates/` — Notification/email templates

---

## Core design principles

**Idempotency over hope.** Every transaction-mutating endpoint is built to be safely retried. Duplicate charges are a solved problem here, not a fire drill (see: `worker_handlers.rs`).

**Event-driven settlement.** Kafka carries transaction and account events between the API layer and background workers, so payout settlement, notifications, and ledger updates happen asynchronously and can be replayed or audited independently.

**Real-time by default.** The `ws/` module pushes live transaction status to connected clients — a user standing at a POS terminal should see confirmation the moment it happens, not on next poll.

**Fail loud, fail typed.** `errors.rs` centralizes error variants so every failure mode across modules maps to a predictable, typed API response — no silent `500`s.

---

## Tech stack

| Layer | Technology |
|---|---|
| Language | Rust |
| Database | PostgreSQL (via `sqlx`, compile-time checked queries) |
| Cache / ephemeral state | Redis |
| Event streaming | Kafka |
| Real-time transport | WebSockets |
| Scheduled jobs | Custom cron runner |

---

## Getting started

### Prerequisites

- Rust (stable toolchain)
- PostgreSQL
- Redis
- Kafka broker (local or remote)

### Setup

```bash
# Clone
git clone <repo-url>
cd pressend_server_engine

# Configure environment
cp .env.example .env   # fill in DB, Redis, Kafka credentials

# Run migrations
sqlx migrate run

# Build & run
cargo run
```

### Running checks

```bash
cargo check
cargo test
cargo fmt --check
```

---

## Module ownership map

| Module | Responsibility |
|---|---|
| `account` | Biometric enrollment, identity verification, auth |
| `transactions` | Swap quotes, off-ramp execution, payout lifecycle |
| `beneficiary` | Managing linked recipients for transfers |
| `vas` | Value-added services beyond core cash-out |
| `webhook` | Partner/provider webhook ingestion & dispatch |
| `push_notifications` | Device token registry, push delivery |
| `admin` | Internal operations tooling |
| `legacy_plan` | Backward-compatible plan/pricing support |

---

## Contributing

- Every schema change ships as a migration in `migrations/` with a corresponding `.sqlx` metadata update.
- Worker logic changes require a matching event definition in `worker_events.rs` before a handler is added to `worker_handlers.rs`.
- Keep commit messages specific — future-you debugging a duplicate charge at 2am will thank present-you.

---

## Status

Actively developed. Core transaction, account, and notification modules are production-stable; VAS and legacy plan support are evolving alongside product scope.
