# Pressend Server Engine

The core backend powering Pressend — biometric-authenticated crypto and fiat cash-out infrastructure. Built in Rust for correctness and throughput at the point of transaction: a user should never wait, and a transaction should never duplicate.

---

## 🛠️ Pressend Ecosystem Repository Map

To assist Stellar Development Foundation (SDF) reviewers, our architecture is split into separate repositories:

- **Backend Server Engine & Smart Contracts (this repo):** Houses our Rust-based server modules, Kafka workers, and the upcoming Soroban on-chain contracts.
- **Mobile Client Application:** [Our Mobile Application](https://github.com/ishmaeldestinyo/pressend_mobile_app) — Houses our user-facing front-end codebase and biometric scan interface wrappers.

---

## What this service does

Pressend lets users withdraw crypto or fiat as physical cash at accredited POS centres using only a palmprint or faceprint — no card, no phone. This engine is the system of record and orchestration layer behind that interaction. It authenticates the request, manages on-chain asset states, routes swaps through native liquidity pools, coordinates fiat bridges via provider APIs, settles the payout, and keeps every downstream system (notifications, admin, ledger, VAS) in sync in real time.

---

## 🚀 Stellar Ecosystem Development Roadmap (SCF Track)

This workspace tracks our non-dilutive ecosystem integration track. The codebase expands upon our production-stable fiat-rail infrastructure to deploy native Stellar settlement and Soroban smart contract protection features over a 6-month, 3-phase delivery schedule.

### 📋 Technical Integration Benchmarks

Our development milestones are scoped to align with the timeline standards of the official Stellar developer documentation:

- **Gateway Layer (Mercuryo):** Uses standard B2B API pipelines for a streamlined 1-to-2 week core integration window.
- **On-Chain Architecture (Soroban/Rust):** Implements cross-contract authorization and footprint state-rental profiles, with engineering time allocated to avoid transaction race conditions and storage bloat.
- **Infrastructure Optimization (K8s / Kafka):** Configured across 4 highly available processing replicas to insulate runtime execution from cascading node drops or memory starvation.

---

## Architecture at a glance

```
src/
├── cron/                # Scheduled jobs (reconciliation, expiry, referral payouts)
├── middlewares/         # Request auth, rate limiting, request validation
├── modules/
│   ├── account/         # User identity, biometric enrollment & auth
│   ├── admin/           # Internal admin/ops tooling
│   ├── beneficiary/     # Linked beneficiaries & payout recipients
│   ├── legacy_plan/     # Legacy pricing/plan support
│   ├── push_notifications/  # Device token management & push delivery
│   ├── soroban/         # NEW: Soroban contract bindings, XDR signature verification
│   ├── transactions/    # Core lifecycle (Native Stellar DEX path payments & off-ramps)
│   ├── vas/             # Value-added services (bill pay, airtime, etc.)
│   └── webhook/         # Inbound/outbound provider webhook handlers (Mercuryo loops)
├── scripts/             # One-off and maintenance scripts
├── utils/               # Shared helpers
├── ws/                  # WebSocket layer (live transaction/status updates)
├── config.rs            # Environment & runtime configuration
├── db.rs                # Postgres connection pool & query layer
├── errors.rs            # Unified error types & API error mapping
├── kafka.rs             # Kafka producer/consumer setup
├── redis.rs             # Redis client (caching, rate limits, ephemeral state)
├── worker_events.rs     # Event definitions consumed by background workers
├── worker_handlers.rs   # Background worker logic (async settlement, retries)
├── lib.rs
└── main.rs              # Service entrypoint
```

Supporting directories:

- `contracts/` — **NEW:** Rust source code upon init, for native Soroban smart contracts (Duress state locks, fee-routers)
- `migrations/` — Postgres schema migrations
- `.sqlx/` — Compile-time verified PostgreSQL query metadata (via `sqlx`)
- `templates/` — Notification/email templates

---

## Core design principles

**Idempotency over hope.** Every transaction-mutating endpoint is built to be safely retried. Duplicate charges are a solved problem here, not a fire drill (see `worker_handlers.rs`).

**Event-driven settlement.** Kafka carries transaction and account events between the API layer and background workers, so payout settlement, notifications, and ledger updates happen asynchronously and can be replayed or audited independently.

**Real-time by default.** The `ws/` module pushes live transaction status to connected clients — a user standing at a POS terminal should see confirmation the moment it happens, not on next poll.

**Fail loud, fail typed.** `errors.rs` centralizes error variants so every failure mode across modules maps to a predictable, typed API response — no silent `500`s.

**Device-independent security.** No biometric templates touch the blockchain. Sensitive facial and palm data reside in encrypted hardware enclaves. Only a derived cryptographic signature is passed on-chain to handle state parameters, without breaking user data privacy.

---

## Tech stack

| Layer | Technology |
|---|---|
| Language | Rust (Engine & Soroban Smart Contracts) |
| Database | PostgreSQL (via `sqlx`, compile-time checked queries) |
| Cache / ephemeral state | Redis |
| Event streaming | Kafka |
| Real-time transport | WebSockets |
| Scheduled jobs | Custom cron runner |
| Gateway / Rail Integrations | Mercuryo API / Stellar Native DEX Path Payments |

---

## Getting started

### Prerequisites

- Rust (stable toolchain)
- Soroban CLI (for smart contract testing/compilation)
- PostgreSQL
- Redis
- Kafka broker (local or remote)

### Setup

```bash
# Clone the repository
git clone https://github.com/YOUR_ORG/pressend_server_engine.git
cd pressend_server_engine

# Configure environment
cp .env.example .env   # fill in DB, Redis, Kafka, and Mercuryo API credentials

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
| `account` | Biometric enrollment, identity verification, auth, enclave signature ingestion |
| `soroban` | Parsing on-chain transaction logs, managing contract state, handling event payloads |
| `transactions` | Swap quotes, Native Stellar DEX path payment routing, Mercuryo fiat off-ramp settlement |
| `beneficiary` | Managing linked recipients for transfers |
| `vas` | Value-added services beyond core cash-out |
| `webhook` | Inbound Mercuryo settlement webhook ingestion & transactional worker dispatch |
| `push_notifications` | Device token registry, push delivery |
| `admin` | Internal operations tooling |
| `legacy_plan` | Backward-compatible plan/pricing support |

---

## 🔒 The Soroban Security Primitive: Account Duress Engine

A core public good introduced by this platform is our specialized on-chain anti-theft layer.

1. **The Panic Switch:** Users assign a custom auxiliary palm or facial profile to act as a stealth alarm vector.
2. **On-Chain Execution:** If the payload verification signature sent by the hardware sensor maps to the panic profile hash, the engine bypasses standard settlement logic.
3. **Ledger-Level Lock:** The `contracts/` logic writes a state-lock modifier to the ledger, instantly freezing the wallet account from accepting outgoing transfers until an off-chain identity reclamation checkpoint clears the lock.

---

## Contributing

- Every schema change ships as a migration in `migrations/` with a corresponding `.sqlx` metadata update.
- Worker logic changes require a matching event definition in `worker_events.rs` before a handler is added to `worker_handlers.rs`.
- All smart contract modifications require full test suites in the `contracts/` module before deploying compiled `.wasm` to Futurenet.
- Keep commit messages specific — future-you debugging a duplicate charge at 2am will thank present-you.

---

## Status

Actively developed. Core transaction, account, and notification modules are production-stable; native Stellar DEX routing, Soroban security locks, and Mercuryo pipelines are moving through their benchmarked delivery tracks.
