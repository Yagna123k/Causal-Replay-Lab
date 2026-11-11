# Causal Replay Lab

**A deterministic event-replay, idempotency, and fault-recovery platform for transaction-processing systems in Go and PostgreSQL.**

[![CI](https://github.com/Yagna123k/Causal-Replay-Lab/actions/workflows/ci.yml/badge.svg)](https://github.com/Yagna123k/Causal-Replay-Lab/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/Go-1.23+-00ADD8?logo=go)](https://go.dev/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?logo=docker)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Causal Replay Lab captures immutable transaction event histories, enforces at-most-once processing under concurrent and out-of-order delivery, and replays historical workflows deterministically in an isolated sandbox to pinpoint the exact event and state transition behind a failure—without ever triggering live financial side effects.

---

## Key Engineering Guarantees

| Capability | How It Is Enforced |
|---|---|
| **Immutable Event Log** | PostgreSQL `BEFORE UPDATE \| DELETE \| TRUNCATE` triggers reject mutations on `events`; processing state lives separately in `event_processing`. |
| **Idempotent Ingestion** | Canonical JSON + SHA-256 `payload_hash` with `INSERT ... ON CONFLICT DO NOTHING`. Exact retries return `200 OK`; payload or sequence conflicts return `409 Conflict`. |
| **Side-Effect Isolation** | Event handlers (`Apply`) are pure functions receiving injected `Clock`, `IDGenerator`, and `EffectSink`. The replay engine (`replay.Engine` / `RecoveryManager`) only accepts `SimulatedEffectSink` and has no compile-time path to `LiveEffectSink`. |
| **Deterministic Diffing** | Replays hash `TransactionState` canonically at every step and compare against the stored `ORIGINAL` baseline (or another replay) to report the exact `first_divergence`. |
| **Concurrency & Ordering** | `READ COMMITTED` + `SELECT ... FROM transactions ... FOR UPDATE` row locks serialize per-transaction workers. `committed_effects` enforces `(event_id)`, `(idempotency_key)`, and `(transaction_id, sequence_number)` uniqueness in one ACID tx with `idempotency_records`. |
| **Out-of-Order Policies** | Configurable per-server or per-request: `BUFFER` (persists to `pending_events` and auto-drains contiguously when gaps fill), `REJECT` (`409` + `Retry-After`), or `FAIL_AND_FLAG`. |
| **Atomic Checkpoints & Leases** | Checkpoints (`state_snapshot` + `state_hash`) commit in the same transaction as `replay_executions`. Resumption verifies snapshot integrity and uses `locked_by` / `lease_expires_at` leases so two workers never resume the same session. |
| **Controlled Retries & DLQ** | Error classification (`Retryable` vs `NonRetryable`), exponential backoff with seeded jitter, full `replay_execution_attempts` audit trail, and `DEAD` status when retries exhaust. |
| **Observability & Hardening** | Context-propagated correlation IDs (`request_id`, `replay_id`, `transaction_id`, `event_id`), Prometheus `/metrics`, `/healthz` + `/readyz`, optional `API_KEY` auth on `POST` routes, and graceful shutdown. |

---

## Architecture

Structured as a modular Go monolith backed by PostgreSQL so cross-cutting invariants (effects + idempotency records, executions + checkpoints) execute inside single ACID transactions without distributed two-phase commit.

```text
  REST API (api) ──► Ingestion & Validation (ingestion) ──► Immutable Event Store (eventstore)
        │                                                               │
        ├──► Live Processor (processing)                                │
        │      ├── Idempotency Manager (idempotency)                    │
        │      └── Ordering Manager (ordering: BUFFER/REJECT/FLAG)      │
        │                                                               ▼
        └──► Replay & Recovery (replay) ◄────────────────── Execution Store & Checkpoints
               ├── Controlled Deps (EventClock, DeterministicIDGen, SimulatedEffectSink)
               ├── Retry Policy (seeded backoff + DLQ) & Lease Recovery
               └── Comparison & Timeline Debugger (execution) ──► /metrics & Structured Logs
```

<details>
<summary><strong>Repository Package Map</strong></summary>

| Path | Responsibility |
|---|---|
| `cmd/server` | HTTP server entrypoint, dependency wiring, graceful shutdown |
| `cmd/migrate` | Embedded SQL migration runner (`up` / `down`) using PostgreSQL advisory locks |
| `internal/ingestion` | Request validation, canonical JSON normalization, SHA-256 payload hashing |
| `internal/eventstore` | Immutable `events`, `transactions`, and `event_processing` persistence |
| `internal/idempotency` | `idempotency_records` atomic claims & processing state machine |
| `internal/ordering` | Sequence gap detection, `BUFFER`/`REJECT`/`FAIL_AND_FLAG`, pending expiry |
| `internal/processing` | Row-locked live event processor with automatic contiguous buffer draining |
| `internal/execution` | Canonical state hashing, session store, comparison diffing, timeline & failure queries |
| `internal/checkpoint` | Single-transaction checkpoint commits & hash integrity verification |
| `internal/replay` | Pure handler registry, controlled deps, deterministic replay, retries, lease recovery |
| `internal/observability` | Correlation ID propagation (`slog`) & Prometheus `/metrics` exposition |
| `internal/api` | REST routing, error envelope, middleware, optional `API_KEY` auth |
| `migrations/` | Versioned SQL schemas (`0001_init` through `0004_recovery`) |

</details>

---

## Quick Start & Demo

### 1. Run the full stack with Docker Compose

Starts PostgreSQL 16, applies all migrations, and launches the API server on `:8080`:

```bash
docker compose up -d --build
```

### 2. Run the Failure → Replay → Diagnosis Demo

Ingests a multi-step transaction that fails at step 3 due to insufficient balance, captures the `ORIGINAL` baseline, runs an isolated replay, and queries `/failures`, `/checkpoints`, and `/replay-comparisons`:

```bash
# Linux / macOS
bash scripts/demo.sh

# Windows (PowerShell)
pwsh -File scripts/demo.ps1
```

### 3. Local Go Development & Tests

```bash
# Start only PostgreSQL in Docker, migrate, and run server locally
docker compose up -d --wait postgres
make migrate-up
DATABASE_URL=postgres://replay:replay@localhost:5432/replay?sslmode=disable go run ./cmd/server

# Unit tests (no DB required)
go test ./...

# Full PostgreSQL integration, concurrency, and failure-injection suite
TEST_DATABASE_URL=postgres://replay:replay@localhost:5432/replay?sslmode=disable go test -race -count=1 ./...
```

---

## API Overview

Full contract: [`docs/openapi.yaml`](docs/openapi.yaml)

| Group | Endpoints |
|---|---|
| **Ingestion & Query** | `POST /api/v1/events` · `GET /api/v1/events/{id}` · `GET /api/v1/transactions/{id}/events` |
| **Consistent Processing** | `POST /api/v1/events/{id}/process` · `POST /api/v1/transactions/{id}/process-events` · `GET /api/v1/transactions/{id}/status` |
| **Deterministic Replay** | `POST /api/v1/transactions/{id}/process` · `POST /api/v1/replays` · `POST /api/v1/replays/{id}/start` · `GET /api/v1/replays/{id}` · `GET /api/v1/replays/{id}/executions` · `GET /api/v1/replay-comparisons?left={id}&right=original` |
| **Recovery & Checkpoints** | `POST /api/v1/replays/{id}/pause` · `POST /api/v1/replays/{id}/resume` · `GET /api/v1/replays/{id}/checkpoints` · `GET /api/v1/replays/{id}/attempts` · `POST /api/v1/replay-recovery/orphans` |
| **Debugging & Ops** | `GET /api/v1/transactions/{id}` · `GET /api/v1/transactions/{id}/timeline` · `GET /api/v1/replays/{id}/failures` · `GET /metrics` · `GET /healthz` · `GET /readyz` |

---

## Measured Performance & CI

All numbers below are measured directly on `windows/amd64` (`Intel Core i5-8250U @ 1.60GHz`) with `go test -run=NONE -bench=Benchmark -benchmem -count=3` (in-memory store; full methodology in [`docs/benchmarks.md`](docs/benchmarks.md)):

- **Event normalization & SHA-256 hashing:** `14.1–19.2 µs/op` (`3,192 B/op`, `67 allocs/op`)
- **5-event deterministic replay workflow:** `33.6–53.4 µs/op` (`19,410 B/op`, `102 allocs/op`)
- **Execution diff & divergence detection:** `13.5–19.0 µs/op` (`5,594 B/op`, `60 allocs/op`)

GitHub Actions CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs `go vet`, `golangci-lint`, `go test -race -coverprofile=coverage.out ./...` against PostgreSQL 16 (including 20-goroutine concurrency, mid-transaction failure injection, corrupt checkpoint detection, and lease takeover tests), and verifies the multi-stage non-root Docker build.

---

## Documentation

- [Architecture & Database Invariants](docs/architecture.md)
- [Architecture Decision Records (ADRs)](docs/adr/)
- [OpenAPI 3.0 Specification](docs/openapi.yaml)
- [Demo Walkthrough](docs/demo.md)
- [Benchmark Methodology & Results](docs/benchmarks.md)
- [5-Phase Execution Plan](Execution-plan.md)

## License

MIT — see [`LICENSE`](LICENSE).
