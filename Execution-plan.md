# Causal Replay Lab — 5-Phase Execution Plan

| # | Phase | Focus |
|---|---|---|
| 01 | Foundation & Event Ingestion | Go, PostgreSQL, schemas, ingestion APIs, DB-level idempotency |
| 02 | Event Processing & Replay Engine | Deterministic execution, controlled dependencies, ordering |
| 03 | Idempotency & Data Consistency | Processing-level idempotency, concurrency, out-of-order policy |
| 04 | Checkpointing & Fault Recovery | Atomic checkpoints, recovery, retries, failure handling |
| 05 | Observability, Testing & Production Readiness | Debugging APIs, CI/CD, benchmarks, documentation |

> [!NOTE]
> Testing is not deferred to Phase 5. Every phase ships with its own unit, integration, and (where relevant) concurrency/failure tests. Phase 5 adds failure injection, benchmarks, and hardening.

---

## Phase 1 — Foundation & Event Ingestion

**Goal:** Build the backend foundation and a reliable, immutable event store.

### Tasks

1. Initialize the Go project with a clean modular structure (`cmd/server`, `internal/{api,ingestion,eventstore,replay,idempotency,checkpoint,execution,config}`).
2. Configure PostgreSQL and Docker Compose.
3. Implement database migrations (e.g. `golang-migrate`) and the `make migrate-up/down` targets referenced in the README.
4. Design the event data model (see below).
5. Build REST APIs for event ingestion and retrieval.
6. Implement request validation and structured error handling (consistent error envelope).
7. Add transactions and indexes; enforce event immutability in the database (no UPDATE/DELETE on `events`, via trigger or revoked privileges).
8. Implement structured logging (`log/slog`), configuration management, and a `/healthz` endpoint from day one.
9. Set up a minimal GitHub Actions workflow (build + vet + test) early, and extend it later.

### Database entities

- `events` — immutable: `event_id` (PK), `transaction_id`, `event_type`, `payload` (JSONB), `sequence_number`, `created_at` (producer time), `received_at` (server time), `payload_hash`, `metadata` (JSONB), `schema_version`.
- `transactions` — one row per aggregate: `transaction_id`, status, `last_sequence`, timestamps.
- Processing state is kept **separate** from the immutable event (e.g. `event_processing`), so events themselves are never mutated.

Key constraints:

- `UNIQUE (event_id)`
- `UNIQUE (transaction_id, sequence_number)`
- `INDEX (transaction_id, sequence_number)` for history queries

### Ingestion idempotency (decided here, not later)

Re-delivery of the same `event_id` must be safe from the first commit:

- Same `event_id` + same `payload_hash` → return the original result (HTTP 200, not an error).
- Same `event_id` + different payload → `409 Conflict` (this signals a producer bug).
- Same `(transaction_id, sequence_number)` + different `event_id` → `409 Conflict`.
- Use `INSERT ... ON CONFLICT`, not "SELECT then INSERT".

### APIs

| Method | Endpoint | Purpose |
|---|---|---|
| POST | `/api/v1/events` | Ingest event |
| GET | `/api/v1/events/{id}` | Retrieve event |
| GET | `/api/v1/transactions/{id}/events` | Retrieve event history (ordered by sequence number) |
| GET | `/healthz` | Liveness / readiness |

### Completion criteria

- Events are persisted successfully.
- Invalid payloads are rejected with structured errors.
- Duplicate event identifiers are handled safely, including under concurrent POSTs (tested).
- Event histories are returned in deterministic order.
- Events cannot be modified after insertion.
- Migrations work from a clean database.
- Unit and integration tests pass in CI.

**Deliverable:** A working event ingestion and storage service.

---

## Phase 2 — Event Processing & Deterministic Replay Engine

**Goal:** Build the core functionality that differentiates this project. This is the most important phase.

### Tasks

1. Define the processing contract: a pure `Apply(state, event) -> (newState, effects, error)` handler interface per event type.
2. Define **controlled dependencies** as interfaces: `Clock`, `IDGenerator`/randomness, and an `EffectSink` (ledger / notification gateway). Provide a `Recorder/Simulated` implementation for replay and a `Live` implementation for original processing.
3. Record the **original execution** (outcome, resulting state, effects) when events are first processed. Replay comparison is meaningless without a stored baseline.
4. Implement the replay engine: load a transaction's events, order by sequence number, apply to a controlled initial state.
5. Detect sequence gaps/duplicates at replay time and fail with a clear error (the full ordering policy comes in Phase 3).
6. Implement transaction state reconstruction, with a canonical state hash (stable serialization) for equality checks.
7. Persist replay sessions and per-event execution results.
8. Track event-level execution status.
9. Implement replay comparison (original vs replay, replay vs replay) — diff on state hash, effects list, and first divergent event.

### Core concepts

**A. Event sequencing**

```
Event 1: TRANSACTION_CREATED
Event 2: ACCOUNT_VALIDATED
Event 3: DEBIT_INITIATED
Event 4: LEDGER_UPDATED
Event 5: TRANSACTION_COMPLETED
```

**B. State reconstruction** — apply events to a controlled initial state.

**C. Deterministic execution** — the same events, initial state, logic, and controlled dependencies produce equivalent logical results.

- No `time.Now()`, `rand`, map-iteration-order dependence, or real external calls inside handlers. Time comes from the event's timestamp or the injected `Clock`.
- Replay uses an isolated state and the simulated `EffectSink`. A replay can **never** reach the live ledger — enforce this structurally (the replay engine is not constructed with live dependencies) and test it.
- Add a determinism test: run the same replay N times and assert identical state hashes.

### Replay session lifecycle

`CREATED → RUNNING → COMPLETED | FAILED` (`PAUSED/RESUMABLE` added in Phase 4). Transitions are validated and persisted.

### APIs

| Method | Endpoint | Purpose |
|---|---|---|
| POST | `/api/v1/replays` | Create replay session (transaction ID, optional range / initial state) |
| POST | `/api/v1/replays/{id}/start` | Start replay |
| GET | `/api/v1/replays/{id}` | Get replay status |
| GET | `/api/v1/replays/{id}/executions` | Inspect execution history |
| GET | `/api/v1/replay-comparisons?left={id}&right={id}` | Compare two executions (`right` may be `original`) |

> The comparison route was moved off `/replays/compare` to avoid ambiguity with `/replays/{id}`.

### Completion criteria

- The replay engine processes historical event sequences.
- Event ordering is deterministic.
- Original execution baselines are stored.
- Each execution result is persisted.
- Failed events are identifiable.
- Original and replayed outcomes can be compared, with the first divergence reported.
- Repeated replay with identical controlled inputs produces identical state hashes.
- Replay is proven not to invoke live side effects (test with a failing-if-called live sink).

**Deliverable:** A functioning deterministic event replay engine.

---

## Phase 3 — Idempotency & Data Consistency

**Goal:** Handle duplicate events, concurrent processing, and event-ordering problems safely. Ingestion-level duplicate handling was built in Phase 1; this phase covers **processing-level** guarantees.

### Tasks

1. Add idempotency keys for processing effects (separate from the event ID where a producer supplies one).
2. Create an `idempotency_records` table (key, request hash, status, result, created/expires) with a unique constraint.
3. Prevent duplicate processing effects using atomic claim: `INSERT ... ON CONFLICT DO NOTHING` / `SELECT ... FOR UPDATE SKIP LOCKED`, and commit the effect and the idempotency record in **the same DB transaction**.
4. Handle concurrent event ingestion and concurrent processing of the same transaction (per-transaction serialization via row lock or advisory lock).
5. Implement the **Ordering Manager**: out-of-order detection, missing-sequence identification, and a configurable policy.
6. Persist a `pending/awaiting` buffer for events that arrive before their prerequisites, and release them when gaps fill. Add a timeout/expiry state for gaps that never fill.
7. Define the event-processing state machine: `RECEIVED → PENDING_PREREQUISITE → PROCESSING → PROCESSED | FAILED | DEAD`.
8. Ensure atomic database updates, and document invariants (see below).

### Important scenarios

**A — Duplicate delivery:** `event_101` sent 3 times → exactly one committed effect.

**B — Out-of-order delivery:** received `1 → 3 → 2`. Policy options (document the choice, and make it configurable):

- `BUFFER` — store 3 as pending, process 1, 2, then 3.
- `REJECT` — return a retryable error for 3 until 2 arrives.
- `FAIL_AND_FLAG` — mark the transaction as having a gap for debugging.

**C — Concurrent processing:** two workers pick the same event → only one commits the effect.

**D — Same sequence, different event:** conflicting writes at the same sequence number are rejected and flagged.

### Documented invariants (enforced in DB where possible)

- An event has at most one committed processing effect.
- Within a transaction, events are processed in strictly increasing, gap-free sequence order.
- State and idempotency record change together or not at all.

### Engineering requirements

- PostgreSQL unique constraints and CHECK constraints.
- Explicit transaction isolation level choice (document why `READ COMMITTED` + row locks or `SERIALIZABLE` + retry on serialization failure).
- Idempotency records and explicit ordering policies.
- Concurrency tests run with `-race` and many goroutines against a real Postgres.

### Completion criteria

- Duplicate requests cannot produce duplicate committed effects.
- Concurrent processing is safe (N parallel workers, exactly one effect).
- Out-of-order events are detected and handled per the configured policy.
- Missing events are identifiable via API.
- Processing state stays consistent after injected failures mid-transaction.

**Deliverable:** A reliable event-processing layer with idempotency and consistency guarantees.

---

## Phase 4 — Checkpointing, Retries & Fault Recovery

**Goal:** Make replay and processing sessions recoverable after interruptions and failures.

### Tasks

1. Implement replay checkpoints, written in the **same DB transaction** as the state/effects they represent.
2. Persist the last successfully committed event position plus the state snapshot (or state hash) at that position.
3. Implement replay resumption: load the latest valid checkpoint, verify the state hash, continue from the next sequence.
4. Add controlled retry policies: max attempts, exponential backoff with jitter (jitter injected via a seedable source to stay testable), retryable vs non-retryable error classification.
5. Track failed and retried executions (attempt count, last error, next retry time).
6. Handle database and application interruptions: context cancellation, graceful shutdown that finishes or safely abandons the in-flight event.
7. Implement the **Recovery Manager** and the full session lifecycle: `CREATED → RUNNING → PAUSED/INTERRUPTED → RESUMING → COMPLETED | FAILED`.
8. Detect orphaned sessions after a crash using a lease/heartbeat (`locked_by`, `lease_expires_at`) so that a restarted instance can safely take over and two instances never resume the same session.
9. Prevent duplicate effects during recovery (reuse Phase 3 idempotency).
10. Add a dead-letter state for events that exhaust retries.

### Example

A replay session contains 10 events.

```
Event 1  ✓
Event 2  ✓
Event 3  ✓
Event 4  ✓
Event 5  ✓
Event 6  ✗
Event 7  -
Event 8  -
Event 9  -
Event 10 -
```

The application crashes after Event 6 fails. After restart:

- Load the latest valid checkpoint (after Event 5).
- Inspect the failed event and its attempt history.
- Resume according to the retry/recovery policy.
- Continue processing remaining events.
- Verify that already committed effects are not repeated.

### Important distinction

A checkpoint must represent a consistent committed state. Saving a sequence number without the corresponding state, in a separate transaction, is not sufficient. A crash between the two writes must be impossible by construction, and tested.

### Completion criteria

- Replay sessions resume after interruption (tested by killing the process/context mid-replay).
- Checkpoints represent consistent processing state.
- A recovered replay yields the same final state hash as an uninterrupted replay.
- Retry behavior is controlled and observable.
- Failed events remain inspectable.
- Only one worker can own a session at a time.
- Recovery does not duplicate committed effects.

**Deliverable:** A fault-tolerant replay and recovery mechanism.

---

## Phase 5 — Observability, Testing & Production Readiness

**Goal:** Turn the backend into a demonstrable, well-tested engineering project.

### Tasks

1. Build debugging and inspection APIs (see below).
2. Implement structured execution logs with correlation IDs (`request_id`, `replay_id`, `transaction_id`, `event_id`) propagated through context.
3. Add replay failure inspection (failing event, error, state before/after).
4. Expose execution comparison through APIs (completes the Phase 2 comparison).
5. Add basic metrics (`/metrics`, Prometheus format): replay duration, events processed, duplicates detected, retries, gaps.
6. Fill unit test gaps; enforce a coverage report in CI (no arbitrary coverage target).
7. Write PostgreSQL integration tests using Testcontainers or the CI Postgres service.
8. Add failure-injection tests (kill DB connection mid-transaction, crash between steps, context cancel).
9. Run a **benchmark** on ingestion and replay (throughput, p50/p95/p99 latency, hardware noted) and publish only measured numbers.
10. Finalize GitHub Actions CI: lint (`golangci-lint`), `go vet`, `go test -race`, integration job, Docker build.
11. Containerize the application (multi-stage build, non-root user).
12. Document architecture, API contracts (OpenAPI), ADR-style design decisions, and a **demo walkthrough** (a reproducible scripted failure → replay → diagnosis).

### Debugging API additions

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/api/v1/transactions/{id}` | Transaction state, gaps, processing status |
| GET | `/api/v1/transactions/{id}/timeline` | Merged view: events, executions, checkpoints, retries |
| GET | `/api/v1/replays/{id}/failures` | Failed events with error info |
| GET | `/api/v1/replays/{id}/checkpoints` | Checkpoint & recovery history |
| GET | `/readyz` | Readiness (DB reachable) |

### Testing scenarios

| Test | Expected behavior |
|---|---|
| Duplicate event | No duplicate effect |
| Same ID, different payload | 409 Conflict |
| Out-of-order event | Correct ordering policy applied |
| Partial failure | Failure identified |
| Application restart | Replay recovers from checkpoint |
| Database interruption | Processing state remains consistent |
| Concurrent requests | No duplicate committed effect |
| Repeated replay | Identical state hash |
| Replay with live sink | Live sink never called |
| Invalid payload | Validation error returned |

### Production-readiness considerations

- Environment-based configuration with validation at startup.
- Database connection pooling (`pgxpool`) with tuned limits.
- Graceful shutdown.
- Request and DB query timeouts.
- Structured logs and metrics.
- Health and readiness endpoints.
- Migration management.
- Automated CI checks.
- API documentation.
- Basic API key auth on mutating endpoints (replay start/ingestion), or an explicit documented decision to defer it.

### Completion criteria

- All critical tests pass, including with `-race`.
- Failure scenarios are reproducible via scripted demo.
- Replay execution can be inspected end to end.
- Application starts with `docker compose up`.
- CI runs automated tests.
- Benchmarks are published with methodology.
- README and architecture documentation are complete and match the actual implementation (including the README Getting Started commands).

**Deliverable:** A documented, tested, containerized backend suitable for a public GitHub portfolio.

---

# Final project architecture

By the end, the system should contain these major components:

| Component | Final responsibility |
|---|---|
| Event Ingestion | Accept and validate events |
| Event Store | Persist immutable event histories |
| Replay Engine | Reconstruct transaction execution with controlled dependencies |
| Idempotency Manager | Prevent duplicate effects |
| Ordering Manager | Handle event sequence inconsistencies |
| Checkpoint Manager | Persist replay progress atomically with state |
| Recovery Manager | Resume interrupted executions (lease-based ownership) |
| Execution Tracker | Record original and replay outcomes |
| Debugging API | Inspect and compare execution history |

Reference: [Lightweight implementation of Event Sourcing using PostgreSQL as an event store](https://dev.to/eugene-khyst/lightweight-implementation-of-event-sourcing-using-postgresql-as-an-event-store-59h7)

---

# Engineering standards

These are non-negotiable if the project is to demonstrate real backend engineering.

1. **No premature microservices.** Start with a modular monolith in Go. Split only with a demonstrated reason.
2. **No fake scalability claims.** Benchmark real throughput, latency, and database performance before publishing numbers.
3. **No superficial tests.** Test concurrency, database failures, partial execution, and recovery.
4. **No uncontrolled replay side effects.** Replay uses isolated state and controlled dependencies.
5. **No silent data corruption.** Define invariants and enforce them at both the database and application layers.
6. **Keep README and plan in sync.** Update docs whenever a design decision changes.

---

# Project milestones & tracker

Check items off as completed. (20 tasks total.)

### Phase 1 — Foundation
- [ ] Go project initialized
- [ ] PostgreSQL and migrations configured
- [ ] Event ingestion APIs implemented (with DB-level idempotency)
- [ ] Event validation and persistence tested

### Phase 2 — Replay Engine
- [ ] Event sequencing implemented
- [ ] Replay engine with controlled dependencies implemented
- [ ] Replay sessions and original-execution baselines persisted
- [ ] Execution history and comparison implemented

### Phase 3 — Consistency
- [ ] Processing idempotency implemented
- [ ] Duplicate event handling tested
- [ ] Out-of-order event policy implemented
- [ ] Concurrent processing tested

### Phase 4 — Recovery
- [ ] Atomic checkpointing implemented
- [ ] Replay resumption implemented
- [ ] Retry policies implemented
- [ ] Failure recovery tested

### Phase 5 — Production Readiness
- [ ] Debugging APIs implemented
- [ ] Unit, integration, and failure-injection tests completed
- [ ] CI pipeline and benchmarks completed
- [ ] Docker and documentation completed
</Content>
</invoke>
