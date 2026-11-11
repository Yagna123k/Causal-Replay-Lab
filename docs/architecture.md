# Architecture

Causal Replay Lab is structured as a modular Go monolith backed by PostgreSQL.

## Package Layout

- `cmd/server`: HTTP server entrypoint, configuration loading, dependency wiring, graceful shutdown.
- `cmd/migrate`: Embedded SQL migration runner (`up` / `down`) guarded by PostgreSQL advisory locks.
- `internal/config`: Typed environment configuration with startup validation.
- `internal/db`: `pgxpool` connection pooling and schema migration engine.
- `internal/ingestion`: Request validation, canonical JSON normalization, and logical payload SHA-256 hashing.
- `internal/eventstore`: Immutable `events` table repository (`INSERT ... ON CONFLICT DO NOTHING`), `transactions`, and `event_processing`.
- `internal/idempotency`: `idempotency_records` atomic claims (`IN_PROGRESS`, `COMMITTED`, `FAILED`) and processing state machine (`RECEIVED` → `PENDING_PREREQUISITE` → `PROCESSING` → `PROCESSED` | `FAILED` | `DEAD`).
- `internal/ordering`: Sequence gap calculation, out-of-order policies (`BUFFER`, `REJECT`, `FAIL_AND_FLAG`), pending buffer expiry.
- `internal/processing`: Transaction-serialized live event processor tying together row locks (`FOR UPDATE`), idempotency, ordering, and automatic contiguous buffer draining.
- `internal/execution`: Canonical `TransactionState` hashing, `replay_sessions` / `replay_executions` / `replay_execution_attempts` store, `Compare` diffing, and timeline/failure debugging queries.
- `internal/checkpoint`: Atomic checkpoint persistence (`CommitEventWithCheckpoint`) and hash integrity verification (`Verify`).
- `internal/replay`: Pure `Handler` registry, controlled dependency interfaces (`Clock`, `IDGenerator`, `EffectSink`), isolated deterministic replay `Engine`, `OriginalRunner`, `RetryPolicy`, and lease-based `RecoveryManager`.
- `internal/observability`: Context-propagated correlation IDs (`request_id`, `replay_id`, `transaction_id`, `event_id`) and Prometheus `/metrics`.
- `internal/api`: REST handlers, structured error envelope, request ID, recovery, access logging, optional API key auth, and request timeouts.

## Database Invariants

1. **Event immutability:** `events` has `BEFORE UPDATE OR DELETE` and `BEFORE TRUNCATE` triggers raising `restrict_violation`.
2. **At-most-once committed effect:** `committed_effects` enforces `PRIMARY KEY (event_id)`, `UNIQUE (idempotency_key)`, and `UNIQUE (transaction_id, sequence_number)`.
3. **Atomic checkpoints:** `replay_executions`, `replay_execution_attempts`, `replay_checkpoints`, and `replay_sessions.last_checkpoint_seq` commit in a single PostgreSQL transaction.
4. **Single-worker replay lease:** `AcquireLease` atomically checks `locked_by IS NULL OR locked_by = $worker OR lease_expires_at <= $now`.
