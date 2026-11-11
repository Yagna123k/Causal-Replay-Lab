# ADR 0003: READ COMMITTED + Row Locks and Optional API Key Auth

## Status
Accepted

## Context
1. Multiple workers can process events for the same transaction concurrently.
2. Mutating endpoints (ingestion, replay start/resume) need a lightweight authentication option for production deployments without complicating local development.

## Decision
1. **Concurrency control:** Use `READ COMMITTED` transactions paired with `SELECT ... FROM transactions WHERE transaction_id = $1 FOR UPDATE` and `INSERT ... ON CONFLICT DO NOTHING` on `idempotency_records` and `committed_effects`. This serializes workers per transaction without `SERIALIZABLE` retry storms while keeping distinct transactions concurrent.
2. **API Key auth:** Enforce `X-API-Key` or `Authorization: Bearer <key>` on mutating (`POST`) routes whenever `API_KEY` is non-empty, leaving it off by default when `API_KEY` is unset for local development.

## Consequences
- Concurrent duplicate deliveries and concurrent same-transaction events remain safe and deadlock-free.
- Production deployments enable auth strictly via environment configuration without code changes.
