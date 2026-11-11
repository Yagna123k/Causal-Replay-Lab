# ADR 0001: Modular Monolith in Go with PostgreSQL Event Store

## Status
Accepted

## Context
The system needs strict transactional guarantees across event ingestion, idempotency records, out-of-order buffers, replay checkpoints, and execution tracking.

## Decision
Build a modular monolith in Go with explicit internal package boundaries and PostgreSQL as the single persistent store.

## Consequences
- Cross-cutting invariants (such as committing an effect together with its idempotency record, or an execution together with its checkpoint) run inside single ACID transactions without distributed two-phase commit.
- Local development and CI only require Go and PostgreSQL.
- Internal package interfaces allow extracting individual components later if workload bottlenecks warrant it.
