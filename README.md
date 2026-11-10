# Causal Replay Lab

**A deterministic event replay and debugging platform for transaction-processing systems.**

[![Go](https://img.shields.io/badge/Go-1.23+-00ADD8?logo=go)](https://go.dev/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-4169E1?logo=postgresql)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?logo=docker)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Causal Replay Lab is a backend engineering project designed to capture, replay, and investigate event-driven transaction-processing workflows.

It provides a controlled environment for reproducing transaction failures using persisted event histories, analyzing execution sequences, detecting duplicate events, handling out-of-order delivery, and recovering interrupted processing.

The primary objective is to make transaction-processing failures reproducible, observable, and easier to debug.

---

## 1. Problem Statement

Modern financial and distributed systems process events across multiple services. A single transaction may involve validation, authorization, ledger updates, notifications, and downstream processing.

Failures can occur at any stage.

For example:

1. A transaction is received.
2. Account validation succeeds.
3. The ledger is updated.
4. The service crashes before sending confirmation.
5. The transaction is retried.
6. The same event is delivered again.

This can introduce problems such as:

- Duplicate transaction processing.
- Inconsistent processing states.
- Out-of-order event execution.
- Partial transaction failures.
- Difficult-to-reproduce production issues.
- Unclear recovery behavior.

Traditional debugging often requires manually reconstructing the original execution sequence.

Causal Replay Lab addresses this problem by preserving event histories and providing deterministic replay capabilities.

## 2. Project Objectives

The system aims to:

- Persist event histories with ordering and execution metadata.
- Reproduce transaction-processing workflows from historical events.
- Detect duplicate events and enforce idempotent processing.
- Handle out-of-order event delivery using explicit ordering policies.
- Track execution progress through checkpoints.
- Recover interrupted replay sessions.
- Compare execution results across replay attempts.
- Provide debugging APIs for investigating failures.
- Validate system behavior through automated tests.

## 3. Core Features

### 3.1 Event Ingestion

A REST API accepts events from external producers.

Each event contains:

- Unique event identifier.
- Transaction or aggregate identifier.
- Event type.
- Event payload.
- Sequence number.
- Event creation timestamp.
- Optional metadata.

The ingestion layer validates incoming events and persists them for future processing.

### 3.2 Event Store

PostgreSQL serves as the persistent event store.

Responsibilities:

- Store immutable event records.
- Maintain event ordering.
- Retrieve event histories by transaction ID.
- Track processing status.
- Preserve execution metadata.
- Support efficient event-history queries.

### 3.3 Deterministic Replay Engine

The replay engine reconstructs and executes an event sequence from persisted event history.

Capabilities:

- Replay events in a defined order.
- Reproduce previous processing scenarios.
- Record execution outcomes.
- Identify the event responsible for a failure.
- Compare original execution with replayed execution.

**Determinism requirement:** Given the same event sequence, initial state, processing logic, and controlled dependencies, replay should produce the same logical outcome.

External side effects must be isolated or simulated during replay. The system must not accidentally repeat real financial operations. Handlers receive their dependencies (clock, ID generation, effect sink) through interfaces, and the replay engine is only ever constructed with simulated implementations.

The original execution outcome is stored as a baseline, so any replay can be compared against what actually happened and the first divergent event reported.

### 3.4 Idempotency Management

The system prevents duplicate events from producing duplicate processing effects.

Capabilities:

- Unique event identification.
- Duplicate-event detection.
- Idempotency keys.
- Safe handling of repeated requests.
- Atomic processing-state updates.

Idempotency enforcement must account for concurrent requests rather than relying solely on an application-level existence check. It is enforced with database unique constraints and atomic inserts. Re-sending an event with the same ID and payload is a safe no-op; the same ID with a different payload is rejected with `409 Conflict`.

### 3.5 Out-of-Order Event Handling

Distributed systems do not always deliver events in their original order.

The system supports:

- Sequence-number validation.
- Detection of missing events.
- Identification of out-of-order delivery.
- Configurable handling policies.
- Tracking of events awaiting prerequisites.

### 3.6 Checkpoint and Recovery

Replay sessions maintain execution checkpoints.

Capabilities:

- Record the last successfully processed event.
- Resume interrupted replay sessions.
- Track failed events.
- Prevent duplicate effects during recovery.
- Retry failed events under a controlled policy, with dead-letter handling when retries are exhausted.
- Inspect replay progress.

### 3.7 Execution Debugging and Observability

The debugging layer provides visibility into replay execution.

Capabilities:

- Inspect transaction event histories.
- View event execution status.
- Identify failed processing stages.
- Retrieve error information.
- Compare replay executions.
- Inspect checkpoint and recovery history.

---

## 4. Architecture

The initial architecture follows a modular monolith approach.

This keeps the system straightforward to develop while maintaining clear boundaries between components.

```text
                 External Event Producers
                           |
                           v
                  +------------------+
                  |   REST API Layer |
                  +------------------+
                           |
                           v
                  +------------------+
                  |  Event Ingestion |
                  +------------------+
                           |
                           v
                  +------------------+
                  |    Event Store   |
                  |   (PostgreSQL)   |
                  +------------------+
                           |
                           v
                  +------------------+
                  |   Replay Engine  |
                  +------------------+
                           |
             +-------------+-------------+
             |             |             |
             v             v             v
       Idempotency     Checkpoint     Execution
        Manager         Manager        Tracker
          Ordering        Recovery
          Manager         Manager
             |             |             |
             +-------------+-------------+
                           |
                           v
                  +------------------+
                  | Debugging APIs   |
                  +------------------+
                           |
                           v
                   Replay Results
```

### Architectural Modules

| Module | Responsibility |
|---|---|
| API | HTTP routing, request validation, response handling |
| Ingestion | Event validation and persistence |
| Event Store | Event history and metadata management |
| Replay Engine | Event sequencing and replay execution |
| Idempotency | Duplicate detection and processing guarantees |
| Ordering | Sequence-gap detection and configurable out-of-order policies (`BUFFER`, `REJECT`, `FAIL_AND_FLAG`) |
| Checkpoint | Replay progress, persisted atomically with the state it represents |
| Recovery | Resuming interrupted sessions using lease-based ownership |
| Execution | Original and replay results, errors, and execution history |
| Observability | Structured logs and debugging information |

---

## 5. Technology Stack

| Technology | Usage |
|---|---|
| Go | Backend services and replay engine |
| PostgreSQL | Event storage and transactional consistency |
| REST APIs | Event ingestion and replay interfaces |
| Docker | Local development and reproducible environments |
| Go Testing | Unit and integration testing |
| GitHub Actions | Continuous integration |
| Structured Logging | Execution tracing and debugging |

The implementation will initially use a modular Go application rather than introducing unnecessary microservices.

---

## 6. Getting Started

Prerequisites:

- Go 1.23 or later.
- PostgreSQL.
- Docker and Docker Compose.
- Git.

Clone the repository:

```bash
git clone https://github.com/Yagna123k/causal-replay-lab.git

cd causal-replay-lab
```

Install dependencies:

```bash
go mod tidy
```

Start the local infrastructure:

```bash
docker compose up -d
```

Run database migrations:

```bash
make migrate-up
```

Start the application:

```bash
go run ./cmd/server
```

Run tests:

```bash
go test ./...
```

*These commands represent the intended development workflow and will be finalized as the corresponding implementation files are added.*

---

## 7. Engineering Principles

The project prioritizes:

- **Correctness over throughput:** Event processing must preserve defined invariants.
- **Deterministic execution:** Replay should be reproducible under controlled inputs.
- **Data integrity:** Persistence and processing state must remain consistent.
- **Failure transparency:** Every replay failure should be inspectable.
- **Idempotency:** Repeated delivery must not create unintended duplicate effects.
- **Recoverability:** Interrupted processing must be recoverable.
- **Testability:** Critical system behavior must be verified through automated tests.

---

## 8. License

This project is licensed under the MIT License. See the `LICENSE` file for details.

---

**Causal Replay Lab - Making transaction-processing failures reproducible.**
