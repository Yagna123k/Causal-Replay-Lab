# Reproducible Failure → Replay → Diagnosis Demo

This walkthrough demonstrates ingesting a transaction workflow where step 3 fails due to insufficient balance, recording the original baseline, replaying deterministically, and inspecting the failure, timeline, checkpoints, and comparison.

## Prerequisites

Start the stack (`postgres`, `migrate`, `app` on `:8080`):

```bash
docker compose up -d --build
```

Or run locally:

```bash
docker compose up -d postgres
make migrate-up
make run
```

## Automated Scripts

- Bash / Linux / macOS: `bash scripts/demo.sh`
- PowerShell / Windows: `pwsh -File scripts/demo.ps1`

## What the Demo Executes

1. Ingests 3 events for transaction `txn-demo-1` (`TRANSACTION_CREATED` with `initial_balance: 100`, `ACCOUNT_VALIDATED`, `DEBIT_INITIATED` requesting `amount: 250`).
2. Runs `POST /api/v1/transactions/txn-demo-1/process` to capture the `ORIGINAL` baseline (which fails on sequence 3).
3. Creates a replay session (`POST /api/v1/replays`) and runs it (`POST /api/v1/replays/{id}/start`).
4. Inspects:
   - `GET /api/v1/replays/{id}/failures` (pinpoints sequence 3, error `"insufficient balance: have 100, need 250"`, and before/after state).
   - `GET /api/v1/replays/{id}/checkpoints` (shows checkpoints persisted for sequences 1 and 2).
   - `GET /api/v1/transactions/txn-demo-1/timeline` (unified view of events, sessions, executions, checkpoints, attempts).
   - `GET /api/v1/replay-comparisons?left={id}&right=original` (confirms deterministic reproduction).
