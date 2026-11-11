# Benchmarks

Measured locally using `go test -run=NONE -bench=Benchmark -benchmem -count=3 ./internal/ingestion ./internal/replay`.

## Environment

- **OS / Arch:** `windows/amd64`
- **CPU:** `Intel(R) Core(TM) i5-8250U CPU @ 1.60GHz` (8 logical cores)
- **Go:** `1.23` (`go.mod`) / `1.27.3` toolchain

## Methodology

- `BenchmarkNormalizeAndHash`: validates a full `ingestion.Request`, canonicalizes JSON payload & metadata, and computes the SHA-256 `PayloadHash`.
- `BenchmarkReplayFiveEventWorkflow`: creates a replay session and executes a 5-event transaction workflow (`TRANSACTION_CREATED` → `ACCOUNT_VALIDATED` → `DEBIT_INITIATED` → `LEDGER_UPDATED` → `TRANSACTION_COMPLETED`) using `EventClock`, `DeterministicIDGenerator`, and `SimulatedEffectSink` against an in-memory store (isolating replay engine CPU/memory cost from PostgreSQL network round-trips).
- `BenchmarkCompareExecutions`: diffs two completed 5-event sessions across state hashes and logical effect signatures.

## Measured Results (`-count=3`)

| Benchmark | Run 1 (ns/op) | Run 2 (ns/op) | Run 3 (ns/op) | Bytes/op | Allocs/op |
|---|---:|---:|---:|---:|---:|
| `BenchmarkNormalizeAndHash-8` | 14,825 | 19,231 | 14,137 | 3,192 | 67 |
| `BenchmarkReplayFiveEventWorkflow-8` | 50,841 | 53,373 | 33,607 | 19,410 | 102 |
| `BenchmarkCompareExecutions-8` | 13,897 | 18,969 | 13,492 | 5,594 | 60 |

> Note: End-to-end throughput with PostgreSQL persistence depends on disk fsync latency and connection pool sizing (`DB_MAX_CONNS`); run `make bench` or `go test -bench=. -benchmem ./...` on your target environment before sizing production capacity.
