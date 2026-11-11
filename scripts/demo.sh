#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
TXN="txn-demo-$(date +%s)"

echo "1. Ingesting workflow for $TXN..."
curl -sf -X POST "$BASE_URL/api/v1/events" -H "Content-Type: application/json" -d "{
  \"event_id\":\"${TXN}-e1\",\"transaction_id\":\"${TXN}\",\"event_type\":\"TRANSACTION_CREATED\",
  \"sequence_number\":1,\"created_at\":\"2026-05-01T10:00:01Z\",
  \"payload\":{\"account_id\":\"acc-demo\",\"amount\":250,\"currency\":\"USD\",\"initial_balance\":100}
}" >/dev/null

curl -sf -X POST "$BASE_URL/api/v1/events" -H "Content-Type: application/json" -d "{
  \"event_id\":\"${TXN}-e2\",\"transaction_id\":\"${TXN}\",\"event_type\":\"ACCOUNT_VALIDATED\",
  \"sequence_number\":2,\"created_at\":\"2026-05-01T10:00:02Z\",
  \"payload\":{\"valid\":true}
}" >/dev/null

curl -sf -X POST "$BASE_URL/api/v1/events" -H "Content-Type: application/json" -d "{
  \"event_id\":\"${TXN}-e3\",\"transaction_id\":\"${TXN}\",\"event_type\":\"DEBIT_INITIATED\",
  \"sequence_number\":3,\"created_at\":\"2026-05-01T10:00:03Z\",
  \"payload\":{\"amount\":250,\"currency\":\"USD\"}
}" >/dev/null

echo "2. Processing original baseline..."
curl -sf -X POST "$BASE_URL/api/v1/transactions/${TXN}/process" | head -c 300; echo ""

echo "3. Creating & starting deterministic replay..."
REPLAY_ID=$(curl -sf -X POST "$BASE_URL/api/v1/replays" -H "Content-Type: application/json" \
  -d "{\"transaction_id\":\"${TXN}\"}" | sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
curl -sf -X POST "$BASE_URL/api/v1/replays/${REPLAY_ID}/start" | head -c 300; echo ""

echo "4. Inspecting failure report (/failures)..."
curl -sf "$BASE_URL/api/v1/replays/${REPLAY_ID}/failures"; echo ""

echo "5. Inspecting checkpoints (/checkpoints)..."
curl -sf "$BASE_URL/api/v1/replays/${REPLAY_ID}/checkpoints"; echo ""

echo "6. Comparing replay vs original (/replay-comparisons)..."
curl -sf "$BASE_URL/api/v1/replay-comparisons?left=${REPLAY_ID}&right=original"; echo ""
