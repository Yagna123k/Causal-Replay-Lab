package ingestion

import (
	"encoding/json"
	"testing"
	"time"
)

func BenchmarkNormalizeAndHash(b *testing.B) {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	req := Request{
		EventID:        "evt-bench-1",
		TransactionID:  "txn-bench-1",
		EventType:      "LEDGER_UPDATED",
		Payload:        json.RawMessage(`{"amount":250,"currency":"USD","operation":"debit","account":"acc-123"}`),
		SequenceNumber: 4,
		CreatedAt:      &ts,
		Metadata:       json.RawMessage(`{"producer":"gateway-1","region":"us-east"}`),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Normalize(req); err != nil {
			b.Fatal(err)
		}
	}
}
