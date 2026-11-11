package observability

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestCorrelationContextAndLogger(t *testing.T) {
	ctx := WithCorrelation(context.Background(), Correlation{
		RequestID:     "req-1",
		TransactionID: "txn-1",
	})
	ctx = WithCorrelation(ctx, Correlation{
		ReplayID: "rpl-1",
		EventID:  "evt-1",
	})
	got := GetCorrelation(ctx)
	if got.RequestID != "req-1" || got.TransactionID != "txn-1" || got.ReplayID != "rpl-1" || got.EventID != "evt-1" {
		t.Fatalf("unexpected correlation: %+v", got)
	}

	var buf bytes.Buffer
	base := slog.New(slog.NewJSONHandler(&buf, nil))
	Logger(ctx, base).Info("hello")
	out := buf.String()
	for _, want := range []string{`"request_id":"req-1"`, `"replay_id":"rpl-1"`, `"transaction_id":"txn-1"`, `"event_id":"evt-1"`} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s in log %s", want, out)
		}
	}
}

func TestPrometheusExposition(t *testing.T) {
	m := NewMetrics()
	m.IncHTTPRequest("POST", "/api/v1/events", 201)
	m.IncIngested(false)
	m.IncIngested(true)
	m.IncProcessed("PROCESSED")
	m.IncRetry()
	m.IncGap()
	m.ObserveReplay("COMPLETED", 20*time.Millisecond)

	var buf bytes.Buffer
	m.WritePrometheus(&buf)
	out := buf.String()
	for _, want := range []string{
		`causal_replay_http_requests_total{method="POST",path="/api/v1/events",status="201"} 1`,
		`causal_replay_events_ingested_total{duplicate="false"} 1`,
		`causal_replay_events_ingested_total{duplicate="true"} 1`,
		`causal_replay_duplicates_detected_total{stage="ingestion"} 1`,
		`causal_replay_retries_total 1`,
		`causal_replay_sequence_gaps_total 1`,
		`causal_replay_replay_duration_seconds_count 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s in prometheus output:\n%s", want, out)
		}
	}
}
