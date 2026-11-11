package api

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Yagna123k/causal-replay-lab/internal/execution"
	"github.com/Yagna123k/causal-replay-lab/internal/observability"
	"github.com/Yagna123k/causal-replay-lab/internal/ordering"
)

type fakeDebugger struct {
	tl  execution.TransactionTimeline
	rep execution.ReplayFailuresReport
	err error
}

func (f *fakeDebugger) GetTransactionTimeline(context.Context, string) (execution.TransactionTimeline, error) {
	return f.tl, f.err
}

func (f *fakeDebugger) GetReplayFailures(context.Context, string) (execution.ReplayFailuresReport, error) {
	return f.rep, f.err
}

func TestPhase5DebuggingMetricsAndAuth(t *testing.T) {
	metrics := observability.NewMetrics()
	dbg := &fakeDebugger{
		tl: execution.TransactionTimeline{
			TransactionID:     "t1",
			ProcessedSequence: 2,
			LastSequence:      2,
			Events:            []execution.TimelineEvent{{EventID: "e1", SequenceNumber: 1, Processing: "PROCESSED"}},
		},
		rep: execution.ReplayFailuresReport{
			SessionID: "rpl_1",
			Status:    execution.StatusFailed,
			Failures:  []execution.FailureDetail{{EventID: "e3", SequenceNumber: 3, Status: execution.EventFailed, Error: "boom"}},
		},
	}
	ins := &fakeInspector{st: ordering.TransactionStatus{TransactionID: "t1", ProcessedSequence: 2}}

	h := NewHandler(Deps{
		Ingester:       fakeIngester{},
		Reader:         &fakeReader{},
		Inspector:      ins,
		Debugger:       dbg,
		Metrics:        metrics,
		APIKey:         "secret-key",
		RequestTimeout: time.Second,
		MaxBodyBytes:   1024,
	})

	// GET endpoints work without API key.
	if r := do(h, "GET", "/api/v1/transactions/t1", ""); r.Code != 200 {
		t.Fatalf("GET transaction: %d %s", r.Code, r.Body.String())
	}
	if r := do(h, "GET", "/api/v1/transactions/t1/timeline", ""); r.Code != 200 {
		t.Fatalf("GET timeline: %d %s", r.Code, r.Body.String())
	}
	if r := do(h, "GET", "/api/v1/replays/rpl_1/failures", ""); r.Code != 200 {
		t.Fatalf("GET failures: %d %s", r.Code, r.Body.String())
	}

	// Mutating POST fails with 401 when API_KEY is configured and missing/wrong.
	if r := do(h, "POST", "/api/v1/events", `{}`); r.Code != 401 {
		t.Fatalf("POST without key: %d, want 401", r.Code)
	}
	reqBad := httptest.NewRequest("POST", "/api/v1/events", strings.NewReader(`{}`))
	reqBad.Header.Set("X-API-Key", "wrong")
	recBad := httptest.NewRecorder()
	h.ServeHTTP(recBad, reqBad)
	if recBad.Code != 401 {
		t.Fatalf("POST wrong key: %d, want 401", recBad.Code)
	}

	// Valid Bearer token succeeds.
	reqOK := httptest.NewRequest("POST", "/api/v1/events", strings.NewReader(`{}`))
	reqOK.Header.Set("Authorization", "Bearer secret-key")
	recOK := httptest.NewRecorder()
	h.ServeHTTP(recOK, reqOK)
	if recOK.Code != 201 {
		t.Fatalf("POST valid key: %d %s", recOK.Code, recOK.Body.String())
	}

	// Prometheus /metrics endpoint reports scraped counters.
	mRec := do(h, "GET", "/metrics", "")
	if mRec.Code != 200 || !strings.Contains(mRec.Body.String(), "causal_replay_events_ingested_total") {
		t.Fatalf("/metrics unexpected: %d\n%s", mRec.Code, mRec.Body.String())
	}
}
