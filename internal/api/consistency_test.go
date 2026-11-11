package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Yagna123k/causal-replay-lab/internal/idempotency"
	"github.com/Yagna123k/causal-replay-lab/internal/ordering"
	"github.com/Yagna123k/causal-replay-lab/internal/processing"
)

type fakeProcessor struct {
	outcome  processing.ProcessOutcome
	outcomes []processing.ProcessOutcome
	err      error
	gotKey   string
	gotPol   ordering.Policy
}

func (f *fakeProcessor) ProcessEvent(_ context.Context, _ string, key string, pol ordering.Policy) (processing.ProcessOutcome, error) {
	f.gotKey, f.gotPol = key, pol
	return f.outcome, f.err
}

func (f *fakeProcessor) ProcessTransaction(_ context.Context, _ string, pol ordering.Policy) ([]processing.ProcessOutcome, error) {
	f.gotPol = pol
	return f.outcomes, f.err
}

type fakeInspector struct {
	st  ordering.TransactionStatus
	err error
}

func (f *fakeInspector) InspectTransaction(context.Context, string) (ordering.TransactionStatus, error) {
	return f.st, f.err
}

func newConsistencyHandler(p EventProcessor, ins OrderingInspector) http.Handler {
	return NewHandler(Deps{
		Ingester:       fakeIngester{},
		Reader:         &fakeReader{},
		Processor:      p,
		Inspector:      ins,
		RequestTimeout: time.Second,
		MaxBodyBytes:   1024,
	})
}

func TestConsistencyEndpoints(t *testing.T) {
	proc := &fakeProcessor{
		outcome: processing.ProcessOutcome{
			EventID:        "e1",
			TransactionID:  "t1",
			SequenceNumber: 1,
			Status:         idempotency.StatusProcessed,
		},
		outcomes: []processing.ProcessOutcome{{EventID: "e1", Status: idempotency.StatusProcessed}},
	}
	ins := &fakeInspector{
		st: ordering.TransactionStatus{
			TransactionID:     "t1",
			ProcessedSequence: 1,
			LastSequence:      3,
			HasSequenceGap:    true,
			MissingSequences:  []int64{2},
		},
	}
	h := newConsistencyHandler(proc, ins)

	// Process event with Idempotency-Key header
	req := httptest.NewRequest("POST", "/api/v1/events/e1/process", strings.NewReader(`{"policy":"REJECT"}`))
	req.Header.Set("Idempotency-Key", "k-123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 || proc.gotKey != "k-123" || proc.gotPol != ordering.PolicyReject {
		t.Fatalf("code=%d key=%s pol=%s body=%s", rec.Code, proc.gotKey, proc.gotPol, rec.Body.String())
	}

	// Buffered returns 202 Accepted
	proc.outcome = processing.ProcessOutcome{EventID: "e3", Buffered: true, Status: idempotency.StatusPendingPrerequisite}
	if rec := do(h, "POST", "/api/v1/events/e3/process", ""); rec.Code != 202 {
		t.Fatalf("buffered status = %d, want 202", rec.Code)
	}

	// Out-of-order REJECT returns 409 + Retry-After
	proc.err = ordering.ErrOutOfOrder
	if rec := do(h, "POST", "/api/v1/events/e3/process", ""); rec.Code != 409 || rec.Header().Get("Retry-After") != "1" || errCode(t, rec) != "out_of_order_retryable" {
		t.Fatalf("reject: code=%d retry=%s body=%s", rec.Code, rec.Header().Get("Retry-After"), rec.Body.String())
	}

	// Idempotency key conflict returns 409
	proc.err = idempotency.ErrKeyConflict
	if rec := do(h, "POST", "/api/v1/events/e2/process", ""); rec.Code != 409 || errCode(t, rec) != "idempotency_conflict" {
		t.Fatalf("key conflict: code=%d body=%s", rec.Code, rec.Body.String())
	}

	// Invalid policy returns 400
	proc.err = nil
	if rec := do(h, "POST", "/api/v1/events/e1/process", `{"policy":"BOGUS"}`); rec.Code != 400 || errCode(t, rec) != "invalid_policy" {
		t.Fatalf("invalid policy: code=%d body=%s", rec.Code, rec.Body.String())
	}

	// Transaction batch process & status inspection
	if rec := do(h, "POST", "/api/v1/transactions/t1/process-events", ""); rec.Code != 200 {
		t.Fatalf("process-events: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(h, "GET", "/api/v1/transactions/t1/status", ""); rec.Code != 200 {
		t.Fatalf("status: %d %s", rec.Code, rec.Body.String())
	} else {
		var st ordering.TransactionStatus
		_ = json.Unmarshal(rec.Body.Bytes(), &st)
		if !st.HasSequenceGap || len(st.MissingSequences) != 1 || st.MissingSequences[0] != 2 {
			t.Fatalf("unexpected status: %+v", st)
		}
	}
}
