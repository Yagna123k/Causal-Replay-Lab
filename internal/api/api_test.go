package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Yagna123k/causal-replay-lab/internal/eventstore"
	"github.com/Yagna123k/causal-replay-lab/internal/ingestion"
)

type fakeIngester struct {
	res ingestion.Result
	err error
}

func (f fakeIngester) Ingest(context.Context, ingestion.Request) (ingestion.Result, error) {
	return f.res, f.err
}

type fakeReader struct {
	events  []eventstore.Event
	txnErr  error
	listErr error
	lastAft int64
	lastLim int
}

func (f *fakeReader) GetEvent(_ context.Context, id string) (eventstore.Event, error) {
	for _, e := range f.events {
		if e.EventID == id {
			return e, nil
		}
	}
	return eventstore.Event{}, eventstore.ErrNotFound
}
func (f *fakeReader) GetTransaction(context.Context, string) (eventstore.Transaction, error) {
	return eventstore.Transaction{}, f.txnErr
}
func (f *fakeReader) ListByTransaction(_ context.Context, _ string, after int64, limit int) ([]eventstore.Event, error) {
	f.lastAft, f.lastLim = after, limit
	return f.events, f.listErr
}

type fakePinger struct{ err error }

func (f fakePinger) Ping(context.Context) error { return f.err }

func newTestHandler(ing Ingester, rd Reader, p Pinger) http.Handler {
	return NewHandler(Deps{Ingester: ing, Reader: rd, Pinger: p, RequestTimeout: time.Second, MaxBodyBytes: 256})
}

func do(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func errCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var b ErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil {
		t.Fatalf("bad error body %q: %v", rec.Body.String(), err)
	}
	return b.Error.Code
}

func TestIngestStatusMapping(t *testing.T) {
	ev := eventstore.Event{EventID: "e1", Payload: json.RawMessage(`{}`), Metadata: json.RawMessage(`{}`)}
	cases := []struct {
		name   string
		ing    fakeIngester
		body   string
		status int
		code   string
	}{
		{"created", fakeIngester{res: ingestion.Result{Event: ev}}, `{}`, 201, ""},
		{"duplicate", fakeIngester{res: ingestion.Result{Event: ev, Duplicate: true}}, `{}`, 200, ""},
		{"validation", fakeIngester{err: &ingestion.ValidationError{Fields: []ingestion.FieldError{{Field: "x", Message: "bad"}}}}, `{}`, 422, "validation_failed"},
		{"payload conflict", fakeIngester{err: eventstore.ErrPayloadConflict}, `{}`, 409, "event_conflict"},
		{"sequence conflict", fakeIngester{err: eventstore.ErrSequenceConflict}, `{}`, 409, "sequence_conflict"},
		{"internal", fakeIngester{err: errors.New("db exploded")}, `{}`, 500, "internal_error"},
		{"bad json", fakeIngester{}, `{not json`, 400, "invalid_json"},
		{"unknown field", fakeIngester{}, `{"bogus":1}`, 400, "invalid_json"},
		{"trailing data", fakeIngester{}, `{} {}`, 400, "invalid_json"},
		{"too large", fakeIngester{}, `{"event_id":"` + strings.Repeat("a", 400) + `"}`, 413, "body_too_large"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(newTestHandler(tc.ing, &fakeReader{}, nil), "POST", "/api/v1/events", tc.body)
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d (%s)", rec.Code, tc.status, rec.Body.String())
			}
			if tc.code != "" && errCode(t, rec) != tc.code {
				t.Fatalf("code = %s, want %s", errCode(t, rec), tc.code)
			}
		})
	}
}

func TestValidationDetailsReturned(t *testing.T) {
	ing := fakeIngester{err: &ingestion.ValidationError{Fields: []ingestion.FieldError{{Field: "event_id", Message: "is required"}}}}
	rec := do(newTestHandler(ing, &fakeReader{}, nil), "POST", "/api/v1/events", `{}`)
	var b ErrorBody
	_ = json.Unmarshal(rec.Body.Bytes(), &b)
	if len(b.Error.Details) != 1 || b.Error.Details[0].Field != "event_id" {
		t.Fatalf("details missing: %s", rec.Body.String())
	}
}

func TestGetEvent(t *testing.T) {
	rd := &fakeReader{events: []eventstore.Event{{EventID: "e1", Payload: json.RawMessage(`{}`), Metadata: json.RawMessage(`{}`)}}}
	h := newTestHandler(fakeIngester{}, rd, nil)
	if rec := do(h, "GET", "/api/v1/events/e1", ""); rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	rec := do(h, "GET", "/api/v1/events/nope", "")
	if rec.Code != 404 || errCode(t, rec) != "not_found" {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
}

func TestListTransactionEvents(t *testing.T) {
	mk := func(seq int64) eventstore.Event {
		return eventstore.Event{EventID: "e", SequenceNumber: seq, Payload: json.RawMessage(`{}`), Metadata: json.RawMessage(`{}`)}
	}
	rd := &fakeReader{events: []eventstore.Event{mk(1), mk(2)}}
	h := newTestHandler(fakeIngester{}, rd, nil)

	rec := do(h, "GET", "/api/v1/transactions/t1/events?limit=2&after_sequence=0", "")
	if rec.Code != 200 || rd.lastLim != 2 || rd.lastAft != 0 {
		t.Fatalf("status %d lim %d", rec.Code, rd.lastLim)
	}
	var resp historyResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.NextAfterSequence == nil || *resp.NextAfterSequence != 2 {
		t.Fatalf("expected next_after_sequence=2, got %+v", resp)
	}

	rec = do(h, "GET", "/api/v1/transactions/t1/events?limit=10", "")
	resp = historyResponse{}
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.NextAfterSequence != nil {
		t.Fatal("did not expect a next cursor")
	}

	for _, q := range []string{"limit=0", "limit=abc", "limit=5000", "after_sequence=-1", "after_sequence=x"} {
		if rec := do(h, "GET", "/api/v1/transactions/t1/events?"+q, ""); rec.Code != 400 {
			t.Errorf("%s: status %d", q, rec.Code)
		}
	}

	rd.txnErr = eventstore.ErrNotFound
	if rec := do(h, "GET", "/api/v1/transactions/t1/events", ""); rec.Code != 404 {
		t.Fatalf("unknown transaction: status %d", rec.Code)
	}
}

func TestHealth(t *testing.T) {
	ok := newTestHandler(fakeIngester{}, &fakeReader{}, fakePinger{})
	if rec := do(ok, "GET", "/healthz", ""); rec.Code != 200 {
		t.Fatalf("healthz %d", rec.Code)
	}
	if rec := do(ok, "GET", "/readyz", ""); rec.Code != 200 {
		t.Fatalf("readyz %d", rec.Code)
	}
	bad := newTestHandler(fakeIngester{}, &fakeReader{}, fakePinger{err: errors.New("down")})
	if rec := do(bad, "GET", "/readyz", ""); rec.Code != 503 {
		t.Fatalf("readyz %d", rec.Code)
	}
}

func TestRequestIDAndNotFoundAndPanic(t *testing.T) {
	h := newTestHandler(fakeIngester{}, &fakeReader{}, nil)
	rec := do(h, "GET", "/nope", "")
	if rec.Code != 404 || rec.Header().Get("X-Request-ID") == "" || errCode(t, rec) != "not_found" {
		t.Fatalf("status %d headers %v", rec.Code, rec.Header())
	}

	panicky := chain(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") }),
		requestID, recoverer(newTestHandlerLogger()))
	if rec := do(panicky, "GET", "/", ""); rec.Code != 500 || errCode(t, rec) != "internal_error" {
		t.Fatalf("panic not recovered: %d", rec.Code)
	}
}
