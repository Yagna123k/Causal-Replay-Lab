package api

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Yagna123k/causal-replay-lab/internal/execution"
	"github.com/Yagna123k/causal-replay-lab/internal/replay"
)

type fakeReplayer struct {
	sess   execution.Session
	start  replay.StartResult
	execs  []execution.EventExecution
	cmp    execution.ComparisonResult
	err    error
	cmpErr error
}

func (f *fakeReplayer) CreateSession(context.Context, replay.CreateRequest) (execution.Session, error) {
	return f.sess, f.err
}
func (f *fakeReplayer) StartSession(context.Context, string) (replay.StartResult, error) {
	return f.start, f.err
}
func (f *fakeReplayer) GetSession(context.Context, string) (execution.Session, error) {
	return f.sess, f.err
}
func (f *fakeReplayer) ListExecutions(context.Context, string) ([]execution.EventExecution, error) {
	return f.execs, f.err
}
func (f *fakeReplayer) Compare(context.Context, string, string) (execution.ComparisonResult, error) {
	return f.cmp, f.cmpErr
}

type fakeOriginal struct {
	sess execution.Session
	err  error
}

func (f *fakeOriginal) ProcessOriginal(context.Context, string) (execution.Session, error) {
	return f.sess, f.err
}

func newReplayTestHandler(rep Replayer, orig OriginalProcessor) http.Handler {
	return NewHandler(Deps{
		Ingester:       fakeIngester{},
		Reader:         &fakeReader{},
		Replayer:       rep,
		Original:       orig,
		RequestTimeout: time.Second,
		MaxBodyBytes:   1024,
	})
}

func TestReplayEndpoints(t *testing.T) {
	rep := &fakeReplayer{
		sess:  execution.Session{SessionID: "rpl_1", TransactionID: "t1", Mode: execution.ModeReplay, Status: execution.StatusCreated},
		start: replay.StartResult{Session: execution.Session{SessionID: "rpl_1", Status: execution.StatusCompleted}},
		execs: []execution.EventExecution{{ExecutionID: 1, SessionID: "rpl_1", SequenceNumber: 1, Status: execution.EventSucceeded}},
		cmp:   execution.ComparisonResult{LeftSessionID: "rpl_1", RightSessionID: "orig_1", Equivalent: true},
	}
	orig := &fakeOriginal{
		sess: execution.Session{SessionID: "orig_1", TransactionID: "t1", Mode: execution.ModeOriginal, Status: execution.StatusCompleted},
	}
	h := newReplayTestHandler(rep, orig)

	if rec := do(h, "POST", "/api/v1/transactions/t1/process", ""); rec.Code != 200 {
		t.Fatalf("process original: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(h, "POST", "/api/v1/replays", `{"transaction_id":"t1"}`); rec.Code != 201 {
		t.Fatalf("create replay: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(h, "POST", "/api/v1/replays/rpl_1/start", ""); rec.Code != 200 {
		t.Fatalf("start replay: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(h, "GET", "/api/v1/replays/rpl_1", ""); rec.Code != 200 {
		t.Fatalf("get replay: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(h, "GET", "/api/v1/replays/rpl_1/executions", ""); rec.Code != 200 {
		t.Fatalf("list executions: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(h, "GET", "/api/v1/replay-comparisons?left=rpl_1&right=original", ""); rec.Code != 200 {
		t.Fatalf("compare: %d %s", rec.Code, rec.Body.String())
	} else {
		var cmp execution.ComparisonResult
		_ = json.Unmarshal(rec.Body.Bytes(), &cmp)
		if !cmp.Equivalent {
			t.Fatalf("expected equivalent=true: %+v", cmp)
		}
	}

	if rec := do(h, "GET", "/api/v1/replay-comparisons?left=rpl_1", ""); rec.Code != 400 {
		t.Fatalf("missing right param: %d", rec.Code)
	}

	rep.err = execution.ErrSessionNotFound
	if rec := do(h, "GET", "/api/v1/replays/missing", ""); rec.Code != 404 {
		t.Fatalf("not found: %d", rec.Code)
	}

	rep.err = execution.ErrInvalidTransition
	if rec := do(h, "POST", "/api/v1/replays/rpl_1/start", ""); rec.Code != 409 {
		t.Fatalf("invalid transition: %d", rec.Code)
	}

	rep.cmpErr = execution.ErrOriginalNotFound
	if rec := do(h, "GET", "/api/v1/replay-comparisons?left=rpl_1&right=original", ""); rec.Code != 404 {
		t.Fatalf("missing original baseline: %d", rec.Code)
	}
}
