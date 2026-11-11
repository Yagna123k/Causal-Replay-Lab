package replay_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Yagna123k/causal-replay-lab/internal/api"
	"github.com/Yagna123k/causal-replay-lab/internal/eventstore"
	"github.com/Yagna123k/causal-replay-lab/internal/execution"
	"github.com/Yagna123k/causal-replay-lab/internal/ingestion"
	"github.com/Yagna123k/causal-replay-lab/internal/replay"
	"github.com/Yagna123k/causal-replay-lab/internal/testutil"
)

func TestReplayEndToEndPostgres(t *testing.T) {
	pool := testutil.NewPool(t)
	ctx := context.Background()

	repo := eventstore.NewRepository(pool)
	ing := ingestion.NewService(repo)
	execStore := execution.NewStore(pool)
	reg := replay.DefaultRegistry()
	liveSink := replay.NewLiveEffectSink(nil)
	orig := replay.NewOriginalRunner(repo, execStore, reg, liveSink)
	eng := replay.NewEngine(repo, execStore, reg)

	srv := httptest.NewServer(api.NewHandler(api.Deps{
		Ingester: ing,
		Reader:   repo,
		Replayer: eng,
		Original: orig,
		Pinger:   repo,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}))
	defer srv.Close()

	// Ingest a 5-event transaction workflow.
	events := []struct {
		id      string
		seq     int64
		evType  string
		payload string
	}{
		{"e1", 1, "TRANSACTION_CREATED", `{"account_id":"acc-99","amount":300,"currency":"USD","initial_balance":1000}`},
		{"e2", 2, "ACCOUNT_VALIDATED", `{"valid":true}`},
		{"e3", 3, "DEBIT_INITIATED", `{"amount":300}`},
		{"e4", 4, "LEDGER_UPDATED", `{"amount":300,"operation":"debit"}`},
		{"e5", 5, "TRANSACTION_COMPLETED", `{}`},
	}
	for _, ev := range events {
		body := fmt.Sprintf(`{"event_id":%q,"transaction_id":"txn-e2e","event_type":%q,"sequence_number":%d,"created_at":%q,"payload":%s}`,
			ev.id, ev.evType, ev.seq, time.Date(2026, 2, 1, 12, 0, int(ev.seq), 0, time.UTC).Format(time.RFC3339), ev.payload)
		resp, err := http.Post(srv.URL+"/api/v1/events", "application/json", bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("ingest %s: %d", ev.id, resp.StatusCode)
		}
	}

	// Process original baseline.
	resp, err := http.Post(srv.URL+"/api/v1/transactions/txn-e2e/process", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	var origSess execution.Session
	_ = json.NewDecoder(resp.Body).Decode(&origSess)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || origSess.Status != execution.StatusCompleted {
		t.Fatalf("original process failed: %d %+v", resp.StatusCode, origSess)
	}

	// Create and start two replay sessions.
	var replayIDs []string
	var hashes []string
	for i := 0; i < 2; i++ {
		cResp, err := http.Post(srv.URL+"/api/v1/replays", "application/json", bytes.NewBufferString(`{"transaction_id":"txn-e2e"}`))
		if err != nil {
			t.Fatal(err)
		}
		var created execution.Session
		_ = json.NewDecoder(cResp.Body).Decode(&created)
		cResp.Body.Close()
		if cResp.StatusCode != http.StatusCreated {
			t.Fatalf("create replay: %d", cResp.StatusCode)
		}

		sResp, err := http.Post(srv.URL+"/api/v1/replays/"+created.SessionID+"/start", "application/json", nil)
		if err != nil {
			t.Fatal(err)
		}
		var started replay.StartResult
		_ = json.NewDecoder(sResp.Body).Decode(&started)
		sResp.Body.Close()
		if sResp.StatusCode != http.StatusOK || started.Session.Status != execution.StatusCompleted {
			t.Fatalf("start replay: %d %+v", sResp.StatusCode, started)
		}
		replayIDs = append(replayIDs, created.SessionID)
		hashes = append(hashes, started.Session.FinalStateHash)
	}

	if hashes[0] != hashes[1] || hashes[0] != origSess.FinalStateHash {
		t.Fatalf("hashes mismatch: r1=%s r2=%s orig=%s", hashes[0], hashes[1], origSess.FinalStateHash)
	}

	// Compare replay 1 vs original baseline.
	cmpResp, err := http.Get(srv.URL + "/api/v1/replay-comparisons?left=" + replayIDs[0] + "&right=original")
	if err != nil {
		t.Fatal(err)
	}
	var cmp execution.ComparisonResult
	_ = json.NewDecoder(cmpResp.Body).Decode(&cmp)
	cmpResp.Body.Close()
	if cmpResp.StatusCode != http.StatusOK || !cmp.Equivalent {
		t.Fatalf("comparison failed: %d %+v", cmpResp.StatusCode, cmp)
	}

	// Inspect execution history.
	exResp, err := http.Get(srv.URL + "/api/v1/replays/" + replayIDs[0] + "/executions")
	if err != nil {
		t.Fatal(err)
	}
	var exBody struct {
		Executions []execution.EventExecution `json:"executions"`
	}
	_ = json.NewDecoder(exResp.Body).Decode(&exBody)
	exResp.Body.Close()
	if exResp.StatusCode != http.StatusOK || len(exBody.Executions) != 5 {
		t.Fatalf("executions failed: %d len=%d", exResp.StatusCode, len(exBody.Executions))
	}
	_ = ctx
}
