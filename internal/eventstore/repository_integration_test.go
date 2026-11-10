package eventstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Yagna123k/causal-replay-lab/internal/api"
	"github.com/Yagna123k/causal-replay-lab/internal/db"
	"github.com/Yagna123k/causal-replay-lab/internal/eventstore"
	"github.com/Yagna123k/causal-replay-lab/internal/ingestion"
	"github.com/Yagna123k/causal-replay-lab/internal/testutil"
	"github.com/Yagna123k/causal-replay-lab/migrations"
)

func req(id, txn string, seq int64, amount int) ingestion.Request {
	ts := time.Date(2026, 1, 1, 0, 0, int(seq), 0, time.UTC)
	return ingestion.Request{
		EventID:        id,
		TransactionID:  txn,
		EventType:      "LEDGER_UPDATED",
		Payload:        json.RawMessage(fmt.Sprintf(`{"amount":%d}`, amount)),
		SequenceNumber: seq,
		CreatedAt:      &ts,
	}
}

func setup(t *testing.T) (*pgxpool.Pool, *eventstore.Repository, *ingestion.Service) {
	pool := testutil.NewPool(t)
	repo := eventstore.NewRepository(pool)
	return pool, repo, ingestion.NewService(repo)
}

func TestMigrationsUpDownUp(t *testing.T) {
	pool := testutil.NewPool(t)
	ctx := context.Background()

	if done, err := db.MigrateUp(ctx, pool, migrations.FS); err != nil || len(done) != 0 {
		t.Fatalf("second up should be a no-op: %v %v", done, err)
	}
	v, err := db.MigrateDown(ctx, pool, migrations.FS)
	if err != nil || v != 1 {
		t.Fatalf("down: %v %v", v, err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables
		WHERE table_schema = current_schema() AND table_name IN ('events','transactions','event_processing')`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("tables should be gone: n=%d err=%v", n, err)
	}
	if done, err := db.MigrateUp(ctx, pool, migrations.FS); err != nil || len(done) != 1 {
		t.Fatalf("up again: %v %v", done, err)
	}
}

func TestIngestAndRetrieve(t *testing.T) {
	_, repo, svc := setup(t)
	ctx := context.Background()

	res, err := svc.Ingest(ctx, req("e1", "t1", 1, 10))
	if err != nil || res.Duplicate {
		t.Fatalf("ingest: %+v %v", res, err)
	}
	got, err := repo.GetEvent(ctx, "e1")
	if err != nil || got.TransactionID != "t1" || got.SequenceNumber != 1 || got.ReceivedAt.IsZero() {
		t.Fatalf("get: %+v %v", got, err)
	}
	if _, err := repo.GetEvent(ctx, "missing"); !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	txn, err := repo.GetTransaction(ctx, "t1")
	if err != nil || txn.LastSequence != 1 {
		t.Fatalf("txn: %+v %v", txn, err)
	}
}

func TestHistoryIsDeterministicallyOrdered(t *testing.T) {
	_, repo, svc := setup(t)
	ctx := context.Background()

	for _, seq := range []int64{3, 1, 5, 2, 4} { // arrive out of order
		if _, err := svc.Ingest(ctx, req(fmt.Sprintf("e%d", seq), "t1", seq, int(seq))); err != nil {
			t.Fatal(err)
		}
	}
	evs, err := repo.ListByTransaction(ctx, "t1", 0, 100)
	if err != nil || len(evs) != 5 {
		t.Fatalf("list: %d %v", len(evs), err)
	}
	for i, e := range evs {
		if e.SequenceNumber != int64(i+1) {
			t.Fatalf("position %d has sequence %d", i, e.SequenceNumber)
		}
	}
	page, _ := repo.ListByTransaction(ctx, "t1", 2, 2)
	if len(page) != 2 || page[0].SequenceNumber != 3 || page[1].SequenceNumber != 4 {
		t.Fatalf("pagination wrong: %+v", page)
	}
	txn, _ := repo.GetTransaction(ctx, "t1")
	if txn.LastSequence != 5 {
		t.Fatalf("last_sequence = %d", txn.LastSequence)
	}
}

func TestDuplicateDeliveryIsIdempotent(t *testing.T) {
	pool, _, svc := setup(t)
	ctx := context.Background()

	first, err := svc.Ingest(ctx, req("e1", "t1", 1, 10))
	if err != nil || first.Duplicate {
		t.Fatal(err)
	}
	// Retry with different key order and metadata: logically identical.
	retry := req("e1", "t1", 1, 10)
	retry.Metadata = json.RawMessage(`{"attempt":2}`)
	second, err := svc.Ingest(ctx, retry)
	if err != nil || !second.Duplicate {
		t.Fatalf("expected duplicate, got %+v %v", second, err)
	}
	if !second.Event.ReceivedAt.Equal(first.Event.ReceivedAt) {
		t.Fatal("duplicate must return the original stored event")
	}
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&n)
	if n != 1 {
		t.Fatalf("events = %d, want 1", n)
	}
}

func TestConflicts(t *testing.T) {
	_, _, svc := setup(t)
	ctx := context.Background()
	if _, err := svc.Ingest(ctx, req("e1", "t1", 1, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Ingest(ctx, req("e1", "t1", 1, 999)); !errors.Is(err, eventstore.ErrPayloadConflict) {
		t.Fatalf("same id different payload: %v", err)
	}
	if _, err := svc.Ingest(ctx, req("e1", "t1", 2, 10)); !errors.Is(err, eventstore.ErrPayloadConflict) {
		t.Fatalf("same id different sequence: %v", err)
	}
	if _, err := svc.Ingest(ctx, req("e2", "t1", 1, 10)); !errors.Is(err, eventstore.ErrSequenceConflict) {
		t.Fatalf("same sequence different id: %v", err)
	}
}

func TestConcurrentDuplicatesStoreOnce(t *testing.T) {
	pool, _, svc := setup(t)
	ctx := context.Background()

	const workers = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	created, dups := 0, 0
	errs := []error{}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := svc.Ingest(ctx, req("e1", "t1", 1, 10))
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				errs = append(errs, err)
			case res.Duplicate:
				dups++
			default:
				created++
			}
		}()
	}
	wg.Wait()
	if len(errs) > 0 {
		t.Fatalf("errors: %v", errs)
	}
	if created != 1 || dups != workers-1 {
		t.Fatalf("created=%d dups=%d", created, dups)
	}
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&n)
	if n != 1 {
		t.Fatalf("events = %d", n)
	}
}

func TestConcurrentDistinctEventsSameTransaction(t *testing.T) {
	_, repo, svc := setup(t)
	ctx := context.Background()

	const n = 30
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(seq int64) {
			defer wg.Done()
			_, err := svc.Ingest(ctx, req(fmt.Sprintf("e%d", seq), "t1", seq, int(seq)))
			errCh <- err
		}(int64(i))
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	evs, _ := repo.ListByTransaction(ctx, "t1", 0, 100)
	if len(evs) != n {
		t.Fatalf("got %d events", len(evs))
	}
	txn, _ := repo.GetTransaction(ctx, "t1")
	if txn.LastSequence != n {
		t.Fatalf("last_sequence = %d", txn.LastSequence)
	}
}

func TestConcurrentSameSequenceDifferentEvents(t *testing.T) {
	pool, _, svc := setup(t)
	ctx := context.Background()

	const workers = 10
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, conflicts := 0, 0
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := svc.Ingest(ctx, req(fmt.Sprintf("e%d", i), "t1", 1, 10))
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				ok++
			} else if errors.Is(err, eventstore.ErrSequenceConflict) {
				conflicts++
			} else {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if ok != 1 || conflicts != workers-1 {
		t.Fatalf("ok=%d conflicts=%d", ok, conflicts)
	}
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&n)
	if n != 1 {
		t.Fatalf("events = %d", n)
	}
}

func TestEventsAreImmutable(t *testing.T) {
	pool, _, svc := setup(t)
	ctx := context.Background()
	if _, err := svc.Ingest(ctx, req("e1", "t1", 1, 10)); err != nil {
		t.Fatal(err)
	}
	for name, stmt := range map[string]string{
		"update":   `UPDATE events SET event_type = 'HACKED' WHERE event_id = 'e1'`,
		"delete":   `DELETE FROM events WHERE event_id = 'e1'`,
		"truncate": `TRUNCATE events`,
	} {
		if _, err := pool.Exec(ctx, stmt); err == nil {
			t.Errorf("%s should be rejected", name)
		}
	}
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE event_type = 'LEDGER_UPDATED'`).Scan(&n)
	if n != 1 {
		t.Fatal("event was modified")
	}
}

func TestDatabaseConstraintsRejectBadRows(t *testing.T) {
	pool, _, _ := setup(t)
	ctx := context.Background()
	_, _ = pool.Exec(ctx, `INSERT INTO transactions (transaction_id) VALUES ('t1')`)
	bad := []string{
		`INSERT INTO events (event_id, transaction_id, event_type, payload, sequence_number, created_at, payload_hash) VALUES ('a','t1','X','{}',0,now(),'h')`,
		`INSERT INTO events (event_id, transaction_id, event_type, payload, sequence_number, created_at, payload_hash) VALUES ('b','t1','X','[]',1,now(),'h')`,
		`INSERT INTO events (event_id, transaction_id, event_type, payload, sequence_number, created_at, payload_hash) VALUES ('c','nope','X','{}',1,now(),'h')`,
		`INSERT INTO events (event_id, transaction_id, event_type, payload, sequence_number, created_at, payload_hash) VALUES ('','t1','X','{}',1,now(),'h')`,
	}
	for _, s := range bad {
		if _, err := pool.Exec(ctx, s); err == nil {
			t.Errorf("expected constraint violation for: %s", s)
		}
	}
}

func TestEndToEndHTTP(t *testing.T) {
	_, repo, svc := setup(t)
	srv := httptest.NewServer(api.NewHandler(api.Deps{
		Ingester: svc, Reader: repo, Pinger: repo,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}))
	defer srv.Close()

	post := func(body string) (*http.Response, string) {
		resp, err := http.Post(srv.URL+"/api/v1/events", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp, string(b)
	}
	body := func(id string, seq int, amount int) string {
		return fmt.Sprintf(`{"event_id":%q,"transaction_id":"t1","event_type":"LEDGER_UPDATED","sequence_number":%d,"created_at":"2026-01-01T00:00:00Z","payload":{"amount":%d}}`, id, seq, amount)
	}

	if r, b := post(body("e2", 2, 5)); r.StatusCode != 201 {
		t.Fatalf("create: %d %s", r.StatusCode, b)
	}
	if r, b := post(body("e1", 1, 5)); r.StatusCode != 201 {
		t.Fatalf("create: %d %s", r.StatusCode, b)
	}
	if r, _ := post(body("e1", 1, 5)); r.StatusCode != 200 {
		t.Fatalf("duplicate should be 200, got %d", r.StatusCode)
	}
	if r, _ := post(body("e1", 1, 6)); r.StatusCode != 409 {
		t.Fatalf("conflict should be 409, got %d", r.StatusCode)
	}
	if r, _ := post(`{"event_id":"x"}`); r.StatusCode != 422 {
		t.Fatalf("invalid should be 422, got %d", r.StatusCode)
	}

	resp, err := http.Get(srv.URL + "/api/v1/transactions/t1/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var hist struct {
		Events []eventstore.Event `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&hist); err != nil {
		t.Fatal(err)
	}
	if len(hist.Events) != 2 || hist.Events[0].EventID != "e1" || hist.Events[1].EventID != "e2" {
		t.Fatalf("history not ordered by sequence: %+v", hist.Events)
	}

	if r, _ := http.Get(srv.URL + "/api/v1/transactions/unknown/events"); r.StatusCode != 404 {
		t.Fatalf("unknown txn: %d", r.StatusCode)
	}
	if r, _ := http.Get(srv.URL + "/readyz"); r.StatusCode != 200 {
		t.Fatalf("readyz: %d", r.StatusCode)
	}
}
