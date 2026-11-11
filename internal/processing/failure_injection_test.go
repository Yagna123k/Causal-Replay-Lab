package processing_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Yagna123k/causal-replay-lab/internal/eventstore"
	"github.com/Yagna123k/causal-replay-lab/internal/ingestion"
	"github.com/Yagna123k/causal-replay-lab/internal/processing"
	"github.com/Yagna123k/causal-replay-lab/internal/testutil"
)

func TestDatabaseInterruptionKeepsStateConsistent(t *testing.T) {
	pool := testutil.NewPool(t)
	ctx := context.Background()
	repo := eventstore.NewRepository(pool)
	ing := ingestion.NewService(repo)

	ingest(t, ing, "db-int-1", "txn-db-int", "TRANSACTION_CREATED", 1, `{"account_id":"a1","amount":25,"initial_balance":100}`)

	// Create a dedicated pool pointed at the same schema and interrupt connections
	// mid-transaction inside BeforeCommit to simulate a DB outage before commit.
	cfg := pool.Config().Copy()
	tempPool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	proc := processing.NewProcessor(tempPool, processing.Options{
		BeforeCommit: func(ctx context.Context, _ eventstore.Event) error {
			// Cancel an already-expired child context or close a backend PID to break the connection.
			_, _ = pool.Exec(context.Background(),
				`SELECT pg_terminate_backend(pid)
				   FROM pg_stat_activity
				  WHERE pid <> pg_backend_pid()
				    AND application_name = ''
				    AND state = 'idle in transaction'`)
			// Also return context.Canceled as if the DB connection dropped.
			return context.Canceled
		},
	})

	if _, err := proc.ProcessEvent(ctx, "db-int-1", "key-db-int", ""); err == nil {
		tempPool.Close()
		t.Fatal("expected error after DB interruption")
	}
	tempPool.Close()

	// Verify state on the surviving pool remained completely consistent (rolled back).
	var procSeq int64
	_ = pool.QueryRow(ctx, `SELECT processed_sequence FROM transactions WHERE transaction_id = 'txn-db-int'`).Scan(&procSeq)
	var epStatus string
	_ = pool.QueryRow(ctx, `SELECT status FROM event_processing WHERE event_id = 'db-int-1'`).Scan(&epStatus)
	var effCount int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM committed_effects WHERE event_id = 'db-int-1'`).Scan(&effCount)

	if procSeq != 0 || epStatus != "RECEIVED" || effCount != 0 {
		t.Fatalf("inconsistent state after DB interruption: procSeq=%d epStatus=%s effCount=%d",
			procSeq, epStatus, effCount)
	}

	// Recovery on a healthy processor succeeds cleanly.
	healthyProc := processing.NewProcessor(pool, processing.Options{})
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := healthyProc.ProcessEvent(cctx, "db-int-1", "key-db-int", "")
	if err != nil || out.SequenceNumber != 1 {
		t.Fatalf("recovery after DB interruption failed: %+v %v", out, err)
	}
}
