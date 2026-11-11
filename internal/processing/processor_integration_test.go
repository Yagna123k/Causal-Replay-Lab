package processing_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Yagna123k/causal-replay-lab/internal/eventstore"
	"github.com/Yagna123k/causal-replay-lab/internal/execution"
	"github.com/Yagna123k/causal-replay-lab/internal/idempotency"
	"github.com/Yagna123k/causal-replay-lab/internal/ingestion"
	"github.com/Yagna123k/causal-replay-lab/internal/ordering"
	"github.com/Yagna123k/causal-replay-lab/internal/processing"
	"github.com/Yagna123k/causal-replay-lab/internal/replay"
	"github.com/Yagna123k/causal-replay-lab/internal/testutil"
)

func ingest(t *testing.T, svc *ingestion.Service, id, txn, evType string, seq int64, payload string) {
	t.Helper()
	ts := time.Date(2026, 3, 1, 10, 0, int(seq), 0, time.UTC)
	_, err := svc.Ingest(context.Background(), ingestion.Request{
		EventID:        id,
		TransactionID:  txn,
		EventType:      evType,
		SequenceNumber: seq,
		CreatedAt:      &ts,
		Payload:        json.RawMessage(payload),
	})
	if err != nil {
		t.Fatalf("ingest %s: %v", id, err)
	}
}

func TestDuplicateProcessingCommitsOnce(t *testing.T) {
	pool := testutil.NewPool(t)
	ctx := context.Background()
	repo := eventstore.NewRepository(pool)
	ing := ingestion.NewService(repo)

	var sinkCalls atomic.Int32
	sink := replay.NewLiveEffectSink(func(context.Context, execution.Effect) error {
		sinkCalls.Add(1)
		return nil
	})
	proc := processing.NewProcessor(pool, processing.Options{LiveSink: sink})

	ingest(t, ing, "evt-101", "txn-dup", "TRANSACTION_CREATED", 1, `{"account_id":"a1","amount":100,"initial_balance":500}`)

	for i := 0; i < 3; i++ {
		out, err := proc.ProcessEvent(ctx, "evt-101", "idem-101", "")
		if err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
		if i == 0 && out.Duplicate {
			t.Fatal("first attempt should not be duplicate")
		}
		if i > 0 && !out.Duplicate {
			t.Fatalf("attempt %d should be marked duplicate", i)
		}
	}

	if sinkCalls.Load() != 1 {
		t.Fatalf("sinkCalls = %d, want 1", sinkCalls.Load())
	}
	var count int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM committed_effects WHERE event_id = 'evt-101'`).Scan(&count)
	if count != 1 {
		t.Fatalf("committed_effects count = %d, want 1", count)
	}

	// Reusing the same idempotency key for a different event payload must fail with ErrKeyConflict.
	ingest(t, ing, "evt-102", "txn-dup", "ACCOUNT_VALIDATED", 2, `{"valid":true}`)
	if _, err := proc.ProcessEvent(ctx, "evt-102", "idem-101", ""); !errors.Is(err, idempotency.ErrKeyConflict) {
		t.Fatalf("expected ErrKeyConflict, got %v", err)
	}
}

func TestConcurrentProcessingCommitsExactlyOnce(t *testing.T) {
	pool := testutil.NewPool(t)
	ctx := context.Background()
	repo := eventstore.NewRepository(pool)
	ing := ingestion.NewService(repo)

	var sinkCalls atomic.Int32
	sink := replay.NewLiveEffectSink(func(context.Context, execution.Effect) error {
		sinkCalls.Add(1)
		return nil
	})
	proc := processing.NewProcessor(pool, processing.Options{LiveSink: sink})

	ingest(t, ing, "evt-conc", "txn-conc", "TRANSACTION_CREATED", 1, `{"account_id":"a1","amount":50,"initial_balance":200}`)

	const workers = 20
	var wg sync.WaitGroup
	var fresh, dups, errs atomic.Int32
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := proc.ProcessEvent(ctx, "evt-conc", "key-conc", "")
			if err != nil {
				errs.Add(1)
				return
			}
			if out.Duplicate {
				dups.Add(1)
			} else {
				fresh.Add(1)
			}
		}()
	}
	wg.Wait()

	if errs.Load() != 0 || fresh.Load() != 1 || dups.Load() != workers-1 {
		t.Fatalf("fresh=%d dups=%d errs=%d", fresh.Load(), dups.Load(), errs.Load())
	}
	if sinkCalls.Load() != 1 {
		t.Fatalf("sinkCalls = %d, want 1", sinkCalls.Load())
	}
}

func TestOutOfOrderPoliciesAndDrain(t *testing.T) {
	pool := testutil.NewPool(t)
	ctx := context.Background()
	repo := eventstore.NewRepository(pool)
	ing := ingestion.NewService(repo)
	ordMgr := ordering.NewManager(pool)
	proc := processing.NewProcessor(pool, processing.Options{DefaultPolicy: ordering.PolicyBuffer})

	// Ingest 1, 3, 2
	ingest(t, ing, "e1", "txn-ooo", "TRANSACTION_CREATED", 1, `{"account_id":"a1","amount":100,"initial_balance":1000}`)
	ingest(t, ing, "e3", "txn-ooo", "DEBIT_INITIATED", 3, `{"amount":100}`)
	ingest(t, ing, "e2", "txn-ooo", "ACCOUNT_VALIDATED", 2, `{"valid":true}`)

	// Process 1 -> PROCESSED
	o1, err := proc.ProcessEvent(ctx, "e1", "", "")
	if err != nil || o1.Status != idempotency.StatusProcessed {
		t.Fatalf("e1: %+v %v", o1, err)
	}

	// Process 3 under REJECT -> ErrOutOfOrder
	if _, err := proc.ProcessEvent(ctx, "e3", "", ordering.PolicyReject); !errors.Is(err, ordering.ErrOutOfOrder) {
		t.Fatalf("reject expected ErrOutOfOrder, got %v", err)
	}

	// Process 3 under BUFFER -> buffered, missing=[2]
	o3, err := proc.ProcessEvent(ctx, "e3", "key-e3", ordering.PolicyBuffer)
	if err != nil || !o3.Buffered || o3.Status != idempotency.StatusPendingPrerequisite || len(o3.MissingSequences) != 1 || o3.MissingSequences[0] != 2 {
		t.Fatalf("buffer e3: %+v %v", o3, err)
	}

	// InspectTransaction reports gap & pending event
	st, err := ordMgr.InspectTransaction(ctx, "txn-ooo")
	if err != nil || !st.HasSequenceGap || len(st.MissingSequences) != 1 || st.MissingSequences[0] != 2 || len(st.PendingEvents) != 1 {
		t.Fatalf("inspect: %+v %v", st, err)
	}

	// Process 2 -> PROCESSED and automatically drains 3!
	o2, err := proc.ProcessEvent(ctx, "e2", "", ordering.PolicyBuffer)
	if err != nil || o2.Status != idempotency.StatusProcessed || len(o2.Drained) != 1 || o2.Drained[0].EventID != "e3" {
		t.Fatalf("e2 + drain: %+v %v", o2, err)
	}

	stAfter, err := ordMgr.InspectTransaction(ctx, "txn-ooo")
	if err != nil || stAfter.ProcessedSequence != 3 || stAfter.HasSequenceGap || len(stAfter.PendingEvents) != 0 {
		t.Fatalf("after drain: %+v %v", stAfter, err)
	}

	// FAIL_AND_FLAG scenario on separate transaction
	ingest(t, ing, "f1", "txn-flag", "TRANSACTION_CREATED", 1, `{"account_id":"a1","amount":10,"initial_balance":100}`)
	ingest(t, ing, "f3", "txn-flag", "DEBIT_INITIATED", 3, `{"amount":10}`)
	_, _ = proc.ProcessEvent(ctx, "f1", "", "")
	if _, err := proc.ProcessEvent(ctx, "f3", "", ordering.PolicyFailAndFlag); !errors.Is(err, ordering.ErrSequenceGapFlagged) {
		t.Fatalf("expected ErrSequenceGapFlagged, got %v", err)
	}
	stFlag, _ := ordMgr.InspectTransaction(ctx, "txn-flag")
	if !stFlag.HasSequenceGap || stFlag.GapDetails == "" {
		t.Fatalf("transaction should be flagged with gap: %+v", stFlag)
	}
}

func TestExpirePendingMarksDead(t *testing.T) {
	pool := testutil.NewPool(t)
	ctx := context.Background()
	repo := eventstore.NewRepository(pool)
	ing := ingestion.NewService(repo)
	ordMgr := ordering.NewManager(pool)

	now := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	proc := processing.NewProcessor(pool, processing.Options{
		DefaultPolicy: ordering.PolicyBuffer,
		PendingGapTTL: time.Minute,
		Clock:         &replay.EventClock{Current: now},
	})

	ingest(t, ing, "x2", "txn-exp", "ACCOUNT_VALIDATED", 2, `{"valid":true}`)
	out, err := proc.ProcessEvent(ctx, "x2", "", "")
	if err != nil || !out.Buffered {
		t.Fatalf("expected buffered, got %+v %v", out, err)
	}

	expired, err := ordMgr.ExpirePending(ctx, now.Add(2*time.Minute))
	if err != nil || expired != 1 {
		t.Fatalf("expired=%d err=%v", expired, err)
	}

	var status string
	_ = pool.QueryRow(ctx, `SELECT status FROM event_processing WHERE event_id = 'x2'`).Scan(&status)
	if status != "DEAD" {
		t.Fatalf("expected DEAD after expiry, got %s", status)
	}
}

func TestMidTransactionFailureRollsBackAtomically(t *testing.T) {
	pool := testutil.NewPool(t)
	ctx := context.Background()
	repo := eventstore.NewRepository(pool)
	ing := ingestion.NewService(repo)

	var sinkCalls atomic.Int32
	sink := replay.NewLiveEffectSink(func(context.Context, execution.Effect) error {
		sinkCalls.Add(1)
		return nil
	})
	failNext := true
	proc := processing.NewProcessor(pool, processing.Options{
		LiveSink: sink,
		BeforeCommit: func(context.Context, eventstore.Event) error {
			if failNext {
				return fmt.Errorf("simulated DB crash before commit")
			}
			return nil
		},
	})

	ingest(t, ing, "m1", "txn-atom", "TRANSACTION_CREATED", 1, `{"account_id":"a1","amount":10,"initial_balance":100}`)
	if _, err := proc.ProcessEvent(ctx, "m1", "k-atom", ""); err == nil {
		t.Fatal("expected injected pre-commit error")
	}

	// Verify nothing was committed: processed_sequence=0, 0 committed_effects, 0 idempotency_records, status=RECEIVED.
	var procSeq int64
	_ = pool.QueryRow(ctx, `SELECT processed_sequence FROM transactions WHERE transaction_id = 'txn-atom'`).Scan(&procSeq)
	var effCnt, idemCnt int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM committed_effects WHERE event_id = 'm1'`).Scan(&effCnt)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM idempotency_records WHERE idempotency_key = 'k-atom'`).Scan(&idemCnt)
	var epStatus string
	_ = pool.QueryRow(ctx, `SELECT status FROM event_processing WHERE event_id = 'm1'`).Scan(&epStatus)

	if procSeq != 0 || effCnt != 0 || idemCnt != 0 || epStatus != "RECEIVED" || sinkCalls.Load() != 0 {
		t.Fatalf("partial state leaked on rollback: procSeq=%d eff=%d idem=%d ep=%s sink=%d",
			procSeq, effCnt, idemCnt, epStatus, sinkCalls.Load())
	}

	// Retry once transient DB failure clears -> succeeds cleanly!
	failNext = false
	out, err := proc.ProcessEvent(ctx, "m1", "k-atom", "")
	if err != nil || out.Status != idempotency.StatusProcessed || sinkCalls.Load() != 1 {
		t.Fatalf("retry after rollback failed: %+v %v sink=%d", out, err, sinkCalls.Load())
	}
}
