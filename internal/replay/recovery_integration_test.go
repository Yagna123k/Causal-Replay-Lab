package replay_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Yagna123k/causal-replay-lab/internal/checkpoint"
	"github.com/Yagna123k/causal-replay-lab/internal/eventstore"
	"github.com/Yagna123k/causal-replay-lab/internal/execution"
	"github.com/Yagna123k/causal-replay-lab/internal/ingestion"
	"github.com/Yagna123k/causal-replay-lab/internal/replay"
	"github.com/Yagna123k/causal-replay-lab/internal/testutil"
)

func seedTenEvents(t *testing.T, svc *ingestion.Service, txnID string) {
	t.Helper()
	ctx := context.Background()
	ingestOne := func(seq int64, evType, payload string) {
		ts := time.Date(2026, 5, 1, 10, 0, int(seq), 0, time.UTC)
		if _, err := svc.Ingest(ctx, ingestion.Request{
			EventID:        fmt.Sprintf("%s-e%d", txnID, seq),
			TransactionID:  txnID,
			EventType:      evType,
			SequenceNumber: seq,
			CreatedAt:      &ts,
			Payload:        json.RawMessage(payload),
		}); err != nil {
			t.Fatalf("ingest seq %d: %v", seq, err)
		}
	}

	ingestOne(1, "TRANSACTION_CREATED", `{"account_id":"acc-10","amount":100,"currency":"USD","initial_balance":1000}`)
	ingestOne(2, "ACCOUNT_VALIDATED", `{"valid":true}`)
	for seq := int64(3); seq <= 9; seq++ {
		ingestOne(seq, "LEDGER_UPDATED", `{"amount":50,"operation":"debit"}`)
	}
	ingestOne(10, "TRANSACTION_COMPLETED", `{}`)
}

func TestAtomicCheckpointRollbackOnCrash(t *testing.T) {
	pool := testutil.NewPool(t)
	ctx := context.Background()

	cpMgr := checkpoint.NewManager(pool)
	cpMgr.BeforeCommit = func(context.Context, int64) error {
		return errors.New("simulated crash before commit")
	}

	repo := eventstore.NewRepository(pool)
	ing := ingestion.NewService(repo)
	seedTenEvents(t, ing, "txn-cp-atom")

	execStore := execution.NewStore(pool)
	eng := replay.NewEngine(repo, execStore, nil)
	sess, err := eng.CreateSession(ctx, replay.CreateRequest{TransactionID: "txn-cp-atom"})
	if err != nil {
		t.Fatal(err)
	}

	st := execution.NewInitialState("txn-cp-atom")
	if _, err := cpMgr.CommitEventWithCheckpoint(ctx, execution.EventExecution{
		SessionID:      sess.SessionID,
		EventID:        "txn-cp-atom-e1",
		EventType:      "TRANSACTION_CREATED",
		SequenceNumber: 1,
		Status:         execution.EventSucceeded,
		StateHash:      st.Hash(),
		StateAfter:     st,
		ExecutedAt:     time.Now().UTC(),
	}, 1); err == nil {
		t.Fatal("expected pre-commit failure")
	}

	// Verify neither execution nor checkpoint nor session progress was committed.
	var execCount, cpCount int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM replay_executions WHERE session_id = $1`, sess.SessionID).Scan(&execCount)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM replay_checkpoints WHERE session_id = $1`, sess.SessionID).Scan(&cpCount)
	gotSess, _ := execStore.GetSession(ctx, sess.SessionID)
	if execCount != 0 || cpCount != 0 || gotSess.LastCheckpointSeq != 0 {
		t.Fatalf("partial checkpoint state persisted: exec=%d cp=%d last_seq=%d",
			execCount, cpCount, gotSess.LastCheckpointSeq)
	}
}

func TestInterruptAndResumeMatchesUninterruptedHash(t *testing.T) {
	pool := testutil.NewPool(t)
	ctx := context.Background()

	repo := eventstore.NewRepository(pool)
	ing := ingestion.NewService(repo)
	seedTenEvents(t, ing, "txn-resume")

	execStore := execution.NewStore(pool)
	cpMgr := checkpoint.NewManager(pool)
	eng := replay.NewEngine(repo, execStore, nil)

	// 1. Uninterrupted baseline replay for comparison.
	baselineSess, err := eng.CreateSession(ctx, replay.CreateRequest{TransactionID: "txn-resume"})
	if err != nil {
		t.Fatal(err)
	}
	baselineRes, err := eng.StartSession(ctx, baselineSess.SessionID)
	if err != nil {
		t.Fatalf("baseline replay failed: %v", err)
	}

	// 2. Interrupted replay: cancel context right after event 5 commits.
	recSess, err := eng.CreateSession(ctx, replay.CreateRequest{TransactionID: "txn-resume"})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	rmInterrupt := replay.NewRecoveryManager(pool, repo, execStore, cpMgr, replay.RecoveryOptions{
		AfterEvent: func(_ context.Context, seq int64) error {
			if seq == 5 {
				cancel()
			}
			return nil
		},
	})

	intRes, intErr := rmInterrupt.StartWithRecovery(runCtx, recSess.SessionID, "worker-1")
	if !errors.Is(intErr, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", intErr)
	}
	if intRes.Session.Status != execution.StatusInterrupted || intRes.Session.LastCheckpointSeq != 5 {
		t.Fatalf("expected INTERRUPTED at seq 5, got %+v", intRes.Session)
	}

	// 3. Resume from checkpoint 5 -> completes 6..10 with identical final state hash and no duplicates.
	rmResume := replay.NewRecoveryManager(pool, repo, execStore, cpMgr, replay.RecoveryOptions{})
	resumed, err := rmResume.ResumeSession(ctx, recSess.SessionID, "worker-2")
	if err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	if resumed.Session.Status != execution.StatusCompleted || resumed.Session.EventCount != 10 {
		t.Fatalf("unexpected resumed session: %+v", resumed.Session)
	}
	if resumed.Session.FinalStateHash != baselineRes.Session.FinalStateHash {
		t.Fatalf("resumed hash %s != baseline hash %s",
			resumed.Session.FinalStateHash, baselineRes.Session.FinalStateHash)
	}

	execs, _ := execStore.ListExecutions(ctx, recSess.SessionID)
	if len(execs) != 10 {
		t.Fatalf("expected exactly 10 execution rows (no duplicates), got %d", len(execs))
	}
	cps, _ := rmResume.ListCheckpoints(ctx, recSess.SessionID)
	if len(cps) != 10 {
		t.Fatalf("expected 10 checkpoints, got %d", len(cps))
	}
}

func TestCorruptCheckpointDetectedOnResume(t *testing.T) {
	pool := testutil.NewPool(t)
	ctx := context.Background()

	repo := eventstore.NewRepository(pool)
	ing := ingestion.NewService(repo)
	seedTenEvents(t, ing, "txn-corrupt")

	execStore := execution.NewStore(pool)
	cpMgr := checkpoint.NewManager(pool)
	eng := replay.NewEngine(repo, execStore, nil)
	sess, _ := eng.CreateSession(ctx, replay.CreateRequest{TransactionID: "txn-corrupt"})

	runCtx, cancel := context.WithCancel(ctx)
	rm := replay.NewRecoveryManager(pool, repo, execStore, cpMgr, replay.RecoveryOptions{
		AfterEvent: func(_ context.Context, seq int64) error {
			if seq == 2 {
				cancel()
			}
			return nil
		},
	})
	_, _ = rm.StartWithRecovery(runCtx, sess.SessionID, "w1")

	// Tamper with snapshot in DB without changing state_hash.
	if _, err := pool.Exec(ctx,
		`UPDATE replay_checkpoints SET state_snapshot = '{"transaction_id":"txn-corrupt","balance":999999}'::jsonb
		  WHERE session_id = $1 AND last_sequence = 2`, sess.SessionID); err != nil {
		t.Fatal(err)
	}

	rm2 := replay.NewRecoveryManager(pool, repo, execStore, cpMgr, replay.RecoveryOptions{})
	if _, err := rm2.ResumeSession(ctx, sess.SessionID, "w2"); !errors.Is(err, checkpoint.ErrCorruptCheckpoint) {
		t.Fatalf("expected ErrCorruptCheckpoint, got %v", err)
	}
}

func TestRetriesAndDeadLetter(t *testing.T) {
	pool := testutil.NewPool(t)
	ctx := context.Background()

	repo := eventstore.NewRepository(pool)
	ing := ingestion.NewService(repo)
	ts := time.Date(2026, 5, 1, 10, 0, 1, 0, time.UTC)
	_, _ = ing.Ingest(ctx, ingestion.Request{
		EventID: "r1", TransactionID: "txn-retry", EventType: "TRANSACTION_CREATED",
		SequenceNumber: 1, CreatedAt: &ts, Payload: json.RawMessage(`{"account_id":"a","amount":10,"initial_balance":100}`),
	})
	_, _ = ing.Ingest(ctx, ingestion.Request{
		EventID: "r2", TransactionID: "txn-retry", EventType: "ACCOUNT_VALIDATED",
		SequenceNumber: 2, CreatedAt: &ts, Payload: json.RawMessage(`{"valid":true}`),
	})

	execStore := execution.NewStore(pool)
	cpMgr := checkpoint.NewManager(pool)
	eng := replay.NewEngine(repo, execStore, nil)

	// Case 1: Fails twice transiently, succeeds on 3rd attempt.
	var calls atomic.Int32
	reg := replay.DefaultRegistry()
	base, _ := reg.Lookup("ACCOUNT_VALIDATED")
	reg.Register("ACCOUNT_VALIDATED", replay.HandlerFunc(func(deps replay.ControlledDeps, st execution.TransactionState, ev eventstore.Event) (execution.TransactionState, []execution.Effect, error) {
		if calls.Add(1) <= 2 {
			return st, nil, replay.Retryable(errors.New("transient gateway timeout"))
		}
		return base.Apply(deps, st, ev)
	}))

	s1, _ := eng.CreateSession(ctx, replay.CreateRequest{TransactionID: "txn-retry"})
	rm := replay.NewRecoveryManager(pool, repo, execStore, cpMgr, replay.RecoveryOptions{
		Registry:    reg,
		RetryPolicy: replay.NewRetryPolicy(3, time.Millisecond, 5*time.Millisecond, 0, 1),
	})
	res, err := rm.StartWithRecovery(ctx, s1.SessionID, "w1")
	if err != nil || res.Session.Status != execution.StatusCompleted {
		t.Fatalf("expected completion after retries, got %+v %v", res, err)
	}
	atts, _ := rm.ListAttempts(ctx, s1.SessionID)
	// 1 attempt for seq 1 + 3 attempts for seq 2 = 4 attempts total.
	if len(atts) != 4 {
		t.Fatalf("expected 4 attempts, got %d: %+v", len(atts), atts)
	}

	// Case 2: Exhausts retries -> DEAD letter.
	regAlwaysFail := replay.DefaultRegistry()
	regAlwaysFail.Register("ACCOUNT_VALIDATED", replay.HandlerFunc(func(_ replay.ControlledDeps, st execution.TransactionState, _ eventstore.Event) (execution.TransactionState, []execution.Effect, error) {
		return st, nil, replay.Retryable(errors.New("persistent outage"))
	}))
	s2, _ := eng.CreateSession(ctx, replay.CreateRequest{TransactionID: "txn-retry"})
	rmFail := replay.NewRecoveryManager(pool, repo, execStore, cpMgr, replay.RecoveryOptions{
		Registry:    regAlwaysFail,
		RetryPolicy: replay.NewRetryPolicy(2, time.Millisecond, 5*time.Millisecond, 0, 1),
	})
	res2, err := rmFail.StartWithRecovery(ctx, s2.SessionID, "w1")
	if err == nil || res2.Session.Status != execution.StatusFailed {
		t.Fatalf("expected FAILED session after exhausted retries, got %+v %v", res2, err)
	}
	execs2, _ := execStore.ListExecutions(ctx, s2.SessionID)
	if len(execs2) != 2 || execs2[1].Status != execution.EventDead || !execs2[1].IsDeadLetter || execs2[1].AttemptCount != 2 {
		t.Fatalf("expected DEAD execution on seq 2, got %+v", execs2)
	}
}

func TestLeaseExclusivityAndOrphanRecovery(t *testing.T) {
	pool := testutil.NewPool(t)
	ctx := context.Background()

	repo := eventstore.NewRepository(pool)
	ing := ingestion.NewService(repo)
	seedTenEvents(t, ing, "txn-lease")

	execStore := execution.NewStore(pool)
	cpMgr := checkpoint.NewManager(pool)
	eng := replay.NewEngine(repo, execStore, nil)
	sess, _ := eng.CreateSession(ctx, replay.CreateRequest{TransactionID: "txn-lease"})

	rm := replay.NewRecoveryManager(pool, repo, execStore, cpMgr, replay.RecoveryOptions{
		LeaseTTL: 30 * time.Second,
	})

	now := time.Now().UTC()
	if err := rm.AcquireLease(ctx, sess.SessionID, "worker-A", now); err != nil {
		t.Fatal(err)
	}
	// Worker B cannot acquire lease while Worker A's lease is active.
	if err := rm.AcquireLease(ctx, sess.SessionID, "worker-B", now.Add(5*time.Second)); !errors.Is(err, execution.ErrSessionLocked) {
		t.Fatalf("expected ErrSessionLocked for worker-B, got %v", err)
	}

	// Simulate Worker A crashing mid-session in RUNNING status with an expired lease.
	_, _ = pool.Exec(ctx,
		`UPDATE replay_sessions SET status = 'RUNNING', lease_expires_at = $2 WHERE session_id = $1`,
		sess.SessionID, now.Add(-time.Second))

	orphans, err := rm.DetectOrphanedSessions(ctx, now)
	if err != nil || len(orphans) != 1 || orphans[0].SessionID != sess.SessionID || orphans[0].Status != execution.StatusInterrupted {
		t.Fatalf("expected 1 orphaned session marked INTERRUPTED, got %+v %v", orphans, err)
	}

	// Worker B can now take over and resume to completion.
	resumed, err := rm.ResumeSession(ctx, sess.SessionID, "worker-B")
	if err != nil || resumed.Session.Status != execution.StatusCompleted || resumed.Session.EventCount != 10 {
		t.Fatalf("orphan resume failed: %+v %v", resumed, err)
	}
}
