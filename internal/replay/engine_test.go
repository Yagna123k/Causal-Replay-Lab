package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Yagna123k/causal-replay-lab/internal/eventstore"
	"github.com/Yagna123k/causal-replay-lab/internal/execution"
)

type memEventReader struct {
	txns   map[string]eventstore.Transaction
	events map[string][]eventstore.Event
}

func newMemReader(txnID string, evs ...eventstore.Event) *memEventReader {
	return &memEventReader{
		txns:   map[string]eventstore.Transaction{txnID: {TransactionID: txnID, Status: "OPEN"}},
		events: map[string][]eventstore.Event{txnID: evs},
	}
}

func (m *memEventReader) GetTransaction(_ context.Context, id string) (eventstore.Transaction, error) {
	t, ok := m.txns[id]
	if !ok {
		return eventstore.Transaction{}, eventstore.ErrNotFound
	}
	return t, nil
}

func (m *memEventReader) ListByTransaction(_ context.Context, txnID string, after int64, limit int) ([]eventstore.Event, error) {
	var out []eventstore.Event
	for _, ev := range m.events[txnID] {
		if ev.SequenceNumber > after {
			out = append(out, ev)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

type memSessionStore struct {
	mu       sync.Mutex
	sessions map[string]execution.Session
	execs    map[string][]execution.EventExecution
	order    []string
	nextExec int64
}

func newMemSessionStore() *memSessionStore {
	return &memSessionStore{
		sessions: map[string]execution.Session{},
		execs:    map[string][]execution.EventExecution{},
	}
}

func (s *memSessionStore) CreateSession(_ context.Context, sess execution.Session) (execution.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess.Status = execution.StatusCreated
	sess.CreatedAt = time.Now().UTC()
	s.sessions[sess.SessionID] = sess
	s.order = append(s.order, sess.SessionID)
	return sess, nil
}

func (s *memSessionStore) GetSession(_ context.Context, id string) (execution.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[id]
	if !ok {
		return execution.Session{}, execution.ErrSessionNotFound
	}
	return sess, nil
}

func (s *memSessionStore) GetLatestOriginal(_ context.Context, txnID string) (execution.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(s.order) - 1; i >= 0; i-- {
		sess := s.sessions[s.order[i]]
		if sess.TransactionID == txnID && sess.Mode == execution.ModeOriginal &&
			(sess.Status == execution.StatusCompleted || sess.Status == execution.StatusFailed) {
			return sess, nil
		}
	}
	return execution.Session{}, execution.ErrOriginalNotFound
}

func (s *memSessionStore) MarkRunning(_ context.Context, id string, startedAt time.Time) (execution.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[id]
	if !ok {
		return execution.Session{}, execution.ErrSessionNotFound
	}
	if !execution.CanTransition(sess.Status, execution.StatusRunning) {
		return execution.Session{}, execution.ErrInvalidTransition
	}
	sess.Status = execution.StatusRunning
	t := startedAt.UTC()
	sess.StartedAt = &t
	s.sessions[id] = sess
	return sess, nil
}

func (s *memSessionStore) RecordExecution(_ context.Context, ex execution.EventExecution) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextExec++
	ex.ExecutionID = s.nextExec
	s.execs[ex.SessionID] = append(s.execs[ex.SessionID], ex)
	return nil
}

func (s *memSessionStore) CompleteSession(
	_ context.Context,
	id string,
	status execution.SessionStatus,
	count int,
	finalState *execution.TransactionState,
	finalHash string,
	errMsg string,
	completedAt time.Time,
) (execution.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[id]
	if !ok {
		return execution.Session{}, execution.ErrSessionNotFound
	}
	if !execution.CanTransition(sess.Status, status) {
		return execution.Session{}, execution.ErrInvalidTransition
	}
	sess.Status = status
	sess.EventCount = count
	if finalState != nil {
		cp := finalState.Clone()
		sess.FinalState = &cp
	}
	sess.FinalStateHash = finalHash
	sess.Error = errMsg
	t := completedAt.UTC()
	sess.CompletedAt = &t
	s.sessions[id] = sess
	return sess, nil
}

func (s *memSessionStore) ListExecutions(_ context.Context, id string) ([]execution.EventExecution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]execution.EventExecution, len(s.execs[id]))
	copy(out, s.execs[id])
	return out, nil
}

func makeEvent(id, txn, evType string, seq int64, payload string) eventstore.Event {
	return eventstore.Event{
		EventID:        id,
		TransactionID:  txn,
		EventType:      evType,
		SequenceNumber: seq,
		Payload:        json.RawMessage(payload),
		CreatedAt:      time.Date(2026, 1, 1, 10, 0, int(seq), 0, time.UTC),
	}
}

func standardWorkflow(txnID string) []eventstore.Event {
	return []eventstore.Event{
		makeEvent("e1", txnID, "TRANSACTION_CREATED", 1, `{"account_id":"acc-1","amount":250,"currency":"USD","initial_balance":1000}`),
		makeEvent("e2", txnID, "ACCOUNT_VALIDATED", 2, `{"valid":true}`),
		makeEvent("e3", txnID, "DEBIT_INITIATED", 3, `{"amount":250,"currency":"USD"}`),
		makeEvent("e4", txnID, "LEDGER_UPDATED", 4, `{"amount":250,"operation":"debit"}`),
		makeEvent("e5", txnID, "TRANSACTION_COMPLETED", 5, `{}`),
	}
}

func TestDeterministicReplayRepeatedRuns(t *testing.T) {
	ctx := context.Background()
	rd := newMemReader("txn-1", standardWorkflow("txn-1")...)
	st := newMemSessionStore()
	eng := NewEngine(rd, st, nil)

	const runs = 10
	hashes := make([]string, 0, runs)
	var firstExecs []execution.EventExecution

	for i := 0; i < runs; i++ {
		sess, err := eng.CreateSession(ctx, CreateRequest{TransactionID: "txn-1"})
		if err != nil {
			t.Fatal(err)
		}
		res, err := eng.StartSession(ctx, sess.SessionID)
		if err != nil {
			t.Fatalf("run %d failed: %v", i, err)
		}
		if res.Session.Status != execution.StatusCompleted || res.Session.EventCount != 5 {
			t.Fatalf("unexpected session: %+v", res.Session)
		}
		if res.Session.FinalState.Balance != 750 || res.Session.FinalState.Status != "COMPLETED" {
			t.Fatalf("unexpected state: %+v", res.Session.FinalState)
		}
		if len(res.SimulatedEffects) != 5 {
			t.Fatalf("expected 5 simulated effects, got %d", len(res.SimulatedEffects))
		}
		hashes = append(hashes, res.Session.FinalStateHash)

		execs, _ := eng.ListExecutions(ctx, sess.SessionID)
		if i == 0 {
			firstExecs = execs
		} else {
			for j := range execs {
				if execs[j].StateHash != firstExecs[j].StateHash {
					t.Fatalf("run %d step %d state hash diverged", i, j)
				}
			}
		}
	}

	for i := 1; i < runs; i++ {
		if hashes[i] != hashes[0] {
			t.Fatalf("run %d final hash %s != run 0 %s", i, hashes[i], hashes[0])
		}
	}
}

func TestReplayNeverInvokesLiveEffectSink(t *testing.T) {
	ctx := context.Background()
	rd := newMemReader("txn-1", standardWorkflow("txn-1")...)
	st := newMemSessionStore()

	liveCalls := 0
	failingLiveSink := NewLiveEffectSink(func(context.Context, execution.Effect) error {
		liveCalls++
		return nil
	})
	origRunner := NewOriginalRunner(rd, st, nil, failingLiveSink)
	orig, err := origRunner.ProcessOriginal(ctx, "txn-1")
	if err != nil {
		t.Fatalf("original failed: %v", err)
	}
	if liveCalls != 5 {
		t.Fatalf("expected 5 live calls during original processing, got %d", liveCalls)
	}

	// Now create a replay engine. Even if liveCalls is reset and a failing live sink exists
	// in the system, Engine has no reference to it and must never increment liveCalls.
	liveCalls = 0
	_ = NewLiveEffectSink(func(context.Context, execution.Effect) error {
		t.Fatal("live sink must never be called during replay")
		return errors.New("live sink called")
	})

	eng := NewEngine(rd, st, nil)
	sess, err := eng.CreateSession(ctx, CreateRequest{TransactionID: "txn-1"})
	if err != nil {
		t.Fatal(err)
	}
	res, err := eng.StartSession(ctx, sess.SessionID)
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if liveCalls != 0 {
		t.Fatalf("live sink invoked %d times during replay", liveCalls)
	}
	if len(res.SimulatedEffects) != 5 {
		t.Fatalf("expected 5 simulated effects, got %d", len(res.SimulatedEffects))
	}

	// Compare replay against original baseline: should be logically equivalent!
	cmp, err := eng.Compare(ctx, res.Session.SessionID, "original")
	if err != nil {
		t.Fatal(err)
	}
	if !cmp.Equivalent || cmp.RightSessionID != orig.SessionID {
		t.Fatalf("expected replay to match original: %+v", cmp)
	}
}

func TestReplayDetectsSequenceGap(t *testing.T) {
	ctx := context.Background()
	rd := newMemReader("txn-gap",
		makeEvent("e1", "txn-gap", "TRANSACTION_CREATED", 1, `{"account_id":"a","amount":10,"initial_balance":100}`),
		makeEvent("e3", "txn-gap", "LEDGER_UPDATED", 3, `{"amount":10}`),
	)
	st := newMemSessionStore()
	eng := NewEngine(rd, st, nil)

	sess, err := eng.CreateSession(ctx, CreateRequest{TransactionID: "txn-gap"})
	if err != nil {
		t.Fatal(err)
	}
	res, err := eng.StartSession(ctx, sess.SessionID)
	if !errors.Is(err, execution.ErrSequenceGap) {
		t.Fatalf("expected ErrSequenceGap, got %v", err)
	}
	if res.Session.Status != execution.StatusFailed {
		t.Fatalf("expected FAILED status, got %s", res.Session.Status)
	}
}

func TestReplayIdentifiesFailedEvent(t *testing.T) {
	ctx := context.Background()
	rd := newMemReader("txn-fail",
		makeEvent("e1", "txn-fail", "TRANSACTION_CREATED", 1, `{"account_id":"a","amount":500,"initial_balance":100}`),
		makeEvent("e2", "txn-fail", "ACCOUNT_VALIDATED", 2, `{"valid":true}`),
		makeEvent("e3", "txn-fail", "DEBIT_INITIATED", 3, `{"amount":500}`), // insufficient balance!
		makeEvent("e4", "txn-fail", "TRANSACTION_COMPLETED", 4, `{}`),
	)
	st := newMemSessionStore()
	eng := NewEngine(rd, st, nil)

	sess, _ := eng.CreateSession(ctx, CreateRequest{TransactionID: "txn-fail"})
	res, err := eng.StartSession(ctx, sess.SessionID)
	if err == nil {
		t.Fatal("expected failure on insufficient balance")
	}
	if res.Session.Status != execution.StatusFailed || res.Session.EventCount != 2 {
		t.Fatalf("unexpected session: %+v", res.Session)
	}

	execs, _ := eng.ListExecutions(ctx, sess.SessionID)
	if len(execs) != 3 {
		t.Fatalf("expected 3 execution records (2 succeeded, 1 failed), got %d", len(execs))
	}
	if execs[2].SequenceNumber != 3 || execs[2].Status != execution.EventFailed || execs[2].Error == "" {
		t.Fatalf("failed event not properly recorded: %+v", execs[2])
	}
}

func TestReplayComparisonDetectsDivergence(t *testing.T) {
	ctx := context.Background()
	rd := newMemReader("txn-1", standardWorkflow("txn-1")...)
	st := newMemSessionStore()

	origRunner := NewOriginalRunner(rd, st, nil, nil)
	if _, err := origRunner.ProcessOriginal(ctx, "txn-1"); err != nil {
		t.Fatal(err)
	}

	// Register a modified handler for LEDGER_UPDATED to simulate a code change / bug fix
	// and verify Compare pinpoints sequence 4 as the first divergent event.
	modReg := DefaultRegistry()
	modReg.Register("LEDGER_UPDATED", HandlerFunc(func(deps ControlledDeps, state execution.TransactionState, ev eventstore.Event) (execution.TransactionState, []execution.Effect, error) {
		next := state.Clone()
		next.Balance -= 100 // altered amount instead of 250
		next.Status = "LEDGER_UPDATED"
		next.LastSequence = ev.SequenceNumber
		next.AppliedEvents++
		return next, []execution.Effect{{Type: "LEDGER_ENTRY", Target: next.AccountID, Amount: -100, Currency: next.Currency, Timestamp: deps.Clock.Now()}}, nil
	}))

	eng := NewEngine(rd, st, modReg)
	sess, _ := eng.CreateSession(ctx, CreateRequest{TransactionID: "txn-1"})
	res, err := eng.StartSession(ctx, sess.SessionID)
	if err != nil {
		t.Fatal(err)
	}

	cmp, err := eng.Compare(ctx, res.Session.SessionID, "original")
	if err != nil {
		t.Fatal(err)
	}
	if cmp.Equivalent || cmp.FinalStateMatch || cmp.FirstDivergence == nil {
		t.Fatalf("expected divergence, got %+v", cmp)
	}
	if cmp.FirstDivergence.SequenceNumber != 4 {
		t.Fatalf("first divergence = seq %d, want 4", cmp.FirstDivergence.SequenceNumber)
	}
}

func TestReplayWithWindowAndInitialStateAndInvalidTransition(t *testing.T) {
	ctx := context.Background()
	rd := newMemReader("txn-1", standardWorkflow("txn-1")...)
	st := newMemSessionStore()
	eng := NewEngine(rd, st, nil)

	from, to := int64(4), int64(5)
	initState := &execution.TransactionState{
		TransactionID:    "txn-1",
		Status:           "DEBIT_INITIATED",
		AccountID:        "acc-1",
		Currency:         "USD",
		Balance:          1000,
		PendingDebit:     250,
		AccountValidated: true,
		LastSequence:     3,
		AppliedEvents:    3,
	}
	sess, err := eng.CreateSession(ctx, CreateRequest{
		TransactionID: "txn-1",
		FromSequence:  &from,
		ToSequence:    &to,
		InitialState:  initState,
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := eng.StartSession(ctx, sess.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if res.Session.EventCount != 2 || res.Session.FinalState.Balance != 750 || res.Session.FinalState.Status != "COMPLETED" {
		t.Fatalf("unexpected windowed replay result: %+v", res.Session)
	}

	// Starting the same session a second time must fail with ErrInvalidTransition.
	if _, err := eng.StartSession(ctx, sess.SessionID); !errors.Is(err, execution.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}

	// Invalid range rejected at creation time.
	badFrom, badTo := int64(5), int64(2)
	if _, err := eng.CreateSession(ctx, CreateRequest{TransactionID: "txn-1", FromSequence: &badFrom, ToSequence: &badTo}); !errors.Is(err, execution.ErrInvalidSessionInput) {
		t.Fatalf("expected ErrInvalidSessionInput, got %v", err)
	}
	_ = fmt.Sprintf("unused")
}
