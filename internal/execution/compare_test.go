package execution

import (
	"testing"
	"time"
)

func TestStateHashDeterministic(t *testing.T) {
	a := TransactionState{
		TransactionID: "txn-1",
		Status:        "COMPLETED",
		AccountID:     "acc-1",
		Currency:      "USD",
		Balance:       500,
		Attributes:    map[string]string{"b": "2", "a": "1"},
	}
	b := TransactionState{
		TransactionID: "txn-1",
		Status:        "COMPLETED",
		AccountID:     "acc-1",
		Currency:      "USD",
		Balance:       500,
		Attributes:    map[string]string{"a": "1", "b": "2"},
	}
	if a.Hash() != b.Hash() {
		t.Fatalf("canonical hashes should match: %s vs %s", a.Hash(), b.Hash())
	}

	b.Balance = 499
	if a.Hash() == b.Hash() {
		t.Fatal("different balance must change hash")
	}
}

func TestCanTransition(t *testing.T) {
	valid := [][2]SessionStatus{
		{StatusCreated, StatusRunning},
		{StatusCreated, StatusPaused},
		{StatusRunning, StatusPaused},
		{StatusRunning, StatusInterrupted},
		{StatusRunning, StatusCompleted},
		{StatusRunning, StatusFailed},
		{StatusPaused, StatusResuming},
		{StatusPaused, StatusInterrupted},
		{StatusInterrupted, StatusResuming},
		{StatusResuming, StatusRunning},
		{StatusResuming, StatusCompleted},
		{StatusResuming, StatusFailed},
	}
	for _, p := range valid {
		if !CanTransition(p[0], p[1]) {
			t.Errorf("%s -> %s should be allowed", p[0], p[1])
		}
	}
	if CanTransition(StatusCreated, StatusCompleted) ||
		CanTransition(StatusCompleted, StatusRunning) ||
		CanTransition(StatusFailed, StatusRunning) ||
		CanTransition(StatusInterrupted, StatusCompleted) {
		t.Fatal("invalid transition was allowed")
	}
}

func TestCompareEquivalentAndDivergent(t *testing.T) {
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	left := Session{
		SessionID:      "rpl-1",
		TransactionID:  "txn-1",
		Mode:           ModeReplay,
		Status:         StatusCompleted,
		FinalStateHash: "hash-final",
	}
	right := Session{
		SessionID:      "orig-1",
		TransactionID:  "txn-1",
		Mode:           ModeOriginal,
		Status:         StatusCompleted,
		FinalStateHash: "hash-final",
	}
	eff := []Effect{{Type: "LEDGER_DEBIT", Amount: 100, Currency: "USD", ReferenceID: "ref-a", Timestamp: ts}}
	effLive := []Effect{{Type: "LEDGER_DEBIT", Amount: 100, Currency: "USD", ReferenceID: "live-999", Timestamp: ts.Add(time.Hour)}}

	leftExecs := []EventExecution{
		{EventID: "e1", SequenceNumber: 1, Status: EventSucceeded, StateHash: "h1", Effects: nil},
		{EventID: "e2", SequenceNumber: 2, Status: EventSucceeded, StateHash: "hash-final", Effects: eff},
	}
	rightExecs := []EventExecution{
		{EventID: "e1", SequenceNumber: 1, Status: EventSucceeded, StateHash: "h1", Effects: nil},
		{EventID: "e2", SequenceNumber: 2, Status: EventSucceeded, StateHash: "hash-final", Effects: effLive},
	}

	eq := Compare(left, leftExecs, right, rightExecs)
	if !eq.Equivalent || eq.FirstDivergence != nil {
		t.Fatalf("expected equivalent comparison, got %+v", eq)
	}

	// Now introduce divergence at sequence 2.
	leftExecs[1].StateHash = "hash-diverged"
	left.FinalStateHash = "hash-diverged"
	leftExecs[1].Effects = []Effect{{Type: "LEDGER_DEBIT", Amount: 200, Currency: "USD"}}

	diff := Compare(left, leftExecs, right, rightExecs)
	if diff.Equivalent || diff.FirstDivergence == nil {
		t.Fatalf("expected divergence, got %+v", diff)
	}
	if diff.FirstDivergence.SequenceNumber != 2 {
		t.Fatalf("first divergence seq = %d, want 2", diff.FirstDivergence.SequenceNumber)
	}
	if diff.EffectsMatch {
		t.Fatal("effects should not match")
	}
}
