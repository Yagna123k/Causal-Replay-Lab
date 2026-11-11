package checkpoint

import (
	"errors"
	"testing"

	"github.com/Yagna123k/causal-replay-lab/internal/execution"
)

func TestVerifyCheckpointIntegrity(t *testing.T) {
	st := execution.TransactionState{
		TransactionID: "txn-1",
		Status:        "DEBIT_INITIATED",
		Balance:       750,
		LastSequence:  3,
	}
	cp := Checkpoint{
		SessionID:     "rpl_1",
		LastSequence:  3,
		StateHash:     st.Hash(),
		StateSnapshot: st,
	}
	if err := Verify(cp); err != nil {
		t.Fatalf("expected valid checkpoint: %v", err)
	}

	cp.StateSnapshot.Balance = 9999 // tamper without updating StateHash
	if err := Verify(cp); !errors.Is(err, ErrCorruptCheckpoint) {
		t.Fatalf("expected ErrCorruptCheckpoint, got %v", err)
	}
}
