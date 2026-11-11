package replay

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Yagna123k/causal-replay-lab/internal/checkpoint"
	"github.com/Yagna123k/causal-replay-lab/internal/execution"
)

func TestClassifyError(t *testing.T) {
	if ClassifyError(Retryable(errors.New("timeout"))) != ClassRetryable {
		t.Fatal("expected retryable")
	}
	if ClassifyError(context.DeadlineExceeded) != ClassRetryable {
		t.Fatal("DeadlineExceeded should be retryable")
	}
	if ClassifyError(NonRetryable(errors.New("bad"))) != ClassNonRetryable {
		t.Fatal("expected non-retryable")
	}
	if ClassifyError(fmt.Errorf("wrap: %w", execution.ErrSequenceGap)) != ClassNonRetryable {
		t.Fatal("ErrSequenceGap should be non-retryable")
	}
	if ClassifyError(checkpoint.ErrCorruptCheckpoint) != ClassNonRetryable {
		t.Fatal("ErrCorruptCheckpoint should be non-retryable")
	}
}

func TestSeededBackoffDeterministic(t *testing.T) {
	p1 := NewRetryPolicy(5, 10*time.Millisecond, 100*time.Millisecond, 0.5, 12345)
	p2 := NewRetryPolicy(5, 10*time.Millisecond, 100*time.Millisecond, 0.5, 12345)

	for attempt := 1; attempt <= 5; attempt++ {
		d1, d2 := p1.Backoff(attempt), p2.Backoff(attempt)
		if d1 != d2 {
			t.Fatalf("attempt %d: seeded backoffs differed (%v vs %v)", attempt, d1, d2)
		}
		if d1 < 10*time.Millisecond || d1 > 200*time.Millisecond {
			t.Fatalf("attempt %d: out of range %v", attempt, d1)
		}
	}
}
