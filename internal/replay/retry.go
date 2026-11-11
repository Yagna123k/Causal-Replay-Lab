package replay

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/Yagna123k/causal-replay-lab/internal/checkpoint"
	"github.com/Yagna123k/causal-replay-lab/internal/execution"
)

// ErrorClass distinguishes transient failures eligible for retry from permanent ones.
type ErrorClass string

const (
	ClassRetryable    ErrorClass = "RETRYABLE"
	ClassNonRetryable ErrorClass = "NON_RETRYABLE"
)

type classifiedError struct {
	class ErrorClass
	err   error
}

func (e *classifiedError) Error() string { return e.err.Error() }
func (e *classifiedError) Unwrap() error { return e.err }

// Retryable wraps err so the recovery runner treats it as transient and retryable.
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &classifiedError{class: ClassRetryable, err: err}
}

// NonRetryable wraps err so the recovery runner fails immediately without retrying.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &classifiedError{class: ClassNonRetryable, err: err}
}

// ClassifyError determines whether err is retryable under a retry policy.
func ClassifyError(err error) ErrorClass {
	if err == nil {
		return ClassNonRetryable
	}
	var ce *classifiedError
	if errors.As(err, &ce) {
		return ce.class
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ClassRetryable
	}
	if errors.Is(err, execution.ErrSequenceGap) ||
		errors.Is(err, execution.ErrHandlerNotFound) ||
		errors.Is(err, execution.ErrInvalidSessionInput) ||
		errors.Is(err, checkpoint.ErrCorruptCheckpoint) {
		return ClassNonRetryable
	}
	return ClassNonRetryable
}

// RetryPolicy controls retry attempts and exponential backoff with seedable jitter.
type RetryPolicy struct {
	MaxAttempts  int
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
	JitterFactor float64 // fraction in [0, 1] of computed backoff added as jitter

	mu  sync.Mutex
	rng *rand.Rand
}

// NewRetryPolicy builds a RetryPolicy with a deterministic jitter seed (when seed != 0).
func NewRetryPolicy(maxAttempts int, baseBackoff, maxBackoff time.Duration, jitterFactor float64, seed int64) *RetryPolicy {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	if baseBackoff <= 0 {
		baseBackoff = 25 * time.Millisecond
	}
	if maxBackoff < baseBackoff {
		maxBackoff = baseBackoff
	}
	if jitterFactor < 0 {
		jitterFactor = 0
	}
	if jitterFactor > 1 {
		jitterFactor = 1
	}
	if seed == 0 {
		seed = 1
	}
	return &RetryPolicy{
		MaxAttempts:  maxAttempts,
		BaseBackoff:  baseBackoff,
		MaxBackoff:   maxBackoff,
		JitterFactor: jitterFactor,
		rng:          rand.New(rand.NewSource(seed)),
	}
}

// DefaultRetryPolicy returns sensible defaults for replay recovery.
func DefaultRetryPolicy() *RetryPolicy {
	return NewRetryPolicy(3, 25*time.Millisecond, 500*time.Millisecond, 0.25, 42)
}

// Backoff returns the delay before retry attempt `attempt` (1-indexed: 1 = first retry).
func (p *RetryPolicy) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	mult := math.Pow(2, float64(attempt-1))
	delay := time.Duration(float64(p.BaseBackoff) * mult)
	if delay > p.MaxBackoff || delay <= 0 {
		delay = p.MaxBackoff
	}
	if p.JitterFactor == 0 {
		return delay
	}
	p.mu.Lock()
	if p.rng == nil {
		p.rng = rand.New(rand.NewSource(1))
	}
	u := p.rng.Float64()
	p.mu.Unlock()
	jitter := time.Duration(float64(delay) * p.JitterFactor * u)
	total := delay + jitter
	if total > p.MaxBackoff*2 {
		return p.MaxBackoff * 2
	}
	return total
}
