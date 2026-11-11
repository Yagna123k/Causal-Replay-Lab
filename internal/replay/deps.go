// Package replay implements deterministic event replay with controlled dependencies.
package replay

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/Yagna123k/causal-replay-lab/internal/execution"
)

// Clock abstracts time retrieval so replay handlers never call time.Now() directly.
type Clock interface {
	Now() time.Time
}

// IDGenerator abstracts ID / randomness generation so replay stays deterministic.
type IDGenerator interface {
	NewID(prefix string) string
}

// EffectSink receives side effects emitted during event processing.
type EffectSink interface {
	Emit(ctx context.Context, effect execution.Effect) error
}

// ControlledDeps supplies isolated dependencies to event handlers during execution.
type ControlledDeps struct {
	Clock Clock
	IDGen IDGenerator
}

// SystemClock returns real wall-clock UTC time (for live original execution).
type SystemClock struct{}

// Now returns time.Now().UTC().
func (SystemClock) Now() time.Time { return time.Now().UTC() }

// EventClock returns a fixed timestamp derived from the event being applied.
type EventClock struct {
	Current time.Time
}

// Now returns the deterministic event timestamp in UTC.
func (c *EventClock) Now() time.Time { return c.Current.UTC() }

// RandomIDGenerator generates cryptographically random IDs for live execution.
type RandomIDGenerator struct{}

// NewID returns a random hex-encoded identifier with the given prefix.
func (RandomIDGenerator) NewID(prefix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s_%s", prefix, hex.EncodeToString(b[:]))
}

// DeterministicIDGenerator produces repeatable IDs scoped to a session and sequence.
type DeterministicIDGenerator struct {
	SessionID string
	Sequence  int64
	counter   int
}

// NewID returns a deterministic identifier derived from session, sequence, and call count.
func (g *DeterministicIDGenerator) NewID(prefix string) string {
	g.counter++
	return fmt.Sprintf("%s_%s_s%d_%d", prefix, g.SessionID, g.Sequence, g.counter)
}

// LiveEffectSink represents an external side-effect target used only during original
// live execution. The replay engine never holds a reference to a LiveEffectSink.
type LiveEffectSink struct {
	mu       sync.Mutex
	onEmit   func(context.Context, execution.Effect) error
	recorded []execution.Effect
}

// NewLiveEffectSink creates a live sink with an optional callback (e.g. ledger gateway).
func NewLiveEffectSink(onEmit func(context.Context, execution.Effect) error) *LiveEffectSink {
	return &LiveEffectSink{onEmit: onEmit}
}

// Emit dispatches a side effect to the live callback and records it in memory.
func (s *LiveEffectSink) Emit(ctx context.Context, effect execution.Effect) error {
	if s.onEmit != nil {
		if err := s.onEmit(ctx, effect); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recorded = append(s.recorded, effect)
	return nil
}

// Emitted returns a copy of effects sent through the live sink.
func (s *LiveEffectSink) Emitted() []execution.Effect {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]execution.Effect, len(s.recorded))
	copy(out, s.recorded)
	return out
}

// SimulatedEffectSink captures side effects during deterministic replay without
// touching any external or live financial systems.
type SimulatedEffectSink struct {
	mu      sync.Mutex
	effects []execution.Effect
}

// NewSimulatedEffectSink creates an isolated in-memory effect recorder for replay.
func NewSimulatedEffectSink() *SimulatedEffectSink {
	return &SimulatedEffectSink{}
}

// Emit records the simulated effect in memory.
func (s *SimulatedEffectSink) Emit(_ context.Context, effect execution.Effect) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.effects = append(s.effects, effect)
	return nil
}

// Recorded returns a copy of all simulated effects captured during replay.
func (s *SimulatedEffectSink) Recorded() []execution.Effect {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]execution.Effect, len(s.effects))
	copy(out, s.effects)
	return out
}
