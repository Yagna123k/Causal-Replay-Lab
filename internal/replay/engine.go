package replay

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/Yagna123k/causal-replay-lab/internal/eventstore"
	"github.com/Yagna123k/causal-replay-lab/internal/execution"
)

// EventReader reads events and transactions from the event store.
type EventReader interface {
	GetTransaction(ctx context.Context, id string) (eventstore.Transaction, error)
	ListByTransaction(ctx context.Context, transactionID string, afterSequence int64, limit int) ([]eventstore.Event, error)
}

// SessionStore persists replay sessions and per-event executions.
type SessionStore interface {
	CreateSession(ctx context.Context, sess execution.Session) (execution.Session, error)
	GetSession(ctx context.Context, sessionID string) (execution.Session, error)
	GetLatestOriginal(ctx context.Context, transactionID string) (execution.Session, error)
	MarkRunning(ctx context.Context, sessionID string, startedAt time.Time) (execution.Session, error)
	RecordExecution(ctx context.Context, ex execution.EventExecution) error
	CompleteSession(
		ctx context.Context,
		sessionID string,
		status execution.SessionStatus,
		eventCount int,
		finalState *execution.TransactionState,
		finalHash string,
		errMsg string,
		completedAt time.Time,
	) (execution.Session, error)
	ListExecutions(ctx context.Context, sessionID string) ([]execution.EventExecution, error)
}

// CreateRequest configures a new replay session.
type CreateRequest struct {
	TransactionID string                      `json:"transaction_id"`
	FromSequence  *int64                      `json:"from_sequence,omitempty"`
	ToSequence    *int64                      `json:"to_sequence,omitempty"`
	InitialState  *execution.TransactionState `json:"initial_state,omitempty"`
}

// StartResult returns the completed/failed replay session along with simulated effects.
type StartResult struct {
	Session          execution.Session  `json:"session"`
	SimulatedEffects []execution.Effect `json:"simulated_effects"`
}

// Engine executes deterministic replays using strictly simulated dependencies.
// By construction it holds no LiveEffectSink and cannot trigger live side effects.
type Engine struct {
	events   EventReader
	sessions SessionStore
	registry *Registry
	// newSimulatedSink isolates every replay session with a fresh in-memory sink.
	newSimulatedSink func() *SimulatedEffectSink
}

// NewEngine creates a deterministic replay engine.
func NewEngine(events EventReader, sessions SessionStore, registry *Registry) *Engine {
	if registry == nil {
		registry = DefaultRegistry()
	}
	return &Engine{
		events:           events,
		sessions:         sessions,
		registry:         registry,
		newSimulatedSink: NewSimulatedEffectSink,
	}
}

func newSessionID(prefix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s_%s", prefix, hex.EncodeToString(b[:]))
}

// CreateSession validates parameters and creates a replay session in CREATED state.
func (e *Engine) CreateSession(ctx context.Context, req CreateRequest) (execution.Session, error) {
	if req.TransactionID == "" {
		return execution.Session{}, fmt.Errorf("%w: transaction_id is required", execution.ErrInvalidSessionInput)
	}
	if req.FromSequence != nil && *req.FromSequence < 1 {
		return execution.Session{}, fmt.Errorf("%w: from_sequence must be >= 1", execution.ErrInvalidSessionInput)
	}
	if req.ToSequence != nil && *req.ToSequence < 1 {
		return execution.Session{}, fmt.Errorf("%w: to_sequence must be >= 1", execution.ErrInvalidSessionInput)
	}
	if req.FromSequence != nil && req.ToSequence != nil && *req.FromSequence > *req.ToSequence {
		return execution.Session{}, fmt.Errorf("%w: from_sequence cannot exceed to_sequence", execution.ErrInvalidSessionInput)
	}
	if _, err := e.events.GetTransaction(ctx, req.TransactionID); err != nil {
		return execution.Session{}, err
	}

	sess := execution.Session{
		SessionID:     newSessionID("rpl"),
		TransactionID: req.TransactionID,
		Mode:          execution.ModeReplay,
		Status:        execution.StatusCreated,
		FromSequence:  req.FromSequence,
		ToSequence:    req.ToSequence,
		InitialState:  req.InitialState,
	}
	return e.sessions.CreateSession(ctx, sess)
}

// StartSession transitions a CREATED replay session to RUNNING, loads events in
// deterministic sequence order, applies handlers with controlled dependencies, and
// persists per-event executions and the final outcome.
func (e *Engine) StartSession(ctx context.Context, sessionID string) (StartResult, error) {
	sess, err := e.sessions.MarkRunning(ctx, sessionID, time.Now().UTC())
	if err != nil {
		return StartResult{}, err
	}

	simSink := e.newSimulatedSink()
	completed, runErr := e.executeSession(ctx, sess, false, simSink, nil)
	res := StartResult{Session: completed, SimulatedEffects: simSink.Recorded()}
	if runErr != nil {
		return res, runErr
	}
	return res, nil
}

// GetSession fetches a replay session by ID.
func (e *Engine) GetSession(ctx context.Context, sessionID string) (execution.Session, error) {
	return e.sessions.GetSession(ctx, sessionID)
}

// ListExecutions returns the ordered event executions for a session.
func (e *Engine) ListExecutions(ctx context.Context, sessionID string) ([]execution.EventExecution, error) {
	if _, err := e.sessions.GetSession(ctx, sessionID); err != nil {
		return nil, err
	}
	return e.sessions.ListExecutions(ctx, sessionID)
}

// Compare compares two sessions. Either leftID or rightID may be "original"
// to resolve the latest ORIGINAL baseline for the other session's transaction.
func (e *Engine) Compare(ctx context.Context, leftID, rightID string) (execution.ComparisonResult, error) {
	if leftID == "" || rightID == "" {
		return execution.ComparisonResult{}, fmt.Errorf("%w: left and right session IDs are required", execution.ErrInvalidSessionInput)
	}
	if leftID == "original" && rightID == "original" {
		return execution.ComparisonResult{}, fmt.Errorf("%w: at least one concrete session ID is required", execution.ErrInvalidSessionInput)
	}

	var left, right execution.Session
	var err error

	if leftID != "original" {
		left, err = e.sessions.GetSession(ctx, leftID)
		if err != nil {
			return execution.ComparisonResult{}, err
		}
	}
	if rightID != "original" {
		right, err = e.sessions.GetSession(ctx, rightID)
		if err != nil {
			return execution.ComparisonResult{}, err
		}
	}
	if leftID == "original" {
		left, err = e.sessions.GetLatestOriginal(ctx, right.TransactionID)
		if err != nil {
			return execution.ComparisonResult{}, err
		}
	}
	if rightID == "original" {
		right, err = e.sessions.GetLatestOriginal(ctx, left.TransactionID)
		if err != nil {
			return execution.ComparisonResult{}, err
		}
	}

	leftExecs, err := e.sessions.ListExecutions(ctx, left.SessionID)
	if err != nil {
		return execution.ComparisonResult{}, err
	}
	rightExecs, err := e.sessions.ListExecutions(ctx, right.SessionID)
	if err != nil {
		return execution.ComparisonResult{}, err
	}
	return execution.Compare(left, leftExecs, right, rightExecs), nil
}

// OriginalRunner executes a transaction's events with live dependencies and stores
// the ORIGINAL execution baseline for future replay comparisons.
type OriginalRunner struct {
	engine   *Engine
	liveSink EffectSink
	clock    Clock
	idGen    IDGenerator
}

// NewOriginalRunner creates a runner that processes original executions with live deps.
func NewOriginalRunner(events EventReader, sessions SessionStore, registry *Registry, liveSink EffectSink) *OriginalRunner {
	if liveSink == nil {
		liveSink = NewLiveEffectSink(nil)
	}
	return &OriginalRunner{
		engine:   NewEngine(events, sessions, registry),
		liveSink: liveSink,
		clock:    SystemClock{},
		idGen:    RandomIDGenerator{},
	}
}

// ProcessOriginal runs the transaction's events using live dependencies and persists
// an ORIGINAL session and its event executions.
func (r *OriginalRunner) ProcessOriginal(ctx context.Context, transactionID string) (execution.Session, error) {
	if transactionID == "" {
		return execution.Session{}, fmt.Errorf("%w: transaction_id is required", execution.ErrInvalidSessionInput)
	}
	if _, err := r.engine.events.GetTransaction(ctx, transactionID); err != nil {
		return execution.Session{}, err
	}
	created, err := r.engine.sessions.CreateSession(ctx, execution.Session{
		SessionID:     newSessionID("orig"),
		TransactionID: transactionID,
		Mode:          execution.ModeOriginal,
		Status:        execution.StatusCreated,
	})
	if err != nil {
		return execution.Session{}, err
	}
	running, err := r.engine.sessions.MarkRunning(ctx, created.SessionID, time.Now().UTC())
	if err != nil {
		return execution.Session{}, err
	}
	return r.engine.executeSession(ctx, running, true, r.liveSink, r.liveDepsForSession())
}

func (r *OriginalRunner) liveDepsForSession() *ControlledDeps {
	return &ControlledDeps{Clock: r.clock, IDGen: r.idGen}
}

func (e *Engine) loadWindow(ctx context.Context, sess execution.Session) ([]eventstore.Event, error) {
	var after int64
	if sess.FromSequence != nil && *sess.FromSequence > 1 {
		after = *sess.FromSequence - 1
	}
	const pageSize = 1000
	var all []eventstore.Event
	for {
		batch, err := e.events.ListByTransaction(ctx, sess.TransactionID, after, pageSize)
		if err != nil {
			return nil, err
		}
		for _, ev := range batch {
			if sess.ToSequence != nil && ev.SequenceNumber > *sess.ToSequence {
				return all, nil
			}
			all = append(all, ev)
		}
		if len(batch) < pageSize {
			return all, nil
		}
		after = batch[len(batch)-1].SequenceNumber
	}
}

func validateContiguousSequence(events []eventstore.Event, from *int64) error {
	if len(events) == 0 {
		return execution.ErrNoEventsToReplay
	}
	expected := int64(1)
	if from != nil {
		expected = *from
	}
	for _, ev := range events {
		if ev.SequenceNumber != expected {
			return fmt.Errorf("%w: expected sequence %d, got %d (event_id=%s)",
				execution.ErrSequenceGap, expected, ev.SequenceNumber, ev.EventID)
		}
		expected++
	}
	return nil
}

func (e *Engine) executeSession(
	ctx context.Context,
	sess execution.Session,
	isLive bool,
	sink EffectSink,
	liveDeps *ControlledDeps,
) (execution.Session, error) {
	events, err := e.loadWindow(ctx, sess)
	if err != nil {
		completed, _ := e.sessions.CompleteSession(ctx, sess.SessionID, execution.StatusFailed, 0, nil, "", err.Error(), time.Now().UTC())
		return completed, err
	}
	if err := validateContiguousSequence(events, sess.FromSequence); err != nil {
		completed, _ := e.sessions.CompleteSession(ctx, sess.SessionID, execution.StatusFailed, 0, nil, "", err.Error(), time.Now().UTC())
		return completed, err
	}

	state := execution.NewInitialState(sess.TransactionID)
	if sess.InitialState != nil {
		state = sess.InitialState.Clone()
		if state.TransactionID == "" {
			state.TransactionID = sess.TransactionID
		}
	}

	applied := 0
	for _, ev := range events {
		var deps ControlledDeps
		var execTime time.Time
		if isLive && liveDeps != nil {
			deps = *liveDeps
			execTime = deps.Clock.Now()
		} else {
			clk := &EventClock{Current: ev.CreatedAt}
			deps = ControlledDeps{
				Clock: clk,
				IDGen: &DeterministicIDGenerator{SessionID: sess.SessionID, Sequence: ev.SequenceNumber},
			}
			execTime = clk.Now()
		}

		handler, ok := e.registry.Lookup(ev.EventType)
		if !ok {
			herr := fmt.Errorf("%w: %s", execution.ErrHandlerNotFound, ev.EventType)
			_ = e.sessions.RecordExecution(ctx, execution.EventExecution{
				SessionID:      sess.SessionID,
				EventID:        ev.EventID,
				EventType:      ev.EventType,
				SequenceNumber: ev.SequenceNumber,
				Status:         execution.EventFailed,
				StateHash:      state.Hash(),
				StateAfter:     state,
				Error:          herr.Error(),
				ExecutedAt:     execTime,
			})
			completed, cerr := e.sessions.CompleteSession(ctx, sess.SessionID, execution.StatusFailed, applied, &state, state.Hash(), herr.Error(), time.Now().UTC())
			if cerr != nil {
				return completed, errors.Join(herr, cerr)
			}
			return completed, herr
		}

		nextState, effects, applyErr := handler.Apply(deps, state, ev)
		if applyErr == nil {
			for _, eff := range effects {
				if emitErr := sink.Emit(ctx, eff); emitErr != nil {
					applyErr = fmt.Errorf("emit effect %s: %w", eff.Type, emitErr)
					break
				}
			}
		}

		if applyErr != nil {
			_ = e.sessions.RecordExecution(ctx, execution.EventExecution{
				SessionID:      sess.SessionID,
				EventID:        ev.EventID,
				EventType:      ev.EventType,
				SequenceNumber: ev.SequenceNumber,
				Status:         execution.EventFailed,
				StateHash:      state.Hash(),
				StateAfter:     state,
				Error:          applyErr.Error(),
				ExecutedAt:     execTime,
			})
			completed, cerr := e.sessions.CompleteSession(ctx, sess.SessionID, execution.StatusFailed, applied, &state, state.Hash(), applyErr.Error(), time.Now().UTC())
			if cerr != nil {
				return completed, errors.Join(applyErr, cerr)
			}
			return completed, applyErr
		}

		state = nextState
		applied++
		if err := e.sessions.RecordExecution(ctx, execution.EventExecution{
			SessionID:      sess.SessionID,
			EventID:        ev.EventID,
			EventType:      ev.EventType,
			SequenceNumber: ev.SequenceNumber,
			Status:         execution.EventSucceeded,
			StateHash:      state.Hash(),
			StateAfter:     state,
			Effects:        effects,
			ExecutedAt:     execTime,
		}); err != nil {
			completed, _ := e.sessions.CompleteSession(ctx, sess.SessionID, execution.StatusFailed, applied, &state, state.Hash(), err.Error(), time.Now().UTC())
			return completed, err
		}
	}

	completed, err := e.sessions.CompleteSession(ctx, sess.SessionID, execution.StatusCompleted, applied, &state, state.Hash(), "", time.Now().UTC())
	if err != nil {
		return completed, err
	}
	return completed, nil
}
