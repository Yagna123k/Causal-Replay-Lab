package replay

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Yagna123k/causal-replay-lab/internal/checkpoint"
	"github.com/Yagna123k/causal-replay-lab/internal/eventstore"
	"github.com/Yagna123k/causal-replay-lab/internal/execution"
)

// RecoveryOptions configures lease duration, retries, and test hooks for RecoveryManager.
type RecoveryOptions struct {
	LeaseTTL    time.Duration
	RetryPolicy *RetryPolicy
	Registry    *Registry
	// AfterEvent is an optional hook invoked immediately after committing an event's
	// checkpoint (useful in tests to simulate a crash/cancellation mid-replay).
	AfterEvent func(ctx context.Context, seq int64) error
}

// RecoveryManager coordinates checkpointed replay execution, lease-based single-worker
// ownership, controlled retries, dead-lettering, and orphan session recovery.
// Like Engine, it only ever uses SimulatedEffectSink for replay.
type RecoveryManager struct {
	pool        *pgxpool.Pool
	events      EventReader
	execStore   *execution.Store
	checkpoints *checkpoint.Manager
	registry    *Registry
	retry       *RetryPolicy
	leaseTTL    time.Duration
	afterEvent  func(ctx context.Context, seq int64) error
}

// NewRecoveryManager creates a RecoveryManager backed by pool.
func NewRecoveryManager(
	pool *pgxpool.Pool,
	events EventReader,
	execStore *execution.Store,
	checkpoints *checkpoint.Manager,
	opts RecoveryOptions,
) *RecoveryManager {
	if opts.LeaseTTL <= 0 {
		opts.LeaseTTL = 30 * time.Second
	}
	if opts.RetryPolicy == nil {
		opts.RetryPolicy = DefaultRetryPolicy()
	}
	if opts.Registry == nil {
		opts.Registry = DefaultRegistry()
	}
	return &RecoveryManager{
		pool:        pool,
		events:      events,
		execStore:   execStore,
		checkpoints: checkpoints,
		registry:    opts.Registry,
		retry:       opts.RetryPolicy,
		leaseTTL:    opts.LeaseTTL,
		afterEvent:  opts.AfterEvent,
	}
}

// AcquireLease atomically claims ownership of sessionID for workerID unless
// another worker holds an unexpired lease.
func (m *RecoveryManager) AcquireLease(ctx context.Context, sessionID, workerID string, now time.Time) error {
	if workerID == "" {
		workerID = newSessionID("wrk")
	}
	expiresAt := now.UTC().Add(m.leaseTTL)
	tag, err := m.pool.Exec(ctx,
		`UPDATE replay_sessions
		    SET locked_by = $2,
		        lease_expires_at = $3,
		        heartbeat_at = $4
		  WHERE session_id = $1
		    AND (locked_by IS NULL OR locked_by = $2 OR lease_expires_at <= $4)`,
		sessionID, workerID, expiresAt, now.UTC())
	if err != nil {
		return fmt.Errorf("acquire lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if _, gerr := m.execStore.GetSession(ctx, sessionID); errors.Is(gerr, execution.ErrSessionNotFound) {
			return execution.ErrSessionNotFound
		}
		return execution.ErrSessionLocked
	}
	return nil
}

// RenewLease refreshes the worker's lease heartbeat on sessionID.
func (m *RecoveryManager) RenewLease(ctx context.Context, sessionID, workerID string, now time.Time) error {
	expiresAt := now.UTC().Add(m.leaseTTL)
	tag, err := m.pool.Exec(ctx,
		`UPDATE replay_sessions
		    SET lease_expires_at = $3,
		        heartbeat_at = $4
		  WHERE session_id = $1 AND locked_by = $2`,
		sessionID, workerID, expiresAt, now.UTC())
	if err != nil {
		return fmt.Errorf("renew lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return execution.ErrSessionLocked
	}
	return nil
}

// ReleaseLease releases workerID's lease on sessionID.
func (m *RecoveryManager) ReleaseLease(ctx context.Context, sessionID, workerID string) error {
	_, err := m.pool.Exec(ctx,
		`UPDATE replay_sessions
		    SET locked_by = NULL,
		        lease_expires_at = NULL
		  WHERE session_id = $1 AND locked_by = $2`,
		sessionID, workerID)
	return err
}

// PauseSession transitions a CREATED, RUNNING, or RESUMING session to PAUSED.
func (m *RecoveryManager) PauseSession(ctx context.Context, sessionID string) (execution.Session, error) {
	return m.transitionSession(ctx, sessionID,
		[]execution.SessionStatus{execution.StatusCreated, execution.StatusRunning, execution.StatusResuming},
		execution.StatusPaused, "")
}

// InterruptSession transitions a RUNNING, RESUMING, or PAUSED session to INTERRUPTED.
func (m *RecoveryManager) InterruptSession(ctx context.Context, sessionID, reason string) (execution.Session, error) {
	return m.transitionSession(ctx, sessionID,
		[]execution.SessionStatus{execution.StatusRunning, execution.StatusResuming, execution.StatusPaused},
		execution.StatusInterrupted, reason)
}

func (m *RecoveryManager) transitionSession(
	ctx context.Context,
	sessionID string,
	from []execution.SessionStatus,
	to execution.SessionStatus,
	errMsg string,
) (execution.Session, error) {
	fromStrs := make([]string, len(from))
	for i, s := range from {
		fromStrs[i] = string(s)
	}
	var errPtr *string
	if errMsg != "" {
		e := errMsg
		errPtr = &e
	}
	tag, err := m.pool.Exec(ctx,
		`UPDATE replay_sessions
		    SET status = $2,
		        error = COALESCE($3, error),
		        locked_by = NULL,
		        lease_expires_at = NULL
		  WHERE session_id = $1 AND status = ANY($4)`,
		sessionID, string(to), errPtr, fromStrs)
	if err != nil {
		return execution.Session{}, fmt.Errorf("transition session to %s: %w", to, err)
	}
	if tag.RowsAffected() == 0 {
		if _, gerr := m.execStore.GetSession(ctx, sessionID); errors.Is(gerr, execution.ErrSessionNotFound) {
			return execution.Session{}, execution.ErrSessionNotFound
		}
		return execution.Session{}, execution.ErrInvalidTransition
	}
	return m.execStore.GetSession(ctx, sessionID)
}

// DetectOrphanedSessions finds RUNNING or RESUMING sessions whose worker lease expired
// and marks them INTERRUPTED so they can be safely resumed.
func (m *RecoveryManager) DetectOrphanedSessions(ctx context.Context, now time.Time) ([]execution.Session, error) {
	rows, err := m.pool.Query(ctx,
		`UPDATE replay_sessions
		    SET status = 'INTERRUPTED',
		        error = 'worker lease expired',
		        locked_by = NULL,
		        lease_expires_at = NULL
		  WHERE status IN ('RUNNING', 'RESUMING')
		    AND lease_expires_at IS NOT NULL
		    AND lease_expires_at <= $1
		  RETURNING session_id`, now.UTC())
	if err != nil {
		return nil, fmt.Errorf("detect orphaned sessions: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]execution.Session, 0, len(ids))
	for _, id := range ids {
		s, err := m.execStore.GetSession(ctx, id)
		if err == nil {
			out = append(out, s)
		}
	}
	return out, nil
}

// StartWithRecovery runs a CREATED replay session with lease ownership, atomic checkpoints,
// controlled retries, and interruption support.
func (m *RecoveryManager) StartWithRecovery(ctx context.Context, sessionID, workerID string) (StartResult, error) {
	if workerID == "" {
		workerID = newSessionID("wrk")
	}
	now := time.Now().UTC()
	if err := m.AcquireLease(ctx, sessionID, workerID, now); err != nil {
		return StartResult{}, err
	}

	tag, err := m.pool.Exec(ctx,
		`UPDATE replay_sessions
		    SET status = 'RUNNING',
		        started_at = COALESCE(started_at, $3)
		  WHERE session_id = $1 AND status = 'CREATED' AND locked_by = $2`,
		sessionID, workerID, now)
	if err != nil {
		_ = m.ReleaseLease(context.WithoutCancel(ctx), sessionID, workerID)
		return StartResult{}, fmt.Errorf("start session: %w", err)
	}
	if tag.RowsAffected() == 0 {
		_ = m.ReleaseLease(context.WithoutCancel(ctx), sessionID, workerID)
		if _, gerr := m.execStore.GetSession(ctx, sessionID); errors.Is(gerr, execution.ErrSessionNotFound) {
			return StartResult{}, execution.ErrSessionNotFound
		}
		return StartResult{}, execution.ErrInvalidTransition
	}

	sess, err := m.execStore.GetSession(ctx, sessionID)
	if err != nil {
		_ = m.ReleaseLease(context.WithoutCancel(ctx), sessionID, workerID)
		return StartResult{}, err
	}

	state := execution.NewInitialState(sess.TransactionID)
	if sess.InitialState != nil {
		state = sess.InitialState.Clone()
		if state.TransactionID == "" {
			state.TransactionID = sess.TransactionID
		}
	}
	return m.runFromPosition(ctx, sess, workerID, sess.FromSequence, 0, state)
}

// ResumeSession resumes a PAUSED or INTERRUPTED replay session from its latest
// verified checkpoint.
func (m *RecoveryManager) ResumeSession(ctx context.Context, sessionID, workerID string) (StartResult, error) {
	if workerID == "" {
		workerID = newSessionID("wrk")
	}
	now := time.Now().UTC()
	if err := m.AcquireLease(ctx, sessionID, workerID, now); err != nil {
		return StartResult{}, err
	}

	// PAUSED | INTERRUPTED -> RESUMING
	tag, err := m.pool.Exec(ctx,
		`UPDATE replay_sessions
		    SET status = 'RESUMING',
		        error = NULL
		  WHERE session_id = $1 AND status IN ('PAUSED', 'INTERRUPTED') AND locked_by = $2`,
		sessionID, workerID)
	if err != nil {
		_ = m.ReleaseLease(context.WithoutCancel(ctx), sessionID, workerID)
		return StartResult{}, fmt.Errorf("transition to RESUMING: %w", err)
	}
	if tag.RowsAffected() == 0 {
		_ = m.ReleaseLease(context.WithoutCancel(ctx), sessionID, workerID)
		if _, gerr := m.execStore.GetSession(ctx, sessionID); errors.Is(gerr, execution.ErrSessionNotFound) {
			return StartResult{}, execution.ErrSessionNotFound
		}
		return StartResult{}, execution.ErrInvalidTransition
	}

	sess, err := m.execStore.GetSession(ctx, sessionID)
	if err != nil {
		_ = m.ReleaseLease(context.WithoutCancel(ctx), sessionID, workerID)
		return StartResult{}, err
	}

	// Load and verify the latest valid checkpoint.
	state := execution.NewInitialState(sess.TransactionID)
	if sess.InitialState != nil {
		state = sess.InitialState.Clone()
		if state.TransactionID == "" {
			state.TransactionID = sess.TransactionID
		}
	}
	fromSeq := sess.FromSequence
	applied := sess.EventCount

	cp, cpErr := m.checkpoints.Latest(ctx, sessionID)
	switch {
	case cpErr == nil:
		state = cp.StateSnapshot.Clone()
		nextSeq := cp.LastSequence + 1
		fromSeq = &nextSeq
		if applied == 0 {
			applied = state.AppliedEvents
		}
	case errors.Is(cpErr, checkpoint.ErrCheckpointNotFound):
		// Nothing checkpointed yet; resume from session start.
	default:
		// Corrupt checkpoint or DB error -> fail session loudly.
		completed, _ := m.execStore.CompleteSession(
			context.WithoutCancel(ctx), sessionID, execution.StatusFailed,
			applied, &state, state.Hash(), cpErr.Error(), time.Now().UTC(),
		)
		return StartResult{Session: completed}, cpErr
	}

	// RESUMING -> RUNNING
	if _, err := m.pool.Exec(ctx,
		`UPDATE replay_sessions SET status = 'RUNNING' WHERE session_id = $1 AND status = 'RESUMING'`,
		sessionID); err != nil {
		_ = m.ReleaseLease(context.WithoutCancel(ctx), sessionID, workerID)
		return StartResult{}, fmt.Errorf("transition RESUMING -> RUNNING: %w", err)
	}
	sess.Status = execution.StatusRunning
	return m.runFromPosition(ctx, sess, workerID, fromSeq, applied, state)
}

// ListCheckpoints returns the checkpoint history for a replay session.
func (m *RecoveryManager) ListCheckpoints(ctx context.Context, sessionID string) ([]checkpoint.Checkpoint, error) {
	if _, err := m.execStore.GetSession(ctx, sessionID); err != nil {
		return nil, err
	}
	return m.checkpoints.List(ctx, sessionID)
}

// ListAttempts returns all execution attempts (initial and retries) for a session.
func (m *RecoveryManager) ListAttempts(ctx context.Context, sessionID string) ([]execution.ExecutionAttempt, error) {
	if _, err := m.execStore.GetSession(ctx, sessionID); err != nil {
		return nil, err
	}
	return m.execStore.ListAttempts(ctx, sessionID)
}

func (m *RecoveryManager) runFromPosition(
	ctx context.Context,
	sess execution.Session,
	workerID string,
	fromSeq *int64,
	applied int,
	state execution.TransactionState,
) (StartResult, error) {
	windowSess := sess
	windowSess.FromSequence = fromSeq
	events, err := (&Engine{events: m.events}).loadWindow(ctx, windowSess)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			interrupted, _ := m.InterruptSession(context.WithoutCancel(ctx), sess.SessionID, "interrupted while loading events")
			return StartResult{Session: interrupted}, err
		}
		completed, _ := m.execStore.CompleteSession(context.WithoutCancel(ctx), sess.SessionID, execution.StatusFailed, applied, &state, state.Hash(), err.Error(), time.Now().UTC())
		return StartResult{Session: completed}, err
	}

	// When resuming past the last event (all events already checkpointed), complete cleanly.
	if len(events) == 0 && sess.LastCheckpointSeq > 0 {
		completed, cerr := m.execStore.CompleteSession(ctx, sess.SessionID, execution.StatusCompleted, applied, &state, state.Hash(), "", time.Now().UTC())
		return StartResult{Session: completed}, cerr
	}
	if err := validateContiguousSequence(events, fromSeq); err != nil {
		completed, _ := m.execStore.CompleteSession(context.WithoutCancel(ctx), sess.SessionID, execution.StatusFailed, applied, &state, state.Hash(), err.Error(), time.Now().UTC())
		return StartResult{Session: completed}, err
	}

	simSink := NewSimulatedEffectSink()
	for _, ev := range events {
		if ctxErr := ctx.Err(); ctxErr != nil {
			interrupted, _ := m.InterruptSession(context.WithoutCancel(ctx), sess.SessionID, "replay interrupted: "+ctxErr.Error())
			return StartResult{Session: interrupted, SimulatedEffects: simSink.Recorded()}, ctxErr
		}
		if err := m.RenewLease(ctx, sess.SessionID, workerID, time.Now().UTC()); err != nil {
			return StartResult{SimulatedEffects: simSink.Recorded()}, err
		}

		// Check if session was externally paused between events.
		curSess, gerr := m.execStore.GetSession(ctx, sess.SessionID)
		if gerr == nil && curSess.Status == execution.StatusPaused {
			_ = m.ReleaseLease(ctx, sess.SessionID, workerID)
			return StartResult{Session: curSess, SimulatedEffects: simSink.Recorded()}, nil
		}

		nextState, effects, attempts, runErr := m.executeEventWithRetries(ctx, sess.SessionID, state, ev, simSink)
		if runErr != nil {
			if errors.Is(runErr, context.Canceled) {
				interrupted, _ := m.InterruptSession(context.WithoutCancel(ctx), sess.SessionID, "replay interrupted: "+runErr.Error())
				return StartResult{Session: interrupted, SimulatedEffects: simSink.Recorded()}, runErr
			}
			completed, cerr := m.execStore.CompleteSession(context.WithoutCancel(ctx), sess.SessionID, execution.StatusFailed, applied, &state, state.Hash(), runErr.Error(), time.Now().UTC())
			if cerr != nil {
				return StartResult{Session: completed, SimulatedEffects: simSink.Recorded()}, errors.Join(runErr, cerr)
			}
			return StartResult{Session: completed, SimulatedEffects: simSink.Recorded()}, runErr
		}

		applied++
		state = nextState
		execTime := (&EventClock{Current: ev.CreatedAt}).Now()
		_, cpErr := m.checkpoints.CommitEventWithCheckpoint(ctx, execution.EventExecution{
			SessionID:      sess.SessionID,
			EventID:        ev.EventID,
			EventType:      ev.EventType,
			SequenceNumber: ev.SequenceNumber,
			Status:         execution.EventSucceeded,
			StateHash:      state.Hash(),
			StateAfter:     state,
			Effects:        effects,
			AttemptCount:   attempts,
			ExecutedAt:     execTime,
		}, applied)
		if cpErr != nil {
			if errors.Is(cpErr, context.Canceled) {
				interrupted, _ := m.InterruptSession(context.WithoutCancel(ctx), sess.SessionID, "interrupted during checkpoint")
				return StartResult{Session: interrupted, SimulatedEffects: simSink.Recorded()}, cpErr
			}
			completed, _ := m.execStore.CompleteSession(context.WithoutCancel(ctx), sess.SessionID, execution.StatusFailed, applied-1, &state, state.Hash(), cpErr.Error(), time.Now().UTC())
			return StartResult{Session: completed, SimulatedEffects: simSink.Recorded()}, cpErr
		}

		if m.afterEvent != nil {
			if hookErr := m.afterEvent(ctx, ev.SequenceNumber); hookErr != nil {
				interrupted, _ := m.InterruptSession(context.WithoutCancel(ctx), sess.SessionID, hookErr.Error())
				return StartResult{Session: interrupted, SimulatedEffects: simSink.Recorded()}, hookErr
			}
		}
	}

	completed, err := m.execStore.CompleteSession(ctx, sess.SessionID, execution.StatusCompleted, applied, &state, state.Hash(), "", time.Now().UTC())
	return StartResult{Session: completed, SimulatedEffects: simSink.Recorded()}, err
}

func (m *RecoveryManager) executeEventWithRetries(
	ctx context.Context,
	sessionID string,
	state execution.TransactionState,
	ev eventstore.Event,
	simSink *SimulatedEffectSink,
) (execution.TransactionState, []execution.Effect, int, error) {
	handler, ok := m.registry.Lookup(ev.EventType)
	if !ok {
		herr := fmt.Errorf("%w: %s", execution.ErrHandlerNotFound, ev.EventType)
		_ = m.recordFailureOutcome(ctx, sessionID, state, ev, 1, execution.EventFailed, false, nil, herr.Error())
		return state, nil, 1, herr
	}

	for attempt := 1; attempt <= m.retry.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return state, nil, attempt, ctx.Err()
		}

		clk := &EventClock{Current: ev.CreatedAt}
		deps := ControlledDeps{
			Clock: clk,
			IDGen: &DeterministicIDGenerator{SessionID: sessionID, Sequence: ev.SequenceNumber},
		}
		nextState, effects, applyErr := handler.Apply(deps, state, ev)
		if applyErr == nil {
			for _, eff := range effects {
				if emitErr := simSink.Emit(ctx, eff); emitErr != nil {
					applyErr = emitErr
					break
				}
			}
		}
		if applyErr == nil {
			return nextState, effects, attempt, nil
		}

		class := ClassifyError(applyErr)
		if class == ClassRetryable && attempt < m.retry.MaxAttempts {
			delay := m.retry.Backoff(attempt)
			nextRetry := time.Now().UTC().Add(delay)
			_ = m.recordFailureOutcome(ctx, sessionID, state, ev, attempt, execution.EventRetrying, false, &nextRetry, applyErr.Error())

			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return state, nil, attempt, ctx.Err()
			case <-timer.C:
			}
			continue
		}

		// Retries exhausted on a retryable error -> dead letter!
		if class == ClassRetryable {
			_ = m.recordFailureOutcome(ctx, sessionID, state, ev, attempt, execution.EventDead, true, nil, applyErr.Error())
			return state, nil, attempt, applyErr
		}

		// Non-retryable error -> fail immediately.
		_ = m.recordFailureOutcome(ctx, sessionID, state, ev, attempt, execution.EventFailed, false, nil, applyErr.Error())
		return state, nil, attempt, applyErr
	}

	return state, nil, m.retry.MaxAttempts, fmt.Errorf("retry loop exhausted")
}

func (m *RecoveryManager) recordFailureOutcome(
	ctx context.Context,
	sessionID string,
	state execution.TransactionState,
	ev eventstore.Event,
	attempt int,
	status execution.EventStatus,
	isDead bool,
	nextRetry *time.Time,
	errMsg string,
) error {
	now := time.Now().UTC()
	if err := m.execStore.RecordAttempt(ctx, execution.ExecutionAttempt{
		SessionID:      sessionID,
		EventID:        ev.EventID,
		SequenceNumber: ev.SequenceNumber,
		AttemptNumber:  attempt,
		Status:         status,
		Error:          errMsg,
		NextRetryAt:    nextRetry,
		AttemptedAt:    now,
	}); err != nil {
		return err
	}
	return m.execStore.RecordExecution(ctx, execution.EventExecution{
		SessionID:      sessionID,
		EventID:        ev.EventID,
		EventType:      ev.EventType,
		SequenceNumber: ev.SequenceNumber,
		Status:         status,
		StateHash:      state.Hash(),
		StateAfter:     state,
		AttemptCount:   attempt,
		NextRetryAt:    nextRetry,
		IsDeadLetter:   isDead,
		Error:          errMsg,
		ExecutedAt:     now,
	})
}
