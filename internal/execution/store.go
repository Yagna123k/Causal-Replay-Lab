package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store persists replay sessions and per-event execution outcomes in PostgreSQL.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore creates an execution store backed by pool.
func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

const sessionColumns = `session_id, transaction_id, mode, status, from_sequence, to_sequence,
	initial_state, event_count, final_state, final_state_hash, error, created_at, started_at, completed_at`

func scanSession(row pgx.Row) (Session, error) {
	var s Session
	var initRaw, finalRaw []byte
	var finalHash, errStr *string
	err := row.Scan(
		&s.SessionID, &s.TransactionID, &s.Mode, &s.Status,
		&s.FromSequence, &s.ToSequence, &initRaw,
		&s.EventCount, &finalRaw, &finalHash, &errStr,
		&s.CreatedAt, &s.StartedAt, &s.CompletedAt,
	)
	if err != nil {
		return Session{}, err
	}
	if len(initRaw) > 0 {
		var st TransactionState
		if err := json.Unmarshal(initRaw, &st); err != nil {
			return Session{}, fmt.Errorf("unmarshal initial_state: %w", err)
		}
		s.InitialState = &st
	}
	if len(finalRaw) > 0 {
		var st TransactionState
		if err := json.Unmarshal(finalRaw, &st); err != nil {
			return Session{}, fmt.Errorf("unmarshal final_state: %w", err)
		}
		s.FinalState = &st
	}
	if finalHash != nil {
		s.FinalStateHash = *finalHash
	}
	if errStr != nil {
		s.Error = *errStr
	}
	return s, nil
}

// CreateSession inserts a new session in CREATED status.
func (s *Store) CreateSession(ctx context.Context, sess Session) (Session, error) {
	var initJSON []byte
	if sess.InitialState != nil {
		var err error
		initJSON, err = sess.InitialState.CanonicalJSON()
		if err != nil {
			return Session{}, fmt.Errorf("marshal initial_state: %w", err)
		}
	}
	out, err := scanSession(s.pool.QueryRow(ctx,
		`INSERT INTO replay_sessions (session_id, transaction_id, mode, status, from_sequence, to_sequence, initial_state)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 RETURNING `+sessionColumns,
		sess.SessionID, sess.TransactionID, string(sess.Mode), string(StatusCreated),
		sess.FromSequence, sess.ToSequence, initJSON))
	if err != nil {
		return Session{}, fmt.Errorf("create session: %w", err)
	}
	return out, nil
}

// GetSession returns a replay or original session by ID.
func (s *Store) GetSession(ctx context.Context, sessionID string) (Session, error) {
	sess, err := scanSession(s.pool.QueryRow(ctx,
		`SELECT `+sessionColumns+` FROM replay_sessions WHERE session_id = $1`, sessionID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Session{}, ErrSessionNotFound
	}
	if err != nil {
		return Session{}, fmt.Errorf("get session: %w", err)
	}
	return sess, nil
}

// GetLatestOriginal returns the most recent completed or failed ORIGINAL execution baseline for a transaction.
func (s *Store) GetLatestOriginal(ctx context.Context, transactionID string) (Session, error) {
	sess, err := scanSession(s.pool.QueryRow(ctx,
		`SELECT `+sessionColumns+`
		   FROM replay_sessions
		  WHERE transaction_id = $1 AND mode = 'ORIGINAL' AND status IN ('COMPLETED', 'FAILED')
		  ORDER BY created_at DESC
		  LIMIT 1`, transactionID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Session{}, ErrOriginalNotFound
	}
	if err != nil {
		return Session{}, fmt.Errorf("get latest original: %w", err)
	}
	return sess, nil
}

// MarkRunning transitions a session from CREATED to RUNNING.
func (s *Store) MarkRunning(ctx context.Context, sessionID string, startedAt time.Time) (Session, error) {
	sess, err := scanSession(s.pool.QueryRow(ctx,
		`UPDATE replay_sessions
		    SET status = 'RUNNING', started_at = $2
		  WHERE session_id = $1 AND status = 'CREATED'
		  RETURNING `+sessionColumns,
		sessionID, startedAt.UTC()))
	if errors.Is(err, pgx.ErrNoRows) {
		// Distinguish not found from invalid transition.
		if _, gerr := s.GetSession(ctx, sessionID); errors.Is(gerr, ErrSessionNotFound) {
			return Session{}, ErrSessionNotFound
		}
		return Session{}, ErrInvalidTransition
	}
	if err != nil {
		return Session{}, fmt.Errorf("mark running: %w", err)
	}
	return sess, nil
}

// RecordExecution persists one event execution outcome within a session.
func (s *Store) RecordExecution(ctx context.Context, ex EventExecution) error {
	stateJSON, err := ex.StateAfter.CanonicalJSON()
	if err != nil {
		return fmt.Errorf("marshal state_after: %w", err)
	}
	effectsJSON := CanonicalEffectsJSON(ex.Effects)
	var errPtr *string
	if ex.Error != "" {
		e := ex.Error
		errPtr = &e
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO replay_executions
		 (session_id, event_id, event_type, sequence_number, status, state_hash, state_after, effects, error, executed_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		ex.SessionID, ex.EventID, ex.EventType, ex.SequenceNumber, string(ex.Status),
		ex.StateHash, stateJSON, effectsJSON, errPtr, ex.ExecutedAt.UTC())
	if err != nil {
		return fmt.Errorf("record execution: %w", err)
	}
	return nil
}

// CompleteSession transitions a RUNNING session to COMPLETED or FAILED.
func (s *Store) CompleteSession(
	ctx context.Context,
	sessionID string,
	status SessionStatus,
	eventCount int,
	finalState *TransactionState,
	finalHash string,
	errMsg string,
	completedAt time.Time,
) (Session, error) {
	if !CanTransition(StatusRunning, status) {
		return Session{}, ErrInvalidTransition
	}
	var finalJSON []byte
	if finalState != nil {
		var err error
		finalJSON, err = finalState.CanonicalJSON()
		if err != nil {
			return Session{}, fmt.Errorf("marshal final_state: %w", err)
		}
	}
	var hashPtr, errPtr *string
	if finalHash != "" {
		h := finalHash
		hashPtr = &h
	}
	if errMsg != "" {
		e := errMsg
		errPtr = &e
	}

	sess, err := scanSession(s.pool.QueryRow(ctx,
		`UPDATE replay_sessions
		    SET status = $2,
		        event_count = $3,
		        final_state = $4,
		        final_state_hash = $5,
		        error = $6,
		        completed_at = $7
		  WHERE session_id = $1 AND status = 'RUNNING'
		  RETURNING `+sessionColumns,
		sessionID, string(status), eventCount, finalJSON, hashPtr, errPtr, completedAt.UTC()))
	if errors.Is(err, pgx.ErrNoRows) {
		if _, gerr := s.GetSession(ctx, sessionID); errors.Is(gerr, ErrSessionNotFound) {
			return Session{}, ErrSessionNotFound
		}
		return Session{}, ErrInvalidTransition
	}
	if err != nil {
		return Session{}, fmt.Errorf("complete session: %w", err)
	}
	return sess, nil
}

// ListExecutions returns all recorded event executions for a session in sequence order.
func (s *Store) ListExecutions(ctx context.Context, sessionID string) ([]EventExecution, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT execution_id, session_id, event_id, event_type, sequence_number,
		        status, state_hash, state_after, effects, error, executed_at
		   FROM replay_executions
		  WHERE session_id = $1
		  ORDER BY sequence_number ASC`, sessionID)
	if err != nil {
		return nil, fmt.Errorf("list executions: %w", err)
	}
	defer rows.Close()

	out := []EventExecution{}
	for rows.Next() {
		var ex EventExecution
		var stateRaw, effectsRaw []byte
		var errPtr *string
		if err := rows.Scan(
			&ex.ExecutionID, &ex.SessionID, &ex.EventID, &ex.EventType, &ex.SequenceNumber,
			&ex.Status, &ex.StateHash, &stateRaw, &effectsRaw, &errPtr, &ex.ExecutedAt,
		); err != nil {
			return nil, fmt.Errorf("scan execution: %w", err)
		}
		if err := json.Unmarshal(stateRaw, &ex.StateAfter); err != nil {
			return nil, fmt.Errorf("unmarshal state_after: %w", err)
		}
		if len(effectsRaw) > 0 {
			if err := json.Unmarshal(effectsRaw, &ex.Effects); err != nil {
				return nil, fmt.Errorf("unmarshal effects: %w", err)
			}
		}
		if errPtr != nil {
			ex.Error = *errPtr
		}
		out = append(out, ex)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list executions: %w", err)
	}
	return out, nil
}
