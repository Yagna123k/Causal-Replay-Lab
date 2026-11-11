package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Yagna123k/causal-replay-lab/internal/eventstore"
)

// TimelineEvent describes an ingested event together with its processing status.
type TimelineEvent struct {
	EventID        string    `json:"event_id"`
	EventType      string    `json:"event_type"`
	SequenceNumber int64     `json:"sequence_number"`
	CreatedAt      time.Time `json:"created_at"`
	ReceivedAt     time.Time `json:"received_at"`
	Processing     string    `json:"processing_status"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	Attempts       int       `json:"attempts"`
	LastError      string    `json:"last_error,omitempty"`
}

// TimelineCheckpoint is a checkpoint summary inside a transaction timeline.
type TimelineCheckpoint struct {
	CheckpointID int64     `json:"checkpoint_id"`
	LastSequence int64     `json:"last_sequence"`
	StateHash    string    `json:"state_hash"`
	CreatedAt    time.Time `json:"created_at"`
}

// TimelineSession bundles one execution/replay session with its executions,
// checkpoints, and retry attempts.
type TimelineSession struct {
	Session     Session              `json:"session"`
	Executions  []EventExecution     `json:"executions"`
	Checkpoints []TimelineCheckpoint `json:"checkpoints"`
	Attempts    []ExecutionAttempt   `json:"attempts"`
}

// TransactionTimeline is the unified debugging view for a transaction.
type TransactionTimeline struct {
	TransactionID     string            `json:"transaction_id"`
	Status            string            `json:"status"`
	ProcessedSequence int64             `json:"processed_sequence"`
	LastSequence      int64             `json:"last_sequence"`
	HasSequenceGap    bool              `json:"has_sequence_gap"`
	GapDetails        string            `json:"gap_details,omitempty"`
	CurrentState      *TransactionState `json:"current_state,omitempty"`
	CurrentStateHash  string            `json:"current_state_hash,omitempty"`
	Events            []TimelineEvent   `json:"events"`
	Sessions          []TimelineSession `json:"sessions"`
}

// FailureDetail inspects a failed or dead-lettered event in a replay session,
// including state before and after the failure and all attempt records.
type FailureDetail struct {
	EventID        string             `json:"event_id"`
	EventType      string             `json:"event_type"`
	SequenceNumber int64              `json:"sequence_number"`
	Status         EventStatus        `json:"status"`
	IsDeadLetter   bool               `json:"is_dead_letter"`
	AttemptCount   int                `json:"attempt_count"`
	Error          string             `json:"error"`
	StateBefore    TransactionState   `json:"state_before"`
	StateAfter     TransactionState   `json:"state_after"`
	Attempts       []ExecutionAttempt `json:"attempts"`
	ExecutedAt     time.Time          `json:"executed_at"`
}

// ReplayFailuresReport summarizes all failed executions in a replay session.
type ReplayFailuresReport struct {
	SessionID     string          `json:"session_id"`
	TransactionID string          `json:"transaction_id"`
	Status        SessionStatus   `json:"status"`
	SessionError  string          `json:"session_error,omitempty"`
	Failures      []FailureDetail `json:"failures"`
}

// GetTransactionTimeline returns events, processing states, replay sessions,
// executions, checkpoints, and retry attempts for transactionID.
func (s *Store) GetTransactionTimeline(ctx context.Context, transactionID string) (TransactionTimeline, error) {
	var tl TransactionTimeline
	var stateRaw []byte
	var gapDetails, stateHash *string

	err := s.pool.QueryRow(ctx,
		`SELECT transaction_id, status, processed_sequence, last_sequence,
		        has_sequence_gap, gap_details, current_state, current_state_hash
		   FROM transactions
		  WHERE transaction_id = $1`, transactionID).
		Scan(&tl.TransactionID, &tl.Status, &tl.ProcessedSequence, &tl.LastSequence,
			&tl.HasSequenceGap, &gapDetails, &stateRaw, &stateHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return TransactionTimeline{}, eventstore.ErrNotFound
	}
	if err != nil {
		return TransactionTimeline{}, fmt.Errorf("load timeline transaction: %w", err)
	}
	if gapDetails != nil {
		tl.GapDetails = *gapDetails
	}
	if stateHash != nil {
		tl.CurrentStateHash = *stateHash
	}
	if len(stateRaw) > 0 {
		var st TransactionState
		if err := json.Unmarshal(stateRaw, &st); err == nil {
			tl.CurrentState = &st
		}
	}

	evRows, err := s.pool.Query(ctx,
		`SELECT e.event_id, e.event_type, e.sequence_number, e.created_at, e.received_at,
		        ep.status, ep.idempotency_key, ep.attempts, ep.last_error
		   FROM events e
		   JOIN event_processing ep ON ep.event_id = e.event_id
		  WHERE e.transaction_id = $1
		  ORDER BY e.sequence_number ASC`, transactionID)
	if err != nil {
		return TransactionTimeline{}, fmt.Errorf("load timeline events: %w", err)
	}
	defer evRows.Close()

	tl.Events = []TimelineEvent{}
	for evRows.Next() {
		var te TimelineEvent
		var key, lastErr *string
		if err := evRows.Scan(&te.EventID, &te.EventType, &te.SequenceNumber, &te.CreatedAt, &te.ReceivedAt,
			&te.Processing, &key, &te.Attempts, &lastErr); err != nil {
			return TransactionTimeline{}, fmt.Errorf("scan timeline event: %w", err)
		}
		if key != nil {
			te.IdempotencyKey = *key
		}
		if lastErr != nil {
			te.LastError = *lastErr
		}
		tl.Events = append(tl.Events, te)
	}
	if err := evRows.Err(); err != nil {
		return TransactionTimeline{}, err
	}

	sRows, err := s.pool.Query(ctx,
		`SELECT `+sessionColumns+`
		   FROM replay_sessions
		  WHERE transaction_id = $1
		  ORDER BY created_at ASC`, transactionID)
	if err != nil {
		return TransactionTimeline{}, fmt.Errorf("load timeline sessions: %w", err)
	}
	var sessions []Session
	for sRows.Next() {
		sess, err := scanSession(sRows)
		if err != nil {
			sRows.Close()
			return TransactionTimeline{}, fmt.Errorf("scan timeline session: %w", err)
		}
		sessions = append(sessions, sess)
	}
	sRows.Close()
	if err := sRows.Err(); err != nil {
		return TransactionTimeline{}, err
	}

	tl.Sessions = make([]TimelineSession, 0, len(sessions))
	for _, sess := range sessions {
		execs, err := s.ListExecutions(ctx, sess.SessionID)
		if err != nil {
			return TransactionTimeline{}, err
		}
		atts, err := s.ListAttempts(ctx, sess.SessionID)
		if err != nil {
			return TransactionTimeline{}, err
		}
		cps, err := s.listTimelineCheckpoints(ctx, sess.SessionID)
		if err != nil {
			return TransactionTimeline{}, err
		}
		tl.Sessions = append(tl.Sessions, TimelineSession{
			Session:     sess,
			Executions:  execs,
			Checkpoints: cps,
			Attempts:    atts,
		})
	}
	return tl, nil
}

func (s *Store) listTimelineCheckpoints(ctx context.Context, sessionID string) ([]TimelineCheckpoint, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT checkpoint_id, last_sequence, state_hash, created_at
		   FROM replay_checkpoints
		  WHERE session_id = $1
		  ORDER BY last_sequence ASC`, sessionID)
	if err != nil {
		return nil, fmt.Errorf("list timeline checkpoints: %w", err)
	}
	defer rows.Close()

	out := []TimelineCheckpoint{}
	for rows.Next() {
		var cp TimelineCheckpoint
		if err := rows.Scan(&cp.CheckpointID, &cp.LastSequence, &cp.StateHash, &cp.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, cp)
	}
	return out, rows.Err()
}

// GetReplayFailures returns detailed failure records (with state before/after and attempts)
// for a replay session.
func (s *Store) GetReplayFailures(ctx context.Context, sessionID string) (ReplayFailuresReport, error) {
	sess, err := s.GetSession(ctx, sessionID)
	if err != nil {
		return ReplayFailuresReport{}, err
	}
	execs, err := s.ListExecutions(ctx, sessionID)
	if err != nil {
		return ReplayFailuresReport{}, err
	}
	atts, err := s.ListAttempts(ctx, sessionID)
	if err != nil {
		return ReplayFailuresReport{}, err
	}

	attsBySeq := map[int64][]ExecutionAttempt{}
	for _, a := range atts {
		attsBySeq[a.SequenceNumber] = append(attsBySeq[a.SequenceNumber], a)
	}

	prevState := NewInitialState(sess.TransactionID)
	if sess.InitialState != nil {
		prevState = sess.InitialState.Clone()
	}

	report := ReplayFailuresReport{
		SessionID:     sess.SessionID,
		TransactionID: sess.TransactionID,
		Status:        sess.Status,
		SessionError:  sess.Error,
		Failures:      []FailureDetail{},
	}

	for _, ex := range execs {
		if ex.Status == EventFailed || ex.Status == EventRetrying || ex.Status == EventDead {
			report.Failures = append(report.Failures, FailureDetail{
				EventID:        ex.EventID,
				EventType:      ex.EventType,
				SequenceNumber: ex.SequenceNumber,
				Status:         ex.Status,
				IsDeadLetter:   ex.IsDeadLetter,
				AttemptCount:   ex.AttemptCount,
				Error:          ex.Error,
				StateBefore:    prevState.Clone(),
				StateAfter:     ex.StateAfter.Clone(),
				Attempts:       attsBySeq[ex.SequenceNumber],
				ExecutedAt:     ex.ExecutedAt,
			})
		}
		if ex.Status == EventSucceeded {
			prevState = ex.StateAfter.Clone()
		}
	}
	return report, nil
}
