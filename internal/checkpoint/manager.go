// Package checkpoint persists and verifies atomic replay execution checkpoints.
package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Yagna123k/causal-replay-lab/internal/execution"
)

var (
	// ErrCheckpointNotFound indicates no checkpoint has been saved for the session.
	ErrCheckpointNotFound = errors.New("checkpoint not found")
	// ErrCorruptCheckpoint indicates the persisted snapshot hash does not match state_hash.
	ErrCorruptCheckpoint = errors.New("checkpoint integrity check failed: state hash mismatch")
)

// Checkpoint represents a consistent committed position in a replay session.
type Checkpoint struct {
	CheckpointID  int64                      `json:"checkpoint_id"`
	SessionID     string                     `json:"session_id"`
	LastSequence  int64                      `json:"last_sequence"`
	StateHash     string                     `json:"state_hash"`
	StateSnapshot execution.TransactionState `json:"state_snapshot"`
	CreatedAt     time.Time                  `json:"created_at"`
}

// Verify checks that StateSnapshot hashes to StateHash.
func Verify(cp Checkpoint) error {
	if cp.StateSnapshot.Hash() != cp.StateHash {
		return fmt.Errorf("%w: seq=%d expected=%s actual=%s",
			ErrCorruptCheckpoint, cp.LastSequence, cp.StateHash, cp.StateSnapshot.Hash())
	}
	return nil
}

// Manager persists and reads atomic replay checkpoints in PostgreSQL.
type Manager struct {
	pool *pgxpool.Pool
	// BeforeCommit is an optional test hook run inside CommitEventWithCheckpoint
	// after writing execution/checkpoint rows and before tx.Commit.
	BeforeCommit func(ctx context.Context, seq int64) error
}

// NewManager creates a checkpoint manager backed by pool.
func NewManager(pool *pgxpool.Pool) *Manager {
	return &Manager{pool: pool}
}

// CommitEventWithCheckpoint atomically writes:
//  1. the succeeded EventExecution row (idempotent on (session_id, sequence_number)),
//  2. the ExecutionAttempt audit entry,
//  3. the replay_checkpoints row with state snapshot & hash, and
//  4. the replay_sessions progress (last_checkpoint_seq, event_count, final_state, final_state_hash)
//
// inside a single PostgreSQL transaction so a crash between execution and checkpoint
// is impossible by construction.
func (m *Manager) CommitEventWithCheckpoint(
	ctx context.Context,
	ex execution.EventExecution,
	eventCount int,
) (Checkpoint, error) {
	stateJSON, err := ex.StateAfter.CanonicalJSON()
	if err != nil {
		return Checkpoint{}, fmt.Errorf("marshal state snapshot: %w", err)
	}
	effectsJSON := execution.CanonicalEffectsJSON(ex.Effects)
	attempts := ex.AttemptCount
	if attempts < 1 {
		attempts = 1
	}

	tx, err := m.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Checkpoint{}, fmt.Errorf("begin checkpoint tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Idempotency guard: if this sequence was already committed as SUCCEEDED in this session,
	// do not duplicate execution or attempt rows.
	var existingStatus string
	err = tx.QueryRow(ctx,
		`SELECT status FROM replay_executions
		  WHERE session_id = $1 AND sequence_number = $2
		  FOR UPDATE`,
		ex.SessionID, ex.SequenceNumber).Scan(&existingStatus)
	if err == nil && existingStatus == string(execution.EventSucceeded) {
		var cp Checkpoint
		var snapRaw []byte
		if gerr := tx.QueryRow(ctx,
			`SELECT checkpoint_id, session_id, last_sequence, state_hash, state_snapshot, created_at
			   FROM replay_checkpoints
			  WHERE session_id = $1 AND last_sequence = $2`,
			ex.SessionID, ex.SequenceNumber).
			Scan(&cp.CheckpointID, &cp.SessionID, &cp.LastSequence, &cp.StateHash, &snapRaw, &cp.CreatedAt); gerr == nil {
			_ = json.Unmarshal(snapRaw, &cp.StateSnapshot)
			_ = tx.Commit(ctx)
			return cp, nil
		}
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return Checkpoint{}, fmt.Errorf("check existing execution: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO replay_executions
		 (session_id, event_id, event_type, sequence_number, status, state_hash, state_after,
		  effects, attempt_count, next_retry_at, is_dead_letter, error, executed_at)
		 VALUES ($1, $2, $3, $4, 'SUCCEEDED', $5, $6, $7, $8, NULL, false, NULL, $9)
		 ON CONFLICT (session_id, sequence_number) DO UPDATE SET
		     status = 'SUCCEEDED',
		     state_hash = EXCLUDED.state_hash,
		     state_after = EXCLUDED.state_after,
		     effects = EXCLUDED.effects,
		     attempt_count = EXCLUDED.attempt_count,
		     next_retry_at = NULL,
		     is_dead_letter = false,
		     error = NULL,
		     executed_at = EXCLUDED.executed_at`,
		ex.SessionID, ex.EventID, ex.EventType, ex.SequenceNumber,
		ex.StateHash, stateJSON, effectsJSON, attempts, ex.ExecutedAt.UTC()); err != nil {
		return Checkpoint{}, fmt.Errorf("upsert execution: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO replay_execution_attempts
		 (session_id, event_id, sequence_number, attempt_number, status, attempted_at)
		 VALUES ($1, $2, $3, $4, 'SUCCEEDED', $5)`,
		ex.SessionID, ex.EventID, ex.SequenceNumber, attempts, ex.ExecutedAt.UTC()); err != nil {
		return Checkpoint{}, fmt.Errorf("insert attempt: %w", err)
	}

	var cp Checkpoint
	var snapRaw []byte
	err = tx.QueryRow(ctx,
		`INSERT INTO replay_checkpoints (session_id, last_sequence, state_hash, state_snapshot)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (session_id, last_sequence) DO UPDATE SET
		     state_hash = EXCLUDED.state_hash,
		     state_snapshot = EXCLUDED.state_snapshot
		 RETURNING checkpoint_id, session_id, last_sequence, state_hash, state_snapshot, created_at`,
		ex.SessionID, ex.SequenceNumber, ex.StateHash, stateJSON).
		Scan(&cp.CheckpointID, &cp.SessionID, &cp.LastSequence, &cp.StateHash, &snapRaw, &cp.CreatedAt)
	if err != nil {
		return Checkpoint{}, fmt.Errorf("insert checkpoint: %w", err)
	}
	if err := json.Unmarshal(snapRaw, &cp.StateSnapshot); err != nil {
		return Checkpoint{}, fmt.Errorf("unmarshal checkpoint snapshot: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`UPDATE replay_sessions
		    SET last_checkpoint_seq = GREATEST(last_checkpoint_seq, $2),
		        event_count = $3,
		        final_state = $4,
		        final_state_hash = $5
		  WHERE session_id = $1`,
		ex.SessionID, ex.SequenceNumber, eventCount, stateJSON, ex.StateHash); err != nil {
		return Checkpoint{}, fmt.Errorf("update session checkpoint progress: %w", err)
	}

	if m.BeforeCommit != nil {
		if err := m.BeforeCommit(ctx, ex.SequenceNumber); err != nil {
			return Checkpoint{}, fmt.Errorf("pre-commit hook aborted checkpoint: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return Checkpoint{}, fmt.Errorf("commit checkpoint tx: %w", err)
	}
	return cp, nil
}

// Latest returns the most recent checkpoint for sessionID after verifying its integrity hash.
func (m *Manager) Latest(ctx context.Context, sessionID string) (Checkpoint, error) {
	var cp Checkpoint
	var snapRaw []byte
	err := m.pool.QueryRow(ctx,
		`SELECT checkpoint_id, session_id, last_sequence, state_hash, state_snapshot, created_at
		   FROM replay_checkpoints
		  WHERE session_id = $1
		  ORDER BY last_sequence DESC
		  LIMIT 1`, sessionID).
		Scan(&cp.CheckpointID, &cp.SessionID, &cp.LastSequence, &cp.StateHash, &snapRaw, &cp.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Checkpoint{}, ErrCheckpointNotFound
	}
	if err != nil {
		return Checkpoint{}, fmt.Errorf("load latest checkpoint: %w", err)
	}
	if err := json.Unmarshal(snapRaw, &cp.StateSnapshot); err != nil {
		return Checkpoint{}, fmt.Errorf("unmarshal checkpoint snapshot: %w", err)
	}
	if err := Verify(cp); err != nil {
		return Checkpoint{}, err
	}
	return cp, nil
}

// List returns all checkpoints for sessionID in ascending sequence order.
func (m *Manager) List(ctx context.Context, sessionID string) ([]Checkpoint, error) {
	rows, err := m.pool.Query(ctx,
		`SELECT checkpoint_id, session_id, last_sequence, state_hash, state_snapshot, created_at
		   FROM replay_checkpoints
		  WHERE session_id = $1
		  ORDER BY last_sequence ASC`, sessionID)
	if err != nil {
		return nil, fmt.Errorf("list checkpoints: %w", err)
	}
	defer rows.Close()

	out := []Checkpoint{}
	for rows.Next() {
		var cp Checkpoint
		var snapRaw []byte
		if err := rows.Scan(&cp.CheckpointID, &cp.SessionID, &cp.LastSequence, &cp.StateHash, &snapRaw, &cp.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan checkpoint: %w", err)
		}
		if err := json.Unmarshal(snapRaw, &cp.StateSnapshot); err != nil {
			return nil, fmt.Errorf("unmarshal checkpoint snapshot: %w", err)
		}
		out = append(out, cp)
	}
	return out, rows.Err()
}
