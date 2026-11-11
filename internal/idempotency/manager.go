// Package idempotency enforces processing-level idempotency and event-processing
// state transitions inside PostgreSQL transactions.
package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ProcessingStatus is the state machine for a single event's processing lifecycle:
// RECEIVED -> PENDING_PREREQUISITE -> PROCESSING -> PROCESSED | FAILED | DEAD
type ProcessingStatus string

const (
	StatusReceived            ProcessingStatus = "RECEIVED"
	StatusPendingPrerequisite ProcessingStatus = "PENDING_PREREQUISITE"
	StatusProcessing          ProcessingStatus = "PROCESSING"
	StatusProcessed           ProcessingStatus = "PROCESSED"
	StatusFailed              ProcessingStatus = "FAILED"
	StatusDead                ProcessingStatus = "DEAD"
)

// CanTransition reports whether moving from `from` to `to` is valid in the
// event-processing state machine.
func CanTransition(from, to ProcessingStatus) bool {
	switch from {
	case StatusReceived:
		return to == StatusPendingPrerequisite || to == StatusProcessing || to == StatusFailed
	case StatusPendingPrerequisite:
		return to == StatusProcessing || to == StatusFailed || to == StatusDead
	case StatusProcessing:
		return to == StatusProcessed || to == StatusFailed || to == StatusDead
	case StatusFailed:
		return to == StatusProcessing || to == StatusDead
	default:
		return false
	}
}

// RecordStatus tracks the lifecycle of an idempotency key.
type RecordStatus string

const (
	RecordInProgress RecordStatus = "IN_PROGRESS"
	RecordCommitted  RecordStatus = "COMMITTED"
	RecordFailed     RecordStatus = "FAILED"
)

var (
	// ErrKeyConflict means the same idempotency key was reused with a different request hash.
	ErrKeyConflict = errors.New("idempotency key already used with different request content")
	// ErrAlreadyInProgress means another worker currently holds an active claim on this key.
	ErrAlreadyInProgress = errors.New("idempotency key is currently in progress")
	// ErrInvalidKey means the supplied idempotency key is empty or too long.
	ErrInvalidKey = errors.New("invalid idempotency key")
)

// Record represents a row in idempotency_records.
type Record struct {
	IdempotencyKey string          `json:"idempotency_key"`
	EventID        string          `json:"event_id"`
	RequestHash    string          `json:"request_hash"`
	Status         RecordStatus    `json:"status"`
	Result         json.RawMessage `json:"result,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	ExpiresAt      time.Time       `json:"expires_at"`
}

// ClaimResult describes the outcome of attempting to claim an idempotency key.
type ClaimResult struct {
	Acquired  bool            // true when this caller owns the lock/claim to process
	Duplicate bool            // true when a committed result already exists
	Record    Record          // populated when Duplicate is true or newly claimed
	Cached    json.RawMessage // cached result from a prior committed execution
}

// Manager performs atomic idempotency claims and state updates inside a pgx.Tx.
type Manager struct {
	ttl time.Duration
}

// NewManager creates an idempotency manager with the given record TTL.
func NewManager(ttl time.Duration) *Manager {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return &Manager{ttl: ttl}
}

// Claim atomically claims key inside tx using INSERT ... ON CONFLICT DO NOTHING.
// Callers must run Claim, effect commit, and Complete inside the same DB transaction
// so state and idempotency record change together or not at all.
func (m *Manager) Claim(ctx context.Context, tx pgx.Tx, key, eventID, requestHash string, now time.Time) (ClaimResult, error) {
	if key == "" || len(key) > 256 {
		return ClaimResult{}, ErrInvalidKey
	}
	expiresAt := now.UTC().Add(m.ttl)

	var rec Record
	err := tx.QueryRow(ctx,
		`INSERT INTO idempotency_records (idempotency_key, event_id, request_hash, status, expires_at)
		 VALUES ($1, $2, $3, 'IN_PROGRESS', $4)
		 ON CONFLICT (idempotency_key) DO NOTHING
		 RETURNING idempotency_key, event_id, request_hash, status, result, created_at, expires_at`,
		key, eventID, requestHash, expiresAt).
		Scan(&rec.IdempotencyKey, &rec.EventID, &rec.RequestHash, &rec.Status, &rec.Result, &rec.CreatedAt, &rec.ExpiresAt)
	if err == nil {
		return ClaimResult{Acquired: true, Record: rec}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ClaimResult{}, fmt.Errorf("insert idempotency claim: %w", err)
	}

	// Lock the existing record so concurrent workers serialize cleanly.
	err = tx.QueryRow(ctx,
		`SELECT idempotency_key, event_id, request_hash, status, result, created_at, expires_at
		   FROM idempotency_records
		  WHERE idempotency_key = $1
		  FOR UPDATE`, key).
		Scan(&rec.IdempotencyKey, &rec.EventID, &rec.RequestHash, &rec.Status, &rec.Result, &rec.CreatedAt, &rec.ExpiresAt)
	if err != nil {
		return ClaimResult{}, fmt.Errorf("load existing idempotency record: %w", err)
	}

	if rec.RequestHash != requestHash {
		return ClaimResult{}, ErrKeyConflict
	}

	switch rec.Status {
	case RecordCommitted:
		return ClaimResult{
			Acquired:  false,
			Duplicate: true,
			Record:    rec,
			Cached:    rec.Result,
		}, nil
	case RecordInProgress:
		if rec.ExpiresAt.After(now.UTC()) {
			return ClaimResult{}, ErrAlreadyInProgress
		}
		// Expired in-progress claim: take over safely.
		return m.reclaim(ctx, tx, key, eventID, expiresAt)
	case RecordFailed:
		return m.reclaim(ctx, tx, key, eventID, expiresAt)
	default:
		return ClaimResult{}, fmt.Errorf("unexpected idempotency status %q", rec.Status)
	}
}

func (m *Manager) reclaim(ctx context.Context, tx pgx.Tx, key, eventID string, expiresAt time.Time) (ClaimResult, error) {
	var rec Record
	err := tx.QueryRow(ctx,
		`UPDATE idempotency_records
		    SET status = 'IN_PROGRESS', event_id = $2, expires_at = $3, result = NULL
		  WHERE idempotency_key = $1
		  RETURNING idempotency_key, event_id, request_hash, status, result, created_at, expires_at`,
		key, eventID, expiresAt).
		Scan(&rec.IdempotencyKey, &rec.EventID, &rec.RequestHash, &rec.Status, &rec.Result, &rec.CreatedAt, &rec.ExpiresAt)
	if err != nil {
		return ClaimResult{}, fmt.Errorf("reclaim idempotency record: %w", err)
	}
	return ClaimResult{Acquired: true, Record: rec}, nil
}

// MarkCommitted marks the idempotency record as COMMITTED with its serialized result.
func (m *Manager) MarkCommitted(ctx context.Context, tx pgx.Tx, key string, result json.RawMessage) error {
	tag, err := tx.Exec(ctx,
		`UPDATE idempotency_records
		    SET status = 'COMMITTED', result = $2
		  WHERE idempotency_key = $1 AND status = 'IN_PROGRESS'`,
		key, []byte(result))
	if err != nil {
		return fmt.Errorf("commit idempotency record: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("idempotency record %q not in IN_PROGRESS state", key)
	}
	return nil
}

// MarkFailed marks the idempotency record as FAILED so a subsequent retry can reclaim it.
func (m *Manager) MarkFailed(ctx context.Context, tx pgx.Tx, key string, errMsg string) error {
	payload, _ := json.Marshal(map[string]string{"error": errMsg})
	_, err := tx.Exec(ctx,
		`UPDATE idempotency_records
		    SET status = 'FAILED', result = $2
		  WHERE idempotency_key = $1`,
		key, payload)
	if err != nil {
		return fmt.Errorf("fail idempotency record: %w", err)
	}
	return nil
}
