// Package execution defines replay sessions, deterministic state reconstruction,
// per-event execution tracking, and outcome comparison.
package execution

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

// Mode distinguishes live original processing from isolated replay.
type Mode string

const (
	ModeOriginal Mode = "ORIGINAL"
	ModeReplay   Mode = "REPLAY"
)

// SessionStatus represents the full lifecycle of a replay/execution session.
// CREATED -> RUNNING -> PAUSED/INTERRUPTED -> RESUMING -> COMPLETED | FAILED
type SessionStatus string

const (
	StatusCreated     SessionStatus = "CREATED"
	StatusRunning     SessionStatus = "RUNNING"
	StatusPaused      SessionStatus = "PAUSED"
	StatusInterrupted SessionStatus = "INTERRUPTED"
	StatusResuming    SessionStatus = "RESUMING"
	StatusCompleted   SessionStatus = "COMPLETED"
	StatusFailed      SessionStatus = "FAILED"
)

// CanTransition reports whether a session status transition is valid.
func CanTransition(from, to SessionStatus) bool {
	switch from {
	case StatusCreated:
		return to == StatusRunning || to == StatusPaused
	case StatusRunning:
		return to == StatusPaused || to == StatusInterrupted || to == StatusCompleted || to == StatusFailed
	case StatusPaused:
		return to == StatusResuming || to == StatusInterrupted
	case StatusInterrupted:
		return to == StatusResuming
	case StatusResuming:
		return to == StatusRunning || to == StatusPaused || to == StatusInterrupted || to == StatusCompleted || to == StatusFailed
	default:
		return false
	}
}

// EventStatus represents the outcome of applying a single event in a session.
type EventStatus string

const (
	EventSucceeded EventStatus = "SUCCEEDED"
	EventRetrying  EventStatus = "RETRYING"
	EventFailed    EventStatus = "FAILED"
	EventDead      EventStatus = "DEAD"
)

var (
	// ErrSessionNotFound indicates the requested replay session does not exist.
	ErrSessionNotFound = errors.New("replay session not found")
	// ErrOriginalNotFound indicates no original execution baseline exists for the transaction.
	ErrOriginalNotFound = errors.New("original execution baseline not found")
	// ErrInvalidTransition indicates a disallowed replay session state transition.
	ErrInvalidTransition = errors.New("invalid replay session status transition")
	// ErrSequenceGap indicates missing or non-contiguous event sequence numbers during replay.
	ErrSequenceGap = errors.New("event sequence gap or mismatch detected")
	// ErrHandlerNotFound indicates no event handler is registered for an event type.
	ErrHandlerNotFound = errors.New("no handler registered for event type")
	// ErrNoEventsToReplay indicates there are zero events matching the replay scope.
	ErrNoEventsToReplay = errors.New("no events found for replay")
	// ErrInvalidSessionInput indicates invalid replay session parameters.
	ErrInvalidSessionInput = errors.New("invalid replay session parameters")
	// ErrSessionLocked indicates another worker holds an active lease on the session.
	ErrSessionLocked = errors.New("replay session is locked by another worker")
)

// Effect represents an external side effect produced while processing an event
// (e.g., ledger debit, ledger entry, notification). During replay, effects are
// captured by a simulated sink rather than dispatched to live systems.
type Effect struct {
	Type        string         `json:"type"`
	Target      string         `json:"target,omitempty"`
	ReferenceID string         `json:"reference_id,omitempty"`
	Amount      int64          `json:"amount,omitempty"`
	Currency    string         `json:"currency,omitempty"`
	Data        map[string]any `json:"data,omitempty"`
	Timestamp   time.Time      `json:"timestamp"`
}

// TransactionState is the reconstructed state of a transaction at a point in
// an execution or replay session.
type TransactionState struct {
	TransactionID    string            `json:"transaction_id"`
	Status           string            `json:"status"`
	AccountID        string            `json:"account_id,omitempty"`
	Currency         string            `json:"currency,omitempty"`
	Balance          int64             `json:"balance"`
	PendingDebit     int64             `json:"pending_debit"`
	AccountValidated bool              `json:"account_validated"`
	LastSequence     int64             `json:"last_sequence"`
	AppliedEvents    int               `json:"applied_events"`
	FailureReason    string            `json:"failure_reason,omitempty"`
	Attributes       map[string]string `json:"attributes,omitempty"`
}

// NewInitialState returns the deterministic empty initial state for a transaction.
func NewInitialState(transactionID string) TransactionState {
	return TransactionState{
		TransactionID: transactionID,
		Status:        "NEW",
	}
}

// Clone returns a deep copy so handlers never mutate a prior state snapshot in place.
func (s TransactionState) Clone() TransactionState {
	out := s
	if s.Attributes != nil {
		out.Attributes = make(map[string]string, len(s.Attributes))
		for k, v := range s.Attributes {
			out.Attributes[k] = v
		}
	}
	return out
}

// CanonicalJSON returns a deterministic JSON encoding of the state.
// Go's encoding/json serializes struct fields in declaration order and sorts map keys.
func (s TransactionState) CanonicalJSON() ([]byte, error) {
	norm := s.Clone()
	if len(norm.Attributes) == 0 {
		norm.Attributes = nil
	}
	return json.Marshal(norm)
}

// Hash returns the SHA-256 hex digest of CanonicalJSON.
func (s TransactionState) Hash() string {
	b, err := s.CanonicalJSON()
	if err != nil {
		// TransactionState only contains deterministic JSON-serializable primitives.
		panic(fmt.Sprintf("canonical json marshal: %v", err))
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Session represents a persisted original execution or replay run.
type Session struct {
	SessionID         string            `json:"session_id"`
	TransactionID     string            `json:"transaction_id"`
	Mode              Mode              `json:"mode"`
	Status            SessionStatus     `json:"status"`
	FromSequence      *int64            `json:"from_sequence,omitempty"`
	ToSequence        *int64            `json:"to_sequence,omitempty"`
	InitialState      *TransactionState `json:"initial_state,omitempty"`
	EventCount        int               `json:"event_count"`
	LastCheckpointSeq int64             `json:"last_checkpoint_seq"`
	FinalState        *TransactionState `json:"final_state,omitempty"`
	FinalStateHash    string            `json:"final_state_hash,omitempty"`
	Error             string            `json:"error,omitempty"`
	LockedBy          string            `json:"locked_by,omitempty"`
	LeaseExpiresAt    *time.Time        `json:"lease_expires_at,omitempty"`
	HeartbeatAt       *time.Time        `json:"heartbeat_at,omitempty"`
	CreatedAt         time.Time         `json:"created_at"`
	StartedAt         *time.Time        `json:"started_at,omitempty"`
	CompletedAt       *time.Time        `json:"completed_at,omitempty"`
}

// EventExecution records the outcome of applying one event within a Session.
type EventExecution struct {
	ExecutionID    int64            `json:"execution_id"`
	SessionID      string           `json:"session_id"`
	EventID        string           `json:"event_id"`
	EventType      string           `json:"event_type"`
	SequenceNumber int64            `json:"sequence_number"`
	Status         EventStatus      `json:"status"`
	StateHash      string           `json:"state_hash"`
	StateAfter     TransactionState `json:"state_after"`
	Effects        []Effect         `json:"effects"`
	AttemptCount   int              `json:"attempt_count"`
	NextRetryAt    *time.Time       `json:"next_retry_at,omitempty"`
	IsDeadLetter   bool             `json:"is_dead_letter,omitempty"`
	Error          string           `json:"error,omitempty"`
	ExecutedAt     time.Time        `json:"executed_at"`
}

// ExecutionAttempt records one attempt (initial or retry) for a single event in a session.
type ExecutionAttempt struct {
	AttemptID      int64       `json:"attempt_id"`
	SessionID      string      `json:"session_id"`
	EventID        string      `json:"event_id"`
	SequenceNumber int64       `json:"sequence_number"`
	AttemptNumber  int         `json:"attempt_number"`
	Status         EventStatus `json:"status"`
	Error          string      `json:"error,omitempty"`
	NextRetryAt    *time.Time  `json:"next_retry_at,omitempty"`
	AttemptedAt    time.Time   `json:"attempted_at"`
}

// CanonicalEffectsHash computes a deterministic hash over a slice of effects.
// Non-deterministic execution-specific metadata (e.g. random ReferenceID or live Timestamp)
// can optionally be compared separately or included depending on comparison needs.
func CanonicalEffectsJSON(effects []Effect) []byte {
	if len(effects) == 0 {
		return []byte("[]")
	}
	norm := make([]Effect, len(effects))
	for i, e := range effects {
		norm[i] = e
		if len(e.Data) > 0 {
			keys := make([]string, 0, len(e.Data))
			for k := range e.Data {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			ordered := make(map[string]any, len(e.Data))
			for _, k := range keys {
				ordered[k] = e.Data[k]
			}
			norm[i].Data = ordered
		}
	}
	b, _ := json.Marshal(norm)
	return b
}
