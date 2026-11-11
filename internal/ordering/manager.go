// Package ordering detects out-of-order events, identifies missing sequence
// numbers, and enforces configurable ordering policies.
package ordering

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Yagna123k/causal-replay-lab/internal/eventstore"
	"github.com/Yagna123k/causal-replay-lab/internal/execution"
)

// Policy determines how an out-of-order event (sequence > processed_sequence + 1) is handled.
type Policy string

const (
	// PolicyBuffer stores out-of-order events in pending_events until prerequisites arrive.
	PolicyBuffer Policy = "BUFFER"
	// PolicyReject returns a retryable ErrOutOfOrder until missing prerequisites are processed.
	PolicyReject Policy = "REJECT"
	// PolicyFailAndFlag flags the transaction as having a sequence gap and marks the event FAILED.
	PolicyFailAndFlag Policy = "FAIL_AND_FLAG"
)

// ParsePolicy validates and normalizes a policy string.
func ParsePolicy(s string) (Policy, error) {
	switch Policy(strings.ToUpper(strings.TrimSpace(s))) {
	case "", PolicyBuffer:
		return PolicyBuffer, nil
	case PolicyReject:
		return PolicyReject, nil
	case PolicyFailAndFlag:
		return PolicyFailAndFlag, nil
	default:
		return "", fmt.Errorf("invalid ordering policy %q (must be BUFFER, REJECT, or FAIL_AND_FLAG)", s)
	}
}

// Decision is the ordering verdict for a single event.
type Decision string

const (
	DecisionInOrder          Decision = "IN_ORDER"
	DecisionAlreadyProcessed Decision = "ALREADY_PROCESSED"
	DecisionBuffer           Decision = "BUFFER"
	DecisionReject           Decision = "REJECT"
	DecisionFailAndFlag      Decision = "FAIL_AND_FLAG"
)

var (
	// ErrOutOfOrder is returned under PolicyReject when prerequisites have not yet been processed.
	// Callers/producers may safely retry once preceding sequence numbers arrive.
	ErrOutOfOrder = errors.New("out-of-order event: missing prerequisite sequence")
	// ErrSequenceGapFlagged is returned under PolicyFailAndFlag when a gap is detected and flagged.
	ErrSequenceGapFlagged = errors.New("sequence gap detected and flagged on transaction")
)

// Evaluate decides how to handle an event with sequence `eventSeq` given the
// transaction's last contiguously processed sequence `processedSeq`.
func Evaluate(processedSeq, eventSeq int64, policy Policy) (Decision, error) {
	if eventSeq < 1 {
		return "", fmt.Errorf("invalid event sequence %d", eventSeq)
	}
	expected := processedSeq + 1
	switch {
	case eventSeq <= processedSeq:
		return DecisionAlreadyProcessed, nil
	case eventSeq == expected:
		return DecisionInOrder, nil
	default:
		switch policy {
		case PolicyBuffer:
			return DecisionBuffer, nil
		case PolicyReject:
			return DecisionReject, fmt.Errorf("%w: expected %d, got %d", ErrOutOfOrder, expected, eventSeq)
		case PolicyFailAndFlag:
			return DecisionFailAndFlag, fmt.Errorf("%w: expected %d, got %d", ErrSequenceGapFlagged, expected, eventSeq)
		default:
			return "", fmt.Errorf("unsupported ordering policy %q", policy)
		}
	}
}

// FindMissingSequences returns all missing sequence numbers strictly greater than
// processedSeq and up to the maximum sequence in knownSeqs, sorted ascending.
func FindMissingSequences(processedSeq int64, knownSeqs []int64) []int64 {
	if len(knownSeqs) == 0 {
		return nil
	}
	present := make(map[int64]bool, len(knownSeqs))
	var maxSeq int64
	for _, s := range knownSeqs {
		if s > processedSeq {
			present[s] = true
			if s > maxSeq {
				maxSeq = s
			}
		}
	}
	if maxSeq <= processedSeq+1 {
		// If maxSeq == processedSeq+1 and it's present, nothing is missing.
		return nil
	}
	var missing []int64
	for seq := processedSeq + 1; seq <= maxSeq; seq++ {
		if !present[seq] {
			missing = append(missing, seq)
		}
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
	return missing
}

// PendingEvent describes an event buffered in pending_events.
type PendingEvent struct {
	EventID        string    `json:"event_id"`
	TransactionID  string    `json:"transaction_id"`
	SequenceNumber int64     `json:"sequence_number"`
	Status         string    `json:"status"`
	CreatedAt      time.Time `json:"created_at"`
	ExpiresAt      time.Time `json:"expires_at"`
}

// EventState summarizes an event's processing status.
type EventState struct {
	EventID        string    `json:"event_id"`
	EventType      string    `json:"event_type"`
	SequenceNumber int64     `json:"sequence_number"`
	Status         string    `json:"status"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	Attempts       int       `json:"attempts"`
	LastError      string    `json:"last_error,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// TransactionStatus provides consistency and gap visibility for a transaction.
type TransactionStatus struct {
	TransactionID     string                      `json:"transaction_id"`
	Status            string                      `json:"status"`
	ProcessedSequence int64                       `json:"processed_sequence"`
	LastSequence      int64                       `json:"last_sequence"`
	HasSequenceGap    bool                        `json:"has_sequence_gap"`
	GapDetails        string                      `json:"gap_details,omitempty"`
	CurrentState      *execution.TransactionState `json:"current_state,omitempty"`
	CurrentStateHash  string                      `json:"current_state_hash,omitempty"`
	MissingSequences  []int64                     `json:"missing_sequences"`
	PendingEvents     []PendingEvent              `json:"pending_events"`
	Events            []EventState                `json:"events"`
}

// Manager inspects transaction sequence consistency and manages pending buffers.
type Manager struct {
	pool *pgxpool.Pool
}

// NewManager creates an ordering manager backed by pool.
func NewManager(pool *pgxpool.Pool) *Manager {
	return &Manager{pool: pool}
}

// InspectTransaction returns a transaction's processed progress, detected sequence gaps,
// buffered pending events, and per-event processing statuses.
func (m *Manager) InspectTransaction(ctx context.Context, transactionID string) (TransactionStatus, error) {
	var st TransactionStatus
	var stateRaw []byte
	var stateHash, gapDetails *string

	err := m.pool.QueryRow(ctx,
		`SELECT transaction_id, status, processed_sequence, last_sequence,
		        has_sequence_gap, gap_details, current_state, current_state_hash
		   FROM transactions
		  WHERE transaction_id = $1`, transactionID).
		Scan(&st.TransactionID, &st.Status, &st.ProcessedSequence, &st.LastSequence,
			&st.HasSequenceGap, &gapDetails, &stateRaw, &stateHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return TransactionStatus{}, eventstore.ErrNotFound
	}
	if err != nil {
		return TransactionStatus{}, fmt.Errorf("load transaction status: %w", err)
	}
	if gapDetails != nil {
		st.GapDetails = *gapDetails
	}
	if stateHash != nil {
		st.CurrentStateHash = *stateHash
	}
	if len(stateRaw) > 0 {
		var ts execution.TransactionState
		if err := json.Unmarshal(stateRaw, &ts); err != nil {
			return TransactionStatus{}, fmt.Errorf("unmarshal current_state: %w", err)
		}
		st.CurrentState = &ts
	}

	rows, err := m.pool.Query(ctx,
		`SELECT e.event_id, e.event_type, e.sequence_number, ep.status,
		        ep.idempotency_key, ep.attempts, ep.last_error, ep.updated_at
		   FROM events e
		   JOIN event_processing ep ON ep.event_id = e.event_id
		  WHERE e.transaction_id = $1
		  ORDER BY e.sequence_number ASC`, transactionID)
	if err != nil {
		return TransactionStatus{}, fmt.Errorf("list event states: %w", err)
	}
	defer rows.Close()

	var knownSeqs []int64
	var activeSeqs []int64
	st.Events = []EventState{}
	for rows.Next() {
		var es EventState
		var idemKey, lastErr *string
		if err := rows.Scan(&es.EventID, &es.EventType, &es.SequenceNumber, &es.Status,
			&idemKey, &es.Attempts, &lastErr, &es.UpdatedAt); err != nil {
			return TransactionStatus{}, fmt.Errorf("scan event state: %w", err)
		}
		if idemKey != nil {
			es.IdempotencyKey = *idemKey
		}
		if lastErr != nil {
			es.LastError = *lastErr
		}
		st.Events = append(st.Events, es)
		knownSeqs = append(knownSeqs, es.SequenceNumber)
		if es.Status == "PROCESSED" || es.Status == "PROCESSING" || es.Status == "PENDING_PREREQUISITE" {
			activeSeqs = append(activeSeqs, es.SequenceNumber)
		}
	}
	if err := rows.Err(); err != nil {
		return TransactionStatus{}, fmt.Errorf("iterate event states: %w", err)
	}

	pRows, err := m.pool.Query(ctx,
		`SELECT event_id, transaction_id, sequence_number, status, created_at, expires_at
		   FROM pending_events
		  WHERE transaction_id = $1 AND status = 'AWAITING'
		  ORDER BY sequence_number ASC`, transactionID)
	if err != nil {
		return TransactionStatus{}, fmt.Errorf("list pending events: %w", err)
	}
	defer pRows.Close()

	st.PendingEvents = []PendingEvent{}
	for pRows.Next() {
		var pe PendingEvent
		if err := pRows.Scan(&pe.EventID, &pe.TransactionID, &pe.SequenceNumber, &pe.Status, &pe.CreatedAt, &pe.ExpiresAt); err != nil {
			return TransactionStatus{}, fmt.Errorf("scan pending event: %w", err)
		}
		st.PendingEvents = append(st.PendingEvents, pe)
	}
	if err := pRows.Err(); err != nil {
		return TransactionStatus{}, fmt.Errorf("iterate pending events: %w", err)
	}

	missingSet := map[int64]bool{}
	for _, s := range FindMissingSequences(st.ProcessedSequence, knownSeqs) {
		missingSet[s] = true
	}
	if len(st.PendingEvents) > 0 {
		for _, s := range FindMissingSequences(st.ProcessedSequence, activeSeqs) {
			missingSet[s] = true
		}
	}
	st.MissingSequences = make([]int64, 0, len(missingSet))
	for s := range missingSet {
		st.MissingSequences = append(st.MissingSequences, s)
	}
	sort.Slice(st.MissingSequences, func(i, j int) bool { return st.MissingSequences[i] < st.MissingSequences[j] })
	if len(st.MissingSequences) > 0 {
		st.HasSequenceGap = true
	}
	return st, nil
}

// ExpirePending transitions buffered events whose gap timeout elapsed into EXPIRED
// and moves their event_processing status from PENDING_PREREQUISITE to DEAD.
func (m *Manager) ExpirePending(ctx context.Context, now time.Time) (int, error) {
	tx, err := m.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("begin expire pending: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx,
		`UPDATE pending_events
		    SET status = 'EXPIRED'
		  WHERE status = 'AWAITING' AND expires_at <= $1
		  RETURNING event_id`, now.UTC())
	if err != nil {
		return 0, fmt.Errorf("expire pending_events: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	if len(ids) > 0 {
		if _, err := tx.Exec(ctx,
			`UPDATE event_processing
			    SET status = 'DEAD',
			        last_error = 'prerequisite sequence timeout expired',
			        updated_at = $2
			  WHERE event_id = ANY($1) AND status = 'PENDING_PREREQUISITE'`,
			ids, now.UTC()); err != nil {
			return 0, fmt.Errorf("mark expired events dead: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit expire pending: %w", err)
	}
	return len(ids), nil
}
