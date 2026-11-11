// Package processing coordinates reliable event processing with atomic
// idempotency enforcement, per-transaction serialization, and sequence ordering.
//
// Transaction Isolation & Concurrency Strategy:
// All state transitions run under PostgreSQL READ COMMITTED isolation combined
// with an explicit row-level lock (`SELECT ... FROM transactions WHERE
// transaction_id = $1 FOR UPDATE`) and unique constraints (`INSERT ... ON
// CONFLICT DO NOTHING` on idempotency_records and committed_effects). This
// serializes concurrent workers touching the same transaction without
// serialization-retry storms while allowing independent transactions to execute
// concurrently.
package processing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Yagna123k/causal-replay-lab/internal/eventstore"
	"github.com/Yagna123k/causal-replay-lab/internal/execution"
	"github.com/Yagna123k/causal-replay-lab/internal/idempotency"
	"github.com/Yagna123k/causal-replay-lab/internal/ordering"
	"github.com/Yagna123k/causal-replay-lab/internal/replay"
)

// Options configures the event processor.
type Options struct {
	DefaultPolicy  ordering.Policy
	IdempotencyTTL time.Duration
	PendingGapTTL  time.Duration
	Clock          replay.Clock
	IDGen          replay.IDGenerator
	LiveSink       replay.EffectSink
	Registry       *replay.Registry
	// BeforeCommit is an optional test hook invoked immediately before committing
	// a processed event's transaction, allowing failure-injection tests to verify atomicity.
	BeforeCommit func(ctx context.Context, ev eventstore.Event) error
}

// ProcessOutcome describes the result of processing a single event.
type ProcessOutcome struct {
	EventID          string                       `json:"event_id"`
	TransactionID    string                       `json:"transaction_id"`
	SequenceNumber   int64                        `json:"sequence_number"`
	IdempotencyKey   string                       `json:"idempotency_key"`
	Status           idempotency.ProcessingStatus `json:"status"`
	Duplicate        bool                         `json:"duplicate"`
	Buffered         bool                         `json:"buffered"`
	StateHash        string                       `json:"state_hash,omitempty"`
	StateAfter       *execution.TransactionState  `json:"state_after,omitempty"`
	Effects          []execution.Effect           `json:"effects,omitempty"`
	MissingSequences []int64                      `json:"missing_sequences,omitempty"`
	Drained          []ProcessOutcome             `json:"drained,omitempty"`
}

// Processor enforces idempotency and ordering guarantees during live event processing.
type Processor struct {
	pool          *pgxpool.Pool
	idem          *idempotency.Manager
	policy        ordering.Policy
	pendingGapTTL time.Duration
	clock         replay.Clock
	idGen         replay.IDGenerator
	liveSink      replay.EffectSink
	registry      *replay.Registry
	beforeCommit  func(ctx context.Context, ev eventstore.Event) error
}

// NewProcessor constructs a Processor backed by pool.
func NewProcessor(pool *pgxpool.Pool, opts Options) *Processor {
	if opts.DefaultPolicy == "" {
		opts.DefaultPolicy = ordering.PolicyBuffer
	}
	if opts.IdempotencyTTL <= 0 {
		opts.IdempotencyTTL = 24 * time.Hour
	}
	if opts.PendingGapTTL <= 0 {
		opts.PendingGapTTL = time.Hour
	}
	if opts.Clock == nil {
		opts.Clock = replay.SystemClock{}
	}
	if opts.IDGen == nil {
		opts.IDGen = replay.RandomIDGenerator{}
	}
	if opts.LiveSink == nil {
		opts.LiveSink = replay.NewLiveEffectSink(nil)
	}
	if opts.Registry == nil {
		opts.Registry = replay.DefaultRegistry()
	}
	return &Processor{
		pool:          pool,
		idem:          idempotency.NewManager(opts.IdempotencyTTL),
		policy:        opts.DefaultPolicy,
		pendingGapTTL: opts.PendingGapTTL,
		clock:         opts.Clock,
		idGen:         opts.IDGen,
		liveSink:      opts.LiveSink,
		registry:      opts.Registry,
		beforeCommit:  opts.BeforeCommit,
	}
}

// ProcessEvent processes a single ingested event with idempotency and ordering guarantees.
// If idempotencyKey is empty, the event_id is used as the processing idempotency key.
// If policyOverride is non-empty, it overrides the processor's default ordering policy.
func (p *Processor) ProcessEvent(ctx context.Context, eventID, idempotencyKey string, policyOverride ordering.Policy) (ProcessOutcome, error) {
	policy := p.policy
	if policyOverride != "" {
		policy = policyOverride
	}
	if idempotencyKey == "" {
		idempotencyKey = eventID
	}

	var txnID string
	err := p.pool.QueryRow(ctx, `SELECT transaction_id FROM events WHERE event_id = $1`, eventID).Scan(&txnID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProcessOutcome{}, eventstore.ErrNotFound
	}
	if err != nil {
		return ProcessOutcome{}, fmt.Errorf("lookup event transaction: %w", err)
	}

	out, err := p.processOne(ctx, txnID, eventID, idempotencyKey, policy)
	if err != nil {
		return out, err
	}

	if policy == ordering.PolicyBuffer && out.Status == idempotency.StatusProcessed && !out.Duplicate {
		drained, derr := p.drainBuffered(ctx, txnID, policy)
		if len(drained) > 0 {
			out.Drained = drained
		}
		if derr != nil {
			return out, derr
		}
	}
	return out, nil
}

// ProcessTransaction processes all unprocessed events for a transaction in ascending sequence order.
func (p *Processor) ProcessTransaction(ctx context.Context, transactionID string, policyOverride ordering.Policy) ([]ProcessOutcome, error) {
	policy := p.policy
	if policyOverride != "" {
		policy = policyOverride
	}

	var exists bool
	err := p.pool.QueryRow(ctx, `SELECT true FROM transactions WHERE transaction_id = $1`, transactionID).Scan(&exists)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, eventstore.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("lookup transaction: %w", err)
	}

	rows, err := p.pool.Query(ctx,
		`SELECT e.event_id, COALESCE(ep.idempotency_key, e.event_id)
		   FROM events e
		   JOIN event_processing ep ON ep.event_id = e.event_id
		  WHERE e.transaction_id = $1 AND ep.status <> 'PROCESSED'
		  ORDER BY e.sequence_number ASC`, transactionID)
	if err != nil {
		return nil, fmt.Errorf("list pending transaction events: %w", err)
	}
	type pair struct{ id, key string }
	var queue []pair
	for rows.Next() {
		var pr pair
		if err := rows.Scan(&pr.id, &pr.key); err != nil {
			rows.Close()
			return nil, err
		}
		queue = append(queue, pr)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var outcomes []ProcessOutcome
	for _, item := range queue {
		out, err := p.processOne(ctx, transactionID, item.id, item.key, policy)
		if err != nil {
			if out.EventID != "" {
				outcomes = append(outcomes, out)
			}
			return outcomes, err
		}
		outcomes = append(outcomes, out)
	}
	return outcomes, nil
}

func (p *Processor) drainBuffered(ctx context.Context, txnID string, policy ordering.Policy) ([]ProcessOutcome, error) {
	var drained []ProcessOutcome
	for {
		var nextEventID, key string
		err := p.pool.QueryRow(ctx,
			`SELECT pe.event_id, COALESCE(ep.idempotency_key, pe.event_id)
			   FROM pending_events pe
			   JOIN transactions t ON t.transaction_id = pe.transaction_id
			   JOIN event_processing ep ON ep.event_id = pe.event_id
			  WHERE pe.transaction_id = $1
			    AND pe.status = 'AWAITING'
			    AND pe.sequence_number = t.processed_sequence + 1
			  LIMIT 1`, txnID).Scan(&nextEventID, &key)
		if errors.Is(err, pgx.ErrNoRows) {
			return drained, nil
		}
		if err != nil {
			return drained, fmt.Errorf("lookup next buffered event: %w", err)
		}

		out, err := p.processOne(ctx, txnID, nextEventID, key, policy)
		if err != nil {
			return drained, err
		}
		drained = append(drained, out)
		if out.Status != idempotency.StatusProcessed {
			return drained, nil
		}
	}
}

func (p *Processor) processOne(ctx context.Context, txnID, eventID, key string, policy ordering.Policy) (ProcessOutcome, error) {
	now := p.clock.Now()

	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ProcessOutcome{}, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Lock the transaction row so concurrent workers for the same transaction serialize cleanly.
	var processedSeq int64
	var stateRaw []byte
	err = tx.QueryRow(ctx,
		`SELECT processed_sequence, current_state
		   FROM transactions
		  WHERE transaction_id = $1
		  FOR UPDATE`, txnID).Scan(&processedSeq, &stateRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProcessOutcome{}, eventstore.ErrNotFound
	}
	if err != nil {
		return ProcessOutcome{}, fmt.Errorf("lock transaction: %w", err)
	}

	var ev eventstore.Event
	var payload, metadata []byte
	var epStatusStr string
	var attempts int
	err = tx.QueryRow(ctx,
		`SELECT e.event_id, e.transaction_id, e.event_type, e.payload, e.sequence_number,
		        e.created_at, e.received_at, e.payload_hash, e.metadata, e.schema_version,
		        ep.status, ep.attempts
		   FROM events e
		   JOIN event_processing ep ON ep.event_id = e.event_id
		  WHERE e.event_id = $1
		  FOR UPDATE OF ep`, eventID).
		Scan(&ev.EventID, &ev.TransactionID, &ev.EventType, &payload, &ev.SequenceNumber,
			&ev.CreatedAt, &ev.ReceivedAt, &ev.PayloadHash, &metadata, &ev.SchemaVersion,
			&epStatusStr, &attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProcessOutcome{}, eventstore.ErrNotFound
	}
	if err != nil {
		return ProcessOutcome{}, fmt.Errorf("load event for processing: %w", err)
	}
	ev.Payload, ev.Metadata = payload, metadata
	epStatus := idempotency.ProcessingStatus(epStatusStr)

	// If already processed, verify idempotency key compatibility and return idempotent result.
	if epStatus == idempotency.StatusProcessed {
		return p.loadCommittedOutcome(ctx, tx, ev, key)
	}

	// Check ordering before claiming idempotency so buffered events don't hold active locks.
	decision, ordErr := ordering.Evaluate(processedSeq, ev.SequenceNumber, policy)
	switch decision {
	case ordering.DecisionAlreadyProcessed:
		return p.loadCommittedOutcome(ctx, tx, ev, key)

	case ordering.DecisionBuffer:
		expiresAt := now.Add(p.pendingGapTTL)
		if _, err := tx.Exec(ctx,
			`INSERT INTO pending_events (event_id, transaction_id, sequence_number, status, expires_at)
			 VALUES ($1, $2, $3, 'AWAITING', $4)
			 ON CONFLICT (transaction_id, sequence_number) DO NOTHING`,
			ev.EventID, ev.TransactionID, ev.SequenceNumber, expiresAt); err != nil {
			return ProcessOutcome{}, fmt.Errorf("buffer pending event: %w", err)
		}
		if epStatus != idempotency.StatusPendingPrerequisite {
			if !idempotency.CanTransition(epStatus, idempotency.StatusPendingPrerequisite) {
				return ProcessOutcome{}, fmt.Errorf("cannot transition event %s from %s to PENDING_PREREQUISITE", ev.EventID, epStatus)
			}
			if _, err := tx.Exec(ctx,
				`UPDATE event_processing
				    SET status = 'PENDING_PREREQUISITE',
				        idempotency_key = $2,
				        expires_at = $3,
				        updated_at = $4
				  WHERE event_id = $1`,
				ev.EventID, key, expiresAt, now); err != nil {
				return ProcessOutcome{}, fmt.Errorf("mark pending prerequisite: %w", err)
			}
		}
		missing, err := p.missingSequencesTx(ctx, tx, txnID, processedSeq)
		if err != nil {
			return ProcessOutcome{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return ProcessOutcome{}, fmt.Errorf("commit buffered event: %w", err)
		}
		return ProcessOutcome{
			EventID:          ev.EventID,
			TransactionID:    ev.TransactionID,
			SequenceNumber:   ev.SequenceNumber,
			IdempotencyKey:   key,
			Status:           idempotency.StatusPendingPrerequisite,
			Buffered:         true,
			MissingSequences: missing,
		}, nil

	case ordering.DecisionReject:
		return ProcessOutcome{}, ordErr

	case ordering.DecisionFailAndFlag:
		gapMsg := ordErr.Error()
		if _, err := tx.Exec(ctx,
			`UPDATE transactions
			    SET has_sequence_gap = true, gap_details = $2, updated_at = $3
			  WHERE transaction_id = $1`,
			txnID, gapMsg, now); err != nil {
			return ProcessOutcome{}, fmt.Errorf("flag transaction gap: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE event_processing
			    SET status = 'FAILED', last_error = $2, idempotency_key = $3, updated_at = $4
			  WHERE event_id = $1`,
			ev.EventID, gapMsg, key, now); err != nil {
			return ProcessOutcome{}, fmt.Errorf("mark gap failure: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return ProcessOutcome{}, fmt.Errorf("commit gap flag: %w", err)
		}
		return ProcessOutcome{
			EventID:        ev.EventID,
			TransactionID:  ev.TransactionID,
			SequenceNumber: ev.SequenceNumber,
			IdempotencyKey: key,
			Status:         idempotency.StatusFailed,
		}, ordErr
	}

	// DecisionInOrder: claim the idempotency key atomically inside this DB transaction.
	claim, err := p.idem.Claim(ctx, tx, key, ev.EventID, ev.PayloadHash, now)
	if err != nil {
		return ProcessOutcome{}, err
	}
	if claim.Duplicate {
		var cached ProcessOutcome
		if len(claim.Cached) > 0 && json.Unmarshal(claim.Cached, &cached) == nil {
			cached.Duplicate = true
			return cached, nil
		}
		return ProcessOutcome{
			EventID:        ev.EventID,
			TransactionID:  ev.TransactionID,
			SequenceNumber: ev.SequenceNumber,
			IdempotencyKey: key,
			Status:         idempotency.StatusProcessed,
			Duplicate:      true,
		}, nil
	}

	if !idempotency.CanTransition(epStatus, idempotency.StatusProcessing) {
		return ProcessOutcome{}, fmt.Errorf("cannot transition event %s from %s to PROCESSING", ev.EventID, epStatus)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE event_processing
		    SET status = 'PROCESSING',
		        idempotency_key = $2,
		        attempts = attempts + 1,
		        updated_at = $3
		  WHERE event_id = $1`,
		ev.EventID, key, now); err != nil {
		return ProcessOutcome{}, fmt.Errorf("mark event processing: %w", err)
	}

	state := execution.NewInitialState(txnID)
	if len(stateRaw) > 0 {
		if err := json.Unmarshal(stateRaw, &state); err != nil {
			return ProcessOutcome{}, fmt.Errorf("unmarshal transaction state: %w", err)
		}
	}

	handler, ok := p.registry.Lookup(ev.EventType)
	if !ok {
		herr := fmt.Errorf("%w: %s", execution.ErrHandlerNotFound, ev.EventType)
		_ = p.recordHandlerFailure(ctx, tx, ev.EventID, key, herr.Error(), now)
		_ = tx.Commit(ctx)
		return ProcessOutcome{
			EventID:        ev.EventID,
			TransactionID:  ev.TransactionID,
			SequenceNumber: ev.SequenceNumber,
			IdempotencyKey: key,
			Status:         idempotency.StatusFailed,
		}, herr
	}

	deps := replay.ControlledDeps{Clock: p.clock, IDGen: p.idGen}
	nextState, effects, applyErr := handler.Apply(deps, state, ev)
	if applyErr != nil {
		_ = p.recordHandlerFailure(ctx, tx, ev.EventID, key, applyErr.Error(), now)
		_ = tx.Commit(ctx)
		return ProcessOutcome{
			EventID:        ev.EventID,
			TransactionID:  ev.TransactionID,
			SequenceNumber: ev.SequenceNumber,
			IdempotencyKey: key,
			Status:         idempotency.StatusFailed,
		}, applyErr
	}

	nextStateJSON, err := nextState.CanonicalJSON()
	if err != nil {
		return ProcessOutcome{}, fmt.Errorf("marshal next state: %w", err)
	}
	nextStateHash := nextState.Hash()
	effectsJSON := execution.CanonicalEffectsJSON(effects)

	var insertedID string
	err = tx.QueryRow(ctx,
		`INSERT INTO committed_effects
		 (event_id, idempotency_key, transaction_id, sequence_number, state_after, state_hash, effects, committed_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT DO NOTHING
		 RETURNING event_id`,
		ev.EventID, key, ev.TransactionID, ev.SequenceNumber, nextStateJSON, nextStateHash, effectsJSON, now).
		Scan(&insertedID)
	if errors.Is(err, pgx.ErrNoRows) {
		return p.loadCommittedOutcome(ctx, tx, ev, key)
	}
	if err != nil {
		return ProcessOutcome{}, fmt.Errorf("insert committed effect: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`UPDATE transactions
		    SET processed_sequence = $2,
		        current_state = $3,
		        current_state_hash = $4,
		        has_sequence_gap = false,
		        gap_details = NULL,
		        updated_at = $5
		  WHERE transaction_id = $1`,
		txnID, ev.SequenceNumber, nextStateJSON, nextStateHash, now); err != nil {
		return ProcessOutcome{}, fmt.Errorf("update transaction state: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`UPDATE event_processing
		    SET status = 'PROCESSED', last_error = NULL, updated_at = $2
		  WHERE event_id = $1`,
		ev.EventID, now); err != nil {
		return ProcessOutcome{}, fmt.Errorf("mark event processed: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`UPDATE pending_events SET status = 'RELEASED' WHERE event_id = $1 AND status = 'AWAITING'`,
		ev.EventID); err != nil {
		return ProcessOutcome{}, fmt.Errorf("release pending event: %w", err)
	}

	outcome := ProcessOutcome{
		EventID:        ev.EventID,
		TransactionID:  ev.TransactionID,
		SequenceNumber: ev.SequenceNumber,
		IdempotencyKey: key,
		Status:         idempotency.StatusProcessed,
		StateHash:      nextStateHash,
		StateAfter:     &nextState,
		Effects:        effects,
	}
	outcomeJSON, _ := json.Marshal(outcome)
	if err := p.idem.MarkCommitted(ctx, tx, key, outcomeJSON); err != nil {
		return ProcessOutcome{}, err
	}

	if p.beforeCommit != nil {
		if err := p.beforeCommit(ctx, ev); err != nil {
			return ProcessOutcome{}, fmt.Errorf("injected pre-commit failure: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return ProcessOutcome{}, fmt.Errorf("commit processing tx: %w", err)
	}

	// Emit external effects only after durable DB commit succeeds.
	for _, eff := range effects {
		_ = p.liveSink.Emit(ctx, eff)
	}
	return outcome, nil
}

func (p *Processor) recordHandlerFailure(ctx context.Context, tx pgx.Tx, eventID, key, errMsg string, now time.Time) error {
	if err := p.idem.MarkFailed(ctx, tx, key, errMsg); err != nil {
		return err
	}
	_, err := tx.Exec(ctx,
		`UPDATE event_processing
		    SET status = 'FAILED', last_error = $2, updated_at = $3
		  WHERE event_id = $1`,
		eventID, errMsg, now)
	return err
}

func (p *Processor) loadCommittedOutcome(ctx context.Context, tx pgx.Tx, ev eventstore.Event, key string) (ProcessOutcome, error) {
	// Ensure a different payload isn't reusing an existing idempotency key.
	var existingHash string
	err := tx.QueryRow(ctx,
		`SELECT request_hash FROM idempotency_records WHERE idempotency_key = $1`, key).
		Scan(&existingHash)
	if err == nil && existingHash != ev.PayloadHash {
		return ProcessOutcome{}, idempotency.ErrKeyConflict
	}

	var stateRaw, effectsRaw []byte
	var stateHash string
	err = tx.QueryRow(ctx,
		`SELECT state_after, state_hash, effects
		   FROM committed_effects
		  WHERE event_id = $1`, ev.EventID).
		Scan(&stateRaw, &stateHash, &effectsRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProcessOutcome{
			EventID:        ev.EventID,
			TransactionID:  ev.TransactionID,
			SequenceNumber: ev.SequenceNumber,
			IdempotencyKey: key,
			Status:         idempotency.StatusProcessed,
			Duplicate:      true,
		}, nil
	}
	if err != nil {
		return ProcessOutcome{}, fmt.Errorf("load committed effect: %w", err)
	}

	var state execution.TransactionState
	_ = json.Unmarshal(stateRaw, &state)
	var effects []execution.Effect
	_ = json.Unmarshal(effectsRaw, &effects)

	return ProcessOutcome{
		EventID:        ev.EventID,
		TransactionID:  ev.TransactionID,
		SequenceNumber: ev.SequenceNumber,
		IdempotencyKey: key,
		Status:         idempotency.StatusProcessed,
		Duplicate:      true,
		StateHash:      stateHash,
		StateAfter:     &state,
		Effects:        effects,
	}, nil
}

func (p *Processor) missingSequencesTx(ctx context.Context, tx pgx.Tx, txnID string, processedSeq int64) ([]int64, error) {
	rows, err := tx.Query(ctx,
		`SELECT e.sequence_number
		   FROM events e
		   JOIN event_processing ep ON ep.event_id = e.event_id
		  WHERE e.transaction_id = $1
		    AND e.sequence_number > $2
		    AND ep.status IN ('PROCESSED', 'PROCESSING', 'PENDING_PREREQUISITE')`,
		txnID, processedSeq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var seqs []int64
	for rows.Next() {
		var s int64
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		seqs = append(seqs, s)
	}
	return ordering.FindMissingSequences(processedSeq, seqs), rows.Err()
}
