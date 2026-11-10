package eventstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Repository is the PostgreSQL-backed event store.
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository creates a repository on top of an existing pool.
func NewRepository(pool *pgxpool.Pool) *Repository { return &Repository{pool: pool} }

// Ping verifies database connectivity.
func (r *Repository) Ping(ctx context.Context) error { return r.pool.Ping(ctx) }

const eventColumns = `event_id, transaction_id, event_type, payload, sequence_number,
	created_at, received_at, payload_hash, metadata, schema_version`

func scanEvent(row pgx.Row) (Event, error) {
	var e Event
	var payload, metadata []byte
	err := row.Scan(&e.EventID, &e.TransactionID, &e.EventType, &payload, &e.SequenceNumber,
		&e.CreatedAt, &e.ReceivedAt, &e.PayloadHash, &metadata, &e.SchemaVersion)
	if err != nil {
		return Event{}, err
	}
	e.Payload, e.Metadata = payload, metadata
	return e, nil
}

// Insert stores an event atomically and idempotently.
//
// It returns (event, true, nil) when a new event was stored and
// (existing, false, nil) when the same event_id was already stored with
// identical content. Conflicting re-use of an event_id or of a
// (transaction_id, sequence_number) pair yields ErrPayloadConflict or
// ErrSequenceConflict. Idempotency relies on INSERT ... ON CONFLICT rather
// than a read-then-write check, so concurrent duplicates are safe.
func (r *Repository) Insert(ctx context.Context, ev Event) (Event, bool, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Event{}, false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`INSERT INTO transactions (transaction_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		ev.TransactionID); err != nil {
		return Event{}, false, fmt.Errorf("ensure transaction: %w", err)
	}

	stored, err := scanEvent(tx.QueryRow(ctx,
		`INSERT INTO events (event_id, transaction_id, event_type, payload, sequence_number,
		                     created_at, payload_hash, metadata, schema_version)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 ON CONFLICT DO NOTHING
		 RETURNING `+eventColumns,
		ev.EventID, ev.TransactionID, ev.EventType, []byte(ev.Payload), ev.SequenceNumber,
		ev.CreatedAt, ev.PayloadHash, []byte(ev.Metadata), ev.SchemaVersion))

	switch {
	case err == nil:
		if _, err := tx.Exec(ctx,
			`INSERT INTO event_processing (event_id) VALUES ($1)`, stored.EventID); err != nil {
			return Event{}, false, fmt.Errorf("init processing state: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE transactions
			    SET last_sequence = GREATEST(last_sequence, $2), updated_at = now()
			  WHERE transaction_id = $1`,
			stored.TransactionID, stored.SequenceNumber); err != nil {
			return Event{}, false, fmt.Errorf("update transaction: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return Event{}, false, fmt.Errorf("commit: %w", err)
		}
		return stored, true, nil

	case errors.Is(err, pgx.ErrNoRows):
		// A unique constraint was hit. Work out which one.
		existing, err := scanEvent(tx.QueryRow(ctx,
			`SELECT `+eventColumns+` FROM events WHERE event_id = $1`, ev.EventID))
		if errors.Is(err, pgx.ErrNoRows) {
			return Event{}, false, ErrSequenceConflict
		}
		if err != nil {
			return Event{}, false, fmt.Errorf("load existing: %w", err)
		}
		if existing.PayloadHash != ev.PayloadHash {
			return Event{}, false, ErrPayloadConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return Event{}, false, fmt.Errorf("commit: %w", err)
		}
		return existing, false, nil

	default:
		return Event{}, false, fmt.Errorf("insert event: %w", err)
	}
}

// GetEvent returns a single event by ID.
func (r *Repository) GetEvent(ctx context.Context, id string) (Event, error) {
	e, err := scanEvent(r.pool.QueryRow(ctx,
		`SELECT `+eventColumns+` FROM events WHERE event_id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Event{}, ErrNotFound
	}
	if err != nil {
		return Event{}, fmt.Errorf("get event: %w", err)
	}
	return e, nil
}

// GetTransaction returns a transaction by ID.
func (r *Repository) GetTransaction(ctx context.Context, id string) (Transaction, error) {
	var t Transaction
	err := r.pool.QueryRow(ctx,
		`SELECT transaction_id, status, last_sequence, created_at, updated_at
		   FROM transactions WHERE transaction_id = $1`, id).
		Scan(&t.TransactionID, &t.Status, &t.LastSequence, &t.CreatedAt, &t.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Transaction{}, ErrNotFound
	}
	if err != nil {
		return Transaction{}, fmt.Errorf("get transaction: %w", err)
	}
	return t, nil
}

// ListByTransaction returns events with sequence_number > afterSequence in
// ascending sequence order (deterministic).
func (r *Repository) ListByTransaction(ctx context.Context, transactionID string, afterSequence int64, limit int) ([]Event, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT `+eventColumns+` FROM events
		  WHERE transaction_id = $1 AND sequence_number > $2
		  ORDER BY sequence_number ASC
		  LIMIT $3`, transactionID, afterSequence, limit)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()

	events := []Event{}
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	return events, nil
}
