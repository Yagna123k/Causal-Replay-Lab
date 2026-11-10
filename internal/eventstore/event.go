// Package eventstore persists immutable events in PostgreSQL.
package eventstore

import (
	"encoding/json"
	"errors"
	"time"
)

// Event is an immutable record of something that happened to a transaction.
type Event struct {
	EventID        string          `json:"event_id"`
	TransactionID  string          `json:"transaction_id"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	SequenceNumber int64           `json:"sequence_number"`
	CreatedAt      time.Time       `json:"created_at"`  // producer time
	ReceivedAt     time.Time       `json:"received_at"` // server time
	PayloadHash    string          `json:"payload_hash"`
	Metadata       json.RawMessage `json:"metadata"`
	SchemaVersion  int             `json:"schema_version"`
}

// Transaction is the aggregate that events belong to.
type Transaction struct {
	TransactionID string    `json:"transaction_id"`
	Status        string    `json:"status"`
	LastSequence  int64     `json:"last_sequence"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

var (
	// ErrNotFound is returned when an event or transaction does not exist.
	ErrNotFound = errors.New("not found")
	// ErrPayloadConflict means the event_id exists with different content.
	ErrPayloadConflict = errors.New("event_id already exists with a different payload")
	// ErrSequenceConflict means (transaction_id, sequence_number) is taken by another event.
	ErrSequenceConflict = errors.New("sequence_number already used by a different event in this transaction")
)
