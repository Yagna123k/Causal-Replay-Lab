// Package ingestion validates incoming events and hands them to the event store.
package ingestion

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/Yagna123k/causal-replay-lab/internal/eventstore"
)

const (
	maxIDLength        = 128
	maxEventTypeLength = 64
	// CurrentSchemaVersion is the event schema version assigned at ingestion.
	CurrentSchemaVersion = 1
)

var eventTypePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

// Request is the wire format of an ingested event.
type Request struct {
	EventID        string          `json:"event_id"`
	TransactionID  string          `json:"transaction_id"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	SequenceNumber int64           `json:"sequence_number"`
	CreatedAt      *time.Time      `json:"created_at"`
	Metadata       json.RawMessage `json:"metadata,omitempty"`
}

// FieldError describes one validation failure.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidationError aggregates field-level validation failures.
type ValidationError struct{ Fields []FieldError }

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation failed: %d invalid field(s)", len(e.Fields))
}

// Store is the persistence dependency of the service.
type Store interface {
	Insert(ctx context.Context, ev eventstore.Event) (eventstore.Event, bool, error)
}

// Result is the outcome of an ingestion call.
type Result struct {
	Event     eventstore.Event
	Duplicate bool // true when an identical event had already been stored
}

// Service validates and persists events.
type Service struct{ store Store }

// NewService creates an ingestion service.
func NewService(store Store) *Service { return &Service{store: store} }

// Ingest validates the request and stores the event idempotently.
func (s *Service) Ingest(ctx context.Context, req Request) (Result, error) {
	ev, err := Normalize(req)
	if err != nil {
		return Result{}, err
	}
	stored, created, err := s.store.Insert(ctx, ev)
	if err != nil {
		return Result{}, err
	}
	return Result{Event: stored, Duplicate: !created}, nil
}

// Normalize validates a request and converts it into an Event with a canonical
// payload and a content hash. It returns *ValidationError on invalid input.
func Normalize(req Request) (eventstore.Event, error) {
	var fields []FieldError
	add := func(field, msg string) { fields = append(fields, FieldError{field, msg}) }

	checkID := func(field, v string) {
		switch {
		case v == "":
			add(field, "is required")
		case len(v) > maxIDLength:
			add(field, fmt.Sprintf("must be at most %d characters", maxIDLength))
		}
	}
	checkID("event_id", req.EventID)
	checkID("transaction_id", req.TransactionID)

	switch {
	case req.EventType == "":
		add("event_type", "is required")
	case len(req.EventType) > maxEventTypeLength:
		add("event_type", fmt.Sprintf("must be at most %d characters", maxEventTypeLength))
	case !eventTypePattern.MatchString(req.EventType):
		add("event_type", "must match ^[A-Z][A-Z0-9_]*$ (e.g. LEDGER_UPDATED)")
	}

	if req.SequenceNumber < 1 {
		add("sequence_number", "is required and must be >= 1")
	}
	if req.CreatedAt == nil || req.CreatedAt.IsZero() {
		add("created_at", "is required (RFC 3339 timestamp)")
	}

	payload, err := canonicalObject(req.Payload, true)
	if err != nil {
		add("payload", err.Error())
	}
	metadata := json.RawMessage(`{}`)
	if len(bytes.TrimSpace(req.Metadata)) > 0 && !bytes.Equal(bytes.TrimSpace(req.Metadata), []byte("null")) {
		metadata, err = canonicalObject(req.Metadata, false)
		if err != nil {
			add("metadata", err.Error())
		}
	}

	if len(fields) > 0 {
		return eventstore.Event{}, &ValidationError{Fields: fields}
	}

	return eventstore.Event{
		EventID:        req.EventID,
		TransactionID:  req.TransactionID,
		EventType:      req.EventType,
		Payload:        payload,
		SequenceNumber: req.SequenceNumber,
		CreatedAt:      req.CreatedAt.UTC(),
		PayloadHash:    Hash(req.TransactionID, req.EventType, req.SequenceNumber, payload),
		Metadata:       metadata,
		SchemaVersion:  CurrentSchemaVersion,
	}, nil
}

// canonicalObject checks that raw is a JSON object and re-encodes it with sorted
// keys and exact number representation so equal content yields equal bytes.
func canonicalObject(raw json.RawMessage, required bool) (json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		if required {
			return nil, errors.New("is required")
		}
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, errors.New("must be valid JSON")
	}
	if dec.More() {
		return nil, errors.New("must be a single JSON value")
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return nil, errors.New("must be a JSON object")
	}
	out, err := json.Marshal(obj) // map keys are sorted by encoding/json
	if err != nil {
		return nil, errors.New("must be valid JSON")
	}
	return out, nil
}

// Hash returns a stable SHA-256 digest of an event's identifying content.
// The event_id, producer timestamp and metadata are intentionally excluded:
// a producer retrying the same logical event must hash identically.
func Hash(transactionID, eventType string, sequence int64, canonicalPayload json.RawMessage) string {
	b, _ := json.Marshal(struct {
		TransactionID string          `json:"transaction_id"`
		EventType     string          `json:"event_type"`
		Sequence      int64           `json:"sequence_number"`
		Payload       json.RawMessage `json:"payload"`
	}{transactionID, eventType, sequence, canonicalPayload})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
