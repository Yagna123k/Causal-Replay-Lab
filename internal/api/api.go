// Package api exposes the REST interface.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/Yagna123k/causal-replay-lab/internal/eventstore"
	"github.com/Yagna123k/causal-replay-lab/internal/execution"
	"github.com/Yagna123k/causal-replay-lab/internal/idempotency"
	"github.com/Yagna123k/causal-replay-lab/internal/ingestion"
	"github.com/Yagna123k/causal-replay-lab/internal/ordering"
	"github.com/Yagna123k/causal-replay-lab/internal/processing"
	"github.com/Yagna123k/causal-replay-lab/internal/replay"
)

// Ingester ingests events.
type Ingester interface {
	Ingest(ctx context.Context, req ingestion.Request) (ingestion.Result, error)
}

// Reader reads stored events and transactions.
type Reader interface {
	GetEvent(ctx context.Context, id string) (eventstore.Event, error)
	GetTransaction(ctx context.Context, id string) (eventstore.Transaction, error)
	ListByTransaction(ctx context.Context, transactionID string, afterSequence int64, limit int) ([]eventstore.Event, error)
}

// Replayer manages deterministic replay sessions and comparisons.
type Replayer interface {
	CreateSession(ctx context.Context, req replay.CreateRequest) (execution.Session, error)
	StartSession(ctx context.Context, sessionID string) (replay.StartResult, error)
	GetSession(ctx context.Context, sessionID string) (execution.Session, error)
	ListExecutions(ctx context.Context, sessionID string) ([]execution.EventExecution, error)
	Compare(ctx context.Context, leftID, rightID string) (execution.ComparisonResult, error)
}

// OriginalProcessor executes a transaction's events with live dependencies to
// record the original baseline session.
type OriginalProcessor interface {
	ProcessOriginal(ctx context.Context, transactionID string) (execution.Session, error)
}

// EventProcessor executes ingested events with idempotency and ordering guarantees.
type EventProcessor interface {
	ProcessEvent(ctx context.Context, eventID, idempotencyKey string, policyOverride ordering.Policy) (processing.ProcessOutcome, error)
	ProcessTransaction(ctx context.Context, transactionID string, policyOverride ordering.Policy) ([]processing.ProcessOutcome, error)
}

// OrderingInspector inspects transaction consistency, sequence gaps, and pending events.
type OrderingInspector interface {
	InspectTransaction(ctx context.Context, transactionID string) (ordering.TransactionStatus, error)
}

// Pinger checks dependency health.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Deps are the collaborators of the HTTP layer.
type Deps struct {
	Ingester       Ingester
	Reader         Reader
	Replayer       Replayer
	Original       OriginalProcessor
	Processor      EventProcessor
	Inspector      OrderingInspector
	Pinger         Pinger
	Logger         *slog.Logger
	RequestTimeout time.Duration
	MaxBodyBytes   int64
}

const (
	defaultListLimit = 1000
	maxListLimit     = 1000
)

type server struct{ Deps }

// NewHandler builds the fully wrapped HTTP handler.
func NewHandler(d Deps) http.Handler {
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	if d.MaxBodyBytes <= 0 {
		d.MaxBodyBytes = 1 << 20
	}
	if d.RequestTimeout <= 0 {
		d.RequestTimeout = 10 * time.Second
	}
	s := &server{d}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.HandleFunc("POST /api/v1/events", s.ingestEvent)
	mux.HandleFunc("GET /api/v1/events/{id}", s.getEvent)
	mux.HandleFunc("POST /api/v1/events/{id}/process", s.processEvent)
	mux.HandleFunc("GET /api/v1/transactions/{id}/events", s.listTransactionEvents)
	mux.HandleFunc("GET /api/v1/transactions/{id}/status", s.getTransactionStatus)
	mux.HandleFunc("POST /api/v1/transactions/{id}/process", s.processOriginal)
	mux.HandleFunc("POST /api/v1/transactions/{id}/process-events", s.processTransactionEvents)
	mux.HandleFunc("POST /api/v1/replays", s.createReplay)
	mux.HandleFunc("POST /api/v1/replays/{id}/start", s.startReplay)
	mux.HandleFunc("GET /api/v1/replays/{id}", s.getReplay)
	mux.HandleFunc("GET /api/v1/replays/{id}/executions", s.listReplayExecutions)
	mux.HandleFunc("GET /api/v1/replay-comparisons", s.compareReplays)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "route not found", nil)
	})

	return chain(mux,
		requestID,
		recoverer(d.Logger),
		accessLog(d.Logger),
		timeout(d.RequestTimeout),
	)
}

func (s *server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.Pinger != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.Pinger.Ping(ctx); err != nil {
			s.Logger.Warn("readiness check failed", "error", err)
			writeError(w, http.StatusServiceUnavailable, "not_ready", "database unavailable", nil)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type ingestResponse struct {
	Duplicate bool             `json:"duplicate"`
	Event     eventstore.Event `json:"event"`
}

func (s *server) ingestEvent(w http.ResponseWriter, r *http.Request) {
	var req ingestion.Request
	if !s.decodeBody(w, r, &req) {
		return
	}
	res, err := s.Ingester.Ingest(r.Context(), req)
	if err != nil {
		s.handleError(w, r, err)
		return
	}
	status := http.StatusCreated
	if res.Duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, ingestResponse{Duplicate: res.Duplicate, Event: res.Event})
}

func (s *server) getEvent(w http.ResponseWriter, r *http.Request) {
	ev, err := s.Reader.GetEvent(r.Context(), r.PathValue("id"))
	if err != nil {
		s.handleError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, ev)
}

type historyResponse struct {
	TransactionID string             `json:"transaction_id"`
	Events        []eventstore.Event `json:"events"`
	// NextAfterSequence is set when more events may exist; pass it as after_sequence.
	NextAfterSequence *int64 `json:"next_after_sequence,omitempty"`
}

func (s *server) listTransactionEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	q := r.URL.Query()

	limit := defaultListLimit
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxListLimit {
			writeError(w, http.StatusBadRequest, "invalid_query", "limit must be an integer between 1 and "+strconv.Itoa(maxListLimit), nil)
			return
		}
		limit = n
	}
	var after int64
	if v := q.Get("after_sequence"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "invalid_query", "after_sequence must be a non-negative integer", nil)
			return
		}
		after = n
	}

	if _, err := s.Reader.GetTransaction(r.Context(), id); err != nil {
		s.handleError(w, r, err)
		return
	}
	events, err := s.Reader.ListByTransaction(r.Context(), id, after, limit)
	if err != nil {
		s.handleError(w, r, err)
		return
	}
	resp := historyResponse{TransactionID: id, Events: events}
	if len(events) == limit {
		next := events[len(events)-1].SequenceNumber
		resp.NextAfterSequence = &next
	}
	writeJSON(w, http.StatusOK, resp)
}

type processEventBody struct {
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	Policy         string `json:"policy,omitempty"`
}

func (s *server) processEvent(w http.ResponseWriter, r *http.Request) {
	if s.Processor == nil {
		writeError(w, http.StatusNotImplemented, "not_configured", "event processor is not configured", nil)
		return
	}
	var body processEventBody
	if r.Body != nil {
		r.Body = http.MaxBytesReader(w, r.Body, s.MaxBodyBytes)
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				writeError(w, http.StatusRequestEntityTooLarge, "body_too_large", "request body too large", nil)
				return
			}
			writeError(w, http.StatusBadRequest, "invalid_json", "failed to read request body", nil)
			return
		}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &body); err != nil {
				writeError(w, http.StatusBadRequest, "invalid_json", "malformed JSON request body: "+err.Error(), nil)
				return
			}
		}
	}
	key := r.Header.Get("Idempotency-Key")
	if body.IdempotencyKey != "" {
		key = body.IdempotencyKey
	}
	var policy ordering.Policy
	if body.Policy != "" {
		var err error
		policy, err = ordering.ParsePolicy(body.Policy)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_policy", err.Error(), nil)
			return
		}
	}

	out, err := s.Processor.ProcessEvent(r.Context(), r.PathValue("id"), key, policy)
	if err != nil {
		s.handleError(w, r, err)
		return
	}
	status := http.StatusOK
	if out.Buffered {
		status = http.StatusAccepted
	}
	writeJSON(w, status, out)
}

func (s *server) processTransactionEvents(w http.ResponseWriter, r *http.Request) {
	if s.Processor == nil {
		writeError(w, http.StatusNotImplemented, "not_configured", "event processor is not configured", nil)
		return
	}
	var policy ordering.Policy
	if p := r.URL.Query().Get("policy"); p != "" {
		var err error
		policy, err = ordering.ParsePolicy(p)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_policy", err.Error(), nil)
			return
		}
	}
	outcomes, err := s.Processor.ProcessTransaction(r.Context(), r.PathValue("id"), policy)
	if err != nil {
		s.handleError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"transaction_id": r.PathValue("id"),
		"outcomes":       outcomes,
	})
}

func (s *server) getTransactionStatus(w http.ResponseWriter, r *http.Request) {
	if s.Inspector == nil {
		writeError(w, http.StatusNotImplemented, "not_configured", "ordering inspector is not configured", nil)
		return
	}
	st, err := s.Inspector.InspectTransaction(r.Context(), r.PathValue("id"))
	if err != nil {
		s.handleError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *server) processOriginal(w http.ResponseWriter, r *http.Request) {
	if s.Original == nil {
		writeError(w, http.StatusNotImplemented, "not_configured", "original processor is not configured", nil)
		return
	}
	sess, err := s.Original.ProcessOriginal(r.Context(), r.PathValue("id"))
	if err != nil {
		if sess.SessionID == "" ||
			errors.Is(err, eventstore.ErrNotFound) ||
			errors.Is(err, execution.ErrInvalidSessionInput) {
			s.handleError(w, r, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, sess)
}

func (s *server) createReplay(w http.ResponseWriter, r *http.Request) {
	if s.Replayer == nil {
		writeError(w, http.StatusNotImplemented, "not_configured", "replay engine is not configured", nil)
		return
	}
	var req replay.CreateRequest
	if !s.decodeBody(w, r, &req) {
		return
	}
	sess, err := s.Replayer.CreateSession(r.Context(), req)
	if err != nil {
		s.handleError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, sess)
}

func (s *server) startReplay(w http.ResponseWriter, r *http.Request) {
	if s.Replayer == nil {
		writeError(w, http.StatusNotImplemented, "not_configured", "replay engine is not configured", nil)
		return
	}
	res, err := s.Replayer.StartSession(r.Context(), r.PathValue("id"))
	if err != nil {
		if res.Session.SessionID == "" ||
			errors.Is(err, execution.ErrSessionNotFound) ||
			errors.Is(err, execution.ErrInvalidTransition) {
			s.handleError(w, r, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *server) getReplay(w http.ResponseWriter, r *http.Request) {
	if s.Replayer == nil {
		writeError(w, http.StatusNotImplemented, "not_configured", "replay engine is not configured", nil)
		return
	}
	sess, err := s.Replayer.GetSession(r.Context(), r.PathValue("id"))
	if err != nil {
		s.handleError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, sess)
}

type executionsResponse struct {
	SessionID  string                     `json:"session_id"`
	Executions []execution.EventExecution `json:"executions"`
}

func (s *server) listReplayExecutions(w http.ResponseWriter, r *http.Request) {
	if s.Replayer == nil {
		writeError(w, http.StatusNotImplemented, "not_configured", "replay engine is not configured", nil)
		return
	}
	id := r.PathValue("id")
	execs, err := s.Replayer.ListExecutions(r.Context(), id)
	if err != nil {
		s.handleError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, executionsResponse{SessionID: id, Executions: execs})
}

func (s *server) compareReplays(w http.ResponseWriter, r *http.Request) {
	if s.Replayer == nil {
		writeError(w, http.StatusNotImplemented, "not_configured", "replay engine is not configured", nil)
		return
	}
	q := r.URL.Query()
	left := q.Get("left")
	right := q.Get("right")
	if left == "" || right == "" {
		writeError(w, http.StatusBadRequest, "invalid_query", "both left and right query parameters are required", nil)
		return
	}
	res, err := s.Replayer.Compare(r.Context(), left, right)
	if err != nil {
		s.handleError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleError maps domain errors to HTTP responses.
func (s *server) handleError(w http.ResponseWriter, r *http.Request, err error) {
	var ve *ingestion.ValidationError
	switch {
	case errors.As(err, &ve):
		writeError(w, http.StatusUnprocessableEntity, "validation_failed", "request validation failed", ve.Fields)
	case errors.Is(err, execution.ErrInvalidSessionInput),
		errors.Is(err, idempotency.ErrInvalidKey):
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), nil)
	case errors.Is(err, eventstore.ErrNotFound),
		errors.Is(err, execution.ErrSessionNotFound),
		errors.Is(err, execution.ErrOriginalNotFound):
		writeError(w, http.StatusNotFound, "not_found", err.Error(), nil)
	case errors.Is(err, eventstore.ErrPayloadConflict):
		writeError(w, http.StatusConflict, "event_conflict", err.Error(), nil)
	case errors.Is(err, eventstore.ErrSequenceConflict):
		writeError(w, http.StatusConflict, "sequence_conflict", err.Error(), nil)
	case errors.Is(err, execution.ErrInvalidTransition):
		writeError(w, http.StatusConflict, "invalid_transition", err.Error(), nil)
	case errors.Is(err, idempotency.ErrKeyConflict):
		writeError(w, http.StatusConflict, "idempotency_conflict", err.Error(), nil)
	case errors.Is(err, idempotency.ErrAlreadyInProgress):
		writeError(w, http.StatusConflict, "idempotency_in_progress", err.Error(), nil)
	case errors.Is(err, ordering.ErrOutOfOrder):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusConflict, "out_of_order_retryable", err.Error(), nil)
	case errors.Is(err, ordering.ErrSequenceGapFlagged):
		writeError(w, http.StatusConflict, "sequence_gap_flagged", err.Error(), nil)
	case errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusGatewayTimeout, "timeout", "request timed out", nil)
	case errors.Is(err, context.Canceled):
		writeError(w, 499, "client_closed_request", "request canceled", nil)
	default:
		s.Logger.Error("internal error", "error", err, "request_id", RequestID(r.Context()))
		writeError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
	}
}

// decodeBody reads a size-limited, strict JSON body. It writes the error response itself.
func (s *server) decodeBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, s.MaxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "body_too_large", "request body too large", nil)
			return false
		}
		writeError(w, http.StatusBadRequest, "invalid_json", "malformed JSON request body: "+err.Error(), nil)
		return false
	}
	if dec.More() {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must contain a single JSON object", nil)
		return false
	}
	return true
}
