package replay

import (
	"encoding/json"
	"fmt"

	"github.com/Yagna123k/causal-replay-lab/internal/eventstore"
	"github.com/Yagna123k/causal-replay-lab/internal/execution"
)

// Handler is the pure processing contract for a single event type.
// Implementations must not invoke time.Now(), global rand, or external I/O;
// all time and ID generation must come from deps or the event itself.
type Handler interface {
	Apply(deps ControlledDeps, state execution.TransactionState, ev eventstore.Event) (execution.TransactionState, []execution.Effect, error)
}

// HandlerFunc adapts a function to the Handler interface.
type HandlerFunc func(deps ControlledDeps, state execution.TransactionState, ev eventstore.Event) (execution.TransactionState, []execution.Effect, error)

// Apply calls f(deps, state, ev).
func (f HandlerFunc) Apply(deps ControlledDeps, state execution.TransactionState, ev eventstore.Event) (execution.TransactionState, []execution.Effect, error) {
	return f(deps, state, ev)
}

// Registry maps event types to deterministic handlers.
type Registry struct {
	handlers map[string]Handler
}

// NewRegistry returns an empty handler registry.
func NewRegistry() *Registry {
	return &Registry{handlers: map[string]Handler{}}
}

// Register binds an event type to a handler.
func (r *Registry) Register(eventType string, h Handler) {
	r.handlers[eventType] = h
}

// Lookup returns the handler for eventType, if registered.
func (r *Registry) Lookup(eventType string) (Handler, bool) {
	h, ok := r.handlers[eventType]
	return h, ok
}

// DefaultRegistry returns a registry populated with standard transaction-processing handlers.
func DefaultRegistry() *Registry {
	r := NewRegistry()
	r.Register("TRANSACTION_CREATED", HandlerFunc(applyTransactionCreated))
	r.Register("ACCOUNT_VALIDATED", HandlerFunc(applyAccountValidated))
	r.Register("DEBIT_INITIATED", HandlerFunc(applyDebitInitiated))
	r.Register("LEDGER_UPDATED", HandlerFunc(applyLedgerUpdated))
	r.Register("TRANSACTION_COMPLETED", HandlerFunc(applyTransactionCompleted))
	r.Register("TRANSACTION_FAILED", HandlerFunc(applyTransactionFailed))
	return r
}

func decodePayload(ev eventstore.Event, dst any) error {
	if err := json.Unmarshal(ev.Payload, dst); err != nil {
		return fmt.Errorf("invalid %s payload: %w", ev.EventType, err)
	}
	return nil
}

func applyTransactionCreated(deps ControlledDeps, state execution.TransactionState, ev eventstore.Event) (execution.TransactionState, []execution.Effect, error) {
	var p struct {
		AccountID       string `json:"account_id"`
		Amount          int64  `json:"amount"`
		Currency        string `json:"currency"`
		InitialBalance  int64  `json:"initial_balance"`
		SimulateFailure bool   `json:"simulate_failure"`
	}
	if err := decodePayload(ev, &p); err != nil {
		return state, nil, err
	}
	if p.SimulateFailure {
		return state, nil, fmt.Errorf("simulated failure on %s", ev.EventType)
	}
	if p.AccountID == "" {
		return state, nil, fmt.Errorf("account_id is required")
	}
	if p.Currency == "" {
		p.Currency = "USD"
	}
	next := state.Clone()
	next.TransactionID = ev.TransactionID
	next.AccountID = p.AccountID
	next.Currency = p.Currency
	if p.InitialBalance != 0 {
		next.Balance = p.InitialBalance
	}
	next.Status = "CREATED"
	next.LastSequence = ev.SequenceNumber
	next.AppliedEvents++
	if p.Amount > 0 {
		next.PendingDebit = p.Amount
	}

	eff := execution.Effect{
		Type:        "TRANSACTION_REGISTERED",
		Target:      p.AccountID,
		ReferenceID: deps.IDGen.NewID("eff"),
		Amount:      p.Amount,
		Currency:    p.Currency,
		Timestamp:   deps.Clock.Now(),
	}
	return next, []execution.Effect{eff}, nil
}

func applyAccountValidated(deps ControlledDeps, state execution.TransactionState, ev eventstore.Event) (execution.TransactionState, []execution.Effect, error) {
	var p struct {
		Valid           *bool  `json:"valid"`
		Reason          string `json:"reason"`
		SimulateFailure bool   `json:"simulate_failure"`
	}
	if err := decodePayload(ev, &p); err != nil {
		return state, nil, err
	}
	if p.SimulateFailure {
		return state, nil, fmt.Errorf("simulated failure on %s", ev.EventType)
	}
	if p.Valid != nil && !*p.Valid {
		reason := p.Reason
		if reason == "" {
			reason = "account validation failed"
		}
		return state, nil, fmt.Errorf("%s", reason)
	}
	next := state.Clone()
	next.AccountValidated = true
	next.Status = "VALIDATED"
	next.LastSequence = ev.SequenceNumber
	next.AppliedEvents++

	eff := execution.Effect{
		Type:        "ACCOUNT_VERIFIED",
		Target:      next.AccountID,
		ReferenceID: deps.IDGen.NewID("eff"),
		Timestamp:   deps.Clock.Now(),
	}
	return next, []execution.Effect{eff}, nil
}

func applyDebitInitiated(deps ControlledDeps, state execution.TransactionState, ev eventstore.Event) (execution.TransactionState, []execution.Effect, error) {
	var p struct {
		Amount          int64  `json:"amount"`
		Currency        string `json:"currency"`
		SimulateFailure bool   `json:"simulate_failure"`
	}
	if err := decodePayload(ev, &p); err != nil {
		return state, nil, err
	}
	if p.SimulateFailure {
		return state, nil, fmt.Errorf("simulated failure on %s", ev.EventType)
	}
	if p.Amount <= 0 {
		return state, nil, fmt.Errorf("debit amount must be positive, got %d", p.Amount)
	}
	next := state.Clone()
	if next.Currency == "" && p.Currency != "" {
		next.Currency = p.Currency
	}
	if next.Balance < p.Amount {
		return state, nil, fmt.Errorf("insufficient balance: have %d, need %d", next.Balance, p.Amount)
	}
	next.PendingDebit = p.Amount
	next.Status = "DEBIT_INITIATED"
	next.LastSequence = ev.SequenceNumber
	next.AppliedEvents++

	eff := execution.Effect{
		Type:        "LEDGER_DEBIT",
		Target:      next.AccountID,
		ReferenceID: deps.IDGen.NewID("eff"),
		Amount:      p.Amount,
		Currency:    next.Currency,
		Timestamp:   deps.Clock.Now(),
	}
	return next, []execution.Effect{eff}, nil
}

func applyLedgerUpdated(deps ControlledDeps, state execution.TransactionState, ev eventstore.Event) (execution.TransactionState, []execution.Effect, error) {
	var p struct {
		Amount          int64  `json:"amount"`
		Delta           int64  `json:"delta"`
		Operation       string `json:"operation"`
		SimulateFailure bool   `json:"simulate_failure"`
	}
	if err := decodePayload(ev, &p); err != nil {
		return state, nil, err
	}
	if p.SimulateFailure {
		return state, nil, fmt.Errorf("simulated failure on %s", ev.EventType)
	}
	delta := p.Delta
	if delta == 0 && p.Amount != 0 {
		if p.Operation == "credit" {
			delta = p.Amount
		} else {
			delta = -p.Amount
		}
	}
	next := state.Clone()
	if next.Balance+delta < 0 {
		return state, nil, fmt.Errorf("ledger update would make balance negative (%d + %d)", next.Balance, delta)
	}
	next.Balance += delta
	if delta < 0 && next.PendingDebit >= -delta {
		next.PendingDebit += delta
	}
	next.Status = "LEDGER_UPDATED"
	next.LastSequence = ev.SequenceNumber
	next.AppliedEvents++

	eff := execution.Effect{
		Type:        "LEDGER_ENTRY",
		Target:      next.AccountID,
		ReferenceID: deps.IDGen.NewID("eff"),
		Amount:      delta,
		Currency:    next.Currency,
		Timestamp:   deps.Clock.Now(),
	}
	return next, []execution.Effect{eff}, nil
}

func applyTransactionCompleted(deps ControlledDeps, state execution.TransactionState, ev eventstore.Event) (execution.TransactionState, []execution.Effect, error) {
	var p struct {
		SimulateFailure bool `json:"simulate_failure"`
	}
	if len(ev.Payload) > 0 {
		if err := decodePayload(ev, &p); err != nil {
			return state, nil, err
		}
	}
	if p.SimulateFailure {
		return state, nil, fmt.Errorf("simulated failure on %s", ev.EventType)
	}
	next := state.Clone()
	next.Status = "COMPLETED"
	next.PendingDebit = 0
	next.LastSequence = ev.SequenceNumber
	next.AppliedEvents++

	eff := execution.Effect{
		Type:        "NOTIFICATION_SENT",
		Target:      next.AccountID,
		ReferenceID: deps.IDGen.NewID("eff"),
		Currency:    next.Currency,
		Data:        map[string]any{"status": "COMPLETED"},
		Timestamp:   deps.Clock.Now(),
	}
	return next, []execution.Effect{eff}, nil
}

func applyTransactionFailed(deps ControlledDeps, state execution.TransactionState, ev eventstore.Event) (execution.TransactionState, []execution.Effect, error) {
	var p struct {
		Reason string `json:"reason"`
	}
	if len(ev.Payload) > 0 {
		if err := decodePayload(ev, &p); err != nil {
			return state, nil, err
		}
	}
	next := state.Clone()
	next.Status = "FAILED"
	next.FailureReason = p.Reason
	next.LastSequence = ev.SequenceNumber
	next.AppliedEvents++

	eff := execution.Effect{
		Type:        "FAILURE_ALERT",
		Target:      next.AccountID,
		ReferenceID: deps.IDGen.NewID("eff"),
		Data:        map[string]any{"reason": p.Reason},
		Timestamp:   deps.Clock.Now(),
	}
	return next, []execution.Effect{eff}, nil
}
