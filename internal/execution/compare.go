package execution

import (
	"bytes"
	"encoding/json"
)

// ComparisonResult summarizes the comparison between two execution sessions
// (for example, a replay session against the original baseline, or two replays).
type ComparisonResult struct {
	LeftSessionID       string            `json:"left_session_id"`
	RightSessionID      string            `json:"right_session_id"`
	LeftMode            Mode              `json:"left_mode"`
	RightMode           Mode              `json:"right_mode"`
	TransactionID       string            `json:"transaction_id"`
	Equivalent          bool              `json:"equivalent"`
	FinalStateMatch     bool              `json:"final_state_match"`
	EffectsMatch        bool              `json:"effects_match"`
	LeftFinalStateHash  string            `json:"left_final_state_hash,omitempty"`
	RightFinalStateHash string            `json:"right_final_state_hash,omitempty"`
	LeftEventCount      int               `json:"left_event_count"`
	RightEventCount     int               `json:"right_event_count"`
	FirstDivergence     *EventDivergence  `json:"first_divergence,omitempty"`
	EventDiffs          []EventDivergence `json:"event_diffs,omitempty"`
}

// EventDivergence describes a mismatch at a specific sequence number.
type EventDivergence struct {
	SequenceNumber int64    `json:"sequence_number"`
	EventID        string   `json:"event_id,omitempty"`
	Reasons        []string `json:"reasons"`
	LeftStatus     string   `json:"left_status,omitempty"`
	RightStatus    string   `json:"right_status,omitempty"`
	LeftStateHash  string   `json:"left_state_hash,omitempty"`
	RightStateHash string   `json:"right_state_hash,omitempty"`
	LeftEffects    []Effect `json:"left_effects,omitempty"`
	RightEffects   []Effect `json:"right_effects,omitempty"`
}

// LogicalEffectsSignature serializes the deterministic logical fields of effects
// (Type, Target, Amount, Currency, Data) so comparisons can detect logical effect differences
// independently of wall-clock timestamps or random live reference IDs, while still checking
// exact effect count and content.
func LogicalEffectsSignature(effects []Effect) []byte {
	type logicalEffect struct {
		Type     string         `json:"type"`
		Target   string         `json:"target,omitempty"`
		Amount   int64          `json:"amount,omitempty"`
		Currency string         `json:"currency,omitempty"`
		Data     map[string]any `json:"data,omitempty"`
	}
	out := make([]logicalEffect, len(effects))
	for i, e := range effects {
		out[i] = logicalEffect{
			Type:     e.Type,
			Target:   e.Target,
			Amount:   e.Amount,
			Currency: e.Currency,
			Data:     e.Data,
		}
	}
	b, _ := json.Marshal(out)
	return b
}

// Compare diffs two sessions and their ordered per-event execution lists.
func Compare(left Session, leftExecs []EventExecution, right Session, rightExecs []EventExecution) ComparisonResult {
	res := ComparisonResult{
		LeftSessionID:       left.SessionID,
		RightSessionID:      right.SessionID,
		LeftMode:            left.Mode,
		RightMode:           right.Mode,
		TransactionID:       left.TransactionID,
		FinalStateMatch:     left.FinalStateHash == right.FinalStateHash && left.Status == right.Status,
		EffectsMatch:        true,
		LeftFinalStateHash:  left.FinalStateHash,
		RightFinalStateHash: right.FinalStateHash,
		LeftEventCount:      len(leftExecs),
		RightEventCount:     len(rightExecs),
	}

	leftBySeq := make(map[int64]EventExecution, len(leftExecs))
	rightBySeq := make(map[int64]EventExecution, len(rightExecs))
	seqSet := make(map[int64]struct{}, len(leftExecs)+len(rightExecs))

	for _, e := range leftExecs {
		leftBySeq[e.SequenceNumber] = e
		seqSet[e.SequenceNumber] = struct{}{}
	}
	for _, e := range rightExecs {
		rightBySeq[e.SequenceNumber] = e
		seqSet[e.SequenceNumber] = struct{}{}
	}

	seqs := make([]int64, 0, len(seqSet))
	for s := range seqSet {
		seqs = append(seqs, s)
	}
	// Insertion sort / sort slice in ascending sequence order.
	for i := 1; i < len(seqs); i++ {
		for j := i; j > 0 && seqs[j-1] > seqs[j]; j-- {
			seqs[j-1], seqs[j] = seqs[j], seqs[j-1]
		}
	}

	for _, seq := range seqs {
		l, hasL := leftBySeq[seq]
		r, hasR := rightBySeq[seq]

		var reasons []string
		div := EventDivergence{SequenceNumber: seq}

		switch {
		case !hasL:
			reasons = append(reasons, "missing_in_left")
			div.EventID = r.EventID
			div.RightStatus = string(r.Status)
			div.RightStateHash = r.StateHash
			div.RightEffects = r.Effects
			res.EffectsMatch = false
		case !hasR:
			reasons = append(reasons, "missing_in_right")
			div.EventID = l.EventID
			div.LeftStatus = string(l.Status)
			div.LeftStateHash = l.StateHash
			div.LeftEffects = l.Effects
			res.EffectsMatch = false
		default:
			div.EventID = l.EventID
			div.LeftStatus = string(l.Status)
			div.RightStatus = string(r.Status)
			div.LeftStateHash = l.StateHash
			div.RightStateHash = r.StateHash
			div.LeftEffects = l.Effects
			div.RightEffects = r.Effects

			if l.EventID != r.EventID {
				reasons = append(reasons, "event_id_mismatch")
			}
			if l.Status != r.Status {
				reasons = append(reasons, "status_mismatch")
			}
			if l.StateHash != r.StateHash {
				reasons = append(reasons, "state_hash_mismatch")
			}
			if !bytes.Equal(LogicalEffectsSignature(l.Effects), LogicalEffectsSignature(r.Effects)) {
				reasons = append(reasons, "effects_mismatch")
				res.EffectsMatch = false
			}
		}

		if len(reasons) > 0 {
			div.Reasons = reasons
			if res.FirstDivergence == nil {
				d := div
				res.FirstDivergence = &d
			}
			res.EventDiffs = append(res.EventDiffs, div)
		}
	}

	res.Equivalent = res.FinalStateMatch && res.EffectsMatch && len(res.EventDiffs) == 0
	return res
}
