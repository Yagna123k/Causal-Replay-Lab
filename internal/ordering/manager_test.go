package ordering

import (
	"errors"
	"reflect"
	"testing"
)

func TestEvaluatePolicies(t *testing.T) {
	// Already processed
	if d, err := Evaluate(3, 2, PolicyBuffer); err != nil || d != DecisionAlreadyProcessed {
		t.Fatalf("got %s %v", d, err)
	}
	// In order
	if d, err := Evaluate(2, 3, PolicyReject); err != nil || d != DecisionInOrder {
		t.Fatalf("got %s %v", d, err)
	}

	// Out-of-order: BUFFER
	if d, err := Evaluate(1, 3, PolicyBuffer); err != nil || d != DecisionBuffer {
		t.Fatalf("buffer: got %s %v", d, err)
	}

	// Out-of-order: REJECT
	if d, err := Evaluate(1, 3, PolicyReject); d != DecisionReject || !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("reject: got %s %v", d, err)
	}

	// Out-of-order: FAIL_AND_FLAG
	if d, err := Evaluate(1, 3, PolicyFailAndFlag); d != DecisionFailAndFlag || !errors.Is(err, ErrSequenceGapFlagged) {
		t.Fatalf("fail_and_flag: got %s %v", d, err)
	}
}

func TestFindMissingSequences(t *testing.T) {
	cases := []struct {
		processed int64
		known     []int64
		want      []int64
	}{
		{0, nil, nil},
		{0, []int64{1, 2, 3}, nil},
		{1, []int64{1, 3}, []int64{2}},
		{0, []int64{2, 4, 5}, []int64{1, 3}},
		{2, []int64{1, 2, 5, 6}, []int64{3, 4}},
	}
	for _, tc := range cases {
		got := FindMissingSequences(tc.processed, tc.known)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("FindMissingSequences(%d, %v) = %v, want %v", tc.processed, tc.known, got, tc.want)
		}
	}
}

func TestParsePolicy(t *testing.T) {
	if p, err := ParsePolicy("buffer"); err != nil || p != PolicyBuffer {
		t.Fatalf("got %s %v", p, err)
	}
	if p, err := ParsePolicy("REJECT"); err != nil || p != PolicyReject {
		t.Fatalf("got %s %v", p, err)
	}
	if p, err := ParsePolicy("fail_and_flag"); err != nil || p != PolicyFailAndFlag {
		t.Fatalf("got %s %v", p, err)
	}
	if _, err := ParsePolicy("unknown"); err == nil {
		t.Fatal("expected error for unknown policy")
	}
}
