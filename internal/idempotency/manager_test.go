package idempotency

import "testing"

func TestProcessingStateMachine(t *testing.T) {
	valid := [][2]ProcessingStatus{
		{StatusReceived, StatusPendingPrerequisite},
		{StatusReceived, StatusProcessing},
		{StatusReceived, StatusFailed},
		{StatusPendingPrerequisite, StatusProcessing},
		{StatusPendingPrerequisite, StatusFailed},
		{StatusPendingPrerequisite, StatusDead},
		{StatusProcessing, StatusProcessed},
		{StatusProcessing, StatusFailed},
		{StatusProcessing, StatusDead},
		{StatusFailed, StatusProcessing},
		{StatusFailed, StatusDead},
	}
	for _, pair := range valid {
		if !CanTransition(pair[0], pair[1]) {
			t.Errorf("expected %s -> %s to be valid", pair[0], pair[1])
		}
	}

	invalid := [][2]ProcessingStatus{
		{StatusReceived, StatusProcessed},
		{StatusProcessed, StatusProcessing},
		{StatusProcessed, StatusFailed},
		{StatusDead, StatusProcessing},
		{StatusPendingPrerequisite, StatusProcessed},
	}
	for _, pair := range invalid {
		if CanTransition(pair[0], pair[1]) {
			t.Errorf("expected %s -> %s to be invalid", pair[0], pair[1])
		}
	}
}
