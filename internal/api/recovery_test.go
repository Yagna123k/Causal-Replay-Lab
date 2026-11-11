package api

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Yagna123k/causal-replay-lab/internal/checkpoint"
	"github.com/Yagna123k/causal-replay-lab/internal/execution"
	"github.com/Yagna123k/causal-replay-lab/internal/replay"
)

type fakeRecovery struct {
	startRes    replay.StartResult
	resumeRes   replay.StartResult
	pauseSess   execution.Session
	checkpoints []checkpoint.Checkpoint
	attempts    []execution.ExecutionAttempt
	orphans     []execution.Session
	err         error
}

func (f *fakeRecovery) StartWithRecovery(context.Context, string, string) (replay.StartResult, error) {
	return f.startRes, f.err
}
func (f *fakeRecovery) ResumeSession(context.Context, string, string) (replay.StartResult, error) {
	return f.resumeRes, f.err
}
func (f *fakeRecovery) PauseSession(context.Context, string) (execution.Session, error) {
	return f.pauseSess, f.err
}
func (f *fakeRecovery) ListCheckpoints(context.Context, string) ([]checkpoint.Checkpoint, error) {
	return f.checkpoints, f.err
}
func (f *fakeRecovery) ListAttempts(context.Context, string) ([]execution.ExecutionAttempt, error) {
	return f.attempts, f.err
}
func (f *fakeRecovery) DetectOrphanedSessions(context.Context, time.Time) ([]execution.Session, error) {
	return f.orphans, f.err
}

func newRecoveryHandler(rec RecoveryCoordinator) http.Handler {
	return NewHandler(Deps{
		Ingester:       fakeIngester{},
		Reader:         &fakeReader{},
		Replayer:       &fakeReplayer{},
		Recovery:       rec,
		RequestTimeout: time.Second,
		MaxBodyBytes:   1024,
	})
}

func TestRecoveryEndpoints(t *testing.T) {
	rec := &fakeRecovery{
		startRes:    replay.StartResult{Session: execution.Session{SessionID: "rpl_1", Status: execution.StatusCompleted}},
		resumeRes:   replay.StartResult{Session: execution.Session{SessionID: "rpl_1", Status: execution.StatusCompleted}},
		pauseSess:   execution.Session{SessionID: "rpl_1", Status: execution.StatusPaused},
		checkpoints: []checkpoint.Checkpoint{{CheckpointID: 1, SessionID: "rpl_1", LastSequence: 3, StateHash: "abc"}},
		attempts:    []execution.ExecutionAttempt{{AttemptID: 1, SessionID: "rpl_1", SequenceNumber: 4, AttemptNumber: 1, Status: execution.EventRetrying}},
		orphans:     []execution.Session{{SessionID: "rpl_old", Status: execution.StatusInterrupted}},
	}
	h := newRecoveryHandler(rec)

	if r := do(h, "POST", "/api/v1/replays/rpl_1/start", ""); r.Code != 200 {
		t.Fatalf("start via recovery: %d %s", r.Code, r.Body.String())
	}
	if r := do(h, "POST", "/api/v1/replays/rpl_1/pause", ""); r.Code != 200 {
		t.Fatalf("pause: %d %s", r.Code, r.Body.String())
	}
	if r := do(h, "POST", "/api/v1/replays/rpl_1/resume", ""); r.Code != 200 {
		t.Fatalf("resume: %d %s", r.Code, r.Body.String())
	}
	if r := do(h, "GET", "/api/v1/replays/rpl_1/checkpoints", ""); r.Code != 200 {
		t.Fatalf("checkpoints: %d %s", r.Code, r.Body.String())
	}
	if r := do(h, "GET", "/api/v1/replays/rpl_1/attempts", ""); r.Code != 200 {
		t.Fatalf("attempts: %d %s", r.Code, r.Body.String())
	}
	if r := do(h, "POST", "/api/v1/replay-recovery/orphans?resume=true", ""); r.Code != 200 {
		t.Fatalf("orphans: %d %s", r.Code, r.Body.String())
	} else {
		var body struct {
			Orphaned []execution.Session `json:"orphaned"`
			Resumed  []replay.StartResult `json:"resumed"`
		}
		_ = json.Unmarshal(r.Body.Bytes(), &body)
		if len(body.Orphaned) != 1 || len(body.Resumed) != 1 {
			t.Fatalf("unexpected orphan response: %+v", body)
		}
	}

	rec.err = execution.ErrSessionLocked
	if r := do(h, "POST", "/api/v1/replays/rpl_1/resume", ""); r.Code != 409 || errCode(t, r) != "session_locked" {
		t.Fatalf("locked: %d %s", r.Code, r.Body.String())
	}

	rec.err = checkpoint.ErrCorruptCheckpoint
	if r := do(h, "POST", "/api/v1/replays/rpl_1/resume", ""); r.Code != 409 || errCode(t, r) != "corrupt_checkpoint" {
		t.Fatalf("corrupt checkpoint: %d %s", r.Code, r.Body.String())
	}
}
