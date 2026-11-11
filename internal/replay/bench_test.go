package replay

import (
	"context"
	"testing"

	"github.com/Yagna123k/causal-replay-lab/internal/execution"
)

func BenchmarkReplayFiveEventWorkflow(b *testing.B) {
	ctx := context.Background()
	events := standardWorkflow("txn-bench")
	rd := newMemReader("txn-bench", events...)
	reg := DefaultRegistry()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st := newMemSessionStore()
		eng := NewEngine(rd, st, reg)
		sess, err := eng.CreateSession(ctx, CreateRequest{TransactionID: "txn-bench"})
		if err != nil {
			b.Fatal(err)
		}
		res, err := eng.StartSession(ctx, sess.SessionID)
		if err != nil || res.Session.Status != execution.StatusCompleted {
			b.Fatalf("replay failed: %v %+v", err, res.Session)
		}
	}
}

func BenchmarkCompareExecutions(b *testing.B) {
	events := standardWorkflow("txn-bench")
	rd := newMemReader("txn-bench", events...)
	st := newMemSessionStore()
	eng := NewEngine(rd, st, nil)
	ctx := context.Background()

	s1, _ := eng.CreateSession(ctx, CreateRequest{TransactionID: "txn-bench"})
	r1, _ := eng.StartSession(ctx, s1.SessionID)
	e1, _ := eng.ListExecutions(ctx, s1.SessionID)

	s2, _ := eng.CreateSession(ctx, CreateRequest{TransactionID: "txn-bench"})
	r2, _ := eng.StartSession(ctx, s2.SessionID)
	e2, _ := eng.ListExecutions(ctx, s2.SessionID)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmp := execution.Compare(r1.Session, e1, r2.Session, e2)
		if !cmp.Equivalent {
			b.Fatal("expected equivalent")
		}
	}
}
