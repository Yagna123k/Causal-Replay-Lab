// Command server runs the Causal Replay Lab HTTP API.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Yagna123k/causal-replay-lab/internal/api"
	"github.com/Yagna123k/causal-replay-lab/internal/checkpoint"
	"github.com/Yagna123k/causal-replay-lab/internal/config"
	"github.com/Yagna123k/causal-replay-lab/internal/db"
	"github.com/Yagna123k/causal-replay-lab/internal/eventstore"
	"github.com/Yagna123k/causal-replay-lab/internal/execution"
	"github.com/Yagna123k/causal-replay-lab/internal/ingestion"
	"github.com/Yagna123k/causal-replay-lab/internal/ordering"
	"github.com/Yagna123k/causal-replay-lab/internal/processing"
	"github.com/Yagna123k/causal-replay-lab/internal/replay"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	opts := &slog.HandlerOptions{Level: cfg.LogLevel}
	var handler slog.Handler = slog.NewJSONHandler(os.Stdout, opts)
	if cfg.LogFormat == "text" {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}
	logger := slog.New(handler)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()

	repo := eventstore.NewRepository(pool)
	execStore := execution.NewStore(pool)
	registry := replay.DefaultRegistry()
	liveSink := replay.NewLiveEffectSink(func(_ context.Context, eff execution.Effect) error {
		logger.Info("live effect emitted", "type", eff.Type, "target", eff.Target, "reference_id", eff.ReferenceID)
		return nil
	})
	replayEngine := replay.NewEngine(repo, execStore, registry)
	origRunner := replay.NewOriginalRunner(repo, execStore, registry, liveSink)
	policy, err := ordering.ParsePolicy(cfg.OrderingPolicy)
	if err != nil {
		return err
	}
	ordMgr := ordering.NewManager(pool)
	proc := processing.NewProcessor(pool, processing.Options{
		DefaultPolicy:  policy,
		IdempotencyTTL: cfg.IdempotencyTTL,
		PendingGapTTL:  cfg.PendingGapTTL,
		LiveSink:       liveSink,
		Registry:       registry,
	})

	cpMgr := checkpoint.NewManager(pool)
	retryPol := replay.NewRetryPolicy(cfg.RetryMaxAttempts, cfg.RetryBaseBackoff, cfg.RetryMaxBackoff, 0.25, 42)
	recMgr := replay.NewRecoveryManager(pool, repo, execStore, cpMgr, replay.RecoveryOptions{
		LeaseTTL:    cfg.LeaseTTL,
		RetryPolicy: retryPol,
		Registry:    registry,
	})

	h := api.NewHandler(api.Deps{
		Ingester:       ingestion.NewService(repo),
		Reader:         repo,
		Replayer:       replayEngine,
		Original:       origRunner,
		Processor:      proc,
		Inspector:      ordMgr,
		Recovery:       recMgr,
		Pinger:         repo,
		Logger:         logger,
		RequestTimeout: cfg.RequestTimeout,
		MaxBodyBytes:   cfg.MaxBodyBytes,
	})

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      cfg.RequestTimeout + 5*time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("server listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	logger.Info("server stopped")
	return nil
}
