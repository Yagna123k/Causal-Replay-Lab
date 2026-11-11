package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := LoadFrom(env(map[string]string{"DATABASE_URL": "postgres://x"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != ":8080" || cfg.RequestTimeout != 10*time.Second || cfg.LogLevel != slog.LevelInfo ||
		cfg.DBMaxConns != 10 || cfg.OrderingPolicy != "BUFFER" || cfg.IdempotencyTTL != 24*time.Hour || cfg.PendingGapTTL != time.Hour {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := LoadFrom(env(map[string]string{
		"DATABASE_URL":    "postgres://x",
		"HTTP_ADDR":       ":9000",
		"LOG_LEVEL":       "debug",
		"LOG_FORMAT":      "TEXT",
		"REQUEST_TIMEOUT": "2s",
		"DB_MAX_CONNS":    "3",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != ":9000" || cfg.LogLevel != slog.LevelDebug || cfg.LogFormat != "text" ||
		cfg.RequestTimeout != 2*time.Second || cfg.DBMaxConns != 3 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestLoadValidation(t *testing.T) {
	_, err := LoadFrom(env(map[string]string{
		"REQUEST_TIMEOUT": "nope",
		"DB_MAX_CONNS":    "-1",
		"LOG_FORMAT":      "xml",
		"LOG_LEVEL":       "loud",
		"ORDERING_POLICY": "chaos",
	}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"DATABASE_URL", "REQUEST_TIMEOUT", "DB_MAX_CONNS", "LOG_FORMAT", "LOG_LEVEL", "ORDERING_POLICY"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %s: %v", want, err)
		}
	}
}
