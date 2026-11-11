// Package config loads and validates application configuration from the environment.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime settings.
type Config struct {
	HTTPAddr        string
	DatabaseURL     string
	LogLevel        slog.Level
	LogFormat       string // "json" or "text"
	RequestTimeout  time.Duration
	ShutdownTimeout time.Duration
	DBMaxConns      int32
	MaxBodyBytes    int64
	OrderingPolicy  string // "BUFFER", "REJECT", or "FAIL_AND_FLAG"
	IdempotencyTTL  time.Duration
	PendingGapTTL   time.Duration
	LeaseTTL        time.Duration
	RetryMaxAttempts int
	RetryBaseBackoff time.Duration
	RetryMaxBackoff  time.Duration
}

// Load reads configuration using os.Getenv.
func Load() (Config, error) { return LoadFrom(os.Getenv) }

// LoadFrom reads configuration using the supplied lookup function (for tests).
func LoadFrom(get func(string) string) (Config, error) {
	var errs []error
	str := func(key, def string) string {
		if v := strings.TrimSpace(get(key)); v != "" {
			return v
		}
		return def
	}
	dur := func(key string, def time.Duration) time.Duration {
		v := strings.TrimSpace(get(key))
		if v == "" {
			return def
		}
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			errs = append(errs, fmt.Errorf("%s: invalid positive duration %q", key, v))
			return def
		}
		return d
	}
	integer := func(key string, def int64) int64 {
		v := strings.TrimSpace(get(key))
		if v == "" {
			return def
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			errs = append(errs, fmt.Errorf("%s: invalid positive integer %q", key, v))
			return def
		}
		return n
	}

	cfg := Config{
		HTTPAddr:        str("HTTP_ADDR", ":8080"),
		DatabaseURL:     str("DATABASE_URL", ""),
		LogFormat:       strings.ToLower(str("LOG_FORMAT", "json")),
		RequestTimeout:  dur("REQUEST_TIMEOUT", 10*time.Second),
		ShutdownTimeout: dur("SHUTDOWN_TIMEOUT", 15*time.Second),
		DBMaxConns:      int32(integer("DB_MAX_CONNS", 10)),
		MaxBodyBytes:    integer("MAX_BODY_BYTES", 1<<20),
		OrderingPolicy:   strings.ToUpper(str("ORDERING_POLICY", "BUFFER")),
		IdempotencyTTL:   dur("IDEMPOTENCY_TTL", 24*time.Hour),
		PendingGapTTL:    dur("PENDING_GAP_TTL", time.Hour),
		LeaseTTL:         dur("LEASE_TTL", 30*time.Second),
		RetryMaxAttempts: int(integer("RETRY_MAX_ATTEMPTS", 3)),
		RetryBaseBackoff: dur("RETRY_BASE_BACKOFF", 25*time.Millisecond),
		RetryMaxBackoff:  dur("RETRY_MAX_BACKOFF", 500*time.Millisecond),
	}

	if cfg.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}
	if cfg.LogFormat != "json" && cfg.LogFormat != "text" {
		errs = append(errs, fmt.Errorf("LOG_FORMAT: must be json or text, got %q", cfg.LogFormat))
	}
	switch cfg.OrderingPolicy {
	case "BUFFER", "REJECT", "FAIL_AND_FLAG":
	default:
		errs = append(errs, fmt.Errorf("ORDERING_POLICY: must be BUFFER, REJECT, or FAIL_AND_FLAG, got %q", cfg.OrderingPolicy))
	}
	if err := cfg.LogLevel.UnmarshalText([]byte(str("LOG_LEVEL", "info"))); err != nil {
		errs = append(errs, fmt.Errorf("LOG_LEVEL: %w", err))
	}

	if len(errs) > 0 {
		return Config{}, errors.Join(errs...)
	}
	return cfg, nil
}
