// Package observability provides context-propagated correlation IDs for
// structured logging and a lightweight Prometheus text-format metrics registry.
package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type corrKey struct{}

// Correlation holds trace identifiers propagated through request and replay contexts.
type Correlation struct {
	RequestID     string
	ReplayID      string
	TransactionID string
	EventID       string
}

// WithCorrelation attaches non-empty correlation fields onto ctx, merging with
// any existing Correlation values.
func WithCorrelation(ctx context.Context, c Correlation) context.Context {
	cur := GetCorrelation(ctx)
	if c.RequestID != "" {
		cur.RequestID = c.RequestID
	}
	if c.ReplayID != "" {
		cur.ReplayID = c.ReplayID
	}
	if c.TransactionID != "" {
		cur.TransactionID = c.TransactionID
	}
	if c.EventID != "" {
		cur.EventID = c.EventID
	}
	return context.WithValue(ctx, corrKey{}, cur)
}

// GetCorrelation retrieves correlation identifiers stored in ctx.
func GetCorrelation(ctx context.Context) Correlation {
	if ctx == nil {
		return Correlation{}
	}
	c, _ := ctx.Value(corrKey{}).(Correlation)
	return c
}

// Logger returns base enriched with every non-empty correlation attribute in ctx.
func Logger(ctx context.Context, base *slog.Logger) *slog.Logger {
	if base == nil {
		base = slog.Default()
	}
	c := GetCorrelation(ctx)
	var attrs []any
	if c.RequestID != "" {
		attrs = append(attrs, "request_id", c.RequestID)
	}
	if c.ReplayID != "" {
		attrs = append(attrs, "replay_id", c.ReplayID)
	}
	if c.TransactionID != "" {
		attrs = append(attrs, "transaction_id", c.TransactionID)
	}
	if c.EventID != "" {
		attrs = append(attrs, "event_id", c.EventID)
	}
	if len(attrs) == 0 {
		return base
	}
	return base.With(attrs...)
}

// Metrics tracks runtime counters and replay latency histograms for Prometheus scraping.
type Metrics struct {
	mu          sync.Mutex
	httpReqs    map[string]*atomic.Int64
	ingested    map[string]*atomic.Int64
	processed   map[string]*atomic.Int64
	duplicates  map[string]*atomic.Int64
	replays     map[string]*atomic.Int64
	retries     atomic.Int64
	gaps        atomic.Int64
	histBuckets []float64
	histCounts  []int64
	histCount   int64
	histSum     float64
}

// NewMetrics creates a Metrics registry with standard replay duration buckets.
func NewMetrics() *Metrics {
	buckets := []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0}
	return &Metrics{
		httpReqs:    map[string]*atomic.Int64{},
		ingested:    map[string]*atomic.Int64{},
		processed:   map[string]*atomic.Int64{},
		duplicates:  map[string]*atomic.Int64{},
		replays:     map[string]*atomic.Int64{},
		histBuckets: buckets,
		histCounts:  make([]int64, len(buckets)),
	}
}

func incMap(mu *sync.Mutex, m map[string]*atomic.Int64, key string) {
	mu.Lock()
	c, ok := m[key]
	if !ok {
		c = &atomic.Int64{}
		m[key] = c
	}
	mu.Unlock()
	c.Add(1)
}

// IncHTTPRequest increments the HTTP request counter.
func (m *Metrics) IncHTTPRequest(method, path string, status int) {
	if m == nil {
		return
	}
	key := fmt.Sprintf(`method=%q,path=%q,status="%d"`, method, path, status)
	incMap(&m.mu, m.httpReqs, key)
}

// IncIngested increments ingested events and duplicate detection counters.
func (m *Metrics) IncIngested(duplicate bool) {
	if m == nil {
		return
	}
	dupStr := "false"
	if duplicate {
		dupStr = "true"
		m.IncDuplicate("ingestion")
	}
	incMap(&m.mu, m.ingested, fmt.Sprintf(`duplicate=%q`, dupStr))
}

// IncProcessed increments processed events by final status.
func (m *Metrics) IncProcessed(status string) {
	if m == nil {
		return
	}
	incMap(&m.mu, m.processed, fmt.Sprintf(`status=%q`, status))
}

// IncDuplicate increments duplicate detections by stage ("ingestion" or "processing").
func (m *Metrics) IncDuplicate(stage string) {
	if m == nil {
		return
	}
	incMap(&m.mu, m.duplicates, fmt.Sprintf(`stage=%q`, stage))
}

// IncRetry increments the replay event retry counter.
func (m *Metrics) IncRetry() {
	if m != nil {
		m.retries.Add(1)
	}
}

// IncGap increments the sequence-gap detection counter.
func (m *Metrics) IncGap() {
	if m != nil {
		m.gaps.Add(1)
	}
}

// ObserveReplay records a replay completion/failure and its wall-clock duration.
func (m *Metrics) ObserveReplay(status string, d time.Duration) {
	if m == nil {
		return
	}
	incMap(&m.mu, m.replays, fmt.Sprintf(`status=%q`, status))
	sec := d.Seconds()
	m.mu.Lock()
	m.histCount++
	m.histSum += sec
	for i, bound := range m.histBuckets {
		if sec <= bound {
			m.histCounts[i]++
		}
	}
	m.mu.Unlock()
}

// WritePrometheus writes all metrics in Prometheus text exposition format (0.0.4).
func (m *Metrics) WritePrometheus(w io.Writer) {
	m.mu.Lock()
	httpSnap := snapshotMap(m.httpReqs)
	ingSnap := snapshotMap(m.ingested)
	procSnap := snapshotMap(m.processed)
	dupSnap := snapshotMap(m.duplicates)
	repSnap := snapshotMap(m.replays)
	buckets := append([]float64(nil), m.histBuckets...)
	counts := append([]int64(nil), m.histCounts...)
	histCount := m.histCount
	histSum := m.histSum
	m.mu.Unlock()

	var sb strings.Builder
	writeLabeled(&sb, "causal_replay_http_requests_total", "counter", "Total HTTP requests handled", httpSnap)
	writeLabeled(&sb, "causal_replay_events_ingested_total", "counter", "Total events ingested", ingSnap)
	writeLabeled(&sb, "causal_replay_events_processed_total", "counter", "Total events processed", procSnap)
	writeLabeled(&sb, "causal_replay_duplicates_detected_total", "counter", "Total duplicate events detected", dupSnap)
	writeLabeled(&sb, "causal_replay_replays_total", "counter", "Total replay sessions finished by status", repSnap)

	fmt.Fprintf(&sb, "# HELP causal_replay_retries_total Total replay event retry attempts\n")
	fmt.Fprintf(&sb, "# TYPE causal_replay_retries_total counter\n")
	fmt.Fprintf(&sb, "causal_replay_retries_total %d\n", m.retries.Load())

	fmt.Fprintf(&sb, "# HELP causal_replay_sequence_gaps_total Total out-of-order sequence gaps detected\n")
	fmt.Fprintf(&sb, "# TYPE causal_replay_sequence_gaps_total counter\n")
	fmt.Fprintf(&sb, "causal_replay_sequence_gaps_total %d\n", m.gaps.Load())

	fmt.Fprintf(&sb, "# HELP causal_replay_replay_duration_seconds Replay execution duration in seconds\n")
	fmt.Fprintf(&sb, "# TYPE causal_replay_replay_duration_seconds histogram\n")
	for i, b := range buckets {
		fmt.Fprintf(&sb, "causal_replay_replay_duration_seconds_bucket{le=\"%g\"} %d\n", b, counts[i])
	}
	fmt.Fprintf(&sb, "causal_replay_replay_duration_seconds_bucket{le=\"+Inf\"} %d\n", histCount)
	fmt.Fprintf(&sb, "causal_replay_replay_duration_seconds_sum %g\n", histSum)
	fmt.Fprintf(&sb, "causal_replay_replay_duration_seconds_count %d\n", histCount)

	_, _ = io.WriteString(w, sb.String())
}

func snapshotMap(m map[string]*atomic.Int64) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v.Load()
	}
	return out
}

func writeLabeled(sb *strings.Builder, name, kind, help string, vals map[string]int64) {
	fmt.Fprintf(sb, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
	keys := make([]string, 0, len(vals))
	for k := range vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(sb, "%s{%s} %d\n", name, k, vals[k])
	}
}
