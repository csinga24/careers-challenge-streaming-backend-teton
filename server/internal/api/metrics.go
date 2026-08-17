package api

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"teton-streaming-backend/internal/intake"
)

// These are process-wide, registered once regardless of how many Server
// instances exist (e.g. across tests in this package) — Inc doesn't
// need a Server reference, so they're plain package vars rather than
// fields threaded through every handler.
var (
	ingestTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "streaming_ingest_events_total",
		Help: "Accepted POST /events, by event type.",
	}, []string{"type"})

	ingestRejectedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "streaming_ingest_rejected_total",
		Help: "Rejected POST /events, by reason.",
	}, []string{"reason"})

	// No counter for durable-log flush failures: Log.Append is just a
	// channel send and can never observe an async flush failure (see
	// durablelog/postgres.go's flushLoop) — a counter incremented from
	// here would be unreachable dead code. A failed flush is logged at
	// Error in flushLoop itself instead.

	alarmsEmittedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "streaming_alarms_emitted_total",
		Help: "Distinct alarms emitted after fall_warn deduplication.",
	})

	// ingestDurationSeconds covers the whole POST /events handler: JSON
	// decode, validation, the intake limiter's Acquire (so it captures
	// real backpressure delay under burst, not just the store write),
	// and the synchronous store.Append. Small, sub-second buckets — a
	// healthy request is expected in the low milliseconds, not seconds.
	ingestDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "streaming_ingest_duration_seconds",
		Help:    "POST /events handler duration (decode, validate, intake limiter wait, store append).",
		Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	})

	// alarmPublishLatencySeconds is the real ingest-to-SSE-emit latency
	// the README's p95 bar (15 points) is about — measured, not assumed
	// from the flush tick's period. See latency.go: it's the gap between
	// a fall_warn event landing on POST /events and the alarm it defines
	// first going out on the live feed.
	alarmPublishLatencySeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "streaming_alarm_publish_latency_seconds",
		Help:    "Wall-clock time from a fall_warn event's ingest to its alarm's first publish on the SSE feed.",
		Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 1, 2, 5},
	})
)

// statsLoop's own running totals (server.go), kept alongside — not
// instead of — the real Prometheus counters above. prometheus.Counter
// and CounterVec don't expose a cheap "current total" read back out
// (Gather() is the supported read path, meant for a scrape, not a
// hot-path log line every 30s), so a plain atomic pairs with each Inc()
// purely for that periodic summary.
var ingestedCount, rejectedCount, alarmsEmittedCount atomic.Int64

// activeLimiter backs queueDepthCollector below. It's a package-level
// pointer, not a per-Server field, because the collector itself is
// registered once at package load (see the same reasoning as the
// counters above) — newServer just points it at whichever limiter is
// currently live.
var activeLimiter atomic.Pointer[intake.Limiter]

// queueDepthDesc/queueDepthCollector exist because the Prometheus client
// doesn't have a built-in "labeled gauge, recomputed fresh on every
// scrape" primitive the way promauto.NewGaugeFunc does for an unlabeled
// one — a custom prometheus.Collector is the standard way to get that,
// reading straight from the limiter's own Depth() so there's one source
// of truth instead of a separately-updated gauge that could drift.
var queueDepthDesc = prometheus.NewDesc(
	"streaming_intake_queue_depth",
	"Current intake limiter lane occupancy.",
	[]string{"lane"}, nil,
)

type queueDepthCollector struct{}

func (queueDepthCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- queueDepthDesc
}

func (queueDepthCollector) Collect(ch chan<- prometheus.Metric) {
	l := activeLimiter.Load()
	if l == nil {
		return
	}
	high, normal := l.Depth()
	ch <- prometheus.MustNewConstMetric(queueDepthDesc, prometheus.GaugeValue, float64(high), "high")
	ch <- prometheus.MustNewConstMetric(queueDepthDesc, prometheus.GaugeValue, float64(normal), "normal")
}

func init() {
	prometheus.MustRegister(queueDepthCollector{})
}
