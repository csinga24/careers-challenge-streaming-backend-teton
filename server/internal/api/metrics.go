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

	durableLogFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "streaming_durable_log_append_failures_total",
		Help: "Durable log append failures (event accepted, kept in memory, not yet durable).",
	})

	alarmsEmittedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "streaming_alarms_emitted_total",
		Help: "Distinct alarms emitted after fall_warn dedup.",
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
