// Package api exposes the event-intake and read endpoints
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"teton-streaming-backend/internal/durablelog"
	"teton-streaming-backend/internal/intake"
	"teton-streaming-backend/internal/store"
)

type Server struct {
	mux           *http.ServeMux
	store         store.Store
	durableLog    durablelog.Log // nil if running without one (tests, no DATABASE_URL)
	broker        *alarmBroker
	limiter       *intake.Limiter
	deduplication *alarmDeduplicationEngine
	fallReceipts  *fallReceiptTracker
}

// New builds a Server backed by an in-memory store, with no durable log —
// state doesn't survive a restart. Used by tests and when DATABASE_URL
// isn't set.
func New() *Server {
	return NewWithStore(store.NewMemoryStore())
}

// NewWithStore builds a Server backed by any Store implementation, with
// no durable log.
func NewWithStore(st store.Store) *Server {
	return newServer(st, nil)
}

// NewWithDurableLog builds a Server whose accepted events are also
// written to a durable log (async, behind the synchronous store write),
// for restart correctness. st should already be populated by replaying
// log before this is called, so reads are correct from the first request.
func NewWithDurableLog(st store.Store, log durablelog.Log) *Server {
	return newServer(st, log)
}

func newServer(st store.Store, log durablelog.Log) *Server {
	receipts := newFallReceiptTracker()
	s := &Server{
		mux:           http.NewServeMux(),
		store:         st,
		durableLog:    log,
		broker:        newAlarmBroker(receipts),
		limiter:       intake.NewLimiter(intake.DefaultHighCapacity, intake.DefaultNormalCapacity),
		deduplication: newAlarmDeduplicationEngine(),
		fallReceipts:  receipts,
	}
	activeLimiter.Store(s.limiter) // backs the streaming_intake_queue_depth gauge (metrics.go)
	s.routes()
	go s.flushAlarmsLoop()
	go s.statsLoop()
	return s
}

// statsLoop periodically logs ingest rate, reject rate, queue depth, and
// alarms emitted — a human-readable complement to /metrics, not a
// replacement for it.
const statsLogInterval = 30 * time.Second

func (s *Server) statsLoop() {
	ticker := time.NewTicker(statsLogInterval)
	defer ticker.Stop()

	var lastIngested, lastRejected int64
	for range ticker.C {
		ingested := ingestedCount.Load()
		rejected := rejectedCount.Load()
		high, normal := s.limiter.Depth()
		slog.Info("stats",
			"ingest_rate_per_sec", float64(ingested-lastIngested)/statsLogInterval.Seconds(),
			"reject_rate_per_sec", float64(rejected-lastRejected)/statsLogInterval.Seconds(),
			"queue_depth_high", high,
			"queue_depth_normal", normal,
			"alarms_emitted", alarmsEmittedCount.Load())
		lastIngested, lastRejected = ingested, rejected
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("POST /events", s.handleEventIntake)
	s.mux.HandleFunc("GET /devices/{device_id}/health", s.handleDeviceHealth)
	s.mux.HandleFunc("GET /rooms/{room_id}/occupancy", s.handleRoomOccupancy)
	s.mux.HandleFunc("GET /alarms", s.handleAlarms)
	s.mux.HandleFunc("GET /alarms/stream", s.handleAlarmsStream)
	s.mux.Handle("GET /metrics", promhttp.Handler())
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Warn("write response failed", "error", err) // usually a client disconnect, not actionable server-side
	}
}
