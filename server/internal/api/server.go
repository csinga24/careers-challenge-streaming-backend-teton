// Package api exposes the event-intake and read endpoints
package api

import (
	"encoding/json"
	"log"
	"net/http"

	"teton-streaming-backend/internal/durablelog"
	"teton-streaming-backend/internal/intake"
	"teton-streaming-backend/internal/store"
)

type Server struct {
	mux        *http.ServeMux
	store      store.Store
	durableLog durablelog.Log // nil if running without one (tests, no DATABASE_URL)
	broker     *alarmBroker
	limiter    *intake.Limiter
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
	s := &Server{
		mux:        http.NewServeMux(),
		store:      st,
		durableLog: log,
		broker:     newAlarmBroker(),
		limiter:    intake.NewLimiter(intake.DefaultHighCapacity, intake.DefaultNormalCapacity),
	}
	s.routes()
	go s.flushAlarmsLoop()
	return s
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
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("write response: %v", err)
	}
}
