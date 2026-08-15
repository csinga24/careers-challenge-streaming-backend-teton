// Package api exposes the event-intake and read endpoints
package api

import (
	"encoding/json"
	"log"
	"net/http"

	"teton-streaming-backend/internal/intake"
	"teton-streaming-backend/internal/store"
)

type Server struct {
	mux     *http.ServeMux
	store   *store.MemoryStore
	broker  *alarmBroker
	limiter *intake.Limiter
}

func New() *Server {
	s := &Server{
		mux:     http.NewServeMux(),
		store:   store.NewMemoryStore(),
		broker:  newAlarmBroker(),
		limiter: intake.NewLimiter(intake.DefaultHighCapacity, intake.DefaultNormalCapacity),
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
