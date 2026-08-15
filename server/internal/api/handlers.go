package api

import "net/http"

func (s *Server) handleEventIntake(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
}

func (s *Server) handleDeviceHealth(w http.ResponseWriter, r *http.Request) {
	_ = r.PathValue("device_id")
	writeJSON(w, http.StatusOK, map[string]any{
		"last_heartbeat_ts": nil,
		"availability_5m":   0.0,
	})
}

func (s *Server) handleRoomOccupancy(w http.ResponseWriter, r *http.Request) {
	_ = r.PathValue("room_id")
	_ = r.URL.Query().Get("window")
	writeJSON(w, http.StatusOK, map[string]any{
		"in_room":      false,
		"occupied_pct": 0.0,
	})
}

func (s *Server) handleAlarms(w http.ResponseWriter, r *http.Request) {
	_ = r.URL.Query().Get("since")
	writeJSON(w, http.StatusOK, map[string]any{
		"alarms": []any{},
	})
}
