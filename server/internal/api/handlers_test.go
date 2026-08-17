package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"teton-streaming-backend/internal/model"
)

func postEvent(t *testing.T, s *Server, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func TestHandleEventIntake(t *testing.T) {
	now := time.Now().UTC().Format(time.RFC3339Nano)

	cases := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "valid heartbeat",
			body:       `{"device_id":"dev_0001","room_id":"room_14","type":"heartbeat","ts":"` + now + `","seq":1}`,
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "invalid json",
			body:       `not json`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing device_id",
			body:       `{"room_id":"room_14","type":"heartbeat","ts":"` + now + `"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "ts too far in the future",
			body:       `{"device_id":"dev_0001","room_id":"room_14","type":"heartbeat","ts":"2099-01-01T00:00:00Z"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := New()
			rec := postEvent(t, s, tc.body)
			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d (body %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
		})
	}
}

func TestHandleEventIntakeStoresAcceptedEvent(t *testing.T) {
	s := New()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	rec := postEvent(t, s, `{"device_id":"dev_0001","room_id":"room_14","type":"heartbeat","ts":"`+now+`","seq":1}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body %s)", rec.Code, rec.Body.String())
	}
	if got := s.store.Events("dev_0001"); len(got) != 1 {
		t.Fatalf("expected 1 stored event, got %d", len(got))
	}
}

func TestHandleDeviceHealthUnknownDevice(t *testing.T) {
	s := New()
	req := httptest.NewRequest(http.MethodGet, "/devices/dev_never_seen/health", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid json body: %v", err)
	}
	if body["last_heartbeat_ts"] != nil {
		t.Errorf("expected last_heartbeat_ts null for unseen device, got %v", body["last_heartbeat_ts"])
	}
	if body["availability_5m"] != 0.0 {
		t.Errorf("expected availability_5m 0 for unseen device, got %v", body["availability_5m"])
	}
}

func TestHandleDeviceHealthReflectsPostedHeartbeat(t *testing.T) {
	s := New()
	now := time.Now().UTC()
	postEvent(t, s, `{"device_id":"dev_0001","room_id":"room_14","type":"heartbeat","ts":"`+now.Format(time.RFC3339Nano)+`","seq":1}`)

	req := httptest.NewRequest(http.MethodGet, "/devices/dev_0001/health", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid json body: %v", err)
	}
	if body["last_heartbeat_ts"] == nil {
		t.Fatalf("expected non-null last_heartbeat_ts")
	}
	avail, _ := body["availability_5m"].(float64)
	if avail <= 0 {
		t.Errorf("expected positive availability_5m after posting a heartbeat, got %v", avail)
	}
}

func TestHandleRoomOccupancyUnknownRoom(t *testing.T) {
	s := New()
	req := httptest.NewRequest(http.MethodGet, "/rooms/room_never_seen/occupancy?window=1m", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid json body: %v", err)
	}
	if body["in_room"] != false {
		t.Errorf("expected in_room false for unseen room, got %v", body["in_room"])
	}
	if body["occupied_pct"] != 0.0 {
		t.Errorf("expected occupied_pct 0 for unseen room, got %v", body["occupied_pct"])
	}
}

func TestHandleRoomOccupancyReflectsPostedPresence(t *testing.T) {
	s := New()
	now := time.Now().UTC()
	postEvent(t, s, `{"device_id":"dev_0001","room_id":"room_14","type":"presence","ts":"`+now.Format(time.RFC3339Nano)+`","seq":1,"in_room":true}`)

	req := httptest.NewRequest(http.MethodGet, "/rooms/room_14/occupancy?window=1m", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid json body: %v", err)
	}
	if body["in_room"] != true {
		t.Errorf("expected in_room true after posting presence, got %v", body["in_room"])
	}
	pct, _ := body["occupied_pct"].(float64)
	if pct <= 0 {
		t.Errorf("expected positive occupied_pct, got %v", pct)
	}
}

func TestHandleRoomOccupancyInvalidWindow(t *testing.T) {
	s := New()
	req := httptest.NewRequest(http.MethodGet, "/rooms/room_14/occupancy?window=bogus", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func getAlarms(t *testing.T, s *Server, query string) (*httptest.ResponseRecorder, []model.Alarm) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/alarms"+query, nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	var body struct {
		Alarms []model.Alarm `json:"alarms"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid json body: %v (raw: %s)", err, rec.Body.String())
	}
	return rec, body.Alarms
}

func TestHandleAlarmsEmpty(t *testing.T) {
	s := New()
	rec, alarms := getAlarms(t, s, "?since=0")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if alarms == nil {
		t.Errorf("expected alarms to be an empty array, got null")
	}
	if len(alarms) != 0 {
		t.Errorf("expected 0 alarms, got %d", len(alarms))
	}
}

func TestHandleAlarmsDeduplicatesJitteredFalls(t *testing.T) {
	s := New()
	now := time.Now().UTC()
	// Two fall_warn events from the same device, a second apart, with the
	// same confidence — sensor jitter resending the same physical fall.
	postEvent(t, s, `{"device_id":"dev_0001","room_id":"room_14","type":"fall_warn","ts":"`+now.Format(time.RFC3339Nano)+`","seq":1,"confidence":0.8}`)
	postEvent(t, s, `{"device_id":"dev_0001","room_id":"room_14","type":"fall_warn","ts":"`+now.Add(time.Second).Format(time.RFC3339Nano)+`","seq":2,"confidence":0.8}`)

	_, alarms := getAlarms(t, s, "?since=0")
	if len(alarms) != 1 {
		t.Fatalf("expected 1 deduplicated alarm, got %d: %+v", len(alarms), alarms)
	}
	if alarms[0].EventID == "" {
		t.Errorf("expected non-empty event_id")
	}
}

func TestHandleAlarmsSinceFilters(t *testing.T) {
	s := New()
	// Comfortably inside the 1h acceptance window, with margin so clock
	// drift during the test can't tip it over the boundary.
	base := time.Now().UTC().Add(-50 * time.Minute)
	rec1 := postEvent(t, s, `{"device_id":"dev_0001","room_id":"room_14","type":"fall_warn","ts":"`+base.Format(time.RFC3339Nano)+`","seq":1,"confidence":0.8}`)
	if rec1.Code != http.StatusAccepted {
		t.Fatalf("first post status = %d, want 202 (body %s)", rec1.Code, rec1.Body.String())
	}
	rec2 := postEvent(t, s, `{"device_id":"dev_0002","room_id":"room_15","type":"fall_warn","ts":"`+base.Add(10*time.Minute).Format(time.RFC3339Nano)+`","seq":1,"confidence":0.9}`)
	if rec2.Code != http.StatusAccepted {
		t.Fatalf("second post status = %d, want 202 (body %s)", rec2.Code, rec2.Body.String())
	}

	_, all := getAlarms(t, s, "?since=0")
	if len(all) != 2 {
		t.Fatalf("expected 2 alarms with since=0, got %d", len(all))
	}

	sinceMidpoint := fmt.Sprintf("%.3f", float64(base.Add(5*time.Minute).UnixNano())/1e9)
	_, after := getAlarms(t, s, "?since="+sinceMidpoint)
	if len(after) != 1 {
		t.Fatalf("expected 1 alarm after the midpoint cursor, got %d", len(after))
	}
	if after[0].DeviceID != "dev_0002" {
		t.Errorf("expected the later alarm (dev_0002), got %+v", after[0])
	}
}

func TestHandleAlarmsInvalidSince(t *testing.T) {
	s := New()
	rec, _ := getAlarms(t, s, "?since=not-a-time")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestUnknownRoute(t *testing.T) {
	s := New()
	req := httptest.NewRequest(http.MethodGet, "/nonexistent", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}
