package api

import (
	"time"

	"teton-streaming-backend/internal/model"
)

const (
	availabilityWindow  = 5 * time.Minute
	expectedHeartbeats5 = 300 // ~1Hz over 5 minutes
)

type deviceHealth struct {
	LastHeartbeatTS *time.Time `json:"last_heartbeat_ts"`
	Availability5m  float64    `json:"availability_5m"`
}

// computeDeviceHealth scans a device's events once for the latest
// heartbeat ts and the count of heartbeats within the last 5 minutes.
// events need not be in any particular order.
func computeDeviceHealth(events []model.Event, now time.Time) deviceHealth {
	windowStart := now.Add(-availabilityWindow)

	var latest *time.Time
	count := 0
	for _, e := range events {
		if e.Type != model.Heartbeat {
			continue
		}
		if latest == nil || e.TS.After(*latest) {
			ts := e.TS
			latest = &ts
		}
		if !e.TS.Before(windowStart) && !e.TS.After(now) {
			count++
		}
	}

	availability := float64(count) / expectedHeartbeats5
	if availability > 1 {
		availability = 1
	}
	return deviceHealth{LastHeartbeatTS: latest, Availability5m: availability}
}
