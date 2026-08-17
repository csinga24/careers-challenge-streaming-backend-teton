package api

import (
	"log/slog"
	"os"
	"strconv"
	"time"

	"teton-streaming-backend/internal/model"
)

const availabilityWindow = 5 * time.Minute

// defaultExpectedHeartbeats5 assumes ~1Hz over the 5-minute availability
// window. Set via EXPECTED_HEARTBEATS_5M if the actual device heartbeat
// rate differs — a mismatch here systematically caps every healthy
// device's reported availability.
const defaultExpectedHeartbeats5 = 300

// expectedHeartbeats5 is the heartbeat count in availabilityWindow a
// fully-available device is expected to produce; computeDeviceHealth
// divides observed count by this to get Availability5m.
var expectedHeartbeats5 = expectedHeartbeats5FromEnv()

// expectedHeartbeats5FromEnv reads EXPECTED_HEARTBEATS_5M, falling back
// to defaultExpectedHeartbeats5 if unset, empty, or invalid. Mirrors
// fallJitterWindowFromEnv's pattern (deduplication.go).
func expectedHeartbeats5FromEnv() float64 {
	raw := os.Getenv("EXPECTED_HEARTBEATS_5M")
	if raw == "" {
		return defaultExpectedHeartbeats5
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		slog.Warn("invalid EXPECTED_HEARTBEATS_5M, using default", "value", raw, "default", defaultExpectedHeartbeats5)
		return defaultExpectedHeartbeats5
	}
	slog.Info("expected heartbeats per 5m window set", "count", n)
	return float64(n)
}

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
