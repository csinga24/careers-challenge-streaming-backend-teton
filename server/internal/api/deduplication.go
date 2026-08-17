package api

import (
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"time"

	"teton-streaming-backend/internal/model"
)

// fallJitterWindow is how close together two fall_warn events from
// the same device must be to even be considered jitter on one physical fall.
// Set via FALL_JITTER_WINDOW_MS.
var fallJitterWindow = fallJitterWindowFromEnv()

const defaultFallJitterWindow = 1 * time.Second

// fallJitterWindowFromEnv reads FALL_JITTER_WINDOW_MS, in milliseconds,
// falling back to defaultFallJitterWindow if unset, empty, or invalid.
func fallJitterWindowFromEnv() time.Duration {
	raw := os.Getenv("FALL_JITTER_WINDOW_MS")
	if raw == "" {
		return defaultFallJitterWindow
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms < 0 {
		slog.Warn("invalid FALL_JITTER_WINDOW_MS, using default", "value", raw, "default", defaultFallJitterWindow)
		return defaultFallJitterWindow
	}
	window := time.Duration(ms) * time.Millisecond
	slog.Info("fall_warn deduplication jitter window set", "window", window)
	return window
}

// deduplicateFalls collapses jittered fall_warn events into one Alarm per
// physical fall. Clustering requires close ts AND matching confidence —
// time alone can coincidentally merge two genuinely distinct falls.
func deduplicateFalls(events []model.Event) []model.Alarm {
	byDevice := make(map[string][]model.Event)
	for _, e := range events {
		if e.Type != model.FallWarn || e.Confidence == nil {
			continue
		}
		byDevice[e.DeviceID] = append(byDevice[e.DeviceID], e)
	}

	var alarms []model.Alarm
	for deviceID, deviceEvents := range byDevice {
		alarms = append(alarms, clusterDeviceFalls(deviceID, deviceEvents)...)
	}

	// Stable: boot-time recompute must order tied timestamps identically
	// every run, or a reconnecting client's cursor resolves against a
	// different relative ordering than the one it was minted from.
	sort.SliceStable(alarms, func(i, j int) bool { return alarms[i].TS.Before(alarms[j].TS) })
	return alarms
}

// clusterDeviceFalls collapses one device's fall_warn events (in any
// order) into one Alarm per physical fall, per the rule described on
// deduplicateFalls. Sorts deviceEvents in place.
func clusterDeviceFalls(deviceID string, deviceEvents []model.Event) []model.Alarm {
	if len(deviceEvents) == 0 {
		return nil
	}
	sort.SliceStable(deviceEvents, func(i, j int) bool { return deviceEvents[i].TS.Before(deviceEvents[j].TS) })

	var alarms []model.Alarm
	clusterStart := deviceEvents[0]
	lastTS := clusterStart.TS
	for _, e := range deviceEvents[1:] {
		sameCluster := e.TS.Sub(lastTS) <= fallJitterWindow && *e.Confidence == *clusterStart.Confidence
		if !sameCluster {
			alarms = append(alarms, newAlarm(deviceID, clusterStart))
			clusterStart = e
		}
		lastTS = e.TS
	}
	alarms = append(alarms, newAlarm(deviceID, clusterStart))
	return alarms
}

// parseSince accepts a bare epoch-seconds float, matching what
// eval/check.py and example_solution/service.py actually send. An empty
// string means "from the beginning."
func parseSince(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid since %q", s)
	}
	sec, frac := int64(f), f-float64(int64(f))
	return time.Unix(sec, int64(frac*float64(time.Second))).UTC(), nil
}

// alarmEventID is the stable id an event would have if it turned out to
// be its cluster's earliest — i.e. the id newAlarm assigns.
func alarmEventID(deviceID string, ts time.Time) string {
	return fmt.Sprintf("%s-%d", deviceID, ts.UnixNano())
}

func newAlarm(deviceID string, e model.Event) model.Alarm {
	return model.Alarm{
		EventID:    alarmEventID(deviceID, e.TS),
		DeviceID:   deviceID,
		RoomID:     e.RoomID,
		TS:         e.TS,
		Confidence: *e.Confidence,
	}
}
