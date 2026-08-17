package api

import (
	"sort"
	"sync"

	"teton-streaming-backend/internal/model"
	"teton-streaming-backend/internal/store"
)

// alarmDeduplicationEngine incrementally maintains the same result
// deduplicateFalls(store.FallWarnEvents()) computes, without paying the
// full O(every fall_warn event ever accepted) cost on every call.
type alarmDeduplicationEngine struct {
	mu        sync.Mutex
	rawOffset int
	byDevice  map[string][]model.Event // full raw fall_warn history per device, append-only
	alarms    map[string][]model.Alarm // cached clustered alarms per device
	merged    []model.Alarm            // cached ts-sorted merge across all devices
}

func newAlarmDeduplicationEngine() *alarmDeduplicationEngine {
	return &alarmDeduplicationEngine{
		byDevice: make(map[string][]model.Event),
		alarms:   make(map[string][]model.Alarm),
	}
}

// snapshot folds in any raw fall_warn events appended since the last
// call and returns the full, freshly merged, ts-sorted alarm list — the
// same value deduplicateFalls(st.FallWarnEvents()) would return.
func (e *alarmDeduplicationEngine) snapshot(st store.Store) []model.Alarm {
	e.mu.Lock()
	defer e.mu.Unlock()

	newEvents, total := st.FallWarnEventsFrom(e.rawOffset)
	e.rawOffset = total
	if len(newEvents) == 0 {
		return e.merged
	}

	changed := make(map[string]bool)
	for _, ev := range newEvents {
		if ev.Confidence == nil {
			continue
		}
		e.byDevice[ev.DeviceID] = append(e.byDevice[ev.DeviceID], ev)
		changed[ev.DeviceID] = true
	}
	for deviceID := range changed {
		e.alarms[deviceID] = clusterDeviceFalls(deviceID, e.byDevice[deviceID])
	}

	merged := make([]model.Alarm, 0, len(e.merged)+len(newEvents))
	for _, alarms := range e.alarms {
		merged = append(merged, alarms...)
	}
	// Stable for the same reason as deduplicateFalls: restart-time
	// recompute must break timestamp ties identically every run.
	sort.SliceStable(merged, func(i, j int) bool { return merged[i].TS.Before(merged[j].TS) })
	e.merged = merged
	return e.merged
}
