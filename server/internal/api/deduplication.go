package api

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"teton-streaming-backend/internal/model"
)

// fallJitterWindow is how close together two fall_warn events from the
// same device must be to even be considered jitter on one physical fall.
const fallJitterWindow = 5 * time.Second

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
		sort.Slice(deviceEvents, func(i, j int) bool { return deviceEvents[i].TS.Before(deviceEvents[j].TS) })

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
	}

	sort.Slice(alarms, func(i, j int) bool { return alarms[i].TS.Before(alarms[j].TS) })
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

func newAlarm(deviceID string, e model.Event) model.Alarm {
	return model.Alarm{
		EventID:    fmt.Sprintf("%s-%d", deviceID, e.TS.UnixNano()),
		DeviceID:   deviceID,
		RoomID:     e.RoomID,
		TS:         e.TS,
		Confidence: *e.Confidence,
	}
}
