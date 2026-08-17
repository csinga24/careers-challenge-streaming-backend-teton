package api

import (
	"fmt"
	"sort"
	"time"

	"teton-streaming-backend/internal/model"
)

type roomOccupancy struct {
	InRoom      bool    `json:"in_room"`
	OccupiedPct float64 `json:"occupied_pct"`
}

// computeRoomOccupancy merges a room's presence events into a ts-ordered
// timeline, then walks it to compute the fraction of window occupied.
func computeRoomOccupancy(events []model.Event, window time.Duration, now time.Time) roomOccupancy {
	presence := make([]model.Event, 0, len(events))
	for _, e := range events {
		if e.Type == model.Presence && e.InRoom != nil {
			presence = append(presence, e)
		}
	}
	if len(presence) == 0 {
		return roomOccupancy{InRoom: false, OccupiedPct: 0}
	}

	sort.Slice(presence, func(i, j int) bool { return presence[i].TS.Before(presence[j].TS) })
	currentInRoom := *presence[len(presence)-1].InRoom

	start := now.Add(-window)
	occupied := occupiedDuration(presence, start, now)
	pct := float64(occupied) / float64(window)
	switch {
	case pct > 1:
		pct = 1
	case pct < 0:
		pct = 0
	}
	return roomOccupancy{InRoom: currentInRoom, OccupiedPct: pct}
}

// occupiedDuration walks a ts-sorted presence timeline and sums how much
// was spent with in room.
func occupiedDuration(sortedPresence []model.Event, start, end time.Time) time.Duration {
	state := false
	i := 0
	for i < len(sortedPresence) && !sortedPresence[i].TS.After(start) {
		state = *sortedPresence[i].InRoom
		i++
	}

	var occupied time.Duration
	cursor := start
	for ; i < len(sortedPresence); i++ {
		e := sortedPresence[i]
		if e.TS.After(end) {
			break
		}
		if state {
			occupied += e.TS.Sub(cursor)
		}
		cursor = e.TS
		state = *e.InRoom
	}
	if state && cursor.Before(end) {
		occupied += end.Sub(cursor)
	}
	return occupied
}

// parseWindow accepts the "1m|5m|1h"-style values.
func parseWindow(s string) (time.Duration, error) {
	if s == "" {
		return 5 * time.Minute, nil
	}
	var n int
	var unit string
	if _, err := fmt.Sscanf(s, "%d%s", &n, &unit); err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid window %q", s)
	}
	switch unit {
	case "m":
		return time.Duration(n) * time.Minute, nil
	case "h":
		return time.Duration(n) * time.Hour, nil
	default:
		return 0, fmt.Errorf("invalid window %q", s)
	}
}
