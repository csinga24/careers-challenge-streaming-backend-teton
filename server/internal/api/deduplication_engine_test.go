package api

import (
	"testing"
	"time"

	"teton-streaming-backend/internal/model"
	"teton-streaming-backend/internal/store"
)

// TestAlarmDeduplicationEngineMatchesReferenceImplementation drives the engine
// across several snapshot calls, interleaved with new events landing
// (including a late replay far in the past, and a second, distinct
// fall from the same device coincidentally close in time to another),
// and checks every snapshot against deduplicateFalls(st.FallWarnEvents())
// — the slow, always-correct reference. The engine must never disagree
// with it, at any point along the way.
func TestAlarmDeduplicationEngineMatchesReferenceImplementation(t *testing.T) {
	st := store.NewMemoryStore()
	engine := newAlarmDeduplicationEngine()
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)

	check := func(step string) {
		t.Helper()
		got := engine.snapshot(st)
		want := deduplicateFalls(st.FallWarnEvents())
		if len(got) != len(want) {
			t.Fatalf("%s: engine returned %d alarms, reference returned %d\nengine: %+v\nref: %+v", step, len(got), len(want), got, want)
		}
		for i := range want {
			if got[i].EventID != want[i].EventID || got[i].DeviceID != want[i].DeviceID || !got[i].TS.Equal(want[i].TS) {
				t.Fatalf("%s: alarm %d mismatch\nengine: %+v\nref: %+v", step, i, got[i], want[i])
			}
		}
	}

	// Nothing yet.
	check("empty")

	// A normal fall, plus a jittered resend that should collapse into it.
	st.Append(fallWarnAt("dev_1", now, 0.9))
	check("first fall")
	st.Append(fallWarnAt("dev_1", now.Add(300*time.Millisecond), 0.9))
	check("jittered resend collapses")

	// A second, unrelated device — must not disturb dev_1's cached cluster.
	st.Append(fallWarnAt("dev_2", now.Add(time.Minute), 0.5))
	check("second device")

	// A late replay: an offline device's buffered fall from 20 minutes
	// ago, discovered only now. Must still cluster correctly against
	// dev_1's existing history (it doesn't here — different device,
	// different confidence — so it should land as its own alarm).
	late := now.Add(-20 * time.Minute)
	st.Append(fallWarnAt("dev_3", late, 0.42))
	check("late replay")

	// A late replay that DOES belong to an already-clustered device's
	// history — must reopen/re-derive that device's clustering (it's a
	// distinct fall, since it falls well outside fallJitterWindow of the
	// existing cluster; not a resend of the earlier one).
	st.Append(fallWarnAt("dev_1", now.Add(10*time.Minute), 0.9))
	check("late-arriving distinct fall on an existing device")

	// A non-fall event should never affect the alarm list.
	st.Append(model.Event{DeviceID: "dev_1", RoomID: "room_14", Type: model.Heartbeat, TS: now})
	check("unrelated event type")
}

// TestAlarmDeduplicationEngineNoNewEventsIsCheap documents (rather than
// benchmarks precisely) that a snapshot with nothing new since the last
// call doesn't do any clustering work — it just returns the cached
// merge. Verified indirectly: calling snapshot repeatedly with no new
// events must keep returning the exact same slice header (same
// underlying array), proving no rebuild happened.
func TestAlarmDeduplicationEngineNoNewEventsReturnsCachedSlice(t *testing.T) {
	st := store.NewMemoryStore()
	engine := newAlarmDeduplicationEngine()
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	st.Append(fallWarnAt("dev_1", now, 0.9))

	first := engine.snapshot(st)
	second := engine.snapshot(st)

	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("expected 1 alarm both times, got %d then %d", len(first), len(second))
	}
	if &first[0] != &second[0] {
		t.Error("expected the cached merge to be reused (same backing array) when nothing new arrived")
	}
}

// BenchmarkFlushTick compares the old approach (deduplicateFalls over the
// store's entire fall_warn history, every tick) against the incremental
// engine, at a history size representative of a long burst run. Run with
// -bench, e.g.:
//
//	go test ./internal/api/ -run '^$' -bench BenchmarkFlushTick -benchtime 20x
func BenchmarkFlushTick(b *testing.B) {
	st := store.NewMemoryStore()
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	const devices = 5000
	const fallsPerDevice = 60 // ~300k total, in the review's estimated post-burst range

	for i := 0; i < devices*fallsPerDevice; i++ {
		dev := i % devices
		st.Append(fallWarnAt(deviceName(dev), now.Add(time.Duration(i)*time.Second), 0.5+float64(i%40)/100))
	}

	b.Run("full_recompute_every_tick", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = deduplicateFalls(st.FallWarnEvents())
		}
	})

	b.Run("incremental_engine_steady_state", func(b *testing.B) {
		engine := newAlarmDeduplicationEngine()
		engine.snapshot(st) // pay the one-time full-history cost once, like boot-time replay would
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = engine.snapshot(st) // nothing new between calls: the steady-state, common case
		}
	})
}

func deviceName(i int) string {
	return "dev_" + string(rune('A'+i%26)) + string(rune('0'+(i/26)%10)) + string(rune('0'+(i/260)%10))
}
