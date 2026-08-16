package logx

import (
	"context"
	"log/slog"
	"testing"
)

func TestLevelFromEnv(t *testing.T) {
	cases := []struct {
		name string
		env  string
		want slog.Level
	}{
		{"empty defaults to info", "", slog.LevelInfo},
		{"debug", "debug", slog.LevelDebug},
		{"DEBUG uppercase", "DEBUG", slog.LevelDebug},
		{"info explicit", "info", slog.LevelInfo},
		{"warn", "warn", slog.LevelWarn},
		{"warning alias", "warning", slog.LevelWarn},
		{"error", "error", slog.LevelError},
		{"invalid falls back to info", "not-a-level", slog.LevelInfo},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("LOG_LEVEL", tc.env)
			if got := levelFromEnv(); got != tc.want {
				t.Errorf("levelFromEnv() with LOG_LEVEL=%q = %v, want %v", tc.env, got, tc.want)
			}
		})
	}
}

func TestInitSetsDefaultLoggerLevel(t *testing.T) {
	t.Setenv("LOG_LEVEL", "error")
	Init()
	t.Cleanup(func() { slog.SetLogLoggerLevel(defaultLevel) })

	if !slog.Default().Enabled(context.Background(), slog.LevelError) {
		t.Error("expected Error to be enabled after Init() with LOG_LEVEL=error")
	}
	if slog.Default().Enabled(context.Background(), slog.LevelWarn) {
		t.Error("expected Warn to be disabled after Init() with LOG_LEVEL=error")
	}
}
