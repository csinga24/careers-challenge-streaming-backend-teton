// Package logx configures the standard library's log/slog default
// logger from LOG_LEVEL.
package logx

import (
	"log/slog"
	"os"
	"strings"
)

const defaultLevel = slog.LevelInfo

// Init configures log/slog's default logger's minimum level from
// LOG_LEVEL (debug|info|warn|error, case-insensitive), falling back to
// Info if unset or invalid. Call once, at the very top of main().
func Init() {
	slog.SetLogLoggerLevel(levelFromEnv())
}

func levelFromEnv() slog.Level {
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		return slog.LevelDebug
	case "", "info":
		return defaultLevel
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		slog.Warn("invalid LOG_LEVEL, using default", "value", os.Getenv("LOG_LEVEL"), "default", defaultLevel)
		return defaultLevel
	}
}
