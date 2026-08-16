package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	"teton-streaming-backend/internal/api"
	"teton-streaming-backend/internal/durablelog"
	"teton-streaming-backend/internal/logx"
	"teton-streaming-backend/internal/model"
	"teton-streaming-backend/internal/store"
)

func main() {
	logx.Init() // configures log/slog's default logger level from LOG_LEVEL

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	srv := newServer()

	slog.Info("listening", "port", port)
	if err := http.ListenAndServe(":"+port, srv); err != nil {
		log.Fatal(err) // process can't run at all; the level gate isn't relevant to a fatal exit
	}
}

// newServer wires up storage: with DATABASE_URL set, accepted events are
// durably logged to Postgres and replayed into the in-memory store on
// boot, so state survives a restart. Without it, the service runs
// in-memory only — used by tests and local runs that don't need restart
// correctness.
func newServer() *api.Server {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		slog.Warn("DATABASE_URL not set: running in-memory only, state will not survive a restart")
		return api.New()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dlog, err := durablelog.OpenPostgres(ctx, databaseURL)
	if err != nil {
		log.Fatalf("open durable log: %v", err)
	}

	memStore := store.NewMemoryStore()
	start := time.Now()
	count := 0
	if err := dlog.Replay(func(e model.Event) error {
		memStore.Append(e)
		count++
		return nil
	}); err != nil {
		log.Fatalf("replay durable log: %v", err)
	}
	slog.Info("replayed durable log", "events", count, "duration", time.Since(start))

	return api.NewWithDurableLog(memStore, dlog)
}
