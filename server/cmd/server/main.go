package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"teton-streaming-backend/internal/api"
	"teton-streaming-backend/internal/durablelog"
	"teton-streaming-backend/internal/model"
	"teton-streaming-backend/internal/store"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	srv := newServer()

	log.Printf("listening on :%s", port)
	if err := http.ListenAndServe(":"+port, srv); err != nil {
		log.Fatal(err)
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
		log.Print("DATABASE_URL not set: running in-memory only, state will not survive a restart")
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
	log.Printf("replayed %d events from durable log in %v", count, time.Since(start))

	return api.NewWithDurableLog(memStore, dlog)
}
