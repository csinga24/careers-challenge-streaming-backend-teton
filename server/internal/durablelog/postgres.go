package durablelog

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"teton-streaming-backend/internal/model"
)

const (
	pgBatchSize = 500
	// pgFlushInterval bounds how many recently-accepted events can be
	// lost to an unclean kill before they're durably written.
	pgFlushInterval = 10 * time.Millisecond
	// pgFlushRetries is how many times a failed CopyFrom is retried
	// before the batch is logged, counted, and dropped. Transient
	// (connection blip, brief Postgres unavailability) errors get a
	// chance to clear; a batch is only ever actually lost after all of
	// these fail.
	pgFlushRetries      = 3
	pgFlushRetryBackoff = 25 * time.Millisecond
)

// PostgresLog is a durablelog.Log backed by Postgres.
type PostgresLog struct {
	pool   *pgxpool.Pool
	buffer chan model.Event
	done   chan struct{}
}

func OpenPostgres(ctx context.Context, connString string) (*PostgresLog, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS events (
			seq        bigserial PRIMARY KEY,
			payload    jsonb NOT NULL
		)`); err != nil {
		pool.Close()
		return nil, err
	}

	l := &PostgresLog{
		pool:   pool,
		buffer: make(chan model.Event, pgBatchSize*4),
		done:   make(chan struct{}),
	}
	go l.flushLoop()
	return l, nil
}

func (l *PostgresLog) Append(e model.Event) error {
	l.buffer <- e
	return nil
}

func (l *PostgresLog) flushLoop() {
	batch := make([]model.Event, 0, pgBatchSize)
	ticker := time.NewTicker(pgFlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		rows := make([][]any, len(batch))
		for i, e := range batch {
			payload, _ := json.Marshal(e)
			rows[i] = []any{payload}
		}

		var err error
		for attempt := 0; attempt <= pgFlushRetries; attempt++ {
			if attempt > 0 {
				time.Sleep(pgFlushRetryBackoff * time.Duration(attempt))
			}
			_, err = l.pool.CopyFrom(context.Background(),
				pgx.Identifier{"events"}, []string{"payload"}, pgx.CopyFromRows(rows))
			if err == nil {
				break
			}
		}
		if err != nil {
			// Every retry failed: these events are durably gone. They're
			// still served from the in-memory store for the life of this
			// process, but won't survive a restart. Logged, not swallowed.
			slog.Error("durable log flush failed after retries, batch dropped",
				"error", err, "events_lost", len(batch), "attempts", pgFlushRetries+1)
		}
		batch = batch[:0]
	}

	for {
		select {
		case e, ok := <-l.buffer:
			if !ok {
				flush()
				close(l.done)
				return
			}
			batch = append(batch, e)
			if len(batch) >= pgBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (l *PostgresLog) Replay(fn func(model.Event) error) error {
	ctx := context.Background()
	rows, err := l.pool.Query(ctx, `SELECT payload FROM events ORDER BY seq`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return err
		}
		var e model.Event
		if err := json.Unmarshal(payload, &e); err != nil {
			return err
		}
		if err := fn(e); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (l *PostgresLog) Close() error {
	close(l.buffer)
	<-l.done
	l.pool.Close()
	return nil
}
