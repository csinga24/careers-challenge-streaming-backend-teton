# Submission, Real-time Streaming Backend

**Your name:**
**Email:**
**Link to your fork or solution:**

---

## Stack and storage

The service is written in Go, chosen for its concurrency primitives
(goroutines/channels), which fit an event-driven ingest workload, for
compiling to a single static binary that's simple to run and containerize,
and because it's a language I already have experience with.

Events are received over plain HTTP POST to `/events`, and the three
required read endpoints are exposed as HTTP GET routes. HTTP was picked
because it's the transport the event generator already speaks and needs no
extra client/broker setup to test against.

For the durable write-ahead log behind it, Postgres is the pick. A
bake-off benchmarked it against BoltDB, SQLite, and a hand-rolled flat
file; SQLite came close on raw write throughput, but that case rested on
avoiding Docker as an extra dependency — since Docker is already being
added for deployment regardless of storage choice, that advantage
disappears. With that constraint gone, Postgres wins outright: highest
write throughput, mature operational tooling, and the natural fit if
this ever needs multiple app instances sharing one durable store, which
SQLite doesn't support gracefully.

The real-time alarm feed uses SSE over WebSocket/long-poll since it's
one-directional, runs over plain HTTP, and gets a standards-based resume
mechanism for free.

`FALL_JITTER_WINDOW_MS` is an env var, not a hardcoded const: the README gives 
no exact number for fall-jitter spacing ("within a few seconds"), so this value 
can't be measured against real-world behavior today, only guessed at — an env var 
means that guess can be corrected with a config change and a container restart 
once real feedback comes in, instead of a rebuild and resubmission.

## Ordering and late events

Writes append in arrival order rather than sorting by `ts`, since a
sorted insert's occasional shift would hold the store's single lock and
stall every device's ingestion, not just the one being written; ordering
is instead resolved per endpoint at read time.

## Backpressure

Backpressure under burst is a bounded, priority-aware concurrency limiter
rather than a queue+worker pool, load shedding, or a broker. It keeps
writes synchronous (preserving read-your-writes) while still delaying
producers instead of dropping events, which a queue or shedding would
either complicate or violate outright.

## Restart correctness

Every accepted event is written synchronously to the in-memory store
(what serves reads) and asynchronously, batched, to a durable Postgres
log. On boot, that log is replayed back into a fresh in-memory store
before the server starts accepting requests, so state is rebuilt, not
guessed at.

## How to run it locally

Two ways to run it, depending on what's on your machine.

**With Docker:**
Running the containerized service does not require a local toolchain dependency or Go installation.
Restart correctness works out of the box.

```bash
docker compose up --build # service on :8080, Postgres on :5432

# When done:
docker compose down -v # stop + remove containers and the data volume
```

**Without Docker:** 
Requires Go 1.25+. Restart correctness then needs a locally running Postgres.

```bash
# Without Postgres — in-memory only, no restart correctness:
make run

# With Postgres — durable, survives a hard kill:
make db      # starts Postgres in Docker on :5432
DATABASE_URL="postgres://postgres:postgres@localhost:5432/streaming?sslmode=disable" make run

# When done:
make db-stop
```

## Reported metrics

- Sustained ingest rate:
- Alarm feed latency p50 / p95:
- Behavior under hard kill + restart:
- Aggregation correctness on replayed events:

## With another week

(One or two paragraphs.)

