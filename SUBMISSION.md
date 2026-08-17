# Submission, Real-time Streaming Backend

**Your name:** Csenge Oszlanyi-Salacz
**Email:** salaczka624@gmail.com
**Link to your fork or solution:**

---

## Stack and storage

The service is written in Go: goroutines and channels fit the concurrent, per-device ingest workload directly, and a single static binary keeps the Docker image trivial and no toolchain dependency for anyone running it. Events arrive via POST /events; reads are HTTP GET.
The real-time alarm feed uses SSE, since it's one-directional, runs over plain HTTP, and gets a resume mechanism (Last-Event-ID) built in.
Postgres is the durable append-only event log. I benchmarked it against BoltDB, SQLite, and a flat file. SQLite performed similarly, but Docker was already required for deployment, so Postgres won on tooling, throughput, and a clearer path to multiple service instances.

## Ordering and late events

Events are appended in arrival order to avoid a sorted insert holding a global store lock during bursts; ordering is resolved on read instead: device health picks the latest heartbeat by timestamp, room occupancy merges and sorts presence events across devices, and fall-warning deduplication sorts each device's events before clustering. Events up to one hour in the past are accepted, so offline devices can replay buffered data and correct historical aggregations; timestamps more than one hour in the future are rejected.

## Backpressure

Backpressure uses bounded, priority-aware concurrency limiters: fall_warn events get their own lane, other events share one. When a lane is full, request handlers block rather than dropping events or returning misleading success responses. Writes stay synchronous, preserving read-your-writes.

## Restart correctness

Accepted events go to the in-memory store, then flush in batches to an append-only Postgres log (monotonic sequence + payload). On startup, the service replays the log into a fresh in-memory store before accepting requests, rebuilding device health, room occupancy, fall history, and alarms.
Tested with 5,268 events accepted, kill -9, restart and then all 5,268 were recovered: an observed result, not a guarantee. The 10ms flush interval shrinks, but doesn't eliminate, that loss window.
The SSE feed resumes by the broker's publish sequence, not event ts, so a late-discovered alarm isn't silently dropped on reconnect. That sequence resets in memory on restart, but the server now deterministically replays it before accepting requests, so a reconnecting client's cursor resolves correctly except when the flush window (above) actually lost the event.

## How to run it locally

Two ways to run it, depending on what's on your machine.

**With Docker:**
Running the containerized service does not require a local toolchain dependency or Go installation.
Restart correctness works out of the box.

```bash
make compose-up   # service on :8080, Postgres on :5432

# When done:
make compose-down # stop + remove containers and the data volume
```

**Optional: Prometheus + Grafana for visualization of some of the metrics.**

```bash
make observability-up
# service on :8080, Prometheus on :9090, Grafana on :3000 (no login)

# When done:
make observability-down
```

**Without Docker:**
Requires Go 1.25+. Restart correctness then needs a locally running Postgres.

```bash
# Without Postgres: in-memory only, no restart correctness:
make run

# With Postgres: durable, survives a hard kill:
make db      # starts Postgres in Docker on :5432
DATABASE_URL="postgres://postgres:postgres@localhost:5432/streaming?sslmode=disable" make run

# When done:
make db-stop
```

## Reported metrics

- Sustained ingest rate: Ten fresh 500-device burst runs averaged 1,236.2 events/sec with a range of 1,154.3–1,268.6 and zero failed events.
- Alarm feed latency p50 / p95: At 500 devices, p50 was at most 0.1s and p95 at most 0.25s. Mean latency was 103.8ms, with 2,036/2,041 alarms matched.
- Behavior under hard kill + restart: 5,268/5,268 events replayed in the largest test run. This is an observed result, not a guaranteed bound (see "Restart correctness").
- Aggregation correctness on replayed events: Offline replay matched 739/739 alarms. The adversarial scenario matched 2,541/2,545, or 99.84%. Ten burst runs averaged 99.86% deduplication accuracy.

## With another week

I'd make ingestion acknowledgment wait on confirmed durable persistence, then measure the latency/throughput cost. I'd also replace heuristic fall deduplication with a stable device-generated ID per physical fall, closing the collision gap outright.