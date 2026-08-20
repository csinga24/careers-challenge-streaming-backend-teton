# Submission, Real-time Streaming Backend

**Your name:** Csenge Oszlanyi-Salacz
**Email:** salaczka624@gmail.com
**Link to your fork or solution:** https://github.com/csinga24/careers-challenge-streaming-backend-teton

---

## Stack and storage

The service is written in Go. I chose Go because of goroutines and channels fit the concurrent, per-device ingest workload directly, and I have personal experience in Go development as well. 
The single static binary of Go keeps the Docker image trivial and no toolchain dependency for anyone running it. 
Events arrive via POST /events, and reads are HTTP GET, fitting stateless request-response point queries with zero client dependencies.
The real-time alarm feed uses SSE, since it's one-directional, runs over plain HTTP, and gets a resume mechanism built in.
Postgres is the durable append-only event log. I benchmarked it against BoltDB, SQLite, and a flat file. SQLite performed similarly, but Postgres won on tooling and throughput.

## Ordering and late events

Events are appended in arrival order to avoid a sorted insert holding a global store lock unnecessarily long during bursts.Ordering is resolved on read instead. 
Events up to one hour in the past are accepted, so offline devices can replay buffered data and correct historical aggregations; timestamps more than one hour in the future are rejected.

## Backpressure

Backpressure uses bounded, priority-aware concurrency limiters: fall_warn events get their own lane, other events share one. When a lane is full, request handlers block rather than dropping events or returning misleading success responses. Writes stay synchronous, preserving read-your-writes.

## Restart correctness

Accepted events write to the in-memory store and flush in batches to an append-only Postgres log, to avoid per-request sync bottleneck. On startup, the service replays the log before opening HTTP routes, reconstructing device health, room occupancy, fall history, and alarms.
Tested with 5,268 accepted events under `kill -9`: all 5,268 were recovered. This is an observed result the 10ms async flush interval shrinks, but does not eliminate, the unconfirmed loss window.
The SSE feed resumes via the broker's monotonic publish sequence rather than event timestamps, preventing late-arriving alarms from being skipped on client reconnects.

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
- Behavior under hard kill + restart: 5,268/5,268 events replayed in the largest test run. This is an observed result, not a guaranteed bound.
- Aggregation correctness on replayed events: Offline replay matched 739/739 alarms. The adversarial scenario matched 2,541/2,545, or 99.84%. Ten burst runs averaged 99.86% deduplication accuracy.

## With another week

I'd make ingestion acknowledgment wait on confirmed durable persistence, then measure the latency and throughput trade-off. 
I'd also replace heuristic fall deduplication with a stable device-generated ID per physical fall, closing the collision gap outright.