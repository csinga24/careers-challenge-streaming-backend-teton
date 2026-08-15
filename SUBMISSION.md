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

## Ordering and late events

(How you handle per-device ordering and out-of-order arrivals from offline devices.)

## Backpressure

(What happens during a 10x burst. What you delay, what you prioritize.)

## Restart correctness

(How state survives a hard kill.)

## How to run it locally

```bash
# steps to bring up your service against event_generator/
```

## Reported metrics

- Sustained ingest rate:
- Alarm feed latency p50 / p95:
- Behavior under hard kill + restart:
- Aggregation correctness on replayed events:

## With another week

(One or two paragraphs.)
