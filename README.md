# StreamForge

StreamForge is a high-throughput, in-memory stream processing system written
in idiomatic Go (standard library only — no external dependencies). It
continuously ingests a stream of synthetic events, classifies them by type,
and produces live metrics (throughput, latency, queue depth, per-type
counts) while applying backpressure instead of losing data when consumers
fall behind.

It's a **push-based / event-streaming** architecture: a generator
continuously produces events and pushes them into the pipeline. There is no
central coordinator that workers pull work from — the buffered channel
between the generator and the worker pool is itself the distribution and
backpressure mechanism.

## V1 scope (deliberately limited)

This is a first version, scoped on purpose:

- **Synthetic data only.** A built-in generator produces `error` /
  `payment` / `user-action` events in JSON-shaped structs, at a rate you
  control live. There is no real data source.
- **In-memory processing only.** Nothing is persisted to disk or a
  database. All counts and metrics live in process memory and are lost on
  restart. Durable storage is explicitly out of scope for V1 and left for
  V2.
- **Single machine.** Parallelism comes from goroutines/worker pools
  within one process, not from distributed nodes.

## Architecture

```
                 ┌────────────┐
 target rate --> │  generator │  produces Event{ID, Type, Payload, Timestamp}
                 └─────┬──────┘
                        │ Buffer.Send(ctx, evt)   <- blocks if full (backpressure)
                        ▼
                 ┌────────────┐
                 │  ingestion │  bounded channel (chan Event, capacity N)
                 │   buffer   │  tracks queue length + blocked-send stats
                 └─────┬──────┘
                        │ fan-out is free: N goroutines read the same chan
              ┌─────────┼─────────┐
              ▼         ▼         ▼
          ┌───────┐ ┌───────┐ ┌───────┐
          │worker0│ │worker1│ │workerN│  classify by type, count, report latency
          └───────┘ └───────┘ └───────┘
                        │
                        ▼
                 ┌────────────┐
                 │  metrics   │  samples throughput/latency/queue/counts on
                 │  reporter  │  a ticker; serves the latest snapshot as JSON
                 └─────┬──────┘
                        │
                 GET /stats (JSON)   POST /rate?value=N (live rate change)
```

### Key design decisions

**Backpressure: blocking, not dropping.**
StreamForge's core requirement is not losing data. A bounded channel
(`ingestion.Buffer`) sits between the generator and the workers; when it's
full, `Send` blocks the generator instead of discarding events. This falls
naturally out of Go's channel semantics — no extra "ask for permission"
protocol is needed, unlike a pull-based design where workers request work
from a coordinator. The cost is that a slow consumer visibly slows down the
producer, which is exactly the signal a load test is meant to surface. Every
blocked send is counted precisely (see below), so backpressure is never a
silent stall.

**Precise backpressure accounting, not a fixed threshold.**
`Buffer.Send` tries a non-blocking send first; only if the buffer is
genuinely full does it fall into a timed, blocking path. An earlier version
used an arbitrary "count it as blocked past 1ms" cutoff, which hid the many
sub-millisecond waits that actually occur at high throughput (a real test
run processed 556k events but reported a single blocked send under that
threshold). Checking buffer-full directly instead gives an exact count of
every genuine backpressure event, however short.

**Metrics are delta-based, not cumulative.**
Throughput and average latency are computed as the change since the
*previous* sampling tick, not as an average since process startup. A
cumulative average would smooth away a sudden slowdown over a long-running
process; a delta-based one shows degradation within one sampling interval —
which matters directly for the load test scenario (see below).

**Rate is adjustable at runtime, not just at startup.**
`generator.RateController` holds the target events/sec atomically and is
read fresh on every tick, so `POST /rate` takes effect immediately without
restarting the process. This is what makes it possible to ramp load up live
and watch `/stats` respond.

**Generator throughput is decoupled from OS/runtime timer resolution.**
An earlier version scheduled one `time.After` timer per event. At high
rates (e.g. 5000/s → a 200µs interval) this ran into the actual timer
resolution available in the runtime environment (~1ms in some
containerized/virtualized environments), capping throughput well before the
buffer or workers became the bottleneck — confirmed with an isolated timer
benchmark, not assumed. The fix: the generator ticks at a fixed, coarser
interval (20ms) and emits however many events that rate implies for that
slice of time (with fractional carry-over so low rates still average out
correctly), instead of one timer per event. Achievable rate is now limited
by buffer capacity and worker speed, which is what a load test should be
measuring.

**No dependency cycles between layers.**
`worker` never imports `metrics` — it depends only on a small `Recorder`
interface it defines itself (`RecordProcessed(latency)`), which
`metrics.Recorder` happens to implement. `metrics` imports `worker` (for
`Counts`) and `ingestion` (for `Buffer`), so the dependency only ever points
one way. The same pattern is used for `generator`'s `sender` interface,
satisfied by `ingestion.Buffer`.

## Project layout

```
streamforge/
├── main.go                    # CLI: wires everything together, serves HTTP
├── internal/
│   ├── event/                 # Event struct and type constants
│   ├── generator/              # synthetic event generator + runtime rate control
│   ├── ingestion/              # bounded buffer, blocking backpressure
│   ├── worker/                 # worker pool: classify, count, report latency
│   └── metrics/                # sampling, /stats JSON, console reporting
└── examples/                  # see examples/README.md
```

## Getting started

Requires Go 1.24+ and no external dependencies.

```bash
go build ./...
go run .
```

By default this starts 4 workers, a buffer of 10, a generator producing 20
events/sec, prints metrics every second, and serves HTTP on `:8080`. Stop it
with `Ctrl+C` (SIGINT) — shutdown is graceful: in-flight events finish, then
final counts and backpressure stats are printed.

### CLI flags

| Flag                | Default | Description                                        |
|----------------------|---------|-----------------------------------------------------|
| `--workers`          | `4`     | number of parallel worker goroutines                |
| `--buffer`           | `10`    | ingestion buffer capacity (bounded, for backpressure)|
| `--rate`             | `20`    | initial generator rate, in events per second        |
| `--metrics-interval` | `1s`    | how often to sample and print metrics               |
| `--addr`             | `:8080` | address to serve `/stats` and `/rate` on            |

Example:

```bash
go run . --workers=8 --buffer=100 --rate=500 --addr=:9090
```

### HTTP endpoints

**`GET /stats`** — latest metrics snapshot as JSON:

```bash
curl -s localhost:8080/stats
```

```json
{
  "timestamp": "2026-09-06T05:36:00Z",
  "target_rate_per_sec": 500,
  "throughput_per_sec": 498.7,
  "avg_latency_ms": 0.03,
  "queue_length": 0,
  "queue_capacity": 100,
  "counts_by_type": {"error": 1042, "payment": 1041, "user-action": 1042},
  "backpressure_blocked_sends": 0,
  "backpressure_blocked_ms": 0
}
```

**`GET /rate`** — current target rate:

```bash
curl -s localhost:8080/rate
# {"rate_per_sec":500}
```

**`POST /rate?value=N`** — change the generator's target rate live, no
restart needed:

```bash
curl -s -X POST "localhost:8080/rate?value=5000"
# {"rate_per_sec":5000}
```

## Running a load test

Ramp the rate up while watching `/stats` to see exactly where the system
starts falling behind — queue length climbing toward capacity and
`backpressure_blocked_sends` increasing are the signals to watch for:

```bash
go run . --workers=2 --buffer=20 --rate=20 &
curl -s -X POST "localhost:8080/rate?value=20000"
curl -s localhost:8080/stats
```

Or use the scripted version — see [`examples/README.md`](examples/README.md).

## Testing

```bash
go test ./... -race
```

All four internal packages (`ingestion`, `worker`, `generator`, `metrics`)
have unit tests, including the timing-sensitive backpressure and rate
behaviors, run with the race detector.

## Examples

See [`examples/README.md`](examples/README.md) for:
- a Go program that embeds the pipeline directly, without the CLI or HTTP
  layer;
- a shell script that runs the CLI and ramps load live through `/rate`.

## V2 (not in this repo yet)

- Durable storage (disk or a database) — all state in V1 is in-memory and
  lost on restart.
- Real data sources instead of the synthetic generator.
- Multi-node distribution instead of single-machine goroutines.
