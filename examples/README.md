# Examples

Two ways to see StreamForge run, beyond `go run .` directly.

## `programmatic/` — embedding the pipeline in Go code

```bash
go run ./examples/programmatic
```

This composes `generator`, `ingestion`, `worker`, and `metrics` directly in
a small `main.go`, with no CLI flags and no HTTP server — the shape you'd
use if you wanted to embed StreamForge's pipeline inside another program
instead of running the `streamforge` binary. It runs for a fixed 3-second
window (2 workers, a 20-slot buffer, 50 events/sec) and prints a summary at
the end:

```
counts by type:
  payment: 50
  user-action: 50
  error: 50
backpressure: 0 blocked sends, 0s total blocked time
```

It can import `streamforge/internal/...` packages because Go's `internal`
visibility rule only blocks imports from *outside* the module — `examples/`
lives inside this module, so it's allowed the same access any other file in
the repo has.

To try different settings, edit the values passed into `ingestion.NewBuffer`,
`generator.NewRateController`, `worker.StartPool`, and the `context.WithTimeout`
call in `programmatic/main.go` (buffer size, initial rate, worker count, run
duration) — they're plain Go values, not flags.

## `loadtest.sh` — ramping load live against the CLI

```bash
./examples/loadtest.sh [workers] [buffer]
```

Both arguments are optional (default: `workers=2`, `buffer=20`). This
script:

1. builds the `streamforge` binary,
2. starts it at a low rate (20 events/sec),
3. ramps the target rate up through `POST /rate` — `20 → 200 → 2,000 →
   20,000 → 200,000` events/sec, pausing 3 seconds at each step,
4. prints `GET /stats` after each step so you can watch the numbers move,
5. stops the process when done.

Run it with the defaults to see backpressure kick in partway through:

```bash
./examples/loadtest.sh
```

Or shrink the buffer / worker count to make the system saturate sooner:

```bash
./examples/loadtest.sh 1 5
```

### What to look for in the output

At low rates, the system keeps up completely:

```json
{"target_rate_per_sec":20,"throughput_per_sec":20.0,"queue_length":0,"queue_capacity":5,"backpressure_blocked_sends":0,"backpressure_blocked_ms":0}
```

Once the requested rate exceeds what the workers can drain, three things
change together — this is backpressure actually engaging, not a generator
slowing down on its own:

```json
{"target_rate_per_sec":200000,"throughput_per_sec":137322.4,"queue_length":5,"queue_capacity":5,"backpressure_blocked_sends":78605,"backpressure_blocked_ms":3012.0}
```

- `throughput_per_sec` falls behind `target_rate_per_sec` — the workers
  can't drain the buffer as fast as the generator wants to fill it.
- `queue_length` sits at `queue_capacity` — the buffer is full and staying
  full.
- `backpressure_blocked_sends` and `backpressure_blocked_ms` climb — the
  generator is being made to wait rather than dropping events.

At the end, the final console summary (`final counts by type` /
`backpressure: N blocked sends, ...`) reflects the whole run, not just the
last sampling interval.

### Cleaning up

The script traps `EXIT` to send `SIGTERM` to the process it started and
remove the temporary binary, so `Ctrl+C` mid-run cleans up correctly too.
