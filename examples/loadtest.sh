#!/usr/bin/env bash
# Demonstrates the load-test scenario: start streamforge at a low rate,
# then ramp it up live via POST /rate while watching GET /stats respond -
# no restart needed. Watch queue_length climb toward queue_capacity and
# backpressure_blocked_sends start increasing once the generator outruns
# the workers.
#
# Usage: ./examples/loadtest.sh [workers] [buffer]
set -euo pipefail

cd "$(dirname "$0")/.."

WORKERS="${1:-2}"
BUFFER="${2:-20}"
ADDR=":8080"
BASE_URL="http://localhost${ADDR}"
BIN="$(mktemp -u /tmp/streamforge-loadtest.XXXXXX)"

echo "building streamforge..."
go build -o "$BIN" .

echo "starting streamforge (workers=${WORKERS} buffer=${BUFFER} rate=20)..."
"$BIN" --workers="$WORKERS" --buffer="$BUFFER" --rate=20 --addr="$ADDR" &
PID=$!
trap 'kill -TERM "$PID" 2>/dev/null || true; rm -f "$BIN"' EXIT

sleep 1

for rate in 20 200 2000 20000 200000; do
	echo
	echo "== ramping to ${rate} events/sec =="
	curl -s -X POST "${BASE_URL}/rate?value=${rate}"
	echo
	sleep 3
	echo "-- /stats --"
	curl -s "${BASE_URL}/stats"
	echo
done

echo
echo "done - stopping streamforge"
