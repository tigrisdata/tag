#!/usr/bin/env bash
# Run one Go benchmark sample. Retry only process attempts that fail before the
# benchmark emits a sample, such as a transient loopback-port collision during
# embedded-cache startup. A successful invocation always writes exactly one Go
# benchmark sample to stdout.

set -euo pipefail

if [ "$#" -lt 3 ]; then
    echo "usage: $0 <benchmark binary> <benchmark name> <benchtime> [test flags...]" >&2
    exit 2
fi

BENCHMARK_BINARY="$1"
BENCHMARK_NAME="$2"
BENCHTIME="$3"
shift 3

# Embedded-cache startup occasionally loses its loopback listener while other
# benchmark processes are starting. Keep retries bounded, and retry only before
# a process has emitted a measurement.
readonly MAX_ATTEMPTS=5
OUTPUT="$(mktemp)"
trap 'rm -f "$OUTPUT"' EXIT

for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
    if "$BENCHMARK_BINARY" \
        -test.bench="^${BENCHMARK_NAME}$" \
        -test.run='^$' \
        -test.count=1 \
        -test.benchtime="$BENCHTIME" \
        -test.benchmem \
        "$@" >"$OUTPUT" 2>&1 && grep -q "^${BENCHMARK_NAME}" "$OUTPUT"; then
        cat "$OUTPUT"
        exit 0
    fi

done

cat "$OUTPUT" >&2
exit 1
