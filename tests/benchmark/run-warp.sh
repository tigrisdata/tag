#!/bin/bash
# Warp benchmark runner for TAG (issue #67)
# Modeled after tests/s3compat/python/run-tests.sh
#
# Drives MinIO warp against a locally running TAG (which proxies to Tigris) and
# benchmarks the core S3 operations: GET, GET RANGE, PUT, HEAD, LIST V2.
# This is a smoke benchmark: it fails if any operation errors, and writes a
# human-readable `warp analyze` summary per operation into results/.
#
# Prerequisites: TAG running locally (e.g. `make s3-test-local`) and AWS creds.
#
# Tunable via env vars (CI-friendly defaults shown):
#   WARP_VERSION       warp version to install            (v1.5.0)
#   WARP_HOST          TAG host:port                      (localhost:${TAG_HTTP_PORT:-8080})
#   WARP_BUCKET        bucket for benchmark data          (tag-warp-benchmark)
#   WARP_REGION        SigV4 region (must match TAG)      (auto)
#   WARP_DURATION      duration per operation             (30s)
#   WARP_CONCURRENT    concurrent operations              (4)
#   WARP_OBJ_SIZE      object size for 4MiB ops           (4MiB)
#   WARP_OBJECTS       object count for 4MiB ops          (100)
#   WARP_RANGE_OBJ_SIZE large object size for GET RANGE   (100MiB)
#   WARP_RANGE_SIZE    range read size for GET RANGE      (4MiB)
#   WARP_RANGE_OBJECTS object count for GET RANGE         (8)

set -uo pipefail

# Track operation failures
FAILED_OPS=()
PASSED_COUNT=0

# Handle Ctrl+C to exit the entire script
trap 'echo -e "\nInterrupted. Exiting..."; exit 130' INT

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$SCRIPT_DIR/.bin"
RESULTS_DIR="$SCRIPT_DIR/results"
WARP="$BIN_DIR/warp"

WARP_VERSION="${WARP_VERSION:-v1.5.0}"

# Check for required environment variables
if [ -z "${AWS_ACCESS_KEY_ID:-}" ] || [ -z "${AWS_SECRET_ACCESS_KEY:-}" ]; then
    echo "Error: AWS credentials not set."
    echo "  export AWS_ACCESS_KEY_ID=<your-key>"
    echo "  export AWS_SECRET_ACCESS_KEY=<your-secret>"
    exit 1
fi

mkdir -p "$BIN_DIR" "$RESULTS_DIR"

# Bootstrap warp: install a pinned version via `go install` if not present.
# warp publishes no release binaries, so we build from source (Go is required
# to build TAG anyway). The binary is cached in .bin/ (git-ignored).
if [ ! -x "$WARP" ]; then
    echo "Installing warp $WARP_VERSION (this may take a minute)..."
    if ! GOBIN="$BIN_DIR" go install "github.com/minio/warp@$WARP_VERSION"; then
        echo "Error: Failed to install warp"
        exit 1
    fi
fi
echo "Using $("$WARP" --version 2>&1 | head -1)"

# Benchmark configuration (env-overridable)
HOST="${WARP_HOST:-localhost:${TAG_HTTP_PORT:-8080}}"
BUCKET="${WARP_BUCKET:-tag-warp-benchmark}"
REGION="${WARP_REGION:-auto}"
DURATION="${WARP_DURATION:-30s}"
CONCURRENT="${WARP_CONCURRENT:-4}"
OBJ_SIZE="${WARP_OBJ_SIZE:-4MiB}"
OBJECTS="${WARP_OBJECTS:-100}"
RANGE_OBJ_SIZE="${WARP_RANGE_OBJ_SIZE:-100MiB}"
RANGE_SIZE="${WARP_RANGE_SIZE:-4MiB}"
RANGE_OBJECTS="${WARP_RANGE_OBJECTS:-8}"

# Flags shared by every operation. TAG is plain HTTP locally, so no --tls.
# --lookup=path matches TAG/SDK path-style addressing; warp clears the bucket
# before and after each run by default (no --noclear), so ops don't interfere.
COMMON=(
    --host="$HOST"
    --access-key="$AWS_ACCESS_KEY_ID"
    --secret-key="$AWS_SECRET_ACCESS_KEY"
    --region="$REGION"
    --bucket="$BUCKET"
    --lookup=path
    --concurrent="$CONCURRENT"
    --duration="$DURATION"
)

echo "TAG host: $HOST | bucket: $BUCKET | region: $REGION | duration: $DURATION | concurrent: $CONCURRENT"

# run_op <name> <warp-subcommand> [op-specific flags...]
# Runs a warp benchmark, tees output to results/<name>.log, and produces a
# readable summary via `warp analyze` in results/<name>.analyze.txt.
run_op() {
    local name="$1"
    shift
    local benchdata="$RESULTS_DIR/$name"

    echo ""
    echo "=== Benchmark: $name ==="
    if "$WARP" "$@" "${COMMON[@]}" --benchdata="$benchdata" 2>&1 | tee "$RESULTS_DIR/$name.log"; then
        # warp appends .csv.zst to the --benchdata prefix
        local matches=( "$benchdata"*.csv.zst )
        local data="${matches[0]}"
        if [ -f "$data" ]; then
            "$WARP" analyze --no-color "$data" 2>&1 | tee "$RESULTS_DIR/$name.analyze.txt"
        fi
        PASSED_COUNT=$((PASSED_COUNT + 1))
    else
        echo "Operation '$name' FAILED"
        FAILED_OPS+=("$name")
    fi
}

# Core operations. All use 4MiB objects except GET RANGE, which reads 4MiB
# ranges from 100MiB objects (--range-size implies ranged GETs).
run_op "get"       get    --obj.size="$OBJ_SIZE"       --objects="$OBJECTS"
run_op "get-range" get    --obj.size="$RANGE_OBJ_SIZE" --objects="$RANGE_OBJECTS" --range-size="$RANGE_SIZE"
run_op "put"       put    --obj.size="$OBJ_SIZE"
run_op "head"      stat   --obj.size="$OBJ_SIZE"       --objects="$OBJECTS"
run_op "list"      list   --obj.size="$OBJ_SIZE"       --objects="$OBJECTS"

# Report results
echo ""
echo "========================================="
echo "Warp Benchmark Summary"
echo "========================================="
TOTAL=$((PASSED_COUNT + ${#FAILED_OPS[@]}))
echo "Total: $TOTAL | Passed: $PASSED_COUNT | Failed: ${#FAILED_OPS[@]}"
echo "Results (logs + analyze summaries): $RESULTS_DIR"
echo ""

if [ ${#FAILED_OPS[@]} -eq 0 ]; then
    echo "All benchmarks completed successfully."
    exit 0
else
    echo "FAILED OPERATIONS:"
    for failed in "${FAILED_OPS[@]}"; do
        echo "  - $failed"
    done
    exit 1
fi
