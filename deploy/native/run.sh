#!/bin/bash
#
# Native runner for TAG with embedded cache
# Downloads pre-built binary from Tigris and runs it as a native process
#

set -euo pipefail

# Configuration (can be overridden via environment variables)
TAG_VERSION="${TAG_VERSION:-v1.12.0}"

# Directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="${BIN_DIR:-${SCRIPT_DIR}/.bin}"
DATA_DIR="${DATA_DIR:-/tmp/native-data}"
# Well-defined subdirectory containing all runtime data - safe to delete on cleanup
TAG_DATA_DIR="${DATA_DIR}/tag-data"
LOG_DIR="${TAG_DATA_DIR}/logs"
PID_DIR="${TAG_DATA_DIR}/pids"
CACHE_DATA_DIR="${TAG_DATA_DIR}/cache-data"

# Ports
TAG_PORT="${TAG_PORT:-8080}"

# Config file passed to the binary when it exists; install.sh writes this path.
# Set to an empty string to run with built-in defaults and env overrides only.
TAG_CONFIG_FILE="${TAG_CONFIG_FILE-/etc/tag/config.yaml}"

# True when the config file enables TLS (both tls_cert_file and tls_key_file set
# to a non-empty value). Mirrors the binary, which serves HTTPS only when both
# are set, so the health probe picks the right scheme even when TLS is configured
# via the file rather than TAG_TLS_CERT_FILE / TAG_TLS_KEY_FILE.
config_enables_tls() {
    local file="$1"
    [ -n "${file}" ] && [ -f "${file}" ] || return 1
    local cert key
    cert=$(sed -n 's/^[[:space:]]*tls_cert_file:[[:space:]]*//p' "${file}" \
        | sed 's/#.*$//; s/["'\'']//g; s/[[:space:]]*$//' | tail -n1)
    key=$(sed -n 's/^[[:space:]]*tls_key_file:[[:space:]]*//p' "${file}" \
        | sed 's/#.*$//; s/["'\'']//g; s/[[:space:]]*$//' | tail -n1)
    [ -n "${cert}" ] && [ -n "${key}" ]
}

# Health-check scheme follows TLS config: TAG serves HTTPS when both a cert and a
# key are set (via env vars or the config file), so probe over HTTPS (and skip
# cert verification for self-signed).
if { [ -n "${TAG_TLS_CERT_FILE:-}" ] && [ -n "${TAG_TLS_KEY_FILE:-}" ]; } \
    || config_enables_tls "${TAG_CONFIG_FILE}"; then
    TAG_HEALTH_SCHEME="https"
    TAG_HEALTH_CURL_OPTS="-k"
else
    TAG_HEALTH_SCHEME="http"
    TAG_HEALTH_CURL_OPTS=""
fi

# Cache settings
TAG_CACHE_MAX_DISK_USAGE="${TAG_CACHE_MAX_DISK_USAGE:-429496729600}"  # 400GB
# Use port 17000 instead of 7000 to avoid conflict with macOS Control Center
TAG_CACHE_CLUSTER_ADDR="${TAG_CACHE_CLUSTER_ADDR:-:17000}"
TAG_CACHE_GRPC_ADDR="${TAG_CACHE_GRPC_ADDR:-:19000}"

# TAG settings
TAG_LOG_LEVEL="${TAG_LOG_LEVEL:-info}"
TAG_PPROF_ENABLED="${TAG_PPROF_ENABLED:-false}"
TAG_MAX_IDLE_CONNS_PER_HOST="${TAG_MAX_IDLE_CONNS_PER_HOST:-100}"

# Seconds to wait for /health during start. Warm-cache boots open the on-disk
# store before the HTTP server listens, which on a large cache can take minutes
# (cf. the Kubernetes startupProbe budget); scale this up for larger caches. The
# wait exits as soon as /health responds, so this only bounds a failed start.
TAG_START_TIMEOUT="${TAG_START_TIMEOUT:-600}"

# Release URLs
TAG_RELEASES_URL="https://tag-releases.t3.storage.dev"

# PID files
TAG_PID_FILE="${PID_DIR}/tag.pid"

# Check required dependencies
check_dependencies() {
    local missing=()

    if ! command -v lsof >/dev/null 2>&1; then
        missing+=("lsof")
    fi

    if ! command -v curl >/dev/null 2>&1; then
        missing+=("curl")
    fi

    if [ ${#missing[@]} -gt 0 ]; then
        echo "Error: Required dependencies not installed: ${missing[*]}"
        exit 1
    fi
}

# Detect OS and architecture
detect_platform() {
    OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
    ARCH="$(uname -m)"

    # Convert architecture names
    case "${ARCH}" in
        x86_64)  ARCH="amd64" ;;
        aarch64) ARCH="arm64" ;;
    esac

    echo "Detected platform: ${OS}-${ARCH}"
}

# Download a binary if it doesn't exist
download_binary() {
    local name="$1"
    local version="$2"
    local url="$3"
    local dest="${BIN_DIR}/${name}-${version}"

    # "latest" is a moving tag, so a cached .bin/<name>-latest can be stale after
    # a new release. Re-download it every start; only cache pinned versions.
    if [ -x "${dest}" ] && [ "${version}" != "latest" ]; then
        echo "${name} ${version} already downloaded"
        return 0
    fi

    echo "Downloading ${name} ${version} for ${OS}-${ARCH}..."
    mkdir -p "${BIN_DIR}"

    # Download to a temp file in the same dir and atomically rename on success, so
    # a failed/partial download never truncates a previously working binary (which
    # would block offline starts, esp. for the always-refreshed "latest").
    local download_url="${url}/${version}/${name}-${OS}-${ARCH}"
    local tmp
    tmp="$(mktemp "${BIN_DIR}/.${name}-${version}.XXXXXX")"
    if ! curl -fsSL "${download_url}" -o "${tmp}"; then
        rm -f "${tmp}"
        echo "Error: Failed to download ${name} from ${download_url}"
        exit 1
    fi

    chmod +x "${tmp}"
    mv -f "${tmp}" "${dest}"
    echo "${name} ${version} downloaded successfully"
}

# Kill processes on a specific port (graceful shutdown)
kill_port() {
    local port="$1"
    local pids
    pids=$(lsof -ti:"${port}" 2>/dev/null || true)
    if [ -n "${pids}" ]; then
        echo "Stopping processes on port ${port}..."
        # Send SIGTERM first for graceful shutdown
        echo "${pids}" | xargs kill -TERM 2>/dev/null || true
        sleep 2
        # Force kill if still running
        pids=$(lsof -ti:"${port}" 2>/dev/null || true)
        if [ -n "${pids}" ]; then
            echo "Force killing processes on port ${port}..."
            echo "${pids}" | xargs kill -9 2>/dev/null || true
        fi
    fi
}

# Kill process by PID file (graceful shutdown)
kill_pid_file() {
    local pid_file="$1"
    local name="$2"

    if [ -f "${pid_file}" ]; then
        local pid
        pid=$(cat "${pid_file}")
        if kill -0 "${pid}" 2>/dev/null; then
            echo "Stopping ${name} (PID: ${pid})..."
            kill -TERM "${pid}" 2>/dev/null || true
            sleep 2
            if kill -0 "${pid}" 2>/dev/null; then
                echo "Force killing ${name}..."
                kill -9 "${pid}" 2>/dev/null || true
            fi
        fi
        rm -f "${pid_file}"
    fi
}

# Wait for a health endpoint to become available
wait_for_health() {
    local name="$1"
    local url="$2"
    local timeout="${3:-30}"

    echo "Waiting for ${name} to be ready..."
    local count=0
    while ! curl -sf ${TAG_HEALTH_CURL_OPTS} "${url}" > /dev/null 2>&1; do
        sleep 1
        count=$((count + 1))
        if [ ${count} -ge ${timeout} ]; then
            echo "Error: ${name} failed to start within ${timeout} seconds"
            return 1
        fi
    done
    echo "${name} is ready"
}

# Check if a service is running on a port
check_port() {
    local port="$1"
    lsof -ti:"${port}" > /dev/null 2>&1
}

# Start services
cmd_start() {
    echo "Starting TAG (native mode with embedded cache)..."

    # Check dependencies
    check_dependencies

    # Check AWS credentials
    if [ -z "${AWS_ACCESS_KEY_ID:-}" ] || [ -z "${AWS_SECRET_ACCESS_KEY:-}" ]; then
        echo "Error: AWS credentials not set."
        echo "  export AWS_ACCESS_KEY_ID=<your-key>"
        echo "  export AWS_SECRET_ACCESS_KEY=<your-secret>"
        exit 1
    fi

    # Detect platform
    detect_platform

    # Stop any existing processes
    kill_pid_file "${TAG_PID_FILE}" "TAG"
    kill_port "${TAG_PORT}"
    sleep 1

    # Download binary
    download_binary "tag" "${TAG_VERSION}" "${TAG_RELEASES_URL}"

    # Create directories (all under TAG_DATA_DIR)
    mkdir -p "${LOG_DIR}"
    mkdir -p "${PID_DIR}"
    mkdir -p "${CACHE_DATA_DIR}"

    local tag_bin="${BIN_DIR}/tag-${TAG_VERSION}"

    # Pass the installed config file to the binary when present, so edits to
    # /etc/tag/config.yaml (TLS, upstream, cache) actually take effect.
    local config_args=()
    if [ -n "${TAG_CONFIG_FILE}" ] && [ -f "${TAG_CONFIG_FILE}" ]; then
        config_args=(--config "${TAG_CONFIG_FILE}")
        echo "Using config file: ${TAG_CONFIG_FILE}"
    fi

    # Start TAG with embedded cache
    echo "Starting TAG with embedded cache..."
    TAG_HTTP_PORT="${TAG_PORT}" \
    TAG_CACHE_NODE_ID="tag-native" \
    TAG_CACHE_DISK_PATH="${CACHE_DATA_DIR}" \
    TAG_CACHE_MAX_DISK_USAGE="${TAG_CACHE_MAX_DISK_USAGE}" \
    TAG_CACHE_CLUSTER_ADDR="${TAG_CACHE_CLUSTER_ADDR}" \
    TAG_CACHE_GRPC_ADDR="${TAG_CACHE_GRPC_ADDR}" \
    TAG_LOG_LEVEL="${TAG_LOG_LEVEL}" \
    TAG_PPROF_ENABLED="${TAG_PPROF_ENABLED}" \
    TAG_MAX_IDLE_CONNS_PER_HOST="${TAG_MAX_IDLE_CONNS_PER_HOST}" \
    "${tag_bin}" ${config_args[@]+"${config_args[@]}"} \
        > "${LOG_DIR}/tag.log" 2>&1 &
    local tag_pid=$!
    echo "${tag_pid}" > "${TAG_PID_FILE}"

    if ! wait_for_health "TAG" "${TAG_HEALTH_SCHEME}://localhost:${TAG_PORT}/health" "${TAG_START_TIMEOUT}"; then
        echo "TAG logs:"
        tail -20 "${LOG_DIR}/tag.log"
        exit 1
    fi

    echo ""
    echo "TAG started successfully with embedded cache!"
    echo "  TAG:   ${TAG_HEALTH_SCHEME}://localhost:${TAG_PORT}"
    echo "  Cache: ${CACHE_DATA_DIR}"
    echo ""
    echo "Logs: ${LOG_DIR}"
}

# Stop services
cmd_stop() {
    echo "Stopping TAG..."

    # Try PID file first, then fall back to port-based killing
    kill_pid_file "${TAG_PID_FILE}" "TAG"
    kill_port "${TAG_PORT}"

    echo "TAG stopped"

    if [ "${1:-}" = "--clean" ]; then
        echo "Cleaning up tag data..."

        # Delete the well-defined tag-data subdirectory (contains logs, pids, cache-data)
        # This is safe because we created it with a known name
        if [ -d "${TAG_DATA_DIR}" ]; then
            rm -rf "${TAG_DATA_DIR}"
            echo "Tag data directory removed: ${TAG_DATA_DIR}"
        else
            echo "Tag data directory does not exist, nothing to clean"
        fi
    fi
}

# Check status of services
cmd_status() {
    echo "Service Status:"
    echo ""

    # TAG status
    local tag_pid=""
    if [ -f "${TAG_PID_FILE}" ]; then
        tag_pid=$(cat "${TAG_PID_FILE}")
    fi

    if check_port "${TAG_PORT}"; then
        echo "  TAG (port ${TAG_PORT}): RUNNING${tag_pid:+ (PID: ${tag_pid})}"
        if curl -sf ${TAG_HEALTH_CURL_OPTS} "${TAG_HEALTH_SCHEME}://localhost:${TAG_PORT}/health" > /dev/null 2>&1; then
            echo "    Health: OK"
        else
            echo "    Health: UNHEALTHY"
        fi
        echo "    Cache:  ${CACHE_DATA_DIR}"
    else
        echo "  TAG (port ${TAG_PORT}): STOPPED"
    fi
}

# Show logs
cmd_logs() {
    local lines="${1:-50}"

    if [ -f "${LOG_DIR}/tag.log" ]; then
        echo "=== TAG Logs ==="
        tail -"${lines}" "${LOG_DIR}/tag.log"
    else
        echo "No TAG logs found"
    fi
}

# Show usage
cmd_help() {
    echo "Native runner for TAG with embedded cache"
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  start           Start TAG service"
    echo "  stop [--clean]  Stop service (--clean removes all tag data)"
    echo "  status          Check status of service"
    echo "  logs [lines]    Show logs (default: 50 lines)"
    echo "  help            Show this help message"
    echo ""
    echo "Environment Variables:"
    echo "  AWS_ACCESS_KEY_ID      AWS access key (required)"
    echo "  AWS_SECRET_ACCESS_KEY  AWS secret key (required)"
    echo "  TAG_VERSION            TAG version (default: ${TAG_VERSION})"
    echo "  TAG_LOG_LEVEL          Log level: debug, info, warn, error (default: ${TAG_LOG_LEVEL})"
    echo "  TAG_PPROF_ENABLED      Enable pprof profiling: true, false (default: ${TAG_PPROF_ENABLED})"
    echo "  TAG_MAX_IDLE_CONNS_PER_HOST  Max idle connections per host (default: ${TAG_MAX_IDLE_CONNS_PER_HOST})"
    echo "  TAG_PORT               TAG HTTP port (default: ${TAG_PORT})"
    echo "  TAG_START_TIMEOUT      Seconds to wait for /health on start (default: ${TAG_START_TIMEOUT})"
    echo "  TAG_CONFIG_FILE        Config file passed to tag if it exists; empty to disable (default: ${TAG_CONFIG_FILE})"
    echo "  TAG_CACHE_MAX_DISK_USAGE  Max cache disk usage in bytes (default: ${TAG_CACHE_MAX_DISK_USAGE})"
    echo "  TAG_CACHE_CLUSTER_ADDR Cluster gossip address (default: ${TAG_CACHE_CLUSTER_ADDR})"
    echo "  TAG_CACHE_GRPC_ADDR    gRPC server address (default: ${TAG_CACHE_GRPC_ADDR})"
    echo "  BIN_DIR                Binary download directory (default: ${BIN_DIR})"
    echo "  DATA_DIR               Data directory (default: ${DATA_DIR})"
    echo ""
    echo "Examples:"
    echo "  export AWS_ACCESS_KEY_ID=<key>"
    echo "  export AWS_SECRET_ACCESS_KEY=<secret>"
    echo "  $0 start"
    echo "  $0 status"
    echo "  $0 logs 100"
    echo "  $0 stop --clean"
}

# Main entry point
main() {
    local command="${1:-help}"
    shift || true

    case "${command}" in
        start)  cmd_start "$@" ;;
        stop)   cmd_stop "$@" ;;
        status) cmd_status "$@" ;;
        logs)   cmd_logs "$@" ;;
        help)   cmd_help ;;
        *)
            echo "Unknown command: ${command}"
            echo ""
            cmd_help
            exit 1
            ;;
    esac
}

main "$@"
