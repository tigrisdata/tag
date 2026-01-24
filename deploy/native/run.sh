#!/bin/bash
#
# Native runner for TAG and OCache
# Downloads pre-built binaries from Tigris and runs them as native processes
#

set -euo pipefail

# Configuration (can be overridden via environment variables)
TAG_VERSION="${TAG_VERSION:-v1.2.0}"
OCACHE_VERSION="${OCACHE_VERSION:-v1.2.2}"

# Directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="${BIN_DIR:-${SCRIPT_DIR}/.bin}"
DATA_DIR="${DATA_DIR:-/tmp/native-data}"
# Well-defined subdirectory containing all runtime data - safe to delete on cleanup
TAG_DATA_DIR="${DATA_DIR}/tag-data"
LOG_DIR="${TAG_DATA_DIR}/logs"
PID_DIR="${TAG_DATA_DIR}/pids"
OCACHE_DATA_DIR="${TAG_DATA_DIR}/ocache-data"

# Ports
TAG_PORT="${TAG_PORT:-8080}"
OCACHE_PORT="${OCACHE_PORT:-9000}"
OCACHE_HTTP_PORT="${OCACHE_HTTP_PORT:-9001}"

# OCache settings
OCACHE_MAX_DISK_USAGE="${OCACHE_MAX_DISK_USAGE:-107374182400}"  # 100GB

# TAG settings
TAG_LOG_LEVEL="${TAG_LOG_LEVEL:-info}"

# Release URLs
TAG_RELEASES_URL="https://tag-releases.t3.storage.dev"
OCACHE_RELEASES_URL="https://ocache-releases.t3.storage.dev"

# PID files
OCACHE_PID_FILE="${PID_DIR}/ocache.pid"
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

    if [ -x "${dest}" ]; then
        echo "${name} ${version} already downloaded"
        return 0
    fi

    echo "Downloading ${name} ${version} for ${OS}-${ARCH}..."
    mkdir -p "${BIN_DIR}"

    local download_url="${url}/${version}/${name}-${OS}-${ARCH}"
    if ! curl -fsSL "${download_url}" -o "${dest}"; then
        echo "Error: Failed to download ${name} from ${download_url}"
        exit 1
    fi

    chmod +x "${dest}"
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
    while ! curl -sf "${url}" > /dev/null 2>&1; do
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
    echo "Starting TAG and OCache (native mode)..."

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
    kill_pid_file "${OCACHE_PID_FILE}" "OCache"
    kill_port "${TAG_PORT}"
    kill_port "${OCACHE_PORT}"
    kill_port "${OCACHE_HTTP_PORT}"
    sleep 1

    # Download binaries
    download_binary "ocache" "${OCACHE_VERSION}" "${OCACHE_RELEASES_URL}"
    download_binary "tag" "${TAG_VERSION}" "${TAG_RELEASES_URL}"

    # Create directories (all under TAG_DATA_DIR)
    mkdir -p "${LOG_DIR}"
    mkdir -p "${PID_DIR}"
    mkdir -p "${OCACHE_DATA_DIR}"

    local ocache_bin="${BIN_DIR}/ocache-${OCACHE_VERSION}"
    local tag_bin="${BIN_DIR}/tag-${TAG_VERSION}"

    # Start OCache
    echo "Starting OCache..."
    "${ocache_bin}" \
        -disk="${OCACHE_DATA_DIR}" \
        -listen-addr=":${OCACHE_PORT}" \
        -listen-http=":${OCACHE_HTTP_PORT}" \
        -max-disk-usage="${OCACHE_MAX_DISK_USAGE}" \
        > "${LOG_DIR}/ocache.log" 2>&1 &
    local ocache_pid=$!
    echo "${ocache_pid}" > "${OCACHE_PID_FILE}"

    if ! wait_for_health "OCache" "http://localhost:${OCACHE_HTTP_PORT}/health"; then
        echo "OCache logs:"
        tail -20 "${LOG_DIR}/ocache.log"
        exit 1
    fi

    # Start TAG
    echo "Starting TAG..."
    TAG_OCACHE_ENDPOINTS="localhost:${OCACHE_PORT}" \
    TAG_LOG_LEVEL="${TAG_LOG_LEVEL}" \
    "${tag_bin}" \
        > "${LOG_DIR}/tag.log" 2>&1 &
    local tag_pid=$!
    echo "${tag_pid}" > "${TAG_PID_FILE}"

    if ! wait_for_health "TAG" "http://localhost:${TAG_PORT}/health"; then
        echo "TAG logs:"
        tail -20 "${LOG_DIR}/tag.log"
        exit 1
    fi

    echo ""
    echo "Services started successfully!"
    echo "  TAG:    http://localhost:${TAG_PORT}"
    echo "  OCache: http://localhost:${OCACHE_PORT} (data), http://localhost:${OCACHE_HTTP_PORT} (http)"
    echo ""
    echo "Logs: ${LOG_DIR}"
}

# Stop services
cmd_stop() {
    echo "Stopping TAG and OCache..."

    # Try PID files first, then fall back to port-based killing
    kill_pid_file "${TAG_PID_FILE}" "TAG"
    kill_pid_file "${OCACHE_PID_FILE}" "OCache"
    kill_port "${TAG_PORT}"
    kill_port "${OCACHE_PORT}"
    kill_port "${OCACHE_HTTP_PORT}"

    echo "Services stopped"

    if [ "${1:-}" = "--clean" ]; then
        echo "Cleaning up tag data..."

        # Delete the well-defined tag-data subdirectory (contains logs, pids, ocache-data)
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
        if curl -sf "http://localhost:${TAG_PORT}/health" > /dev/null 2>&1; then
            echo "    Health: OK"
        else
            echo "    Health: UNHEALTHY"
        fi
    else
        echo "  TAG (port ${TAG_PORT}): STOPPED"
    fi

    # OCache status
    local ocache_pid=""
    if [ -f "${OCACHE_PID_FILE}" ]; then
        ocache_pid=$(cat "${OCACHE_PID_FILE}")
    fi

    if check_port "${OCACHE_PORT}"; then
        echo "  OCache (port ${OCACHE_PORT}): RUNNING${ocache_pid:+ (PID: ${ocache_pid})}"
        if curl -sf "http://localhost:${OCACHE_HTTP_PORT}/health" > /dev/null 2>&1; then
            echo "    Health: OK"
        else
            echo "    Health: UNHEALTHY"
        fi
    else
        echo "  OCache (port ${OCACHE_PORT}): STOPPED"
    fi
}

# Show logs
cmd_logs() {
    local service="${1:-all}"
    local lines="${2:-50}"

    case "${service}" in
        tag)
            if [ -f "${LOG_DIR}/tag.log" ]; then
                echo "=== TAG Logs ==="
                tail -"${lines}" "${LOG_DIR}/tag.log"
            else
                echo "No TAG logs found"
            fi
            ;;
        ocache)
            if [ -f "${LOG_DIR}/ocache.log" ]; then
                echo "=== OCache Logs ==="
                tail -"${lines}" "${LOG_DIR}/ocache.log"
            else
                echo "No OCache logs found"
            fi
            ;;
        all|*)
            if [ -f "${LOG_DIR}/ocache.log" ]; then
                echo "=== OCache Logs ==="
                tail -"${lines}" "${LOG_DIR}/ocache.log"
                echo ""
            fi
            if [ -f "${LOG_DIR}/tag.log" ]; then
                echo "=== TAG Logs ==="
                tail -"${lines}" "${LOG_DIR}/tag.log"
            fi
            ;;
    esac
}

# Show usage
cmd_help() {
    echo "Native runner for TAG and OCache"
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  start           Start TAG and OCache services"
    echo "  stop [--clean]  Stop services (--clean removes all tag data)"
    echo "  status          Check status of services"
    echo "  logs [service]  Show logs (service: tag, ocache, or all)"
    echo "  help            Show this help message"
    echo ""
    echo "Environment Variables:"
    echo "  AWS_ACCESS_KEY_ID      AWS access key (required)"
    echo "  AWS_SECRET_ACCESS_KEY  AWS secret key (required)"
    echo "  TAG_VERSION            TAG version (default: ${TAG_VERSION})"
    echo "  OCACHE_VERSION         OCache version (default: ${OCACHE_VERSION})"
    echo "  TAG_LOG_LEVEL          Log level: debug, info, warn, error (default: ${TAG_LOG_LEVEL})"
    echo "  TAG_PORT               TAG HTTP port (default: ${TAG_PORT})"
    echo "  OCACHE_PORT            OCache data port (default: ${OCACHE_PORT})"
    echo "  OCACHE_HTTP_PORT       OCache HTTP port (default: ${OCACHE_HTTP_PORT})"
    echo "  OCACHE_MAX_DISK_USAGE  Max disk usage in bytes (default: ${OCACHE_MAX_DISK_USAGE})"
    echo "  BIN_DIR                Binary download directory (default: ${BIN_DIR})"
    echo "  DATA_DIR               Data directory (default: ${DATA_DIR})"
    echo ""
    echo "Examples:"
    echo "  export AWS_ACCESS_KEY_ID=<key>"
    echo "  export AWS_SECRET_ACCESS_KEY=<secret>"
    echo "  $0 start"
    echo "  $0 status"
    echo "  $0 logs tag"
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
