# TAG (Tigris Access Gateway)

High-performance S3-compatible caching proxy for Tigris object storage with embedded distributed cache.

## Build & Test Commands

```bash
# Build
make build          # Build the binary (auto-downloads dependencies)
make install-deps   # Install system dependencies (compression libs)

# Unit tests
make test           # Run unit tests
make test-auth      # Run auth package tests
make test-cache     # Run cache package tests
make test-proxy     # Run proxy package tests
make test-race      # Run tests with race detector
make test-coverage  # Generate coverage report

# Integration tests
make test-integration  # Run integration tests
make test-all          # Run all tests (unit + integration)

# Code quality
make lint           # Run linters (vet, gofmt check, mod tidy)
make lint-fix       # Fix linting issues
make fmt            # Format code
make check          # Run all checks (fmt, vet, test)

# Run
make run            # Run TAG with default options
make run-verbose    # Run TAG with debug logging

# S3 compatibility tests
make s3-test-local  # Start TAG locally with embedded cache
make s3-tests       # Run S3 compatibility tests (ceph s3-tests)
make s3-test-local-down  # Stop local TAG and cleanup

# Cleanup
make clean          # Remove binary and generated files
make clean-all      # Remove everything including cache data
```

Filter tests with `TEST` or `TESTRUN` variables:
```bash
TEST=TestGetObject make test
TESTRUN="TestBroadcast" make test-proxy
```

## Core Architecture

- **Embedded OCache**: Uses `github.com/tigrisdata/ocache/embedded` (no external cache server)
- **Optional Clustering**: Memberlist gossip for discovery, gRPC for cache key routing between nodes
- **Broadcast/Subscriber Pattern**: Request coalescing via `proxy/broadcast/` - streams chunks to multiple listeners simultaneously
- **Two-Key Cache Storage**: Objects stored as `meta|bucket|key` (headers/ETag) and `body|bucket|key` (raw bytes)
- **Tombstone Invalidation**: Writes tombstone marker before DELETE to prevent stale async cache writes
- **Semaphore-Gated Background Ops**: `cacheSemaphore` (100 concurrent) limits background cache writes

## Key Dependencies

- `github.com/tigrisdata/ocache/embedded` - Embedded cache with RocksDB storage
- `github.com/tigrisdata/ocache/client` - Cache client interface (CacheClient)
- `github.com/goccy/go-json` - Fast JSON (replaces encoding/json)
- `github.com/aws/aws-sdk-go-v2` - AWS SigV4 signing

## User Preferences

- **Prioritize impact over complexity**: Focus on significant improvements without over-engineering
- **Zero added latency**: Prefer patterns like stream multiplexing over explicit batching delays
- **Linux + macOS support**: OS-specific optimizations must have viable fallbacks
- **Clear plan separation**: When changes span multiple codebases (OCache vs TAG), maintain distinct plans
