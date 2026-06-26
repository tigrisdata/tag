# TAG (Tigris Access Gateway) Makefile

# Variables
BINARY_NAME := tag
CMD_PATH := ./cmd/tag
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Local test/bench data directory for embedded cache
TAG_CACHE_DATA_DIR := /tmp/tag-cache-data

# TLS test certificate directory
TAG_TEST_CERTS_DIR := /tmp/tag-test-certs

# Local test ports (avoid macOS conflicts: 7000 is used by AirPlay Receiver)
TAG_LOCAL_HTTP_PORT := 8080
TAG_LOCAL_GRPC_PORT := 9090
TAG_LOCAL_CLUSTER_PORT := 7070

# Cluster mode ports (2-node cluster for S3 compatibility testing)
TAG_CLUSTER_NODE1_HTTP_PORT := 8080
TAG_CLUSTER_NODE1_GRPC_PORT := 9090
TAG_CLUSTER_NODE1_CLUSTER_PORT := 7070
TAG_CLUSTER_NODE2_HTTP_PORT := 8081
TAG_CLUSTER_NODE2_GRPC_PORT := 9091
TAG_CLUSTER_NODE2_CLUSTER_PORT := 7071
TAG_CLUSTER_CACHE_DIR_1 := /tmp/tag-cluster-data-1
TAG_CLUSTER_CACHE_DIR_2 := /tmp/tag-cluster-data-2

# Detect OS and architecture
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

# Platform mappings for RocksDB static artifacts
# RocksDB artifacts use: Linux-x86_64, Linux-aarch64, macOS-arm64
ifeq ($(UNAME_S),Darwin)
    ROCKSDB_PLATFORM := macOS-arm64
    BREW_PREFIX := $(shell brew --prefix 2>/dev/null || echo "/opt/homebrew")
else
    ifeq ($(UNAME_M),aarch64)
        ROCKSDB_PLATFORM := Linux-aarch64
    else ifeq ($(UNAME_M),arm64)
        ROCKSDB_PLATFORM := Linux-aarch64
    else
        ROCKSDB_PLATFORM := Linux-x86_64
    endif
endif

# RocksDB static build configuration
ROCKSDB_VERSION ?= 10.4.2
ROCKSDB_STATIC_URL := https://ocache-releases.t3.storage.dev/rocksdb/$(ROCKSDB_VERSION)
ROCKSDB_STATIC_ARTIFACT := rocksdb-static-$(ROCKSDB_VERSION)-$(ROCKSDB_PLATFORM).tar.gz
ROCKSDB_STATIC_DIR := $(shell pwd)/rocksdb-static

# CGO configuration for RocksDB (always enabled for embedded cache)
ifeq ($(UNAME_S),Darwin)
    # macOS: link against static rocksdb + statically link compression libs
    # Reference .a files directly to avoid dylib dependencies that break distribution.
    # Using -l flags with Homebrew in the search path causes macOS ld to prefer .dylib,
    # resulting in "Library not loaded: libsnappy.1.dylib" on machines without Homebrew.
    CGO_LDFLAGS := -L$(ROCKSDB_STATIC_DIR)/lib -lrocksdb \
        $(BREW_PREFIX)/opt/snappy/lib/libsnappy.a \
        $(BREW_PREFIX)/opt/lz4/lib/liblz4.a \
        $(BREW_PREFIX)/opt/zstd/lib/libzstd.a \
        $(BREW_PREFIX)/opt/bzip2/lib/libbz2.a \
        $(BREW_PREFIX)/opt/zlib/lib/libz.a \
        -lstdc++ -lm -pthread
    # macOS can't fully static link, just strip symbols
    LDFLAGS := -ldflags "-s -w -X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME) -X main.GitCommit=$(GIT_COMMIT)"
    # Prevent grocksdb from injecting its own -lzstd -llz4 -lz -lsnappy flags,
    # which conflict with the explicit .a paths above.
    BUILD_TAGS := -tags grocksdb_clean_link
else
    # Linux: fully static binary
    CGO_LDFLAGS := -L$(ROCKSDB_STATIC_DIR)/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd -pthread
    LDFLAGS := -ldflags "-linkmode external -extldflags '-static' -X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME) -X main.GitCommit=$(GIT_COMMIT)"
endif

# CGO environment for RocksDB (used across multiple targets)
CGO_ENV := CGO_ENABLED=1 CGO_CFLAGS="-I$(ROCKSDB_STATIC_DIR)/include" CGO_LDFLAGS="$(CGO_LDFLAGS)"

# Allow specifying specific tests with TEST variable (e.g., make test TEST=TestMyFunction)
# or with TESTRUN for pattern matching (e.g., make test TESTRUN=MyFunction)
TEST ?=
TESTRUN ?=
TESTFLAGS := $(if $(TEST),-run $(TEST),$(if $(TESTRUN),-run $(TESTRUN),))

# Build targets
.PHONY: all
all: build

.PHONY: build
build: rocksdb-static
	@echo "Building $(BINARY_NAME) with embedded cache..."
	$(CGO_ENV) go build $(BUILD_TAGS) $(LDFLAGS) -o $(BINARY_NAME) $(CMD_PATH)

# Download and extract RocksDB static artifacts
.PHONY: rocksdb-static
rocksdb-static:
	@if [ ! -d "$(ROCKSDB_STATIC_DIR)/include" ]; then \
		echo "Downloading RocksDB static artifacts for $(ROCKSDB_PLATFORM)..."; \
		mkdir -p $(ROCKSDB_STATIC_DIR); \
		curl -fsSL "$(ROCKSDB_STATIC_URL)/$(ROCKSDB_STATIC_ARTIFACT)" -o $(ROCKSDB_STATIC_DIR)/rocksdb.tar.gz && \
		cd $(ROCKSDB_STATIC_DIR) && tar -xzf rocksdb.tar.gz && \
		rm rocksdb.tar.gz; \
		echo "RocksDB static artifacts extracted to $(ROCKSDB_STATIC_DIR)"; \
	else \
		echo "RocksDB static artifacts already present at $(ROCKSDB_STATIC_DIR)"; \
	fi

# Clean RocksDB static artifacts
.PHONY: rocksdb-static-clean
rocksdb-static-clean:
	@echo "Removing RocksDB static artifacts..."
	rm -rf $(ROCKSDB_STATIC_DIR)

# Install system dependencies for RocksDB compression libraries
.PHONY: install-deps
install-deps:
	@echo "Installing system dependencies for RocksDB..."
ifeq ($(UNAME_S),Darwin)
	@echo "Detected macOS - using Homebrew..."
	brew install snappy lz4 zstd bzip2 zlib
else
	@echo "Detected Linux - using apt-get..."
	sudo apt-get update
	sudo apt-get install -y libsnappy-dev liblz4-dev libzstd-dev libbz2-dev zlib1g-dev
endif
	@echo "System dependencies installed successfully."

# Testing targets
.PHONY: test
test: build
	@echo "Running unit tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	$(CGO_ENV) go test $(BUILD_TAGS) -v -timeout 60s $(TESTFLAGS) ./auth/... ./cache/... ./cmd/... ./config/... ./handlers/... ./metrics/... ./proxy/...

.PHONY: test-all
test-all: test test-integration
	@echo "All tests completed!"

.PHONY: test-auth
test-auth:
	@echo "Running auth tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	go test -v -timeout 30s $(TESTFLAGS) ./auth/...

.PHONY: test-cache
test-cache: rocksdb-static
	@echo "Running cache tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	$(CGO_ENV) go test $(BUILD_TAGS) -v -timeout 30s $(TESTFLAGS) ./cache/...

.PHONY: test-proxy
test-proxy:
	@echo "Running proxy tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	go test -v -timeout 30s $(TESTFLAGS) ./proxy/...

.PHONY: test-race
test-race: rocksdb-static
	@echo "Running unit tests with race detector..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	$(CGO_ENV) go test $(BUILD_TAGS) -race -v -timeout 120s $(TESTFLAGS) ./auth/... ./cache/... ./config/... ./handlers/... ./metrics/... ./proxy/...

.PHONY: test-coverage
test-coverage: rocksdb-static
	@echo "Running unit tests with coverage..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	$(CGO_ENV) go test $(BUILD_TAGS) -coverprofile=coverage.out -timeout 60s $(TESTFLAGS) ./auth/... ./cache/... ./config/... ./handlers/... ./metrics/... ./proxy/...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated at coverage.html"

# Integration test targets (require CGO/RocksDB for embedded cache)
.PHONY: test-integration
test-integration: rocksdb-static
	@echo "Running integration tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	$(CGO_ENV) go test $(BUILD_TAGS) -v -timeout 300s $(TESTFLAGS) ./tests/integration/...

.PHONY: test-integration-short
test-integration-short: rocksdb-static
	@echo "Running integration tests (short mode)..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	$(CGO_ENV) go test $(BUILD_TAGS) -v -short -timeout 30s $(TESTFLAGS) ./tests/integration/...

.PHONY: test-integration-race
test-integration-race: rocksdb-static
	@echo "Running integration tests with race detector..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	$(CGO_ENV) go test $(BUILD_TAGS) -race -v -timeout 300s $(TESTFLAGS) ./tests/integration/...

.PHONY: test-integration-coverage
test-integration-coverage: rocksdb-static
	@echo "Running integration tests with coverage..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	$(CGO_ENV) go test $(BUILD_TAGS) -coverprofile=coverage-integration.out -timeout 300s $(TESTFLAGS) ./tests/integration/...
	go tool cover -html=coverage-integration.out -o coverage-integration.html
	@echo "Integration coverage report generated at coverage-integration.html"

# Code quality targets
.PHONY: lint
lint:
	@echo "Running go vet..."
	go vet ./...
	@echo "Running gofmt check..."
	@gofmt -l -d $$(find . -name '*.go')
	@echo "Running go mod tidy..."
	go mod tidy

.PHONY: lint-ci
lint-ci:
	@echo "Running gofmt check..."
	@gofmt -l -d $$(find . -name '*.go')
	@echo "Running go mod tidy check..."
	go mod tidy
	@git diff --exit-code go.mod go.sum || (echo "go.mod or go.sum is not tidy" && exit 1)

.PHONY: lint-fix
lint-fix:
	@echo "Fixing formatting issues..."
	@gofmt -w $$(find . -name '*.go')
	@echo "Running go mod tidy..."
	go mod tidy

.PHONY: vet
vet:
	go vet ./...

.PHONY: fmt
fmt:
	@gofmt -w $$(find . -name '*.go')

.PHONY: fmt-check
fmt-check:
	@gofmt -l $$(find . -name '*.go')

.PHONY: check
check: fmt-check vet test
	@echo "All checks passed!"

# Run targets
.PHONY: run
run: build
	./$(BINARY_NAME)

.PHONY: run-verbose
run-verbose: build
	TAG_LOG_LEVEL=debug ./$(BINARY_NAME)

# Clean targets
.PHONY: clean
clean:
	rm -f $(BINARY_NAME)
	rm -f coverage.out coverage.html

.PHONY: clean-all
clean-all: clean rocksdb-static-clean
	rm -rf $(TAG_CACHE_DATA_DIR)

# Help target
.PHONY: help
help:
	@echo "TAG (Tigris Access Gateway) Makefile targets:"
	@echo ""
	@echo "Build targets:"
	@echo "  all             - Build the binary (default)"
	@echo "  build           - Build TAG with embedded cache (requires RocksDB)"
	@echo "  rocksdb-static  - Download RocksDB static artifacts (auto-downloaded by build)"
	@echo ""
	@echo "  Build requires RocksDB static artifacts which are auto-downloaded."
	@echo "  Platforms supported: Linux-x86_64, Linux-aarch64, macOS-arm64"
	@echo ""
	@echo "Test targets:"
	@echo "  test          - Run unit tests only"
	@echo "  test-all      - Run all tests (unit + integration)"
	@echo "  test-auth     - Run auth package tests"
	@echo "  test-cache    - Run cache package tests"
	@echo "  test-proxy    - Run proxy package tests"
	@echo "  test-race     - Run unit tests with race detector"
	@echo "  test-coverage - Run unit tests with coverage report"
	@echo ""
	@echo "Integration test targets:"
	@echo "  test-integration          - Run integration tests"
	@echo "  test-integration-short    - Run integration tests (short mode)"
	@echo "  test-integration-race     - Run integration tests with race detector"
	@echo "  test-integration-coverage - Run integration tests with coverage report"
	@echo ""
	@echo "  To run specific tests, use TEST or TESTRUN variable:"
	@echo "    make test TEST=TestMyFunction      - Run exact test name"
	@echo "    make test TESTRUN=MyFunction       - Run tests matching pattern"
	@echo "    make test-auth TEST=TestValidator  - Run specific auth test"
	@echo ""
	@echo "Code quality targets:"
	@echo "  lint          - Run linters (vet, gofmt check, mod tidy)"
	@echo "  lint-fix      - Fix linting issues"
	@echo "  vet           - Run go vet"
	@echo "  fmt           - Format code with gofmt"
	@echo "  fmt-check     - Check code formatting"
	@echo "  check         - Run all quality checks (fmt, vet, test)"
	@echo ""
	@echo "Run targets:"
	@echo "  run           - Run TAG with default options (use --help for CLI flags)"
	@echo "  run-verbose   - Run TAG with debug logging (use --version for version info)"
	@echo ""
	@echo "S3 compatibility test targets:"
	@echo "  s3-test-local          - Start TAG locally with embedded cache"
	@echo "  s3-test-local-cluster  - Start TAG locally as a 2-node cluster"
	@echo "  s3-tests               - Run S3 compatibility tests (Python s3-tests)"
	@echo "  s3-tests-clean         - Remove cloned s3-tests repository"
	@echo "  s3-test-local-down     - Stop local TAG and cleanup"
	@echo "  s3-test-local-cluster-down - Stop local TAG cluster and cleanup"
	@echo ""
	@echo "S3 compatibility test targets (TLS):"
	@echo "  s3-test-local-tls      - Start TAG locally with TLS (auto-generates certs)"
	@echo "  s3-tests-tls           - Run S3 compatibility tests over HTTPS"
	@echo "  s3-test-local-tls-down - Stop local TAG and cleanup (incl. certs)"
	@echo ""
	@echo "SDK test targets (Go SDK tests):"
	@echo "  test-sdk               - Run SDK tests (requires running TAG + AWS creds)"
	@echo ""
	@echo "Benchmark test targets (warp):"
	@echo "  bench-warp             - Benchmark core S3 ops with warp (requires running TAG + AWS creds)"
	@echo "  bench-warp-clean       - Remove cached warp binary and benchmark results"
	@echo ""
	@echo "  Benchmark usage:"
	@echo "    export AWS_ACCESS_KEY_ID=<your-key>"
	@echo "    export AWS_SECRET_ACCESS_KEY=<your-secret>"
	@echo "    make s3-test-local      # Start TAG with embedded cache"
	@echo "    make bench-warp         # Run warp benchmarks (GET/RANGE/PUT/HEAD/LIST/DELETE)"
	@echo "    make s3-test-local-down # Stop local TAG and cleanup"
	@echo ""
	@echo "  Usage:"
	@echo "    export AWS_ACCESS_KEY_ID=<your-key>"
	@echo "    export AWS_SECRET_ACCESS_KEY=<your-secret>"
	@echo "    make s3-test-local      # Start TAG with embedded cache (HTTP)"
	@echo "    make s3-test-local-tls  # Start TAG with embedded cache (HTTPS)"
	@echo "    make s3-tests           # Run S3 compatibility tests"
	@echo "    make s3-tests-tls       # Run S3 compatibility tests (TLS)"
	@echo "    make test-sdk           # Run Go SDK tests"
	@echo "    make s3-test-local-down # Stop local TAG and cleanup"
	@echo ""
	@echo "Other targets:"
	@echo "  clean              - Remove built binary and generated files"
	@echo "  clean-all          - Remove binary, rocksdb artifacts, and cache data"
	@echo "  rocksdb-static-clean - Remove downloaded RocksDB static artifacts"
	@echo "  help               - Show this help message"

# Local development: Run TAG on host with embedded cache
# Uses non-default ports to avoid macOS conflicts (AirPlay uses 7000)
.PHONY: s3-test-local
s3-test-local: build
	@echo "Starting TAG with embedded cache (local mode)..."
	@if [ -z "$$AWS_ACCESS_KEY_ID" ] || [ -z "$$AWS_SECRET_ACCESS_KEY" ]; then \
		echo "Error: AWS credentials not set."; \
		echo "  export AWS_ACCESS_KEY_ID=<your-key>"; \
		echo "  export AWS_SECRET_ACCESS_KEY=<your-secret>"; \
		exit 1; \
	fi
	@# Stop any existing TAG process and free up ports
	-@pkill -f "./$(BINARY_NAME)" 2>/dev/null || true
	-@lsof -ti:$(TAG_LOCAL_CLUSTER_PORT) | xargs kill 2>/dev/null || true
	-@lsof -ti:$(TAG_LOCAL_GRPC_PORT) | xargs kill 2>/dev/null || true
	-@lsof -ti:$(TAG_LOCAL_HTTP_PORT) | xargs kill 2>/dev/null || true
	@sleep 1
	@mkdir -p $(TAG_CACHE_DATA_DIR)
	@echo "Starting TAG locally with embedded cache..."
	@echo "  HTTP: $(TAG_LOCAL_HTTP_PORT), gRPC: $(TAG_LOCAL_GRPC_PORT), Cluster: $(TAG_LOCAL_CLUSTER_PORT)"
	@TAG_UPSTREAM_ENDPOINT=$${TAG_UPSTREAM_ENDPOINT:-https://t3.storage.dev} \
		TAG_CACHE_NODE_ID=tag-local \
		TAG_CACHE_DISK_PATH=$(TAG_CACHE_DATA_DIR) \
		TAG_CACHE_CLUSTER_ADDR=:$(TAG_LOCAL_CLUSTER_PORT) \
		TAG_CACHE_GRPC_ADDR=:$(TAG_LOCAL_GRPC_PORT) \
		TAG_LOG_LEVEL=$${TAG_LOG_LEVEL:-info} \
		TAG_PPROF_ENABLED=true \
		./$(BINARY_NAME) &
	@echo "Waiting for TAG to be ready..."
	@timeout 30 bash -c 'until curl -s http://localhost:$(TAG_LOCAL_HTTP_PORT)/health > /dev/null 2>&1; do sleep 1; done' || \
		(echo "TAG failed to start"; exit 1)
	@echo "TAG is ready at http://localhost:$(TAG_LOCAL_HTTP_PORT)"

.PHONY: s3-test-local-down
s3-test-local-down:
	@echo "Stopping TAG (local mode)..."
	-@pkill -f "./$(BINARY_NAME)" 2>/dev/null || true
	-@lsof -ti:$(TAG_LOCAL_CLUSTER_PORT) | xargs kill 2>/dev/null || true
	-@lsof -ti:$(TAG_LOCAL_GRPC_PORT) | xargs kill 2>/dev/null || true
	-@lsof -ti:$(TAG_LOCAL_HTTP_PORT) | xargs kill 2>/dev/null || true
	@echo "Cleaning up cache data directory..."
	-@rm -rf $(TAG_CACHE_DATA_DIR)

# Local development: Run a 2-node TAG cluster on host with embedded cache
# Uses non-default ports to avoid macOS conflicts (AirPlay uses 7000)
# Node 1 listens on the same HTTP port as single-node mode so s3-tests works unchanged
.PHONY: s3-test-local-cluster
s3-test-local-cluster: build
	@echo "Starting TAG 2-node cluster (local mode)..."
	@if [ -z "$$AWS_ACCESS_KEY_ID" ] || [ -z "$$AWS_SECRET_ACCESS_KEY" ]; then \
		echo "Error: AWS credentials not set (required for gRPC auth)."; \
		echo "  export AWS_ACCESS_KEY_ID=<your-key>"; \
		echo "  export AWS_SECRET_ACCESS_KEY=<your-secret>"; \
		exit 1; \
	fi
	@# Stop any existing TAG processes and free up ports
	-@pkill -f "./$(BINARY_NAME)" 2>/dev/null || true
	-@lsof -ti:$(TAG_CLUSTER_NODE1_HTTP_PORT) | xargs kill 2>/dev/null || true
	-@lsof -ti:$(TAG_CLUSTER_NODE1_GRPC_PORT) | xargs kill 2>/dev/null || true
	-@lsof -ti:$(TAG_CLUSTER_NODE1_CLUSTER_PORT) | xargs kill 2>/dev/null || true
	-@lsof -ti:$(TAG_CLUSTER_NODE2_HTTP_PORT) | xargs kill 2>/dev/null || true
	-@lsof -ti:$(TAG_CLUSTER_NODE2_GRPC_PORT) | xargs kill 2>/dev/null || true
	-@lsof -ti:$(TAG_CLUSTER_NODE2_CLUSTER_PORT) | xargs kill 2>/dev/null || true
	@sleep 1
	@mkdir -p $(TAG_CLUSTER_CACHE_DIR_1) $(TAG_CLUSTER_CACHE_DIR_2)
	@echo "Starting node 1..."
	@echo "  HTTP: $(TAG_CLUSTER_NODE1_HTTP_PORT), gRPC: $(TAG_CLUSTER_NODE1_GRPC_PORT), Cluster: $(TAG_CLUSTER_NODE1_CLUSTER_PORT)"
	@TAG_UPSTREAM_ENDPOINT=$${TAG_UPSTREAM_ENDPOINT:-https://t3.storage.dev} \
		TAG_CACHE_NODE_ID=tag-node-1 \
		TAG_CACHE_DISK_PATH=$(TAG_CLUSTER_CACHE_DIR_1) \
		TAG_CACHE_CLUSTER_ADDR=:$(TAG_CLUSTER_NODE1_CLUSTER_PORT) \
		TAG_CACHE_GRPC_ADDR=:$(TAG_CLUSTER_NODE1_GRPC_PORT) \
		TAG_CACHE_ADVERTISE_ADDR=localhost:$(TAG_CLUSTER_NODE1_GRPC_PORT) \
		TAG_CACHE_SEED_NODES=localhost:$(TAG_CLUSTER_NODE1_CLUSTER_PORT),localhost:$(TAG_CLUSTER_NODE2_CLUSTER_PORT) \
		TAG_LOG_LEVEL=$${TAG_LOG_LEVEL:-info} \
		TAG_PPROF_ENABLED=true \
		./$(BINARY_NAME) &
	@echo "Starting node 2..."
	@echo "  HTTP: $(TAG_CLUSTER_NODE2_HTTP_PORT), gRPC: $(TAG_CLUSTER_NODE2_GRPC_PORT), Cluster: $(TAG_CLUSTER_NODE2_CLUSTER_PORT)"
	@TAG_UPSTREAM_ENDPOINT=$${TAG_UPSTREAM_ENDPOINT:-https://t3.storage.dev} \
		TAG_CACHE_NODE_ID=tag-node-2 \
		TAG_CACHE_DISK_PATH=$(TAG_CLUSTER_CACHE_DIR_2) \
		TAG_CACHE_CLUSTER_ADDR=:$(TAG_CLUSTER_NODE2_CLUSTER_PORT) \
		TAG_CACHE_GRPC_ADDR=:$(TAG_CLUSTER_NODE2_GRPC_PORT) \
		TAG_CACHE_ADVERTISE_ADDR=localhost:$(TAG_CLUSTER_NODE2_GRPC_PORT) \
		TAG_CACHE_SEED_NODES=localhost:$(TAG_CLUSTER_NODE1_CLUSTER_PORT),localhost:$(TAG_CLUSTER_NODE2_CLUSTER_PORT) \
		TAG_HTTP_PORT=$(TAG_CLUSTER_NODE2_HTTP_PORT) \
		TAG_LOG_LEVEL=$${TAG_LOG_LEVEL:-info} \
		TAG_PPROF_ENABLED=true \
		./$(BINARY_NAME) &
	@echo "Waiting for both nodes to be ready..."
	@timeout 30 bash -c 'until curl -s http://localhost:$(TAG_CLUSTER_NODE1_HTTP_PORT)/health > /dev/null 2>&1; do sleep 1; done' || \
		(echo "Node 1 failed to start"; exit 1)
	@echo "Node 1 is ready at http://localhost:$(TAG_CLUSTER_NODE1_HTTP_PORT)"
	@timeout 30 bash -c 'until curl -s http://localhost:$(TAG_CLUSTER_NODE2_HTTP_PORT)/health > /dev/null 2>&1; do sleep 1; done' || \
		(echo "Node 2 failed to start"; exit 1)
	@echo "Node 2 is ready at http://localhost:$(TAG_CLUSTER_NODE2_HTTP_PORT)"
	@echo "TAG 2-node cluster is ready (run 'make s3-tests' to test)"

.PHONY: s3-test-local-cluster-down
s3-test-local-cluster-down:
	@echo "Stopping TAG cluster (local mode)..."
	-@pkill -f "./$(BINARY_NAME)" 2>/dev/null || true
	-@lsof -ti:$(TAG_CLUSTER_NODE1_HTTP_PORT) | xargs kill 2>/dev/null || true
	-@lsof -ti:$(TAG_CLUSTER_NODE1_GRPC_PORT) | xargs kill 2>/dev/null || true
	-@lsof -ti:$(TAG_CLUSTER_NODE1_CLUSTER_PORT) | xargs kill 2>/dev/null || true
	-@lsof -ti:$(TAG_CLUSTER_NODE2_HTTP_PORT) | xargs kill 2>/dev/null || true
	-@lsof -ti:$(TAG_CLUSTER_NODE2_GRPC_PORT) | xargs kill 2>/dev/null || true
	-@lsof -ti:$(TAG_CLUSTER_NODE2_CLUSTER_PORT) | xargs kill 2>/dev/null || true
	@echo "Cleaning up cluster cache data directories..."
	-@rm -rf $(TAG_CLUSTER_CACHE_DIR_1) $(TAG_CLUSTER_CACHE_DIR_2)

.PHONY: s3-tests
s3-tests:
	@echo "Running S3 compatibility tests..."
	cd tests/s3compat/python && ./run-tests.sh

.PHONY: s3-tests-clean
s3-tests-clean:
	@echo "Cleaning up S3 test artifacts..."
	rm -rf tests/s3compat/python/s3-tests

# Generate self-signed TLS certificates for local testing
.PHONY: generate-test-certs
generate-test-certs:
	@if [ ! -f "$(TAG_TEST_CERTS_DIR)/cert.pem" ]; then \
		echo "Generating self-signed TLS certificates..."; \
		mkdir -p $(TAG_TEST_CERTS_DIR); \
		openssl req -x509 -newkey rsa:2048 -keyout $(TAG_TEST_CERTS_DIR)/key.pem \
			-out $(TAG_TEST_CERTS_DIR)/cert.pem -days 365 -nodes \
			-subj "/CN=localhost" 2>/dev/null; \
		echo "TLS certificates generated in $(TAG_TEST_CERTS_DIR)"; \
	else \
		echo "TLS certificates already present at $(TAG_TEST_CERTS_DIR)"; \
	fi

# Local development: Run TAG on host with embedded cache and TLS enabled
.PHONY: s3-test-local-tls
s3-test-local-tls: build generate-test-certs
	@echo "Starting TAG with embedded cache and TLS (local mode)..."
	@if [ -z "$$AWS_ACCESS_KEY_ID" ] || [ -z "$$AWS_SECRET_ACCESS_KEY" ]; then \
		echo "Error: AWS credentials not set."; \
		echo "  export AWS_ACCESS_KEY_ID=<your-key>"; \
		echo "  export AWS_SECRET_ACCESS_KEY=<your-secret>"; \
		exit 1; \
	fi
	@# Stop any existing TAG process and free up ports
	-@pkill -f "./$(BINARY_NAME)" 2>/dev/null || true
	-@lsof -ti:$(TAG_LOCAL_CLUSTER_PORT) | xargs kill 2>/dev/null || true
	-@lsof -ti:$(TAG_LOCAL_GRPC_PORT) | xargs kill 2>/dev/null || true
	-@lsof -ti:$(TAG_LOCAL_HTTP_PORT) | xargs kill 2>/dev/null || true
	@sleep 1
	@mkdir -p $(TAG_CACHE_DATA_DIR)
	@echo "Starting TAG locally with embedded cache and TLS..."
	@echo "  HTTPS: $(TAG_LOCAL_HTTP_PORT), gRPC: $(TAG_LOCAL_GRPC_PORT), Cluster: $(TAG_LOCAL_CLUSTER_PORT)"
	@TAG_UPSTREAM_ENDPOINT=$${TAG_UPSTREAM_ENDPOINT:-https://t3.storage.dev} \
		TAG_CACHE_NODE_ID=tag-local \
		TAG_CACHE_DISK_PATH=$(TAG_CACHE_DATA_DIR) \
		TAG_CACHE_CLUSTER_ADDR=:$(TAG_LOCAL_CLUSTER_PORT) \
		TAG_CACHE_GRPC_ADDR=:$(TAG_LOCAL_GRPC_PORT) \
		TAG_LOG_LEVEL=$${TAG_LOG_LEVEL:-info} \
		TAG_PPROF_ENABLED=true \
		TAG_TLS_CERT_FILE=$(TAG_TEST_CERTS_DIR)/cert.pem \
		TAG_TLS_KEY_FILE=$(TAG_TEST_CERTS_DIR)/key.pem \
		./$(BINARY_NAME) &
	@echo "Waiting for TAG to be ready..."
	@timeout 30 bash -c 'until curl -sk https://localhost:$(TAG_LOCAL_HTTP_PORT)/health > /dev/null 2>&1; do sleep 1; done' || \
		(echo "TAG failed to start"; exit 1)
	@echo "TAG is ready at https://localhost:$(TAG_LOCAL_HTTP_PORT)"

.PHONY: s3-test-local-tls-down
s3-test-local-tls-down:
	@echo "Stopping TAG (local TLS mode)..."
	-@pkill -f "./$(BINARY_NAME)" 2>/dev/null || true
	-@lsof -ti:$(TAG_LOCAL_CLUSTER_PORT) | xargs kill 2>/dev/null || true
	-@lsof -ti:$(TAG_LOCAL_GRPC_PORT) | xargs kill 2>/dev/null || true
	-@lsof -ti:$(TAG_LOCAL_HTTP_PORT) | xargs kill 2>/dev/null || true
	@echo "Cleaning up cache data directory and TLS certificates..."
	-@rm -rf $(TAG_CACHE_DATA_DIR)
	-@rm -rf $(TAG_TEST_CERTS_DIR)

.PHONY: s3-tests-tls
s3-tests-tls:
	@echo "Running S3 compatibility tests (TLS)..."
	cd tests/s3compat/python && S3TEST_CONF_TEMPLATE=s3tests-tls.conf ./run-tests.sh

# SDK tests against external TAG (requires running TAG instance)
.PHONY: test-sdk
test-sdk: rocksdb-static
	@echo "Running SDK tests against external TAG..."
	@if [ -z "$$AWS_ACCESS_KEY_ID" ] || [ -z "$$AWS_SECRET_ACCESS_KEY" ]; then \
		echo "Error: AWS credentials not set."; \
		echo "  export AWS_ACCESS_KEY_ID=<your-key>"; \
		echo "  export AWS_SECRET_ACCESS_KEY=<your-secret>"; \
		exit 1; \
	fi
	@if ! curl -s http://localhost:$(TAG_LOCAL_HTTP_PORT)/health > /dev/null 2>&1; then \
		echo "Error: TAG not running at localhost:$(TAG_LOCAL_HTTP_PORT)"; \
		echo "  Start TAG with: make s3-test-local"; \
		exit 1; \
	fi
	TAG_ENDPOINT=http://localhost:$(TAG_LOCAL_HTTP_PORT) $(CGO_ENV) go test $(BUILD_TAGS) -v -timeout 300s $(TESTFLAGS) ./tests/s3compat/sdk/...

# Benchmark TAG's core S3 operations with warp (requires a running TAG + AWS creds).
# Start TAG first with: make s3-test-local
.PHONY: bench-warp
bench-warp:
	@echo "Running warp benchmarks against TAG..."
	@if [ -z "$$AWS_ACCESS_KEY_ID" ] || [ -z "$$AWS_SECRET_ACCESS_KEY" ]; then \
		echo "Error: AWS credentials not set."; \
		echo "  export AWS_ACCESS_KEY_ID=<your-key>"; \
		echo "  export AWS_SECRET_ACCESS_KEY=<your-secret>"; \
		exit 1; \
	fi
	@if ! curl -s http://localhost:$(TAG_LOCAL_HTTP_PORT)/health > /dev/null 2>&1; then \
		echo "Error: TAG not running at localhost:$(TAG_LOCAL_HTTP_PORT)"; \
		echo "  Start TAG with: make s3-test-local"; \
		exit 1; \
	fi
	cd tests/benchmark && TAG_HTTP_PORT=$(TAG_LOCAL_HTTP_PORT) ./run-warp.sh

.PHONY: bench-warp-clean
bench-warp-clean:
	@echo "Cleaning up warp benchmark artifacts..."
	rm -rf tests/benchmark/.bin tests/benchmark/results

.DEFAULT_GOAL := help
