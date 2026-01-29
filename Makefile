# TAG (Tigris Access Gateway) Makefile

# Variables
BINARY_NAME := tag
CMD_PATH := ./cmd/tag
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Local test/bench data directory for embedded cache
TAG_CACHE_DATA_DIR := /tmp/tag-cache-data

# Local test ports (avoid macOS conflicts: 7000 is used by AirPlay Receiver)
TAG_LOCAL_HTTP_PORT := 8080
TAG_LOCAL_GRPC_PORT := 9090
TAG_LOCAL_CLUSTER_PORT := 7070

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
    # macOS: link against static rocksdb + homebrew compression libs
    CGO_LDFLAGS := -L$(ROCKSDB_STATIC_DIR)/lib -L$(BREW_PREFIX)/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd -pthread
    # macOS can't fully static link, just strip symbols
    LDFLAGS := -ldflags "-s -w -X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME) -X main.GitCommit=$(GIT_COMMIT)"
else
    # Linux: fully static binary
    CGO_LDFLAGS := -L$(ROCKSDB_STATIC_DIR)/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd -pthread
    LDFLAGS := -ldflags "-linkmode external -extldflags '-static' -X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME) -X main.GitCommit=$(GIT_COMMIT)"
endif

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
	CGO_ENABLED=1 \
	CGO_CFLAGS="-I$(ROCKSDB_STATIC_DIR)/include" \
	CGO_LDFLAGS="$(CGO_LDFLAGS)" \
	go build $(LDFLAGS) -o $(BINARY_NAME) $(CMD_PATH)

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
test: rocksdb-static
	@echo "Running unit tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	CGO_ENABLED=1 \
	CGO_CFLAGS="-I$(ROCKSDB_STATIC_DIR)/include" \
	CGO_LDFLAGS="$(CGO_LDFLAGS)" \
	go test -v -timeout 60s $(TESTFLAGS) ./auth/... ./cache/... ./config/... ./handlers/... ./metrics/... ./proxy/...

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
	CGO_ENABLED=1 \
	CGO_CFLAGS="-I$(ROCKSDB_STATIC_DIR)/include" \
	CGO_LDFLAGS="$(CGO_LDFLAGS)" \
	go test -v -timeout 30s $(TESTFLAGS) ./cache/...

.PHONY: test-proxy
test-proxy:
	@echo "Running proxy tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	go test -v -timeout 30s $(TESTFLAGS) ./proxy/...

.PHONY: test-race
test-race: rocksdb-static
	@echo "Running unit tests with race detector..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	CGO_ENABLED=1 \
	CGO_CFLAGS="-I$(ROCKSDB_STATIC_DIR)/include" \
	CGO_LDFLAGS="$(CGO_LDFLAGS)" \
	go test -race -v -timeout 120s $(TESTFLAGS) ./auth/... ./cache/... ./config/... ./handlers/... ./metrics/... ./proxy/...

.PHONY: test-coverage
test-coverage: rocksdb-static
	@echo "Running unit tests with coverage..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	CGO_ENABLED=1 \
	CGO_CFLAGS="-I$(ROCKSDB_STATIC_DIR)/include" \
	CGO_LDFLAGS="$(CGO_LDFLAGS)" \
	go test -coverprofile=coverage.out -timeout 60s $(TESTFLAGS) ./auth/... ./cache/... ./config/... ./handlers/... ./metrics/... ./proxy/...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated at coverage.html"

# Integration test targets (require CGO/RocksDB for embedded cache)
.PHONY: test-integration
test-integration: rocksdb-static
	@echo "Running integration tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	CGO_ENABLED=1 \
	CGO_CFLAGS="-I$(ROCKSDB_STATIC_DIR)/include" \
	CGO_LDFLAGS="$(CGO_LDFLAGS)" \
	go test -v -timeout 300s $(TESTFLAGS) ./tests/integration/...

.PHONY: test-integration-short
test-integration-short: rocksdb-static
	@echo "Running integration tests (short mode)..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	CGO_ENABLED=1 \
	CGO_CFLAGS="-I$(ROCKSDB_STATIC_DIR)/include" \
	CGO_LDFLAGS="$(CGO_LDFLAGS)" \
	go test -v -short -timeout 30s $(TESTFLAGS) ./tests/integration/...

.PHONY: test-integration-race
test-integration-race: rocksdb-static
	@echo "Running integration tests with race detector..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	CGO_ENABLED=1 \
	CGO_CFLAGS="-I$(ROCKSDB_STATIC_DIR)/include" \
	CGO_LDFLAGS="$(CGO_LDFLAGS)" \
	go test -race -v -timeout 300s $(TESTFLAGS) ./tests/integration/...

.PHONY: test-integration-coverage
test-integration-coverage: rocksdb-static
	@echo "Running integration tests with coverage..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	CGO_ENABLED=1 \
	CGO_CFLAGS="-I$(ROCKSDB_STATIC_DIR)/include" \
	CGO_LDFLAGS="$(CGO_LDFLAGS)" \
	go test -coverprofile=coverage-integration.out -timeout 300s $(TESTFLAGS) ./tests/integration/...
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
	@echo "  run           - Run TAG with default options"
	@echo "  run-verbose   - Run TAG with debug logging"
	@echo ""
	@echo "S3 compatibility test targets:"
	@echo "  s3-test-local      - Start TAG locally with embedded cache"
	@echo "  s3-test-local-down - Stop local TAG and cleanup"
	@echo "  s3-tests           - Run S3 compatibility tests (ceph s3-tests)"
	@echo "  s3-tests-clean     - Remove cloned s3-tests repository"
	@echo ""
	@echo "  Usage:"
	@echo "    export AWS_ACCESS_KEY_ID=<your-key>"
	@echo "    export AWS_SECRET_ACCESS_KEY=<your-secret>"
	@echo "    make s3-test-local      # Starts TAG with embedded cache"
	@echo "    make s3-tests           # Run tests"
	@echo "    make s3-test-local-down # Cleanup"
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

.PHONY: s3-tests
s3-tests:
	@echo "Running S3 compatibility tests..."
	cd tests/s3compat && ./run-tests.sh

.PHONY: s3-tests-clean
s3-tests-clean:
	@echo "Cleaning up S3 test artifacts..."
	rm -rf tests/s3compat/s3-tests

.DEFAULT_GOAL := help
