# TAG (Tigris Access Gateway) Makefile

# Variables
BINARY_NAME := tag
CMD_PATH := ./cmd/tag

# Allow specifying specific tests with TEST variable (e.g., make test TEST=TestMyFunction)
# or with TESTRUN for pattern matching (e.g., make test TESTRUN=MyFunction)
TEST ?=
TESTRUN ?=
TESTFLAGS := $(if $(TEST),-run $(TEST),$(if $(TESTRUN),-run $(TESTRUN),))

# Build targets
.PHONY: all
all: build

.PHONY: build
build:
	@echo "Building $(BINARY_NAME)..."
	go build -o $(BINARY_NAME) $(CMD_PATH)

# Testing targets
.PHONY: test
test:
	@echo "Running unit tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
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
test-cache:
	@echo "Running cache tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	go test -v -timeout 30s $(TESTFLAGS) ./cache/...

.PHONY: test-proxy
test-proxy:
	@echo "Running proxy tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	go test -v -timeout 30s $(TESTFLAGS) ./proxy/...

.PHONY: test-race
test-race:
	@echo "Running unit tests with race detector..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	go test -race -v -timeout 120s $(TESTFLAGS) ./auth/... ./cache/... ./config/... ./handlers/... ./metrics/... ./proxy/...

.PHONY: test-coverage
test-coverage:
	@echo "Running unit tests with coverage..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	go test -coverprofile=coverage.out -timeout 60s $(TESTFLAGS) ./auth/... ./cache/... ./config/... ./handlers/... ./metrics/... ./proxy/...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated at coverage.html"

# Integration test targets
.PHONY: test-integration
test-integration:
	@echo "Running integration tests..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	go test -v -timeout 300s $(TESTFLAGS) ./tests/integration/...

.PHONY: test-integration-short
test-integration-short:
	@echo "Running integration tests (short mode)..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	go test -v -short -timeout 30s $(TESTFLAGS) ./tests/integration/...

.PHONY: test-integration-race
test-integration-race:
	@echo "Running integration tests with race detector..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
	go test -race -v -timeout 300s $(TESTFLAGS) ./tests/integration/...

.PHONY: test-integration-coverage
test-integration-coverage:
	@echo "Running integration tests with coverage..."
	$(if $(TEST)$(TESTRUN),@echo "Filter: $(if $(TEST),$(TEST),$(TESTRUN))",)
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

# Help target
.PHONY: help
help:
	@echo "TAG (Tigris Access Gateway) Makefile targets:"
	@echo ""
	@echo "Build targets:"
	@echo "  all           - Build the binary (default)"
	@echo "  build         - Build the TAG binary"
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
	@echo "  s3-test-local      - Start local S3 test env (TAG on host, ocache in Docker)"
	@echo "  s3-test-local-down - Stop local S3 test environment"
	@echo "  s3-tests           - Run S3 compatibility tests (ceph s3-tests)"
	@echo "  s3-tests-clean     - Remove cloned s3-tests repository"
	@echo ""
	@echo "  LOCAL DEVELOPMENT (recommended - no GH_TOKEN needed):"
	@echo "    export AWS_ACCESS_KEY_ID=<your-key>"
	@echo "    export AWS_SECRET_ACCESS_KEY=<your-secret>"
	@echo "    make s3-test-local      # Starts ocache + TAG locally"
	@echo "    make s3-tests           # Run tests"
	@echo "    make s3-test-local-down # Cleanup"
	@echo ""
	@echo "  CI/DOCKER BUILD (requires GH_TOKEN for private repos):"
	@echo "  s3-test-infra      - Start S3 test infrastructure (TAG + ocache in Docker)"
	@echo "  s3-test-infra-down - Stop S3 test infrastructure"
	@echo "    export GH_TOKEN=<your-github-pat>"
	@echo "    export AWS_ACCESS_KEY_ID=<your-key>"
	@echo "    export AWS_SECRET_ACCESS_KEY=<your-secret>"
	@echo "    make s3-test-infra && make s3-tests"
	@echo ""
	@echo "Other targets:"
	@echo "  clean         - Remove built binary and generated files"
	@echo "  help          - Show this help message"

# S3 compatibility tests
S3_TEST_COMPOSE := docker compose -f tests/s3compat/docker-compose.yml

.PHONY: s3-test-infra
s3-test-infra:
	@echo "Starting S3 test infrastructure..."
	@if [ -z "$$GH_TOKEN" ]; then \
		echo "Error: GH_TOKEN environment variable is not set."; \
		echo "Set it to a GitHub Personal Access Token with repo access:"; \
		echo "  export GH_TOKEN=<your-github-pat>"; \
		echo "Create a PAT at: https://github.com/settings/tokens"; \
		exit 1; \
	fi
	$(S3_TEST_COMPOSE) --profile full build
	$(S3_TEST_COMPOSE) --profile full up -d --wait

.PHONY: s3-test-infra-down
s3-test-infra-down:
	@echo "Stopping S3 test infrastructure..."
	$(S3_TEST_COMPOSE) --profile full down -v

# Local development: Run TAG on host, only ocache in Docker (no GH_TOKEN needed)
.PHONY: s3-test-local
s3-test-local: build
	@echo "Starting S3 test infrastructure (local mode)..."
	@if [ -z "$$AWS_ACCESS_KEY_ID" ] || [ -z "$$AWS_SECRET_ACCESS_KEY" ]; then \
		echo "Error: AWS credentials not set."; \
		echo "  export AWS_ACCESS_KEY_ID=<your-key>"; \
		echo "  export AWS_SECRET_ACCESS_KEY=<your-secret>"; \
		exit 1; \
	fi
	$(S3_TEST_COMPOSE) up -d ocache --wait
	@echo "Starting TAG locally..."
	@TAG_UPSTREAM_ENDPOINT=$${TAG_UPSTREAM_ENDPOINT:-https://t3.storage.dev} \
		TAG_OCACHE_ENDPOINTS=localhost:9000 \
		TAG_LOG_LEVEL=$${TAG_LOG_LEVEL:-debug} \
		./$(BINARY_NAME) &
	@echo "Waiting for TAG to be ready..."
	@timeout 30 bash -c 'until curl -s http://localhost:8080/health > /dev/null 2>&1; do sleep 1; done' || \
		(echo "TAG failed to start"; exit 1)
	@echo "TAG is ready at http://localhost:8080"

.PHONY: s3-test-local-down
s3-test-local-down:
	@echo "Stopping S3 test infrastructure (local mode)..."
	-@pkill -f "./$(BINARY_NAME)" 2>/dev/null || true
	$(S3_TEST_COMPOSE) down -v

.PHONY: s3-tests
s3-tests:
	@echo "Running S3 compatibility tests..."
	cd tests/s3compat && ./run-tests.sh

.PHONY: s3-tests-clean
s3-tests-clean:
	@echo "Cleaning up S3 test artifacts..."
	rm -rf tests/s3compat/s3-tests

.DEFAULT_GOAL := help
