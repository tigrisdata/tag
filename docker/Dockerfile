# Build stage - use Debian for CGO/RocksDB support
FROM golang:1.24-bookworm AS builder

# Install build dependencies for RocksDB and make
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    curl \
    make \
    libsnappy-dev \
    liblz4-dev \
    libzstd-dev \
    libbz2-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Configure GOPRIVATE for tigrisdata modules
ENV GOPRIVATE=github.com/tigrisdata

WORKDIR /app

# Copy go mod files first for better layer caching
COPY go.mod go.sum ./

# Download dependencies with git auth for private modules
RUN --mount=type=secret,id=git_auth_token \
    if [ -f /run/secrets/git_auth_token ]; then \
      git config --global url."https://x-access-token:$(cat /run/secrets/git_auth_token)@github.com/".insteadOf "https://github.com/"; \
    fi && \
    go mod download

# Copy source code and Makefile
COPY . .

# Build using Makefile (handles RocksDB download and CGO flags)
# Uses build-docker target for dynamic linking (avoids glibc version mismatch)
RUN make build-docker && cp tag /tag

# Runtime stage - use Debian slim for glibc compatibility
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libsnappy1v5 \
    liblz4-1 \
    libzstd1 \
    libbz2-1.0 \
    zlib1g \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -r -u 1000 -m tag

# Create cache directory
RUN mkdir -p /var/cache/tag && chown tag:tag /var/cache/tag

# Copy binary
COPY --from=builder /tag /usr/local/bin/tag

# Switch to non-root user
USER tag

# Expose ports:
# 8080 - HTTP (S3 API)
# 7000 - Memberlist gossip (cluster discovery)
# 9000 - gRPC (cache routing)
EXPOSE 8080 7000 9000

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -q --spider http://localhost:8080/health || exit 1

ENTRYPOINT ["tag"]
