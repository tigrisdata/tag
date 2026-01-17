# Build stage
FROM golang:1.24-alpine AS builder

# Install git and ca-certificates (needed for go mod download)
RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build statically linked binary (no CGO needed - TAG is stateless)
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /tag ./cmd/tag

# Runtime stage - minimal image
FROM alpine:3.21

# Install ca-certificates for HTTPS upstream connections
RUN apk add --no-cache ca-certificates

# Create non-root user
RUN adduser -D -u 1000 tag

# Copy binary
COPY --from=builder /tag /usr/local/bin/tag

# Switch to non-root user
USER tag

# Expose HTTP port only (TAG is stateless - connects to external ocache)
# 8080 - HTTP (S3 API)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -q --spider http://localhost:8080/health || exit 1

ENTRYPOINT ["tag"]
CMD ["--config", "/etc/tag/config.yaml"]
