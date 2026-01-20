# TAG (Tigris Access Gateway)

TAG is a high-performance S3-compatible caching proxy for [Tigris](https://tigris.dev) object storage. It provides transparent caching with request coalescing to reduce upstream load and improve latency for frequently accessed objects.

## Features

- **S3-Compatible API**: Supports GET, PUT, DELETE, HEAD, and COPY operations
- **Transparent Caching**: Automatic caching of objects with configurable TTL and size thresholds
- **Request Coalescing**: Streaming broadcast pattern reduces duplicate upstream requests under concurrent load
- **Range Request Caching**: Background fetch of full objects on range cache miss for optimal ML training workloads
- **Conditional Requests**: Supports If-None-Match and If-Modified-Since for efficient cache validation
- **AWS SigV4 Authentication**: Full AWS Signature Version 4 validation and re-signing
- **Prometheus Metrics**: Comprehensive metrics for monitoring cache efficiency and performance
- **Kubernetes Ready**: Includes deployment manifests for production use

## Quick Start

### Prerequisites

- Go 1.24 or later
- Access to an [ocache](https://github.com/tigrisdata/ocache) cluster (for caching)
- Tigris account with access credentials

### Developer Setup (One-Time)

This project depends on private Tigris repositories. Configure Go and Git to access them:

```bash
# Tell Go to fetch tigrisdata repos directly (not via proxy)
export GOPRIVATE=github.com/tigrisdata

# Configure Git to use SSH for tigrisdata repos
git config --global url."git@github.com:tigrisdata/".insteadOf "https://github.com/tigrisdata/"
```

Add the `GOPRIVATE` export to your shell profile (e.g., `~/.bashrc` or `~/.zshrc`) for persistence.

### Build

```bash
make build
```

### Run

```bash
# Set credentials via environment variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key

# Run with default configuration
./tag

# Run with custom configuration
./tag --config /path/to/config.yaml

# Run with debug logging
TAG_LOG_LEVEL=debug ./tag
```

### Test

```bash
# Run all tests
make test

# Run specific package tests
make test-auth
make test-cache
make test-proxy

# Run with race detector
make test-race
```

## Configuration

TAG can be configured via YAML file or environment variables. Key settings:

- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` - Tigris credentials (required)
- `TAG_OCACHE_ENDPOINTS` - Comma-separated ocache endpoints for caching
- `TAG_LOG_LEVEL` - Log level: debug, info, warn, error

See [docs/configuration.md](docs/configuration.md) for full configuration reference.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Client    │────▶│    TAG      │────▶│   Tigris    │
│  (S3 SDK)   │◀────│   Proxy     │◀────│   Storage   │
└─────────────┘     └──────┬──────┘     └─────────────┘
                           │
                           ▼
                    ┌─────────────┐
                    │   ocache    │
                    │   Cluster   │
                    └─────────────┘
```

### Request Flow

1. **Cache Check**: TAG first checks if the object exists in the ocache cluster
2. **Cache Hit**: Returns cached object with `X-Cache: HIT` header
3. **Cache Miss**: Forwards request to upstream Tigris, caches response, returns with `X-Cache: MISS`

### Request Coalescing

When multiple concurrent requests arrive for the same uncached object:

1. First request becomes the "fetcher" and streams from upstream
2. Subsequent requests join as "listeners" to the same broadcast
3. All listeners receive data simultaneously as it streams from upstream
4. Only one upstream request is made, regardless of concurrent client count

See [docs/architecture.md](docs/architecture.md) for detailed architecture documentation.

## Metrics

TAG exposes Prometheus metrics at `/metrics` including request counts, latencies, cache hit/miss rates, and broadcast statistics.

See [docs/metrics.md](docs/metrics.md) for complete metrics reference.

## Deployment

### Docker

```bash
# Single node (TAG + ocache)
cd docker
docker-compose up -d

# Cluster mode (2 TAG + 3 ocache)
docker-compose -f docker-compose-cluster.yml up -d
```

### Kubernetes

```bash
kubectl create secret generic tag-credentials \
  --from-literal=AWS_ACCESS_KEY_ID=your_key \
  --from-literal=AWS_SECRET_ACCESS_KEY=your_secret

kubectl apply -f deploy/
```

See [docs/deployment.md](docs/deployment.md) for complete deployment guide including environment setup, cluster configuration, and production considerations.

## API Reference

TAG implements a subset of the S3 API:

| Operation | Endpoint | Description |
|-----------|----------|-------------|
| GetObject | `GET /{bucket}/{key}` | Retrieve object (with caching) |
| PutObject | `PUT /{bucket}/{key}` | Upload object (invalidates cache) |
| DeleteObject | `DELETE /{bucket}/{key}` | Delete object (invalidates cache) |
| HeadObject | `HEAD /{bucket}/{key}` | Get object metadata |
| CopyObject | `PUT /{bucket}/{key}` with `x-amz-copy-source` | Copy object |

### Response Headers

| Header | Description |
|--------|-------------|
| `X-Cache` | Cache status: `HIT`, `MISS`, `BYPASS`, or `DISABLED` |

### Cache Behavior

- Objects larger than `size_threshold` are not cached
- Objects with `Cache-Control: no-store` or `private` are not cached
- Range requests trigger background fetch of full object (if within threshold)
- PUT/DELETE operations invalidate the cache entry

See [docs/usage.md](docs/usage.md) for examples using AWS CLI and Python boto3.

## Development

```bash
# Format code
make fmt

# Run linters
make lint

# Run all checks
make check

# Generate coverage report
make test-coverage
```

## License

See [LICENSE](LICENSE) for details.
