# TAG (Tigris Access Gateway)

TAG is a high-performance S3-compatible caching proxy for [Tigris](https://tigris.dev) object storage. It provides transparent caching with request coalescing to reduce upstream load and improve latency for frequently accessed objects.

## Features

- **S3-Compatible API**: Supports all S3 API endpoints supported by Tigris
- **Transparent Proxy Mode**: Forwards client requests as-is with proxy headers, preserving original signatures (enabled by default)
- **Embedded Cache**: High-performance RocksDB-based cache with automatic cluster discovery
- **Request Coalescing**: Streaming broadcast pattern reduces duplicate upstream requests under concurrent load
- **Range Request Caching**: Background fetch of full objects on range cache miss for optimal ML training workloads
- **Conditional Requests**: Supports If-None-Match and If-Modified-Since for efficient cache validation
- **AWS SigV4 Authentication**: Full AWS Signature Version 4 validation and re-signing
- **Prometheus Metrics**: Comprehensive metrics for monitoring cache efficiency and performance
- **Kubernetes Ready**: Includes deployment manifests for production use

## Quick Start

Install the latest release and run it against Tigris:

```bash
# Install the tag binary to /usr/local/bin and a default config to /etc/tag/config.yaml
curl -fsSL https://tag-releases.t3.storage.dev/latest/install.sh | bash

# TAG uses its own Tigris credentials with read-only access to the buckets it caches
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key

# Run (transparent proxy mode by default; clients use their own credentials)
tag --config /etc/tag/config.yaml
```

TAG listens on `http://localhost:8080`. Point any S3 client at it using path-style addressing — see [Usage](#usage). Prefer Docker or Kubernetes? See [Installation](#installation).

## Installation

### Install script (native binary)

Each release publishes the install script, run script, and a matching `config.yaml` to the release bucket, so you can install without cloning the repo:

```bash
# Latest release
curl -fsSL https://tag-releases.t3.storage.dev/latest/install.sh | bash

# A specific release
curl -fsSL https://tag-releases.t3.storage.dev/v1.10.0/install.sh | bash
```

The script installs the `tag` binary to `/usr/local/bin` and a default config to `/etc/tag/config.yaml`.

### Docker

Pull and run the published image with Docker Compose:

```bash
cd deploy/docker
docker compose -f docker-compose.release.yml up -d
```

See [docs/docker.md](docs/docker.md) for single-node and cluster setups. To build the image from source instead, use the Compose files in [`docker/`](docker/).

### Kubernetes

Deploy as a StatefulSet with an embedded distributed cache using the Kustomize manifests in [`deploy/kubernetes/`](deploy/kubernetes/):

```bash
kubectl apply -k deploy/kubernetes/base/
```

See [docs/deploy.md](docs/deploy.md) for the full guide.

### Build from source

Building TAG requires the Go toolchain and access to the Tigris modules — see [Contributing](#contributing).

## Usage

TAG supports all S3 API endpoints supported by Tigris, including bucket operations, object operations, multipart uploads, and more. See the [Tigris S3 API documentation](https://www.tigrisdata.com/docs/api/s3/) for the complete list of supported operations.

### S3 Client Usage

TAG supports **path-style** S3 access only. Virtual-hosted style requests are not supported.

| Style          | URL Format                         | Supported |
| -------------- | ---------------------------------- | --------- |
| Path-style     | `http://localhost:8080/bucket/key` | Yes       |
| Virtual-hosted | `http://bucket.localhost:8080/key` | No        |

When configuring S3 clients, ensure path-style addressing is enabled. See [docs/usage.md](docs/usage.md) for SDK-specific configuration.

### Response Headers

| Header    | Description                                          |
| --------- | ---------------------------------------------------- |
| `X-Cache` | Cache status: `HIT`, `MISS`, `BYPASS`, or `DISABLED` |

### Cache Behavior

- Objects larger than `size_threshold` are not cached
- Objects with `Cache-Control: no-store` or `private` are not cached
- Range requests trigger background fetch of full object (if within threshold)
- PUT/DELETE operations invalidate the cache entry

See [docs/cache-control.md](docs/cache-control.md) for detailed cache control and revalidation documentation.

## Configuration

TAG can be configured via YAML file or environment variables. Key settings:

- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` - TAG's own Tigris credentials with read-only access (required). In transparent proxy mode (default), clients use their own credentials directly.
- `TAG_CACHE_NODE_ID` - Unique node identifier for cluster mode
- `TAG_CACHE_DISK_PATH` - Path to cache data directory
- `TAG_LOG_LEVEL` - Log level: debug, info, warn, error

See [docs/configuration.md](docs/configuration.md) for full configuration reference.

## Deployment

For production, TAG ships manifests, Compose files, and guides under [`deploy/`](deploy/) and [`docs/`](docs/). The [Installation](#installation) section covers getting a single instance running; the guides below cover production concerns:

- **Kubernetes** — StatefulSet, HPA, and services in [`deploy/kubernetes/`](deploy/kubernetes/); high availability, scaling, and probes in [docs/deploy.md](docs/deploy.md).
- **Docker** — single-node and cluster Compose in [`deploy/docker/`](deploy/docker/); see [docs/docker.md](docs/docker.md).
- **TLS/HTTPS** — see [docs/tls.md](docs/tls.md).
- **Benchmarks** — see [docs/benchmarks.md](docs/benchmarks.md).

## Architecture

```text
┌─────────────┐     ┌─────────────────────────────┐     ┌─────────────┐
│   Client    │────▶│           TAG               │────▶│   Tigris    │
│  (S3 SDK)   │◀────│  ┌─────────────────────┐    │◀────│   Storage   │
└─────────────┘     │  │  Embedded Cache     │    │     └─────────────┘
                    │  │  (RocksDB + Gossip) │    │
                    │  └─────────────────────┘    │
                    └─────────────────────────────┘
```

### Request Flow

1. **Cache Check**: TAG first checks if the object exists in its embedded cache
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

## Security

TAG supports transparent proxy mode (default) with local SigV4 validation and per-bucket authorization caching, as well as signing mode with local credential stores.

See [docs/security.md](docs/security.md) for authentication, access control, and security architecture.

## Contributing

### Prerequisites

- Go 1.24 or later
- Tigris account with access credentials (for running TAG and integration tests)

TAG depends on Tigris Go modules. Configure Go and Git to access them:

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

# Run S3 compatibility tests (requires AWS credentials)
make s3-test-local && make s3-tests
```

See [docs/s3-compatibility-testing.md](docs/s3-compatibility-testing.md) for detailed S3 compatibility testing guide.

### Code Quality

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
