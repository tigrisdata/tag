# TAG (Tigris Acceleration Gateway)

TAG is a high-performance S3-compatible caching proxy for [Tigris](https://tigris.dev) object storage. It provides transparent caching with request coalescing to reduce upstream load and improve latency for frequently accessed objects.

## Features

- **S3-Compatible API**: Supports all S3 API endpoints supported by Tigris
- **Transparent Proxy Mode**: Forwards client requests as-is with proxy headers, preserving original signatures (enabled by default)
- **Embedded Cache**: High-performance RocksDB-based cache with automatic cluster discovery
- **Request Coalescing**: Streaming broadcast pattern reduces duplicate upstream requests under concurrent load
- **Range Request Caching**: Background fetch of full objects on range cache miss for optimal ML training workloads
- **Block-Aligned Caching** *(default)*: Caches large objects as fixed-size blocks (RFC 0001) so a small range read fetches and caches only the covering blocks instead of the whole object — ideal for sparse reads of large files (Parquet footers/row-groups, SST blocks). Enabled by default at 1 MiB blocks; size `block_size` to your read granularity, or set `block_caching_enabled: false` to cache whole objects.
- **Conditional Requests**: Supports If-None-Match and If-Modified-Since for efficient cache validation
- **AWS SigV4 Authentication**: Full AWS Signature Version 4 validation and re-signing; repeated ASCII spaces are normalized only for signature calculation, not in forwarded metadata
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
curl -fsSL https://tag-releases.t3.storage.dev/v1.19.0/install.sh | bash
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

See [docs/deploy.md](docs/deploy.md) for the full guide. On SIGTERM, TAG gives HTTP requests a 30-second drain window and keeps the embedded cache open until admitted handlers finish their synchronous cleanup.
Kubernetes may enforce a separate termination grace period (see [graceful shutdown](docs/deploy.md#graceful-shutdown)).

### Build from source

Building TAG requires the Go toolchain and access to Tigris modules — see [Contributing](#contributing).

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
| `X-Cache` | Cache decision at response commitment: `HIT`, `MISS`, `BYPASS`, or `DISABLED`; a block-mode `HIT` may include a verified upstream remainder |

### Cache Behavior

- Objects larger than `size_threshold` are not cached
- Objects with `Cache-Control: no-store` or `private` are not cached
- CopyObject requests with `Cache-Control: no-store` or `private` still invalidate the destination but skip post-copy cache warming
- Among cacheable objects (within `size_threshold`), those at or above `block_size` are cached as fixed-size blocks (block caching, on by default), so a range read fetches only the covering blocks; smaller objects are whole-cached
- A later multi-block Range within a range-created entry's recorded span stages its payload blocks before `206`, skipping probes only on a complete hit. The ETag/layout-bound hint is advisory; misses keep existing recovery, and speculative reads fall back upstream after five seconds while the request is live.
- Cached range reads are checked against the requested byte count. A committed block GET can recover an unwritten suffix only from an upstream response with the same ETag
- `X-Cache: HIT` identifies a cache-backed response selected at commitment; it does not promise that every byte came from cache or that the transfer completed
- If a committed block GET has a changed-ETag suffix, or a positive short whole-object Range read occurs, TAG keeps the sent status and Content-Length, returns a handler error, and may end with only the cached prefix; client write failures stop without an upstream retry
- Set `block_caching_enabled: false` to cache whole objects instead — a range request then triggers a background fetch of the full object (if within threshold)
- A dropped whole-object cache listener or upstream body-read error does not publish the partial body as a new cache entry
- In tiered mode, a PUT routed to the local tier checks a concrete SigV4 `X-Amz-Content-Sha256` digest after admission and any local precondition pass and the full body passes its declared-length and any Content-MD5 checks; conditional writes against upstream-tier markers, `UNSIGNED-PAYLOAD`, and aws-chunked uploads keep their existing paths
- PUT/DELETE operations invalidate affected cache entries. For cache-enabled bulk DeleteObjects requests with a nonempty parsed key list, TAG completes an upstream 2xx response only after the post-success invalidation attempts return, bounded by a 10-second fence timeout; a fence that does not converge in time is logged as a warning and the 2xx is still returned.
- Fully captured successful multipart completions are cached for five seconds when TAG can identify the caller; replay requires local validation for the same access key and exact bucket, object key, and upload ID
- Legacy ordered metadata populates are refused when the tombstone cannot be read or is malformed, rather than assuming no invalidation. The detached metadata warm remains best-effort, so a successful S3 write response is unchanged.
- If a whole-object cache write returns an error during a successful GET, the upstream response is unchanged and the cache-populate slot becomes available after relay cleanup, so a later eligible GET can retry. A store call that never returns has no completion guarantee.

See [docs/cache-control.md](docs/cache-control.md) for detailed cache control and revalidation documentation. For tuning and sizing block caching in production, see [docs/deploy.md](docs/deploy.md#block-aligned-caching).

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
- **Parquet workloads** — `cache.parquet_optimization` warms footer blocks and `cache.meta_on_write` caches object metadata before readers need them; see [docs/parquet-optimization.md](docs/parquet-optimization.md).
- **Benchmarks** — see [docs/benchmarks.md](docs/benchmarks.md).

On SIGINT or SIGTERM, TAG drains HTTP requests before joining detached cache work and closing embedded storage. A cache operation that does not return after cancellation can extend shutdown; see [Graceful shutdown](docs/deploy.md#graceful-shutdown).

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
2. **Cache Hit**: Serves using a cache entry with `X-Cache: HIT`; a degraded block stream may append a same-ETag upstream remainder
3. **Cache Miss**: Forwards request to upstream Tigris, caches response, returns with `X-Cache: MISS`

For directly streamed upstream responses, TAG aborts the downstream response if copying the body fails. It does not complete a truncated prefix as a successful response. Clients may see a transport or body-read error.

For streamed PUT and DELETE operations, an upstream 2xx still triggers cache invalidation or a tiered marker when caching is enabled. A delivery error alone does not prove the mutation failed.

### Request Coalescing

Concurrent full-object GETs for the same uncached object share one upstream fetch only when callers
subscribe before the first upstream body chunk starts:

1. First request becomes the "fetcher" and streams from upstream
2. Other requests that subscribe before streaming starts join as "listeners" to the same broadcast
3. Those listeners receive the same upstream response as it streams
4. A GET arriving after streaming starts cannot join that broadcast and starts its own upstream fetch

If an upstream fetch fails before TAG commits the client response while requests are still live, each GET waiting on that broadcast can return TAG's normal S3 error response. After commitment, the stream cannot be replaced by an error document.

See [docs/architecture.md](docs/architecture.md) for detailed architecture documentation.

### Tiered Store Mode

In tiered mode, validated plain-object PUTs no larger than `cache.size_threshold` are stored locally, and their metadata is authoritative. A background re-tier started by a GET is bound to the marker version seen by that GET, so it cannot replace metadata committed by a later write. See [docs/tiered-mode.md](docs/tiered-mode.md) for the full behavior.

## Performance

A single TAG node saturates a 100 Gbps NIC at **~85+ Gbps** for objects 1 MiB and larger, serves **~75K ops/sec** for small objects at sub-millisecond p50, and holds low single-digit-millisecond TTFB — all while using around **12% of available CPU**.

| Object size | OPS (64 threads) | Throughput | TTFB p50 |
| ----------- | ---------------- | ---------- | -------- |
| 1 KiB       | ~75,700          | 74 MiB/s   | < 1 ms   |
| 100 KiB     | ~33,300          | 3.2 GiB/s  | 1 ms     |
| 1 MiB       | ~11,000          | 10.7 GiB/s | 1 ms     |
| 4 MiB       | ~2,800           | 10.8 GiB/s | 1 ms     |

Measured with [warp](https://github.com/minio/warp) against a single `i3en.24xlarge` node over a 100 Gbps link. See [docs/benchmarks.md](docs/benchmarks.md) for the full methodology, go-ycsb results, and limitations.

## Metrics

TAG exposes Prometheus metrics at `/metrics` including request counts, latencies, cache hit/miss rates, and broadcast statistics.

See [docs/metrics.md](docs/metrics.md) for complete metrics reference.

## Security

TAG supports transparent proxy mode (default) with local SigV4 validation and per-bucket authorization caching, as well as signing mode with local credential stores. In transparent mode, TAG grants cached-read authorization for a bucket only after a successful upstream `GetObject` or `HeadObject`. Successful writes and other passthrough responses do not grant cached-read access.

After key learning and bucket authorization, local auth can validate a valid SigV4 presigned GET without an `X-Amz-Content-Sha256` header; header-authenticated requests and other methods still require it.

See [docs/security.md](docs/security.md) for authentication, access control, and security architecture.

## Contributing

### Prerequisites

- Go 1.27 or later
- Tigris account with access credentials (for running TAG and integration tests)

All dependencies, including Tigris's [`ocache`](https://github.com/tigrisdata/ocache) modules, are public and fetched normally by the Go toolchain — no extra configuration required.

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

TAG is licensed under the [Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for attribution.

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md). Contributors are required to sign the [Contributor License Agreement](docs/CLA.md).
