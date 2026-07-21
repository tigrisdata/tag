# TAG Configuration

TAG can be configured via a YAML configuration file and/or environment variables. Environment variables take precedence over file configuration.

## Environment Variables

| Variable                          | Description                                                                     | Default                  |
| --------------------------------- | ------------------------------------------------------------------------------- | ------------------------ |
| `AWS_ACCESS_KEY_ID`               | S3 access key (must have read-only access for all buckets accessed through TAG) | (required)               |
| `AWS_SECRET_ACCESS_KEY`           | S3 secret key for authentication                                                | (required)               |
| `TAG_UPSTREAM_ENDPOINT`           | Tigris S3 endpoint URL                                                          | `https://t3.storage.dev` |
| `TAG_MAX_IDLE_CONNS_PER_HOST`     | HTTP connection pool size per upstream host                                     | `100`                    |
| `TAG_CACHE_TTL`                   | Default TTL for cached objects (Go duration, e.g. `12h`, `30m`)                 | `24h`                    |
| `TAG_CACHE_DISABLED`              | Disable caching (`true` or `1`)                                                 | `false`                  |
| `TAG_CACHE_DISK_PATH`             | Path to cache data directory                                                    | `/var/cache/tag`         |
| `TAG_CACHE_MAX_DISK_USAGE`        | Max disk usage in bytes (0 = unlimited)                                         | `0`                      |
| `TAG_CACHE_EVICTION_POLICY`       | Eviction order when the disk cap is hit: `lru` or `fifo` (oldest-written first)  | `lru`                    |
| `TAG_CACHE_WARM_ON_WRITE`         | Warm the cache after a successful write via a background fetch (`true`/`false`)  | `false`                  |
| `TAG_CACHE_NODE_ID`               | Unique node identifier for cluster mode                                         | (none)                   |
| `TAG_CACHE_CLUSTER_ADDR`          | Address for memberlist gossip                                                   | `:7000`                  |
| `TAG_CACHE_GRPC_ADDR`             | Address for gRPC server                                                         | `:9000`                  |
| `TAG_CACHE_ADVERTISE_ADDR`        | Address advertised to other nodes                                               | (defaults to GRPC addr)  |
| `TAG_CACHE_SEED_NODES`            | Comma-separated seed nodes for cluster discovery                                | (none)                   |
| `TAG_CACHE_DELETE_BATCH_SIZE`     | File deletions processed per deletion-queue batch                               | `1000`                   |
| `TAG_CACHE_RECOVERY_WORKERS`      | Parallel workers for startup file recovery                                      | `16`                     |
| `TAG_CACHE_MAX_CONCURRENT_WRITES` | Max concurrent cache-populate operations                                        | `256`                    |
| `TAG_CACHE_MAX_POPULATE_MEMORY`   | Aggregate memory budget (bytes) for concurrent cache-populate buffering         | `1073741824` (1 GiB)     |
| `TAG_MAX_INFLIGHT_REQUESTS`       | Max concurrently-served S3 requests before shedding with 503 SlowDown           | `1024`                   |
| `TAG_LOG_LEVEL`                   | Log level: `debug`, `info`, `warn`, `error`                                     | `info`                   |
| `TAG_LOG_FORMAT`                  | Log format: `json` or `console`                                                 | `json`                   |
| `TAG_TRANSPARENT_PROXY`           | Disable transparent proxy mode (`false` or `0`)                                 | `true`                   |
| `TAG_TLS_CERT_FILE`               | Path to TLS certificate file (PEM format)                                       | (none)                   |
| `TAG_TLS_KEY_FILE`                | Path to TLS private key file (PEM format)                                       | (none)                   |
| `TAG_PPROF_ENABLED`               | Enable pprof endpoints (`true` or `1`)                                          | `false`                  |

## Configuration File

The configuration file uses YAML format. Specify the path with the `--config` flag:

```bash
./tag --config /etc/tag/config.yaml
```

### Full Configuration Reference

```yaml
# Server configuration
server:
  # HTTP port for the S3 API
  # Default: 8080
  http_port: 8080

  # IP address to bind to
  # Default: "0.0.0.0" (all interfaces)
  bind_ip: "0.0.0.0"

  # Enable pprof profiling endpoints
  # Default: false (disabled for security)
  pprof_enabled: false

  # Max concurrently-served S3 requests; excess is shed with 503 SlowDown so
  # overload becomes backpressure instead of unbounded goroutine/memory growth.
  # Operational endpoints (/health, /metrics, /debug/pprof/*) are exempt.
  # Default: 1024 (0 or unset = default; negative = disabled)
  # Override with TAG_MAX_INFLIGHT_REQUESTS env var
  max_inflight_requests: 1024

  # Path to TLS certificate file (PEM format)
  # When both tls_cert_file and tls_key_file are set, TAG serves HTTPS
  # Default: "" (TLS disabled, serves HTTP)
  tls_cert_file: ""

  # Path to TLS private key file (PEM format)
  # Must be set together with tls_cert_file
  # Default: "" (TLS disabled, serves HTTP)
  tls_key_file: ""

# Upstream Tigris configuration
upstream:
  # Tigris S3 endpoint URL
  # Default: "https://t3.storage.dev"
  endpoint: "https://t3.storage.dev"

  # AWS region for request signing
  # Default: "auto"
  region: "auto"

  # HTTP connection pool size per upstream host
  # Higher values improve throughput for cache miss scenarios
  # Default: 100
  max_idle_conns_per_host: 100

  # Enable transparent proxy mode
  # When true (default), client requests are forwarded as-is with proxy headers.
  # When false, TAG validates and re-signs requests (signing mode).
  # Default: true
  transparent_proxy: true

# Cache configuration (embedded OCache)
cache:
  # Enable caching
  # Default: true
  enabled: true

  # Default TTL for cached objects
  # Default: 24h
  # Override with TAG_CACHE_TTL env var
  ttl: 24h

  # Maximum object size to cache (in bytes)
  # Objects larger than this are not cached
  # Default: 1073741824 (1GB)
  size_threshold: 1073741824

  # Path to cache data directory
  # Default: /var/cache/tag
  disk_path: "/var/cache/tag"

  # Max disk usage in bytes (0 = unlimited)
  # Default: 0
  max_disk_usage_bytes: 0

  # Eviction order when the disk cap is reached: "lru" (default) or "fifo".
  # "fifo" evicts oldest-written objects first — better for write-once workloads
  # (e.g. dated parquet) where a rare read of an old object should not keep it
  # resident at the expense of newer, hotter data.
  # NOTE: eviction only runs when max_disk_usage_bytes > 0. With no disk cap the
  # cache is never evicted and this setting has no effect.
  # Override with TAG_CACHE_EVICTION_POLICY env var
  eviction_policy: lru

  # Warm the cache after a successful write (PutObject / CompleteMultipartUpload /
  # CopyObject) by triggering a background full-object fetch, so a read soon after a
  # write hits cache. This is cache-warm-on-write (write-around plus async warming),
  # NOT strict write-through: the write still invalidates, and the warm is a
  # separate, best-effort background GET — deduplicated and shed under the cache
  # populate budget. It costs one extra upstream GET per write, so it defaults off.
  # The warm reads with TAG's own credentials in transparent mode and the client's
  # in signing mode; in signing mode a client authorized only to write will have its
  # warm fetch fail (recorded as a background-fetch failure), same as if it read the
  # object itself.
  # Override with TAG_CACHE_WARM_ON_WRITE env var
  warm_on_write: false

  # Unique node identifier for cluster mode
  # Required for multi-node deployments
  node_id: "tag-node-1"

  # Address for memberlist gossip protocol
  # Default: :7000
  cluster_addr: ":7000"

  # Address for gRPC server (cache routing)
  # Default: :9000
  grpc_addr: ":9000"

  # Address advertised to other nodes
  # Defaults to grpc_addr if not specified
  advertise_addr: "tag-node-1:9000"

  # Seed nodes for cluster discovery
  # List of cluster addresses for other nodes
  seed_nodes:
    - "tag-node-1:7000"
    - "tag-node-2:7000"
    - "tag-node-3:7000"

  # File deletions processed per deletion-queue batch
  # Default: 1000
  # Override with TAG_CACHE_DELETE_BATCH_SIZE env var
  delete_batch_size: 1000

  # Parallel workers for startup file recovery
  # Default: 16
  # Override with TAG_CACHE_RECOVERY_WORKERS env var
  recovery_workers: 16

  # Max concurrent cache-populate operations (upstream fetch + streaming write).
  # When saturated, objects are served from upstream without being cached, so the
  # memory/I/O-heavy write path can't grow unbounded.
  # Default: 256 (0 or unset = default; negative = disabled)
  # Override with TAG_CACHE_MAX_CONCURRENT_WRITES env var
  max_concurrent_writes: 256

  # Aggregate memory budget for concurrent cache-populate buffering. Each populate
  # reserves its object size (capped at the per-populate buffer ceiling,
  # ~(channel_buffer + max(channel_buffer/4, 64)) x chunk_size) against this budget;
  # when it can't fit, the object is served from upstream uncached. Small objects
  # reserve little (high concurrency) while a burst of large objects is throttled.
  # Applied independently of max_concurrent_writes (both limits apply). This is what
  # actually bounds populate memory — a byte-unaware count can pin many GB.
  # Default: 1073741824 (1 GiB) (0 or unset = default; negative = memory cap disabled)
  # Override with TAG_CACHE_MAX_POPULATE_MEMORY env var
  max_populate_memory_bytes: 1073741824

# Broadcast configuration (request coalescing)
broadcast:
  # Chunk size for streaming (in bytes)
  # Default: 65536 (64KB)
  chunk_size: 65536

  # Buffer size per listener (in chunks)
  # Total buffer = chunk_size * channel_buffer
  # Default: 32 (~2MB with default chunk size)
  channel_buffer: 32

# Logging configuration
log:
  # Log level: debug, info, warn, error
  # Default: "info"
  level: "info"

  # Log format: json (fast) or console (human-readable)
  # Default: "json"
  format: "json"
```

## Configuration Sections

### Server

Controls the HTTP server settings.

| Field                   | Type   | Default     | Description                                                                                                                                                           |
| ----------------------- | ------ | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `http_port`             | int    | `8080`      | Port for the S3 API                                                                                                                                                   |
| `bind_ip`               | string | `"0.0.0.0"` | IP address to bind to                                                                                                                                                 |
| `pprof_enabled`         | bool   | `false`     | Enable pprof profiling endpoints                                                                                                                                      |
| `tls_cert_file`         | string | `""`        | Path to TLS certificate file (PEM)                                                                                                                                    |
| `tls_key_file`          | string | `""`        | Path to TLS private key file (PEM)                                                                                                                                    |
| `max_inflight_requests` | int    | `1024`      | Max concurrently-served S3 requests before shedding with 503 SlowDown (`0`/unset = default, negative = disabled). `/health`, `/metrics`, `/debug/pprof/*` are exempt. |

### Upstream

Configures the connection to upstream Tigris storage.

| Field                     | Type   | Default                    | Description                                      |
| ------------------------- | ------ | -------------------------- | ------------------------------------------------ |
| `endpoint`                | string | `"https://t3.storage.dev"` | Tigris S3 endpoint URL                           |
| `region`                  | string | `"auto"`                   | AWS region for request signing                   |
| `max_idle_conns_per_host` | int    | `100`                      | HTTP connection pool size per host               |
| `transparent_proxy`       | bool   | `true`                     | Forward client requests as-is with proxy headers |

**Endpoint Validation:**

In **transparent proxy mode** (the default), the upstream endpoint must be one of the following allowed hosts, and TAG will refuse to start otherwise:

- `localhost` (for local development)
- `*.tigris.dev` (e.g., `fly.storage.tigris.dev`)
- `*.storage.dev` (e.g., `t3.storage.dev`)

This restriction exists because transparent mode adds `X-Tigris-Proxy-*` identity headers that are only meaningful to Tigris. In **signing mode** (`transparent_proxy: false`) the endpoint is not restricted — see below.

**Transparent Proxy Mode:**

When `transparent_proxy` is `true` (default), TAG forwards client requests to Tigris as-is, preserving the original Authorization header and adding proxy headers so Tigris validates the signature against the original host. No local credential store is needed. This mode works only with Tigris.

When `transparent_proxy` is `false` (signing mode), TAG validates incoming request signatures against its local credential store, then re-signs each request for the upstream endpoint with the same credentials. The store is populated from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, so clients must authenticate with those credentials and TAG re-signs upstream with them. See [Credential Handling by Mode](security.md#credential-handling-by-mode) in the security docs.

**Using TAG with other S3-compatible services:**

Signing mode re-signs requests with standard AWS SigV4, so it works against any S3-compatible endpoint (AWS S3, MinIO, Ceph, etc.), not just Tigris. To point TAG at a non-Tigris backend:

```yaml
upstream:
  transparent_proxy: false
  endpoint: "https://s3.us-east-1.amazonaws.com"
  # Set the backend's region; the default "auto" only works with Tigris. AWS and
  # other region-sensitive services reject signatures whose credential scope
  # region does not match the endpoint.
  region: "us-east-1"
```

Notes:

- The `endpoint` must be an absolute `http://` or `https://` URL with a host; TAG refuses to start otherwise (in any mode).
- Set `region` to match the backend. The default `auto` is Tigris-specific; region-sensitive services such as AWS S3 return signature errors unless the region matches the endpoint (e.g., `us-east-1` for `s3.us-east-1.amazonaws.com`).
- Transparent proxy mode and its zero-config, no-double-auth experience are Tigris-only. Third-party backends are supported on a best-effort, community-supported basis.
- TAG logs a warning at startup when signing mode runs against a non-Tigris endpoint.
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` are the credentials for the backend TAG signs requests to; clients must present these same credentials, and they need permissions covering whatever operations clients perform (read-only is insufficient if clients write).
- `upstream.endpoint` is trusted operator configuration and TAG only ever forwards to that single host (never a client-chosen one) — see [Endpoint Validation](security.md#endpoint-validation) in the security docs.

**TLS / HTTPS:**

TAG can serve HTTPS when both `tls_cert_file` and `tls_key_file` are configured. Both must be provided together or TAG will refuse to start. This allows clients inside your environment to connect over an encrypted channel.

```bash
# Enable HTTPS via environment variables
TAG_TLS_CERT_FILE=/etc/tag/tls/cert.pem TAG_TLS_KEY_FILE=/etc/tag/tls/key.pem ./tag
```

Or via configuration file:

```yaml
server:
  tls_cert_file: "/etc/tag/tls/cert.pem"
  tls_key_file: "/etc/tag/tls/key.pem"
```

The certificate file should contain the full chain (server certificate followed by intermediates). For development, you can generate self-signed certificates with:

```bash
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes -subj '/CN=localhost'
```

### Cache

Controls the embedded cache behavior. TAG uses an embedded OCache instance with RocksDB storage.

| Field                   | Type     | Default          | Description                                                                         |
| ----------------------- | -------- | ---------------- | ----------------------------------------------------------------------------------- |
| `enabled`               | bool     | `true`           | Enable caching                                                                      |
| `ttl`                   | duration | `24h`            | Default TTL for cached objects                                                      |
| `size_threshold`        | int64    | `1073741824`     | Max object size to cache (bytes)                                                    |
| `disk_path`             | string   | `/var/cache/tag` | Path to cache data directory                                                        |
| `max_disk_usage_bytes`  | int64    | `0`              | Max disk usage (0 = unlimited)                                                      |
| `eviction_policy`       | string   | `lru`            | Eviction order when the disk cap is hit: `lru` or `fifo` (only applies when `max_disk_usage_bytes` > 0) |
| `warm_on_write`         | bool     | `false`          | Warm the cache after a successful write via a background fetch (one extra upstream GET per write) |
| `node_id`               | string   | `""`             | Unique node identifier for cluster mode                                             |
| `cluster_addr`          | string   | `:7000`          | Address for memberlist gossip                                                       |
| `grpc_addr`             | string   | `:9000`          | Address for gRPC server                                                             |
| `advertise_addr`        | string   | `""`             | Address advertised to other nodes                                                   |
| `seed_nodes`            | []string | `[]`             | Seed nodes for cluster discovery                                                    |
| `delete_batch_size`     | int      | `1000`           | File deletions processed per deletion-queue batch                                   |
| `recovery_workers`      | int      | `16`             | Parallel workers for startup file recovery                                          |
| `max_concurrent_writes` | int      | `256`            | Max concurrent cache-populate operations (`0`/unset = default, negative = disabled) |
| `max_populate_memory_bytes` | int  | `1073741824`     | Aggregate memory budget for concurrent cache-populate buffering; each populate reserves its size (capped at the buffer ceiling), applied independently of `max_concurrent_writes` (`0`/unset = default 1 GiB, negative = memory cap disabled) |

**TTL Format:**

- `24h` - 24 hours (default)
- `1h` - 1 hour
- `60m` - 60 minutes

**Size Threshold Examples:**

- `1073741824` - 1GB (default)
- `104857600` - 100MB
- `536870912` - 512MB

**Cluster Mode:**

For multi-node deployments, configure each node with:

- A unique `node_id`
- The same `seed_nodes` list (all nodes in the cluster)
- Appropriate `advertise_addr` (reachable from other nodes)

### Broadcast

Controls request coalescing behavior for concurrent requests.

| Field            | Type | Default | Description                       |
| ---------------- | ---- | ------- | --------------------------------- |
| `chunk_size`     | int  | `65536` | Streaming chunk size (bytes)      |
| `channel_buffer` | int  | `32`    | Buffer size per listener (chunks) |

**Memory Calculation:**

```text
Memory per broadcast = chunk_size × channel_buffer × num_listeners
```

With defaults (64KB chunks, 32 buffer):

- 10 listeners: ~20MB
- 100 listeners: ~200MB

### Log

Controls logging output.

| Field    | Type   | Default  | Description                     |
| -------- | ------ | -------- | ------------------------------- |
| `level`  | string | `"info"` | Log level                       |
| `format` | string | `"json"` | Log format: `json` or `console` |

**Log Levels:**

- `debug` - Verbose debugging information
- `info` - Normal operation messages
- `warn` - Warning conditions
- `error` - Error conditions only

### Profiling

TAG exposes pprof endpoints for performance profiling when enabled. **Disabled by default** for security (exposes runtime internals).

```bash
# Enable pprof
TAG_PPROF_ENABLED=true ./tag
```

**Endpoints** (when enabled):

- `/debug/pprof/` - Index
- `/debug/pprof/profile?seconds=30` - CPU profile
- `/debug/pprof/heap` - Heap profile
- `/debug/pprof/goroutine` - Goroutine stacks

**Usage with go tool pprof:**

```bash
go tool pprof http://localhost:8080/debug/pprof/profile?seconds=30
go tool pprof http://localhost:8080/debug/pprof/heap
```

## Example Configurations

### Development (Standalone)

```yaml
server:
  http_port: 8080

upstream:
  endpoint: "https://t3.storage.dev"

cache:
  enabled: true
  disk_path: "/tmp/tag-cache"
  node_id: "dev-node"

log:
  level: "debug"
```

### Production (Single Node)

```yaml
server:
  http_port: 8080
  bind_ip: "0.0.0.0"

cache:
  enabled: true
  disk_path: "/var/cache/tag"
  max_disk_usage_bytes: 107374182400 # 100GB
  ttl: 24h
  size_threshold: 1073741824
  node_id: "tag-prod"

broadcast:
  chunk_size: 65536
  channel_buffer: 32

log:
  level: "info"
```

### Production (Single Node with TLS)

```yaml
server:
  http_port: 443
  bind_ip: "0.0.0.0"
  tls_cert_file: "/etc/tag/tls/cert.pem"
  tls_key_file: "/etc/tag/tls/key.pem"

cache:
  enabled: true
  disk_path: "/var/cache/tag"
  max_disk_usage_bytes: 107374182400 # 100GB
  ttl: 24h
  size_threshold: 1073741824
  node_id: "tag-prod"

log:
  level: "info"
```

### Production (Cluster Mode)

```yaml
server:
  http_port: 8080

cache:
  enabled: true
  disk_path: "/var/cache/tag"
  max_disk_usage_bytes: 107374182400 # 100GB per node
  ttl: 1h
  size_threshold: 1073741824

  # Cluster configuration
  node_id: "tag-1" # Unique per node
  cluster_addr: ":7000"
  grpc_addr: ":9000"
  advertise_addr: "tag-1.tag-svc.default.svc.cluster.local:9000"
  seed_nodes:
    - "tag-1.tag-svc.default.svc.cluster.local:7000"
    - "tag-2.tag-svc.default.svc.cluster.local:7000"
    - "tag-3.tag-svc.default.svc.cluster.local:7000"

broadcast:
  chunk_size: 131072 # 128KB chunks
  channel_buffer: 64 # Larger buffer

log:
  level: "info"
```

## Command Line Flags

| Flag              | Description                         |
| ----------------- | ----------------------------------- |
| `--config`        | Path to configuration file          |
| `--disable-cache` | Disable caching (pass-through mode) |

**Examples:**

```bash
# Use configuration file
./tag --config /etc/tag/config.yaml

# Disable caching via flag (overrides config)
./tag --config /etc/tag/config.yaml --disable-cache

# Use environment variables only (no config file)
AWS_ACCESS_KEY_ID=xxx AWS_SECRET_ACCESS_KEY=yyy ./tag
```

## Credential Configuration

Credentials are loaded from environment variables:

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
```

## Configuration Precedence

1. Command line flags (highest priority)
2. Environment variables
3. Configuration file
4. Default values (lowest priority)
