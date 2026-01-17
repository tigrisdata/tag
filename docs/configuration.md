# TAG Configuration

TAG can be configured via a YAML configuration file and/or environment variables. Environment variables take precedence over file configuration.

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `AWS_ACCESS_KEY_ID` | S3 access key for authentication | (required) |
| `AWS_SECRET_ACCESS_KEY` | S3 secret key for authentication | (required) |
| `TAG_UPSTREAM_ENDPOINT` | Tigris S3 endpoint URL | `https://t3.storage.dev` |
| `TAG_OCACHE_ENDPOINTS` | Comma-separated ocache endpoints | (none) |
| `TAG_CACHE_DISABLED` | Disable caching (`true` or `1`) | `false` |
| `TAG_LOG_LEVEL` | Log level: `debug`, `info`, `warn`, `error` | `info` |

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

# Upstream Tigris configuration
upstream:
  # Tigris S3 endpoint URL
  # Default: "https://t3.storage.dev"
  endpoint: "https://t3.storage.dev"

  # AWS region for request signing
  # Default: "auto"
  region: "auto"

# Cache configuration
cache:
  # Enable caching
  # Default: true if endpoints are configured
  enabled: true

  # ocache cluster endpoints
  # If empty, caching is disabled
  endpoints:
    - "ocache-0:9000"
    - "ocache-1:9000"
    - "ocache-2:9000"

  # Default TTL for cached objects
  # Default: 60m
  ttl: 60m

  # Maximum object size to cache (in bytes)
  # Objects larger than this are not cached
  # Default: 1073741824 (1GB)
  size_threshold: 1073741824

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
```

## Configuration Sections

### Server

Controls the HTTP server settings.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `http_port` | int | `8080` | Port for the S3 API |
| `bind_ip` | string | `"0.0.0.0"` | IP address to bind to |

### Upstream

Configures the connection to upstream Tigris storage.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `endpoint` | string | `"https://t3.storage.dev"` | Tigris S3 endpoint URL |
| `region` | string | `"auto"` | AWS region for request signing |

**Endpoint Options:**
- `https://t3.storage.dev` - Default Tigris endpoint

### Cache

Controls the caching behavior and ocache cluster connection.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true`* | Enable caching (*auto-enabled if endpoints configured) |
| `endpoints` | []string | `[]` | ocache cluster endpoints |
| `ttl` | duration | `60m` | Default TTL for cached objects |
| `size_threshold` | int64 | `1073741824` | Max object size to cache (bytes) |

**TTL Format:**
- `60m` - 60 minutes (default)
- `1h` - 1 hour
- `24h` - 24 hours

**Size Threshold Examples:**
- `1073741824` - 1GB (default)
- `104857600` - 100MB
- `536870912` - 512MB

### Broadcast

Controls request coalescing behavior for concurrent requests.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `chunk_size` | int | `65536` | Streaming chunk size (bytes) |
| `channel_buffer` | int | `32` | Buffer size per listener (chunks) |

**Memory Calculation:**
```
Memory per broadcast = chunk_size × channel_buffer × num_listeners
```

With defaults (64KB chunks, 32 buffer):
- 10 listeners: ~20MB
- 100 listeners: ~200MB

### Log

Controls logging output.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `level` | string | `"info"` | Log level |

**Log Levels:**
- `debug` - Verbose debugging information
- `info` - Normal operation messages
- `warn` - Warning conditions
- `error` - Error conditions only

## Example Configurations

### Development (No Caching)

```yaml
server:
  http_port: 8080

upstream:
  endpoint: "https://t3.storage.dev"

log:
  level: "debug"
```

### Production (With Caching)

```yaml
server:
  http_port: 8080
  bind_ip: "0.0.0.0"

cache:
  enabled: true
  endpoints:
    - "ocache-0.ocache.svc.cluster.local:9000"
    - "ocache-1.ocache.svc.cluster.local:9000"
    - "ocache-2.ocache.svc.cluster.local:9000"
  ttl: 60m
  size_threshold: 1073741824

broadcast:
  chunk_size: 65536
  channel_buffer: 32

log:
  level: "info"
```

### High-Throughput (Large Objects)

```yaml
server:
  http_port: 8080

cache:
  enabled: true
  endpoints:
    - "ocache-0:9000"
    - "ocache-1:9000"
  ttl: 1h
  size_threshold: 1073741824  # 1GB

broadcast:
  chunk_size: 131072    # 128KB chunks
  channel_buffer: 64    # Larger buffer

log:
  level: "info"
```

## Command Line Flags

| Flag | Description |
|------|-------------|
| `--config` | Path to configuration file |
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
