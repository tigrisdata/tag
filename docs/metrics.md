# TAG Metrics Reference

TAG exposes Prometheus metrics at the `/metrics` endpoint.

## Accessing Metrics

```bash
# Local
curl http://localhost:8080/metrics

# Kubernetes (port-forward)
kubectl port-forward svc/tag 8080:8080
curl http://localhost:8080/metrics
```

## Metrics Reference

### Request Metrics

#### tag_requests_total

**Type:** Counter

Total number of requests processed by TAG.

| Label | Description |
|-------|-------------|
| `operation` | S3 operation: `GetObject`, `PutObject`, `DeleteObject`, `HeadObject` |
| `status` | Result: `success`, `error`, `auth_error`, `range_not_satisfiable` |

**Example queries:**
```promql
# Request rate by operation
rate(tag_requests_total[5m])

# Error rate
sum(rate(tag_requests_total{status="error"}[5m])) / sum(rate(tag_requests_total[5m]))

# GetObject success rate
rate(tag_requests_total{operation="GetObject",status="success"}[5m]) /
rate(tag_requests_total{operation="GetObject"}[5m])
```

#### tag_request_duration_seconds

**Type:** Histogram

Request duration in seconds.

| Label | Description |
|-------|-------------|
| `operation` | S3 operation |

**Buckets:** Default Prometheus buckets (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)

**Example queries:**
```promql
# P50 latency
histogram_quantile(0.5, rate(tag_request_duration_seconds_bucket[5m]))

# P99 latency by operation
histogram_quantile(0.99, sum(rate(tag_request_duration_seconds_bucket[5m])) by (operation, le))

# Average request duration
rate(tag_request_duration_seconds_sum[5m]) / rate(tag_request_duration_seconds_count[5m])
```

### Cache Metrics

#### tag_cache_hits_total

**Type:** Counter

Total number of cache hits.

**Example queries:**
```promql
# Cache hit rate
rate(tag_cache_hits_total[5m])

# Cache hit ratio
rate(tag_cache_hits_total[5m]) /
(rate(tag_cache_hits_total[5m]) + rate(tag_cache_misses_total[5m]))
```

#### tag_cache_misses_total

**Type:** Counter

Total number of cache misses.

#### tag_cache_operations_total

**Type:** Counter

Total number of cache operations.

| Label | Description |
|-------|-------------|
| `operation` | Operation type: `get`, `put`, `delete` |
| `result` | Result: `hit`, `miss`, `success`, `error` |

**Example queries:**
```promql
# Cache operation breakdown
sum by (operation, result) (rate(tag_cache_operations_total[5m]))
```

#### tag_range_from_cache_hits_total

**Type:** Counter

Number of range requests served from cached full objects.

**Example queries:**
```promql
# Range cache efficiency
rate(tag_range_from_cache_hits_total[5m]) /
rate(tag_requests_total{operation="GetObject"}[5m])
```

### Broadcast Metrics

#### tag_broadcast_shared_total

**Type:** Counter

Number of requests that joined an existing broadcast stream.

**Example queries:**
```promql
# Coalescing ratio (higher is better)
rate(tag_broadcast_shared_total[5m]) /
(rate(tag_broadcast_shared_total[5m]) + rate(tag_broadcast_fetches_total[5m]))
```

#### tag_broadcast_fetches_total

**Type:** Counter

Number of upstream fetches (broadcast initiators).

#### tag_broadcast_slow_consumers_total

**Type:** Counter

Number of listeners disconnected for being too slow.

#### tag_active_broadcasts

**Type:** Gauge

Number of currently active broadcast streams.

**Example queries:**
```promql
# Active broadcasts over time
tag_active_broadcasts

# Max active broadcasts
max_over_time(tag_active_broadcasts[1h])
```

### Background Fetch Metrics

#### tag_background_fetches_triggered_total

**Type:** Counter

Number of background full-object fetches triggered by range requests.

#### tag_background_fetches_succeeded_total

**Type:** Counter

Number of background fetches that completed successfully.

#### tag_background_fetches_failed_total

**Type:** Counter

Number of background fetches that failed.

**Example queries:**
```promql
# Background fetch success rate
rate(tag_background_fetches_succeeded_total[5m]) /
rate(tag_background_fetches_triggered_total[5m])
```

#### tag_active_background_fetches

**Type:** Gauge

Number of currently active background fetches.

### Upstream Metrics

#### tag_upstream_request_duration_seconds

**Type:** Histogram

Upstream (Tigris) request duration in seconds.

| Label | Description |
|-------|-------------|
| `method` | HTTP method: `GET`, `PUT`, `DELETE`, `HEAD` |

**Example queries:**
```promql
# Upstream P99 latency
histogram_quantile(0.99, rate(tag_upstream_request_duration_seconds_bucket[5m]))
```

#### tag_upstream_errors_total

**Type:** Counter

Total number of upstream errors.

| Label | Description |
|-------|-------------|
| `method` | HTTP method |

### Authentication Metrics

#### tag_auth_failures_total

**Type:** Counter

Total number of authentication failures.

| Label | Description |
|-------|-------------|
| `reason` | Failure reason: `invalid_signature`, `unknown_key`, `expired` |

### Connection Metrics

#### tag_active_connections

**Type:** Gauge

Number of active connections.

#### tag_bytes_transferred_total

**Type:** Counter

Total bytes transferred.

| Label | Description |
|-------|-------------|
| `direction` | Transfer direction: `in`, `out` |

**Example queries:**
```promql
# Throughput (bytes/sec)
rate(tag_bytes_transferred_total[5m])

# Outbound throughput
rate(tag_bytes_transferred_total{direction="out"}[5m])
```

## Prometheus Configuration

```yaml
scrape_configs:
  - job_name: 'tag'
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app]
        action: keep
        regex: tag
      - source_labels: [__meta_kubernetes_pod_container_port_number]
        action: keep
        regex: "8080"
```

