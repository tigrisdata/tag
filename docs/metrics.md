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

| Label       | Description                                                          |
| ----------- | -------------------------------------------------------------------- |
| `operation` | S3 operation: `GetObject`, `PutObject`, `DeleteObject`, `HeadObject` |
| `status`    | Result: `success`, `error`, `auth_error`, `range_not_satisfiable`    |

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

| Label       | Description  |
| ----------- | ------------ |
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

| Label       | Description                               |
| ----------- | ----------------------------------------- |
| `operation` | Operation type: `get`, `put`, `delete`    |
| `result`    | Result: `hit`, `miss`, `success`, `error` |

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

#### tag_cache_size_bytes

**Type:** Gauge

Current logical size of **this node's** local cache in bytes (the sum of stored
object lengths), sampled periodically from the embedded cache. Per-node — sum across
nodes for a cluster-wide total.

```promql
# Cluster-wide cache size
sum(tag_cache_size_bytes)

# Fill against a configured disk cap (bytes)
tag_cache_size_bytes / <max_disk_usage_bytes>
```

> The embedded cache also exports `ocache_disk_usage_bytes{type="total"}` (same
> logical size) and `ocache_segment_size_bytes` (physical on-disk segment bytes,
> including fragmentation and not-yet-compacted tombstones) directly at `/metrics`.
> `tag_cache_size_bytes` is the stable, TAG-owned name for the logical size.

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

### Revalidation Metrics

#### tag_revalidations_triggered_total

**Type:** Counter

Number of cache revalidation attempts (conditional GET/HEAD to upstream). Incremented when a client sends `Cache-Control: no-cache` or `max-age=0` and a cached entry with an ETag exists.

**Example queries:**

```promql
# Revalidation rate
rate(tag_revalidations_triggered_total[5m])
```

#### tag_revalidations_not_modified_total

**Type:** Counter

Number of revalidations where upstream returned 304 Not Modified (cached entry is still fresh).

**Example queries:**

```promql
# Revalidation 304 ratio (higher = better cache freshness)
rate(tag_revalidations_not_modified_total[5m]) /
rate(tag_revalidations_triggered_total[5m])
```

#### tag_revalidations_updated_total

**Type:** Counter

Number of revalidations where upstream returned 200 with new data (cached entry was stale).

#### tag_revalidations_failed_total

**Type:** Counter

Number of revalidations that failed due to errors or unexpected status codes.

#### tag_revalidations_stale_served_total

**Type:** Counter

Number of times stale cached data was served because the revalidation request to upstream failed.

**Example queries:**

```promql
# Stale serve ratio (should be low)
rate(tag_revalidations_stale_served_total[5m]) /
rate(tag_revalidations_triggered_total[5m])
```

### Upstream Metrics

#### tag_upstream_request_duration_seconds

**Type:** Histogram

Upstream (Tigris) request duration in seconds.

| Label    | Description                                 |
| -------- | ------------------------------------------- |
| `method` | HTTP method: `GET`, `PUT`, `DELETE`, `HEAD` |

**Example queries:**

```promql
# Upstream P99 latency
histogram_quantile(0.99, rate(tag_upstream_request_duration_seconds_bucket[5m]))
```

#### tag_upstream_errors_total

**Type:** Counter

Total number of upstream errors.

| Label    | Description |
| -------- | ----------- |
| `method` | HTTP method |

### Authentication Metrics

#### tag_auth_failures_total

**Type:** Counter

Total number of authentication failures.

| Label    | Description                                                   |
| -------- | ------------------------------------------------------------- |
| `reason` | Failure reason: `invalid_signature`, `unknown_key`, `expired` |

### Transparent Proxy Metrics

#### tag_local_auth_validations_total

**Type:** Counter

Local authentication validation attempts and results in transparent proxy mode.

| Label    | Description                                                                                                       |
| -------- | ----------------------------------------------------------------------------------------------------------------- |
| `result` | Validation result: `success`, `missing_auth`, `parse_error`, `unknown_key`, `signature_mismatch`, `authz_expired` |

**Example queries:**

```promql
# Local auth success rate
rate(tag_local_auth_validations_total{result="success"}[5m]) /
sum(rate(tag_local_auth_validations_total[5m]))

# Auth failure breakdown
sum by (result) (rate(tag_local_auth_validations_total{result!="success"}[5m]))
```

#### tag_derived_key_store_size

**Type:** Gauge

Number of stored derived signing keys (learned from Tigris responses).

#### tag_authz_cache_size

**Type:** Gauge

Number of active authorization cache entries (per access-key, per bucket).

#### tag_proxy_signing_keys_received_total

**Type:** Counter

Number of signing key sets extracted from Tigris responses (used for local signature validation).

### Connection Metrics

#### tag_active_connections

**Type:** Gauge

Number of active connections.

#### tag_bytes_transferred_total

**Type:** Counter

Total bytes transferred.

| Label       | Description                     |
| ----------- | ------------------------------- |
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
  - job_name: "tag"
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
