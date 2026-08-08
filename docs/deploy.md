# Kubernetes Deployment

Deploy TAG as a StatefulSet with an embedded distributed cache cluster. For running locally, see the [README](../README.md). For all configuration options, see the [Configuration Reference](configuration.md).

## Prerequisites

- A running Kubernetes cluster
- `kubectl` configured to access the cluster
- Tigris access key and secret key with read access to all buckets that will be accessed through TAG

## Deploy

### 1. Create a namespace

```bash
kubectl create namespace tag
```

### 2. Create the credentials secret

```bash
kubectl create secret generic tag-credentials \
  --namespace tag \
  --from-literal=AWS_ACCESS_KEY_ID=your_access_key \
  --from-literal=AWS_SECRET_ACCESS_KEY=your_secret_key
```

### 3. Apply the manifests

```bash
kubectl apply -k deploy/kubernetes/base/ -n tag
```

This deploys a 3-replica StatefulSet with:
- Embedded cache on each pod (400 GiB PVC per pod)
- Gossip-based cluster discovery via a headless service
- A LoadBalancer service for external access on port 8080
- Horizontal Pod Autoscaler (3-10 replicas)

### 4. Verify the deployment

```bash
# Check pod status
kubectl get pods -n tag

# Check health
kubectl exec -n tag tag-0 -- curl -s http://localhost:8080/health
```

## Kubernetes Manifests

The `deploy/kubernetes/base/` directory uses Kustomize:

| File | Description |
|------|-------------|
| `kustomization.yaml` | Kustomize configuration with image tag |
| `statefulset.yaml` | TAG StatefulSet (3 replicas, embedded cache) |
| `service.yaml` | LoadBalancer Service for external access |
| `service-headless.yaml` | Headless Service for cluster discovery |
| `hpa.yaml` | Horizontal Pod Autoscaler |

To customize the image version or other settings, create an overlay:

```yaml
# deploy/kubernetes/overlays/production/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../base
images:
  - name: tigrisdata/tag
    newTag: v1.17.0
```

## Production Considerations

### High Availability

- The StatefulSet deploys 3 replicas by default with pod anti-affinity to distribute across nodes.
- Each TAG pod has its own local cache, so losing a pod only affects cache hit ratio temporarily.
- Health checks (readiness and liveness probes) ensure automatic recovery.

### Scaling

**Horizontal:** The HPA scales from 3 to 10 replicas based on CPU (70%) and memory (80%) utilization. New nodes join the cache cluster automatically. Scaling down may temporarily reduce cache hit ratio.

**Vertical:** Adjust resource requests/limits in the StatefulSet. The default is 2-4 CPUs and 4-8 GiB memory per pod. SSD storage is recommended for cache performance. If you change the PVC volume size, also update `TAG_CACHE_MAX_DISK_USAGE` in the StatefulSet to match (value is in bytes).

> **Memory note:** high pod memory is expected and healthy — most of it is reclaimable Linux page cache over the on-disk RocksDB cache, not the TAG process (its RSS is typically well under 1 GiB). It plateaus below the pod limit; treat it as page cache, not a leak. Only raise the memory limit (never lower the disk cache) if the working set trends up to the limit.

### Block-aligned caching

**Block-aligned caching is on by default** ([RFC 0001](rfcs/0001-block-aligned-caching.md)): any object at or above `block_size` is cached as fixed-size blocks, so a range read fetches and caches only the covering blocks instead of the whole object — ideal for **small ranges of large objects** (Parquet footers/row-groups, SlateDB/SST blocks, columnar analytics). The one knob that matters is `block_size`:

```yaml
env:
  # Block caching is on by default at 1 MiB. Tune block_size to your read granularity:
  - name: TAG_CACHE_BLOCK_SIZE
    value: "1048576" # 1 MiB (default)
  # ...or opt out entirely to cache whole objects:
  # - name: TAG_CACHE_BLOCK_CACHING_ENABLED
  #   value: "false"
```

**Sizing `block_size` to your workload's dominant read size is the critical tuning knob:**

- **Too large** and every cache miss pulls a full block to serve a small range — upstream **read amplification**. A 4 MiB block serving ~400 KB reads amplifies upstream traffic several-fold and can be *worse* than whole-object caching.
- **Too small** adds per-block bookkeeping and more fetches per read.

Start near your median read size; the `1048576` (1 MiB) default suits typical analytics footers/row-groups — raise it for larger reads, or lower it (e.g. `65536` for 64 KiB reads). Then verify with Prometheus:

- **Upstream amplification** — `sum(rate(tag_bytes_transferred_total{direction="in"}[5m])) / sum(rate(tag_bytes_transferred_total{direction="out"}[5m]))`. Aim for **≤ 1** (the cache serves more than it fetches); a value well above 1 means the block size is too large for the read pattern.
- **Block hit ratio** — `tag_cache_block_hits_total / (tag_cache_block_hits_total + tag_cache_block_misses_total)`.
- **Serve latency** — `histogram_quantile(0.95, sum(rate(tag_request_duration_seconds_bucket[5m])) by (le))`.

Cache buffering memory (populate + block-serve staging) is bounded by `TAG_CACHE_MAX_POPULATE_MEMORY` — one honest total (default 2 GiB). See the [Configuration Reference](configuration.md).

### Health Checks

TAG exposes a health endpoint:

```
GET /health
```

Returns `200 OK` when healthy. The StatefulSet configures both readiness and liveness probes against this endpoint.

### Monitoring

TAG exposes Prometheus metrics at `/metrics`. The StatefulSet includes Prometheus annotations for automatic scraping.

Key metrics:
- `tag_requests_total{status="error"}` - error rate
- `tag_cache_hits_total / (tag_cache_hits_total + tag_cache_misses_total)` - cache hit ratio
- `tag_upstream_request_duration_seconds` - upstream latency

## Troubleshooting

### No cache hits

- Check TAG logs for cache initialization errors: `kubectl logs -n tag tag-0`
- Verify the cache PVC is bound: `kubectl get pvc -n tag`
- Ensure the disk path is writable

### Authentication failures

- Verify the credentials secret exists: `kubectl get secret -n tag tag-credentials`
- Check that credentials have read access to the target buckets
- Review signature logs at debug level: set `TAG_LOG_LEVEL=debug` in the StatefulSet

### High latency

- Check upstream endpoint latency
- Monitor cache hit ratio via Prometheus metrics
- Review disk I/O performance on the storage class

### Debug mode

Enable debug logging by updating the StatefulSet:

```yaml
env:
  - name: TAG_LOG_LEVEL
    value: "debug"
```
