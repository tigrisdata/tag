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
    newTag: v1.19.0
```

## Production Considerations

### High Availability

- The StatefulSet deploys 3 replicas by default with pod anti-affinity to distribute across nodes.
- Each TAG pod has its own local cache, so losing a pod only affects cache hit ratio temporarily.
- Health checks (readiness and liveness probes) ensure automatic recovery.

### Scaling

**Horizontal:** The HPA scales from 3 to 10 replicas based on CPU (70%) and memory (80%) utilization. New nodes join the cache cluster automatically. Scaling down may temporarily reduce cache hit ratio.

**Vertical:** Adjust resource requests/limits in the StatefulSet. The default is 2-4 CPUs and 4-8 GiB memory per pod. SSD storage is recommended for cache performance. If you change the PVC volume size, also update `TAG_CACHE_MAX_DISK_USAGE` in the StatefulSet to match (value is in bytes).

#### Sizing memory: measure RSS, not working set

Pod memory has two parts that behave very differently, and the headline number hides the one that matters.

- **Working set** (`container_memory_working_set_bytes`, what `kubectl top` shows) includes reclaimable page cache over the on-disk RocksDB cache. It routinely sits near the limit and that is fine — the kernel reclaims it under pressure.
- **RSS** (`container_memory_rss`) is anonymous memory. It **cannot** be reclaimed, and it is what the OOM killer acts on.

**Size the limit against RSS.** A pod running at 97% working set with RSS at 45% of the limit is healthy; the same working set with RSS at 90% is one traffic burst from being killed. The two look identical in `kubectl top`.

```promql
# The number that decides whether you are safe. Watch this, not working set.
max by (pod) (container_memory_rss{container="tag"}) / max by (pod) (container_spec_memory_limit_bytes{container="tag"})
```

RSS scales with configuration, not just cache size:

| Contributor | Governed by |
| --- | --- |
| Populate + serve staging | `TAG_CACHE_MAX_POPULATE_MEMORY` (a hard cap) |
| Per-request buffers | `TAG_MAX_INFLIGHT_REQUESTS` — budget roughly **10 MiB per in-flight request** |
| Go heap + RocksDB cgo | Grows with load, not directly configurable |

So a rough floor is `MAX_POPULATE_MEMORY + (MAX_INFLIGHT_REQUESTS × 10 MiB) + headroom`. The per-request figure is measured from one production workload and is a starting point, not a constant — confirm it against your own RSS before relying on it.

> **Raising the inflight cap raises the memory floor.** These two settings are sized together. Increasing `TAG_MAX_INFLIGHT_REQUESTS` without matching memory converts reclaimable page cache into non-reclaimable per-request buffers and will OOM the pod — the working set barely moves while RSS climbs, so the change looks safe right up until the kill.

Never lower the disk cache to save memory; raise the limit instead.

### Admission control

`TAG_MAX_INFLIGHT_REQUESTS` (default 1024) bounds concurrently-served S3 requests. Excess is shed with `503 SlowDown`; `/health`, `/metrics`, and `/debug/pprof/*` are exempt. `0` or unset uses the default, negative disables the bound.

```yaml
env:
  - name: TAG_MAX_INFLIGHT_REQUESTS
    value: "1024"
```

Shedding shows up as a non-zero rate here, with inflight pinned at the cap:

```promql
sum(rate(tag_admission_shed_total[5m]))        # requests rejected with 503 SlowDown
max by (pod) (tag_inflight_requests)           # compare against the configured cap
```

Sustained shedding means the cap is too low for the offered load — but **raise it and the memory limit together**, per the sizing note above. Shedding is the gentler failure: a `503 SlowDown` is retried by S3 clients, while an OOM kill drops every in-flight request on the node and discards the warm cache. Prefer some shedding over a cap the memory cannot support.

### Metadata caching on write

`TAG_CACHE_META_ON_WRITE` caches an object's metadata entry when TAG proxies its write, so the first read does not spend an upstream round trip discovering the object exists. It is most useful where writers and readers overlap — an ingest pipeline feeding queries over recent data.

```yaml
env:
  - name: TAG_CACHE_META_ON_WRITE
    value: "true"
```

It changes what a read-after-write observes (metadata is served from cache rather than fetched), and is ETag-guarded against concurrent overwrites. Off by default.

### Parquet optimization

`TAG_CACHE_PARQUET_OPTIMIZATION` caches parquet metadata blocks ahead of the reader that needs them, from both a read trigger and a write trigger. Whether it helps depends on whether your footers exceed the tail block that already gets cached — see [Parquet optimization](parquet-optimization.md) for how to measure that before enabling it, and how to tell afterwards whether it worked.

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

- **Upstream read amplification** — `sum(rate(tag_cache_block_bytes_populated_total[5m])) / sum(rate(tag_bytes_transferred_total{direction="out"}[5m]))` — bytes fetched from upstream into blocks vs bytes served to clients. Aim for **≤ 1**; well above 1 means the block size is too large for the read pattern. (Do **not** use `tag_bytes_transferred_total{direction="in"}` — that counts client upload bodies, not upstream fetches.)
- **Block hit ratio** — `tag_cache_block_hits_total / (tag_cache_block_hits_total + tag_cache_block_misses_total)`.
- **Serve latency** — `histogram_quantile(0.95, sum(rate(tag_request_duration_seconds_bucket[5m])) by (le))`.

Cache buffering memory (populate + block-serve staging) is bounded by `TAG_CACHE_MAX_POPULATE_MEMORY` — one honest total (default 2 GiB). See the [Configuration Reference](configuration.md).

### Compaction I/O throttling on throughput-capped volumes

Cloud block volumes are usually throughput-capped (e.g. OCI Balanced at 240 MB/s, gp3 baseline at 125 MB/s). ocache's background compaction (raw-file → segment consolidation and segment recompaction) is unthrottled by default and, after heavy cache population, can burst well past such caps — starving foreground serving reads and causing multi-second p95 stalls until the backlog drains.

On capped volumes, set a shared compaction budget:

```yaml
env:
  - name: TAG_CACHE_COMPACTION_BPS
    value: "33554432" # 32 MiB/s
```

Sizing guidance:

- **Ceiling:** total compaction I/O ≈ 2× the budget (each byte is read then written); keep that well under half the volume cap so serving always has headroom. `32 MiB/s` on a 240 MB/s volume consumes ~27%.
- **Floor:** the budget must outpace sustained cache churn or raw-file backlog accumulates. One pod at 32 MiB/s drains ~2.7 TB/day.
- Populate writes (the serving path filling the cache) are **never** throttled — only compaction's own source reads (its writes follow implicitly).
- The budget also covers the **liveness walks that gate recompaction** (one ~4 KiB page charged per entry examined), so enabling it bounds reclaim's read load too rather than leaving it unaccounted.
- The throttle deliberately trades slower backlog drain for stable serving latency; watch `rate(ocache_compaction_bytes_compacted_total[5m])` (should plateau near the budget during drain — use a multi-minute window, as the counter advances at batch-commit granularity and short windows read spiky) and serving p95 during post-load consolidation.

### Reclaiming dead space

Objects that are overwritten, deleted, or expired leave dead bytes inside segment files. Those bytes have no metadata pointing at them, so TTL, eviction, and the deletion queue cannot reach them — only segment recompaction frees them, by copying the live entries out and deleting the old segment.

Recompaction decides what to reclaim by **deriving** each cold segment's dead bytes from the segment's own entries checked against metadata ([ocache RFC-009](https://github.com/tigrisdata/ocache/blob/main/docs/rfcs/RFC-009-walk-gated-recompaction.md)), so reclaim does not depend on bookkeeping counters staying perfect. There is nothing to configure; the relevant knobs are ocache defaults (2 h minimum segment age, 0.5 fragmentation threshold).

What to watch:

- `rate(ocache_segment_walks_total[10m])` — segments examined. Should be non-zero on any node with cold segments; flat zero means recompaction is disabled or every segment is younger than the age gate.
- `increase(ocache_recompaction_segments_total[1h])` and `increase(ocache_recompaction_bytes_freed_total[1h])` — reclaim actually happening.
- **Physical vs logical divergence is the real health signal**: compare `kubelet_volume_stats_used_bytes` for the cache PVC against `ocache_disk_usage_bytes` (live bytes). A gap that grows and never shrinks means dead space is accumulating faster than it is reclaimed; a gap that stays small means reclaim is keeping up. Expect the gap to spike during heavy population and while a recompaction holds both the old and new segment on disk.

Because segments must pass the age gate first, reclaim begins roughly two hours after a pod restart, not immediately.

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
