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
    newTag: v1.23.1
```

## Production Considerations

### Operating mode

The manifests run the default **transparent** mode against Tigris. Set `TAG_MODE` to `signing` to front a non-Tigris S3-compatible endpoint, or to `tiered` to run TAG as a two-tier cache over a capacity-priced upstream bucket — see [Tiered store mode](tiered-mode.md) for its semantics, credential requirements, and the block-caching interaction. The [Configuration Reference](configuration.md) covers all three.

### Graceful shutdown

On SIGINT or SIGTERM, TAG stops admitting HTTP requests and drains admitted handlers for up to 20 seconds. A request that finishes inside the drain window retains its normal response. When the drain deadline passes with handlers still running, TAG cancels their request contexts and closes their HTTP connections, logs a warning with the in-flight count, and then keeps waiting for those handlers and their cache-consistency cleanup to return; a longer request may be aborted, but its handler is still joined before shutdown proceeds.

After the handlers return, TAG cancels the Service lifetime and joins admitted detached cache work before closing embedded storage. This join has no deadline: a cache call that does not return after cancellation can extend process shutdown, and TAG logs a warning with the outstanding task count if the join passes 30 seconds. An admitted tiered cleanup continues under its own timeout so it may finish its conditional S3 delete and metadata repair; the join still waits for it.

The base StatefulSet sets `terminationGracePeriodSeconds: 30`, which is a separate process-termination limit. When that grace expires, Kubernetes sends SIGKILL even if TAG is still waiting for handler cleanup or detached cache work. Set the pod's termination grace to the longest cleanup interval you need to allow; no finite grace can guarantee completion for uncooperative storage.

The 20-second drain leaves 10 seconds of that grace for the cache to leave the cluster: the ring `LEAVING` announcement, the memberlist leave (5-second timeout) and closing storage. A pod killed before the leave leaves a stale alive entry in its peers' gossip view; they keep routing that node's keys to a dead address until failure detection declares it dead (seconds with the embedded engine's bounded dial timeout, minutes without), and a restart that comes back on a new address is refused as an address conflict until then. If you raise the drain window, raise the termination grace by the same amount.

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

`MAX_POPULATE_MEMORY + (MAX_INFLIGHT_REQUESTS × 10 MiB)` is the **worst case**, not a floor — it assumes every admission slot is simultaneously occupied by a request streaming a full buffer. Real deployments sit far below it: the cap is a ceiling that a healthy workload rarely approaches, which is why the shipped 8 GiB default is fine against defaults of a 2 GiB budget and a 1024 cap.

Size against your own measured RSS, and use the worst case to understand how much room a configuration change can consume. The 10 MiB figure comes from one saturated production workload — treat it as an order of magnitude, not a constant.

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

> Both this and parquet optimization **require block caching** (on by default). With `TAG_CACHE_BLOCK_CACHING_ENABLED=false`, or a non-positive `block_size`, they are silently inert — the setting reads as enabled and does nothing.

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

### Upgrades and meta coordination

Cache invalidation and populate ordering are coordinated by one of two mechanisms, selected at startup by `TAG_CACHE_LEGACY_COORDINATION` (`cache.legacy_coordination`):

| Value | Mechanism | Cluster requirement |
| --- | --- | --- |
| `true` (default) | Legacy timestamp tombstones | None — safe with any mix of versions, including nodes upgrading straight from ≤v1.20 |
| `false` | Fenced CAS (version-token preconditions) | Every node must run ≥v1.21 **before** the flip |

**Upgrading from ≤v1.20:** roll out the new image with the default. No configuration change is needed and mixed-version clusters stay correct throughout the rollout.

**Switching to CAS coordination** (optional, stronger ordering guarantees):

1. Confirm every pod runs ≥v1.21 — `kubectl get pods -n tag -o jsonpath='{..image}'`.
2. Set the flag in the StatefulSet and apply; the rolling restart performs the flip:

   ```yaml
   env:
     - name: TAG_CACHE_LEGACY_COORDINATION
       value: "false"
   ```

3. Let the rollout complete promptly. During the restart, nodes on different mechanisms do not see each other's ordering (a CAS delete writes no tombstone for a legacy populate to check, and vice versa). The exposure is bounded and converges by TTL, but it lasts as long as the rollout — do not pause it midway.

Parsing is fail-safe: only an explicit `false` or `0` selects CAS; any other value keeps legacy. A standalone (single-node) deployment may flip at any time. **Tiered mode** always runs CAS: it selects CAS when the setting is unset and refuses to start with an explicit `true` (see [Tiered store mode](tiered-mode.md)). Reverting (`true`) carries the same mixed-window exposure, so do it with the same brisk rolling restart.

#### Upgrading to v1.22.3

v1.22.3 tightens request checks and cache integrity; nothing needs reconfiguring, but these behaviours are visible to operators and clients:

- **Cached-read grants (transparent mode)** come only from a successful `GetObject` or `HeadObject`. A principal that only writes no longer unlocks cached reads for the bucket, and in tiered mode its small PUTs forward upstream instead of landing in the local tier. See [Security](security.md).
- **Signed streaming uploads (signing mode)** using `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` have each chunk signature checked as TAG forwards the body, with frames bounded by the declared decoded length. Signature failures return `403 SignatureDoesNotMatch`; a rejected stream may reach upstream as an incomplete body but cannot complete or become a cached object. Other streaming formats (the `-TRAILER` and SigV4A variants) are decoded without chunk-signature checks.
- **Presigned GETs (transparent forwarding)** without `X-Amz-Content-Sha256` are validated locally and can be served from cache. `X-Amz-Expires` must be present, between 1 and 604800 seconds, and not expired; signing dates more than 15 minutes in the future are rejected. In tiered mode a locally validated plain presigned GET for an object TAG has no metadata for answers `NoSuchKey`, matching header-signed requests. Signing forwarding — including tiered deployments on non-Tigris endpoints — still requires `X-Amz-Content-Sha256`.
- **Representation selectors** (`versionId`, `partNumber`, `response-*` overrides) bypass the cache and forward upstream with `X-Cache: BYPASS` in proxy modes. In tiered mode, locally validated requests with these selectors forward only for upstream-tier objects; for local-tier objects or missing metadata they return `501 NotImplemented`.
- **Cache integrity:** truncated or over-long cached bodies are refused and invalidated on detection; every cache read runs under a deadline (size-scaled for buffered reads, five minutes for client-paced streams); bulk `DeleteObjects` waits for best-effort cache invalidation attempts under a shared 10-second deadline. After that deadline, remaining keys are skipped and TAG still returns the upstream result; stale entries may remain until TTL. In tiered mode, a key already in progress can extend the wait by its own 10-second timeout.
- **Shutdown** waits past the drain deadline for in-flight handlers and detached cache work before closing storage (see [Graceful shutdown](#graceful-shutdown)). With the base `terminationGracePeriodSeconds: 30`, a pod still busy at the deadline is SIGKILLed where older versions closed storage early; raise the grace period if clean exits matter.

#### Upgrading to v1.23.0

v1.23.0 adds **disk-budget populate shedding** (embedded engine ocache v1.14.0). It is on by default wherever a disk cap is set, and needs no configuration change:

- **What it does.** While a node's tracked usage is at or above `cache.populate_shed_disk_ratio` (default `1.05`) of `max_disk_usage_bytes`, new cache bodies are not written: eviction has fallen behind ingest, and a body written now would be evicted before it is read. Objects are still served; only the write is skipped. Metadata, markers and tombstones are never shed.
- **Two layers.** The node serving the request pre-checks its own store, and the engine enforces the same ratio at the node that owns the body, so a node under budget cannot fill an overloaded peer. In a cluster, nodes can differ by several percent of the cap.
- **Tiered mode.** When the serving node is over budget, a small PUT that would land in the local tier goes upstream with a marker instead. Conditional writes are exempt from that check (their precondition is evaluated against the local copy); a conditional write against an upstream-tier marker still forwards upstream as before. If only the owning node is over budget and refuses after the body has been consumed, the PUT answers `503 SlowDown`; clients retry as for any throttle. Re-tier-on-read is skipped while over budget.
- **Observing it.** `tag_cache_populate_disk_shed_total{source}` counts sheds by path (`tiered_put`, `retier`, `read_miss`, `warm_on_write` for the local pre-check; `*_owner` for refusals by the owning node); `ocache_rpc_requests_total{status="shed"}` counts them on the engine side. A node that sheds continuously is undersized for its write rate; raise `max_disk_usage_bytes` or add nodes.
- **Tuning.** `TAG_CACHE_POPULATE_SHED_DISK_RATIO` sets the ratio (`0` keeps the default, a negative value disables shedding; non-finite values are ignored). Without a disk cap nothing is ever shed.

#### Upgrading to v1.23.1

v1.23.1 makes rolling restarts converge quickly (embedded engine ocache v1.14.1). No configuration change is needed:

- **Gossip after a restart.** A pod that comes back on a new address is now accepted by its peers within seconds. Previously, peers kept dialing the old address until the kernel connect timeout and refused the new one as a conflict, which could leave the ring split for minutes after a roll, with keys owned by a node marked unhealthy answering errors. The transport now bounds each gossip dial (2s) and write (5s), reclaims a dead node's name after 5s, and rejoins through the seed address every minute.
- **Shutdown budget.** The HTTP drain is 20 seconds instead of 30, leaving 10 seconds of the base 30-second termination grace for the cache to announce leaving and leave the gossip cluster before the pod is killed (see [Graceful shutdown](#graceful-shutdown)). A request still running 20 seconds into shutdown is cancelled 10 seconds earlier than before.
- **Watching a roll.** `memberlist_client_cluster_members_count` on each pod should return to the replica count, and `ring_members{name="ocache",state="Unhealthy"}` to zero, shortly after the last pod is ready.

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
- `tag_cache_populate_disk_shed_total` - cache writes skipped because the store is over its disk budget (should be zero on a correctly sized node)
- `ocache_disk_usage_ratio` - tracked usage against `max_disk_usage_bytes`; shedding starts at `populate_shed_disk_ratio` (default 1.05)

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
