# RFC 0001: Block-aligned caching for large objects

- **Status:** Implemented (core phases 1–4; serve-path performance and robustness in [#141](https://github.com/tigrisdata/tag/pull/141))
- **Tracking issue:** [#137](https://github.com/tigrisdata/tag/issues/137)
- **Related:** [#136](https://github.com/tigrisdata/tag/issues/136) (closed — multipart write-through, superseded), [#135](https://github.com/tigrisdata/tag/pull/135) (write-through tee, merged), [#141](https://github.com/tigrisdata/tag/pull/141) (per-block serve cost cuts: probe-free serves, pipelined streaming, bounded degraded serves)

## Summary

Cache large objects at **block granularity** instead of as whole bodies, so a range read (a Parquet footer, or a few surviving row groups) populates and serves only the bytes actually accessed. An object's body is stored as N fixed-size blocks, each an independent cache blob; a range read touches only its covering blocks, and missing blocks are fetched (coalesced, concurrently) on demand.

## Motivation

TAG caches the **whole object body** (`body|bucket|key|<etag>`). To serve *any* range from cache, the entire object must first be downloaded and stored (`fetchFullObjectToCache`). For an access pattern that only ever reads a small tail plus a few interior ranges of a large object, that is a large, mostly-wasted download.

Columnar analytics readers (Parquet/ORC over engines such as DataFusion, Spark, Trino) are exactly that pattern: they read a small **footer** tail of *many* files for pruning, then **selected row-group ranges** of the files that survive pruning — never whole objects. Footer-heavy pruning (footer/bloom-filter caches, explicit ranged downloads) pushes it further still: many files are touched footer-only.

For large objects on the order of tens to hundreds of MiB with footers of tens–hundreds of KB, caching a whole 88 MiB object just to serve a ~100 KB footer is on the order of **~800× populate amplification**, and for footer-only-pruned files essentially all of that bandwidth and cache space is wasted.

## Goals

- Populate and serve range reads of large objects at block granularity — only touched bytes.
- Serve partial hits: cached blocks from cache, missing blocks fetched on demand and cached for next time.
- Cut populate amplification and populate-budget pressure for range-read workloads.
- Preserve the existing whole-object path for small objects and full-object GETs.
- Self-contained in TAG for single-node deployments; no ocache changes required for phase 1.

## Non-goals

- Sparse/partial blobs inside ocache (that is "Option B" — see Alternatives).
- Cross-node block-locality routing — blocks distribute across cluster nodes by design (see Distributed caching); co-locating an object's blocks is a possible future ocache option, not needed here.
- Footer-specific block sizing / special-casing (a single block size for now).
- Changing multipart upload handling.

## Background

### Current caching model

- **Meta:** one JSON object per key under `MakeMetaKey(bucket,key)` → `meta|bucket|key`, holding `ETag`, `ContentLength`, headers, `StatusCode` (`cache.CachedObjectMeta`).
- **Body:** one whole blob per (object, ETag) under `MakeBodyKey(bucket,key,etag)` → `body|bucket|key|<etag>`. Bodies are ETag-addressed and never deleted (age out by TTL); ETag-versioning prevents torn meta/body pairs.
- **Range read, hit:** `HandleGetObject` → `serveRangeFromCache` → `cache.GetRangeStream(bucket,key,etag,start,end,w)` → ocache `GetRangeStream(bodyKey,…)` reads only the requested bytes **from the whole stored blob**. Efficient — the whole-body requirement is purely a *population* problem. Single range only (multi-range falls through).
- **Range read, miss:** `handleRangeWithBackgroundCache` forwards the range to upstream for low latency, then (deferred) `triggerBackgroundCacheFetch(…, priorityReadMiss)` → `fetchFullObjectToCache` → **full-object** GET stored as one blob.
- **Full GET, hit/miss:** `serveFromCache` (whole blob) / broadcast-coalesced `streamFromUpstream` (whole object, cached via pipe).
- **Warm-on-write:** on a successful write, `warmOnWrite` → `triggerBackgroundCacheFetch(…, priorityWarmWrite)` fetches the **whole** object; capped at `size_threshold`.
- **Write-through tee (#135, merged):** authenticated single PUTs ≤ `write_through_max_size` (25 MiB) are teed on write into a whole-body cache entry, avoiding the warm read-back. Small-object, whole-body only.
- **Populate budget:** `populateWeight` + `acquireCacheSlot`/`releaseCacheSlot` bound concurrent populate memory; a background full fetch reserves the per-populate ceiling.
- **Storage backend:** ocache client exposes whole-blob `Put`/`PutStream` and arbitrary-range `GetRangeStream` (read a range of a whole blob). There is **no** sparse/partial put.

### ocache storage tiering (informs block sizing)

From `ocache/storage/storage.go` (confirmed for pinned **v1.9.0**; TAG uses the defaults, no override):

| Object size | Storage |
|---|---|
| **< 64 KB** (`InlineThreshold`) | inlined in RocksDB metadata |
| **64 KB – 64 MB** (`CompactThreshold`) | packed into shared **256 MB segments** (`SegmentSize`); recompacted when dead space > 50% (min age 2h) |
| **≥ 64 MB** | standalone files, never segment-packed |

Consequences:
- A block size of 4–8 MiB lands in the **segment-packed tier** (~32–64 blocks per 256 MB segment) — dense packing rather than one file per block.
- **`block_size` must stay < 64 MB**, else each block becomes a standalone file.
- Block caching shifts today's ≥64 MB Parquet bodies out of the standalone-file tier into the segment-packed tier: better partial-object density, at the cost of those bytes now participating in ocache segment recompaction. Watch `ocache_segment_size_bytes` after rollout.
- ocache packs blocks into segments by **write order**, not by object, so an object's blocks written at different times may land in different segments (affects disk read locality, not correctness). Not optimized here.

## Design

### Block model

A large object's body is stored as N = `ceil(ContentLength / BlockSize)` blocks. Block `i` holds object bytes `[i·BlockSize, min((i+1)·BlockSize, ContentLength))`. Each block is an independent cache blob. **Block presence is implicit in block-key existence** — there is no central presence bitmap. This avoids the read-modify-write contention a bitmap in meta would create in a distributed cache, and mirrors the existing probe-then-fall-through pattern.

### Keys and metadata

- New key builder `MakeBlockKey(bucket,key,etag,blockSize,blockIdx)` → `blk|bucket|key|<etag>|<blockSize>|<blockIdx>`. Distinct prefix from `body|`; ETag-scoped exactly like body keys, so an overwrite (new ETag) writes new block keys and stale blocks age out by TTL — same torn-pair invariant. The **block size is part of the key** so blocks written under one `block_size` can never be resolved by a meta captured under a different `block_size` (e.g. after the config changes and the entry is re-established for an unchanged ETag), which would read a block at the wrong offsets.
- `cache.CachedObjectMeta` gains three fields:
  - `BlockSize int64` (`json:"block_size,omitempty"`). `0` = whole-body (legacy/small — existing path); `>0` = block-mode with that block size. **The block size is captured at populate time**, so existing entries are immune to a later `block_size` config change: readers compute boundaries from `meta.BlockSize`, not current config.
  - `BlocksComplete bool` (`json:"blocks_complete,omitempty"`). Stamped when a populate wrote every block (the full-stream split) or a full assembly verified them (the promotion, below). A **hint, not an invariant** — blocks and meta evict/expire independently — that lets full-object serves skip the per-block probe pass; `false` (including entries written before the field existed) only means the probe-first path is used.
  - `CachedAt int64` (`json:"cached_at,omitempty"`). Unix time the meta was built from a live upstream response. Lifetime-sensitive rewrites that do not consult upstream (the promotion) use it to carry only the entry's **remaining** TTL, never a fresh one; `0` (pre-existing entries) means age unknown and no such rewrite happens.
- Meta remains a single object under `MakeMetaKey` (unchanged), written once per (object, ETag) with total `ContentLength`, `ETag`, headers, and `BlockSize`. Blocks accrete independently afterward.

Readers dispatch on `meta.BlockSize`: `0` → existing `serveRangeFromCache`/`serveFromCache`; `>0` → block path. Both representations coexist during and after rollout.

### Read path (partial-hit aware)

`serveRangeFromBlockCache`, taken when `meta.BlockSize > 0` (replacing the `serveRangeFromCache` branch in `HandleGetObject`):

1. Parse the range (`parseRangeHeader`). Preserve the existing `serveRangeFromCache` error semantics *before* computing any block indices: a malformed/unsatisfiable range (parse error) or an empty range list → `416` with `Content-Range: bytes */<len>`; a **multi-range** request (`len(ranges) > 1`) → `416` (multi-range from cache is unsupported today and stays so). Only a single, valid range proceeds; compute its covering block indices `b0 = start/BlockSize … bK = end/BlockSize`.
2. **Assembled fast path** (`serveAssembledRange`, for a range no longer than one block — the footer/row-group pattern, at most two covering blocks): the requested bytes are assembled into a pooled buffer **before anything is committed**. Each present block is read exactly once — the read itself is the existence probe, halving warm-serve cache ops versus probe-then-stream — and blocks the read reports absent are fetched (coalesced, bounded) and re-read, all pre-commit, so every failure falls through to upstream with the response untouched. The buffer reserves its actual size against the serve-staging budget (see Memory bounds); on decline the probe-first path serves instead.
3. **Probe-first path** (larger ranges, or a staging-budget decline): probe each covering block with `BlockExistsErr` — a transient probe failure aborts rather than counting as missing, so a network blip can't masquerade as eviction — then fetch the missing ones and stream. At most `maxRangeBlockFanout` (32) absent blocks are fetched per request; a pathologically large client range with more absent blocks than that is served from a single upstream range GET instead of a fetch storm.
4. **Pipelined streaming**: multi-block spans stream through a double-buffered pipeline — a reader goroutine prefetches block *i+1* into a pooled buffer while block *i*'s bytes write to the client, so cache-read and client-write latency overlap instead of adding up (the structural stall that made warm multi-block serves trail whole-object serves). Degrades to a direct sequential stream on staging-budget decline or single-block spans, where there is nothing to overlap.

### Block fetch and populate

`fetchBlocksToCache(ctx, bucket, key, accessKey, secretKey, meta, blockIdxs)`:

- For each missing block, issue an **aligned range GET** to upstream (`bytes=bStart-bEnd`, last block clamped to `ContentLength-1`). The response must be a `206` carrying an ETag equal to `meta.ETag`, a `Content-Length` equal to the exact block length, and `Content-Range` bounds matching the block — anything else is rejected (a `200` whole-object response, a length/offset mismatch, or a missing ETag would corrupt block offsets or mix versions). The body is read fully into a pooled buffer before the store, so a truncated upstream body can never persist as a short block that `BlockExists` cannot distinguish from a complete one.
- **Populate weight = block size, not object size** (`populateWeight(blockLen)`) — the core win. An 88 MiB object's footer reserves ~one block, not the per-populate ceiling. On decline the block is simply not cached this time (`errCachePopulateDeclined`).
- **Coalesce** concurrent fetches of the same block via `singleflight` on the block key: N requests hitting the same footer block issue one upstream GET. The shared fetch (the singleflight leader) runs under a detached, timeout-bounded context so one caller's cancellation can't fail the block for every waiter; each waiter still stops waiting on its own ctx.
- Fetch multiple missing blocks **concurrently** (bounded by `maxConcurrentBlockFetches`). Sibling fetches are not canceled on one block's error, so a fast transient failure can't mask a slower **stale signal** from another block — stale signals are reported in preference to transient ones.
- **Stale signals invalidate centrally**: an ETag mismatch (concurrent overwrite) or upstream `404` (deleted out of band) invalidates the entry *inside* `fetchBlocksToCache` — every block fetch flows through this point, so no caller can forget it. A `403` is deliberately **not** a stale signal: it is principal-level (these credentials), not proof the object is gone, and must not invalidate an entry shared across principals. Invalidation itself is ETag-guarded (`invalidateStaleBlockMeta` deletes only if the stored meta still carries the observed stale ETag), so a concurrently re-established newer entry is never wiped.

On a cold range miss (no meta), write meta from the range response's `Content-Range` total size + headers (ETag etc.) — no extra HEAD — then populate the touched block(s). This replaces the whole-object `triggerBackgroundCacheFetch` in `handleRangeWithBackgroundCache` for block-mode-eligible objects.

### Mode is a function of size, not access pattern

An object's representation is decided purely by size: `≥ block_size` (with block caching on) → **blocks**, below → **whole**. It does **not** depend on whether the object was first touched by a full GET or a range read. This is enforced at a single point — the shared full-object cache writer (`setupCacheListener`, used by both the read-miss inline tee and the warm-on-write read-back) branches on `isBlockEligibleSize(meta.ContentLength)`: block-eligible bodies are split into blocks by `putBlocksFromStream` (buffering one block at a time, so peak memory is bounded regardless of object size), everything else takes the single whole-body write. Blocks are written first, the block-mode meta last (the visibility gate), mirroring the range path.

Because every full-object fetch of a block-eligible object lands as blocks, there is no whole/block ambiguity and none of the collision guards a per-access-pattern rule would need.

### Warm-on-write interaction

`warmOnWrite` populates on write exactly as a read would: a block-eligible object (`≥ block_size`, block caching on) is **block-split** from the read-back stream; a sub-block object is whole-cached (via the in-memory tee where possible, else the streaming read-back). Both directions flow through the same shared cache writer, so warm-on-write and read-miss produce identical representations — there is nothing to reconcile. `block_size` is the whole↔block boundary; `size_threshold` is the ceiling; there is no separate warm-size knob.

### Full-object GET on a block-mode object

Rare for a range-read-dominated workload but must be correct — and fast, since it is the case where per-block serve cost is linear in block count:

- **Cold miss:** the object is streamed once from upstream and split into blocks by the shared writer (`putBlocksFromStream`) — one upstream fetch, all blocks populated, and the meta is stamped `blocks_complete` (every block was just written). Each block is read to its exact expected length before its store; a body shorter or longer than `Content-Length` leaves the meta unwritten.
- **Hit on a complete entry (probe-free):** `blocks_complete` lets the serve skip the probe pass entirely — `2N+1 → N+1` cache ops. The first block is read into a pooled, staging-budget-gated buffer **pre-commit** (a gone or unrecoverable first block still falls through to the miss path with the response untouched), then the rest stream pipelined.
- **Hit on a partial entry (probe-first):** probe all blocks. **Mostly missing** (more than half — e.g. only a footer block was ever cached) bails to the miss path's single streaming re-split instead of a per-block fetch storm, and invalidates the mostly-missing entry (the bail skips the per-block staleness check, so leaving it could serve stale bytes on later range reads). **Mostly cached** assembles the few missing blocks, then **promotes** the entry to `blocks_complete` — async and tombstone-aware (the write-start timestamp is stamped before assembly, so a racing invalidation blocks the write), carrying only the entry's **remaining TTL** computed from `cached_at`. Promotion never extends the entry's lifetime: it does not consult upstream, so a fresh TTL would reset the staleness clock (up to doubling the configured bound after an out-of-band overwrite) and guarantee a window where the meta outlives every block. Each entry pays the probe pass at most once.

So a block-eligible object is **always** block-mode; a full read never produces a competing whole-body entry.

**Bounded degraded serves.** Because `blocks_complete` is a hint and blocks evict independently of the meta, a committed serve (headers already sent) recovers at most `maxInlineFetchesPerServe` (2) absent blocks via individual aligned upstream fetches. Past that cap, on a populate-budget decline, or on a transient cache-read failure, the committed response is completed **byte-exact from a single uncached upstream remainder stream** — validated against the entry's ETag and the exact remaining byte range — instead of truncating, and a cap-tripped (mass-evicted) entry is invalidated so the next GET re-establishes it with one streaming re-split. Only three things still truncate a committed body: a stale signal (a different object version must never be mixed into a committed response), a client write failure (the client is broken, not the cache — tracked explicitly so it is never "salvaged" with a wasted upstream round trip), and a failed remainder stream.

A client-forced revalidation (`Cache-Control: no-cache`) of a block-mode entry likewise invalidates it and lets the miss path re-establish the current version.

### Configuration

A single size knob, `block_size`, is the whole↔block boundary; the earlier `write_through_max_size` and `block_cache_min_size` are removed:

| Key | Default | Meaning |
|---|---|---|
| `cache.block_caching_enabled` | `false` | Rollout flag. |
| `cache.block_size` | 4 MiB (must be < 64 MB) | Block granularity **and** the whole↔block boundary: below one block → whole-cached (blocking a sub-block object just stores one blob), at/above → block-cached. The write-through tee (in-memory) also gates on this, keeping its buffer sub-block. |
| `cache.size_threshold` | 1 GB | Overall cacheability ceiling. Block mode lets TAG cache *parts* of objects up to this ceiling without downloading the whole thing. |

Why one knob: block-caching a sub-block object is identical to whole-caching it, so `block_size` is the natural boundary. It governs every populate path uniformly — read misses, full-GET misses, and warm-on-write all split block-eligible objects into blocks and whole-cache the rest — so representation is a function of size alone, with no separate warm cap and no per-access-pattern collision to guard. (A `block_min_object_size` knob raising the boundary was trialed and removed: with pipelined serving, block mode sits within ~7% of the whole-object ceiling even at 4 blocks/object, so the extra config surface wasn't warranted.)

The existing `cache.max_populate_memory_bytes` additionally sizes the serve-staging budget — see Memory bounds; no new memory knob is introduced.

### Invalidation and consistency

- Blocks are ETag-scoped: an overwrite writes new keys under the new ETag; stale blocks age out by TTL (no explicit block deletion — same as bodies today).
- The **meta is the visibility gate**: blocks are written first with plain block puts, and the block-mode meta is written last via `PutMetaTombstoneAware` with a `writeStartTime` stamped *before* the fetch/assembly began — a DELETE tombstone that lands mid-populate is then provably newer and blocks the meta write, so background work can't resurrect a deleted object.
- Meta `Delete` orphans blocks (age out); the next read re-populates meta + touched blocks.
- The ETag guard on each block fetch prevents mixing versions under one ETag's keys.

### Memory bounds

Two independent byte budgets, both sized by `cache.max_populate_memory_bytes` (so the aggregate budget-bounded buffering is up to **2×** that value; negative disables both):

- **Populate budget** (pre-existing): every block fetch reserves the block's actual size (read-miss priority, non-blocking). A declined fetch means the bytes are served from upstream uncached this time — never a failed request.
- **Serve-staging budget** (new, independent): the assembled-range buffer, the complete-serve first-block buffer, and the pipeline's two staging buffers reserve their **actual sizes** here, never against the populate budget — a warm serve holds staging bytes for its whole (possibly multi-second) response, and sharing one pool let high-concurrency serving starve cold-miss populates, keeping the working set cold. Staging declines degrade the serve (probe-first path, sequential stream); they never fail it.

Block staging buffers are pooled (`sync.Pool`); the pool's retention cap tracks the configured `block_size` so pooling isn't silently disabled for larger-block deployments.

## Distributed caching

In cluster mode ocache routes **per key** via consistent hashing, so a block key (`blk|bucket|key|<etag>|<blockSize>|<blockIdx>`) is routed independently — an object's blocks distribute across cluster nodes. This is intended, not a problem to work around: it is how other sharded caches operate (ceph and most chunked/sharded stores shard at block granularity), and it is beneficial — distributing blocks spreads load evenly across the cluster and lets a multi-block read fetch its blocks from several owners in parallel. Correctness is independent of placement: block presence is probed per key and any missing block is fetched via the same routing, so a read works regardless of which node owns which block.

Co-locating an object's blocks on a single owner (hashing on object identity `bucket|key|etag` rather than the block key) was considered and deliberately not adopted — block distribution is the desired behavior. A block-locality routing mode remains a possible **ocache-side** option should a future workload ever want it, but it is not required by this design.

## Rollout and phasing

Back-compat: legacy whole-body entries (`BlockSize=0`) are served by the existing path; new large-object misses populate block-mode. Both coexist; readers dispatch on `meta.BlockSize`. Gated behind `block_caching_enabled` (default off).

1. **Done** — `meta.BlockSize` + `MakeBlockKey` + config + reader dispatch (block vs whole).
2. **Done** — block-aware range serve: probe-free assembled fast path for sub-block ranges, probe-first path with bounded fan-out for larger ones, partial-hit fetch (coalesced, concurrent).
3. **Done** — single shared cache writer splits block-eligible objects into blocks on every full-object populate path (read miss, full-GET miss, warm-on-write read-back); the tee stays sub-block; the whole↔block boundary is `block_size`; `block_cache_min_size` and `write_through_max_size` removed. (A `block_min_object_size` knob raising the boundary was added and then removed once pipelining closed the mid-size gap to ~7% — see [#141](https://github.com/tigrisdata/tag/pull/141).)
4. **Done** — full-GET block assembly (mostly-cached, with `blocks_complete` promotion) + single-stream re-split fallback (mostly-missing) + metrics.
5. **Done** ([#141](https://github.com/tigrisdata/tag/pull/141)) — per-block serve cost cuts: probe-free serves (`2N+1 → N+1` ops), pipelined block streaming, the serve-staging budget, and bounded degraded serves with the uncached upstream remainder salvage. Measured: warm block-mode full GETs reach put-normalized throughput parity with whole-object caching with median TTFB up to ~22× better on large objects; +89% raw (+~54% bandwidth-normalized) vs `main` at identical 64 KiB-block config.
6. *(optional, not required)* ocache block-locality routing for clusters — only if a future workload ever wants an object's blocks co-located.

Deferred: block-size-vs-footer-fit (single block size for now); density-triggered promotion from per-block demand fetches to one streaming whole-population for range workloads that end up reading most of an object.

## Metrics

Implemented:

- `tag_cache_block_hits_total` / `tag_cache_block_misses_total` — covering blocks already cached vs fetched at serve time. Recorded **only on committed block-cache serves** (a bail or failed fetch that falls through to upstream records none, avoiding hit-ratio skew), and counting only blocks actually read — inline fetches, remainder-streamed bytes, and blocks never reached on an aborted serve all count against the entry, never as hits.
- `tag_cache_block_range_served_total{full_hit|partial_hit}` — block-mode serves by whether every covering block was already cached. Partial-hit rate = `partial_hit / (full_hit + partial_hit)`.
- `tag_cache_block_populated_total` / `tag_cache_block_bytes_populated_total` — blocks and bytes fetched from upstream into cache. Compare bytes populated with `tag_bytes_transferred_total{direction="out"}` for the amplification ratio — the headline KPI for this work.
- Watch existing `ocache_segment_size_bytes` for recompaction impact of the tier shift.

## Alternatives considered

- **Option B — ocache sparse blobs.** Store one body key as a sparse blob with a present-ranges map (`PutRange` + hole-aware `GetRangeStream`). Fewer keys and exact-byte population, but a larger change that lands in **ocache** (separate plan/repo). Kept in mind as a possible future evolution; Option A is chosen for phase 1 because it is self-contained in TAG and incremental.
- **Whole-object multipart write-through (#136).** Pre-warms *whole* bodies on write — right for manifests (read in full), wrong for Parquet (footer + selective ranges). Closed in favor of this RFC. Consequence for manifests: at 300–400 MB they are block-eligible (`≥ block_size`), so with `warm_on_write` on they are **block-split** on write (and otherwise on their first full read). A full read of a manifest then assembles its blocks from cache. Reassembly is `N` cache reads instead of one whole-body stream, but that overhead is dwarfed by the byte transfer; the uniform size-only rule was chosen over Option A's "whole for read-in-full" adaptivity because the target workload's large objects are range-read-dominated, where whole-caching rarely pays off.
- **Optimize whole-object warm (#136 Approach B — parallel-range fetch / lazy warm).** Subsumed here: block-granular read-demand populate is a strictly better "warm on read-demand," and parallel-range fetch survives as the concurrent block-fill implementation detail.
