# RFC 0001: Block-aligned caching for large objects

- **Status:** Draft
- **Tracking issue:** [#137](https://github.com/tigrisdata/tag/issues/137)
- **Related:** [#136](https://github.com/tigrisdata/tag/issues/136) (closed — multipart write-through, superseded), [#135](https://github.com/tigrisdata/tag/pull/135) (write-through tee, merged)

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
- `cache.CachedObjectMeta` gains one field: `BlockSize int64` (`json:"block_size,omitempty"`). `0` = whole-body (legacy/small — existing path); `>0` = block-mode with that block size. **The block size is captured at populate time**, so existing entries are immune to a later `block_size` config change: readers compute boundaries from `meta.BlockSize`, not current config.
- Meta remains a single object under `MakeMetaKey` (unchanged), written once per (object, ETag) with total `ContentLength`, `ETag`, headers, and `BlockSize`. Blocks accrete independently afterward.

Readers dispatch on `meta.BlockSize`: `0` → existing `serveRangeFromCache`/`serveFromCache`; `>0` → block path. Both representations coexist during and after rollout.

### Read path (partial-hit aware)

New `serveRangeFromBlockCache`, taken when `meta.BlockSize > 0` (replacing the `serveRangeFromCache` branch in `HandleGetObject`):

1. Parse the range (`parseRangeHeader`). Preserve the existing `serveRangeFromCache` error semantics *before* computing any block indices: a malformed/unsatisfiable range (parse error) or an empty range list → `416` with `Content-Range: bytes */<len>`; a **multi-range** request (`len(ranges) > 1`) → `416` (multi-range from cache is unsupported today and stays so). Only a single, valid range proceeds; compute its covering block indices `b0 = start/BlockSize … bK = end/BlockSize`.
2. Probe each covering block (a `GetRangeStream` on the block key; not-found = missing).
3. If any are missing → fetch just those blocks (below), populate them, then serve.
4. Stream the range in block order: for each covering block, read its local sub-range `[max(start,bStart)−bStart … min(end,bEnd)−bStart]` from that block's blob and copy to the client. Footer read = 1 block; a few row groups = a few blocks.

The pre-206 first-byte probe from `serveRangeFromCache` (commit headers only after the body resolves; fall through on genuine body-miss) carries over on the first covering block.

### Block fetch and populate

New `fetchBlocksToCache(ctx, bucket, key, etag, meta, blockIdxs)`:

- For each missing block, issue an **aligned range GET** to upstream (`bytes=bStart-bEnd`, last block clamped to `ContentLength-1`), streaming into the block blob via a tombstone-aware `PutStream`.
- **Populate weight = block size, not object size** (`populateWeight(blockSize)`) — the core win. An 88 MiB object's footer reserves ~one block, not the per-populate ceiling.
- **Coalesce** concurrent fetches of the same `(etag, blockIdx)` — extend the broadcast manager keyed on the block key (or a per-block analog of `activeBackgroundFetches`) so N queries hitting the same footer block issue one upstream GET.
- Fetch multiple missing blocks **concurrently** (bounded).
- **ETag guard:** the range response carries an ETag; if it ≠ `meta.ETag`, the object was overwritten mid-flight — invalidate meta and re-fetch rather than cache a torn block (same guard the write-through tee uses).

On a cold range miss (no meta), write meta from the range response's `Content-Range` total size + headers (ETag etc.) — no extra HEAD — then populate the touched block(s). This replaces the whole-object `triggerBackgroundCacheFetch` in `handleRangeWithBackgroundCache` for block-mode-eligible objects.

### Mode is a function of size, not access pattern

An object's representation is decided purely by size: `≥ block_size` (with block caching on) → **blocks**, below → **whole**. It does **not** depend on whether the object was first touched by a full GET or a range read. This is enforced at a single point — the shared full-object cache writer (`setupCacheListener`, used by both the read-miss inline tee and the warm-on-write read-back) branches on `isBlockEligibleSize(meta.ContentLength)`: block-eligible bodies are split into blocks by `putBlocksFromStream` (buffering one block at a time, so peak memory is bounded regardless of object size), everything else takes the single whole-body write. Blocks are written first, the block-mode meta last (the visibility gate), mirroring the range path.

Because every full-object fetch of a block-eligible object lands as blocks, there is no whole/block ambiguity and none of the collision guards a per-access-pattern rule would need.

### Warm-on-write interaction

`warmOnWrite` populates on write exactly as a read would: a block-eligible object (`≥ block_size`, block caching on) is **block-split** from the read-back stream; a sub-block object is whole-cached (via the in-memory tee where possible, else the streaming read-back). Both directions flow through the same shared cache writer, so warm-on-write and read-miss produce identical representations — there is nothing to reconcile. `block_size` is the whole↔block boundary; `size_threshold` is the ceiling; there is no separate warm-size knob.

### Full-object GET on a block-mode object

Rare for a range-read-dominated workload but must be correct:

- **Cold miss:** the object is streamed once from upstream and split into blocks by the shared writer — one upstream fetch, all blocks populated.
- **Hit, mostly cached:** assemble blocks `0…N-1` in order, fetching any few missing via `fetchBlocksToCache`, streaming as we go.
- **Hit, mostly missing** (e.g. only a footer block was ever cached): assembling every missing block as a separate aligned range GET would be a large request amplification, so instead fall through to the miss path — a single streaming upstream GET that re-splits into blocks. The threshold (more than half the covering blocks missing) bounds per-block fan-out to the already-mostly-cached case.

So a block-eligible object is **always** block-mode; a full read never produces a competing whole-body entry.

An entry is invalidated when a block fetch returns a definitive stale signal — an ETag mismatch (concurrent overwrite) or 404/403 (deleted / access revoked) — so a stale entry isn't retried on every read until TTL. A client-forced revalidation (`Cache-Control: no-cache`) of a block-mode entry likewise invalidates it and lets the miss path re-establish the current version.

### Configuration

A single size knob, `block_size`, is the whole↔block boundary; the earlier `write_through_max_size` and `block_cache_min_size` are removed:

| Key | Default | Meaning |
|---|---|---|
| `cache.block_caching_enabled` | `false` | Rollout flag. |
| `cache.block_size` | 4 MiB (must be < 64 MB) | Block granularity **and** the whole↔block boundary: below one block → whole-cached (blocking a sub-block object just stores one blob), at/above → block-cached. The write-through tee (in-memory) also gates on this, keeping its buffer sub-block. |
| `cache.size_threshold` | 1 GB | Overall cacheability ceiling. Block mode lets TAG cache *parts* of objects up to this ceiling without downloading the whole thing. |

Why one knob: block-caching a sub-block object is identical to whole-caching it, so `block_size` is the natural boundary. It governs every populate path uniformly — read misses, full-GET misses, and warm-on-write all split block-eligible objects into blocks and whole-cache the rest — so representation is a function of size alone, with no separate warm cap and no per-access-pattern collision to guard.

### Invalidation and consistency

- Blocks are ETag-scoped: an overwrite writes new keys under the new ETag; stale blocks age out by TTL (no explicit block deletion — same as bodies today).
- DELETE tombstone + `PutWithMetaStreamTombstoneAware` semantics apply per block; `writeStartTime` is stamped before each block fetch.
- Meta `Delete` orphans blocks (age out); the next read re-populates meta + touched blocks.
- The ETag guard on each block fetch prevents mixing versions under one ETag's keys.

## Distributed caching

In cluster mode ocache routes **per key** via consistent hashing, so a block key (`blk|bucket|key|<etag>|<blockSize>|<blockIdx>`) is routed independently — an object's blocks distribute across cluster nodes. This is intended, not a problem to work around: it is how other sharded caches operate (ceph and most chunked/sharded stores shard at block granularity), and it is beneficial — distributing blocks spreads load evenly across the cluster and lets a multi-block read fetch its blocks from several owners in parallel. Correctness is independent of placement: block presence is probed per key and any missing block is fetched via the same routing, so a read works regardless of which node owns which block.

Co-locating an object's blocks on a single owner (hashing on object identity `bucket|key|etag` rather than the block key) was considered and deliberately not adopted — block distribution is the desired behavior. A block-locality routing mode remains a possible **ocache-side** option should a future workload ever want it, but it is not required by this design.

## Rollout and phasing

Back-compat: legacy whole-body entries (`BlockSize=0`) are served by the existing path; new large-object misses populate block-mode. Both coexist; readers dispatch on `meta.BlockSize`. Gated behind `block_caching_enabled` (default off).

1. `meta.BlockSize` + `MakeBlockKey` + config + reader dispatch (block vs whole).
2. Block-aware range serve: per-block probe + partial-hit fetch (coalesced, concurrent).
3. Single shared cache writer splits block-eligible objects into blocks on every full-object populate path (read miss, full-GET miss, warm-on-write read-back); the tee stays sub-block; the whole↔block boundary is `block_size`; remove `block_cache_min_size` and `write_through_max_size`.
4. Full-GET block assembly (mostly-cached) + single-stream re-split fallback (mostly-missing) + metrics.
5. *(optional, not required)* ocache block-locality routing for clusters — only if a future workload ever wants an object's blocks co-located.

Deferred: block-size-vs-footer-fit (single block size for now).

## Metrics

- Block hit/miss and partial-hit rate.
- Blocks fetched per request; block-fetch coalesce rate.
- Bytes populated vs bytes served (amplification ratio) — the headline KPI for this work.
- Watch existing `ocache_segment_size_bytes` for recompaction impact of the tier shift.

## Alternatives considered

- **Option B — ocache sparse blobs.** Store one body key as a sparse blob with a present-ranges map (`PutRange` + hole-aware `GetRangeStream`). Fewer keys and exact-byte population, but a larger change that lands in **ocache** (separate plan/repo). Kept in mind as a possible future evolution; Option A is chosen for phase 1 because it is self-contained in TAG and incremental.
- **Whole-object multipart write-through (#136).** Pre-warms *whole* bodies on write — right for manifests (read in full), wrong for Parquet (footer + selective ranges). Closed in favor of this RFC. Consequence for manifests: at 300–400 MB they are block-eligible (`≥ block_size`), so with `warm_on_write` on they are **block-split** on write (and otherwise on their first full read). A full read of a manifest then assembles its blocks from cache. Reassembly is `N` cache reads instead of one whole-body stream, but that overhead is dwarfed by the byte transfer; the uniform size-only rule was chosen over Option A's "whole for read-in-full" adaptivity because the target workload's large objects are range-read-dominated, where whole-caching rarely pays off.
- **Optimize whole-object warm (#136 Approach B — parallel-range fetch / lazy warm).** Subsumed here: block-granular read-demand populate is a strictly better "warm on read-demand," and parallel-range fetch survives as the concurrent block-fill implementation detail.
