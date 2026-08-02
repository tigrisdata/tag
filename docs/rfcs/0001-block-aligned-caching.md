# RFC 0001: Block-aligned caching for large objects

- **Status:** Draft
- **Tracking issue:** [#137](https://github.com/tigrisdata/tag/issues/137)
- **Related:** [#136](https://github.com/tigrisdata/tag/issues/136) (closed — multipart write-through, superseded), [#135](https://github.com/tigrisdata/tag/pull/135) (write-through tee, merged)

## Summary

Cache large objects at **block granularity** instead of as whole bodies, so a range read (a Parquet footer, or a few surviving row groups) populates and serves only the bytes actually accessed. An object's body is stored as N fixed-size blocks, each an independent cache blob; a range read touches only its covering blocks, and missing blocks are fetched (coalesced, concurrently) on demand.

## Motivation

TAG caches the **whole object body** (`body|bucket|key|<etag>`). To serve *any* range from cache, the entire object must first be downloaded and stored (`fetchFullObjectToCache`). For an access pattern that only ever reads a small tail plus a few interior ranges of a large object, that is a large, mostly-wasted download.

Parseable's query path (DataFusion) is exactly that pattern: it reads a small **footer** tail of *many* Parquet files for pruning, then **selected row-group ranges** of the files that survive pruning — never whole objects. Recent Parseable changes push it further that way: bloom filters in the footer prune harder (more files touched footer-only), a 50 GB in-querier footer cache, and explicit ranged downloads for large objects.

Measured object sizes (prod `parseable-prod-ca-toronto-1`, one day, 53,157 objects): median **88 MiB**, p99 **203 MiB**, ~66% of objects > 25 MiB. A Parquet footer is tens–hundreds of KB. Caching a whole 88 MiB object to serve a ~100 KB footer is on the order of **~800× populate amplification**, and for footer-only-pruned files essentially all of that bandwidth and cache space is wasted.

## Goals

- Populate and serve range reads of large objects at block granularity — only touched bytes.
- Serve partial hits: cached blocks from cache, missing blocks fetched on demand and cached for next time.
- Cut populate amplification and populate-budget pressure for range-read workloads.
- Preserve the existing whole-object path for small objects and full-object GETs.
- Self-contained in TAG for single-node deployments (Parseable's current shape); no ocache changes required for phase 1.

## Non-goals

- Sparse/partial blobs inside ocache (that is "Option B" — see Alternatives).
- Cross-node block-locality routing (deferred; see Distributed considerations).
- Footer-specific block sizing / special-casing (deferred).
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

### Warm-on-write interaction

`warmOnWrite` is a simple **whole-object** size gate, capped by its own knob `warm_on_write_max_size` (independent of the read-side block boundary):

- object **≤ `warm_on_write_max_size`** → whole-object warm (via the in-memory tee for objects below `block_size`, else the streaming read-back). It always produces a whole-mode entry.
- object **> `warm_on_write_max_size`** → **noop** (the fetch aborts after headers; no read-back).

A warmed object is whole-mode regardless of its size, so a later range read serves from the whole body — consistent with the "warm-on-write or full-GET → whole; range-first → blocks" rule. `warm_on_write_max_size` is a blunt instrument (warming a large object whole helps read-in-full objects like manifests, wastes bandwidth for footer-read ones), so it defaults conservative and warming stays opt-in via `warm_on_write`.

### Full-object GET on a block-mode object

Rare for Parseable (it reads ranges) but must be correct: on a **hit**, assemble blocks `0…N-1` in order, fetching any missing via `fetchBlocksToCache`, streaming as we go. A full GET thus warms all blocks — acceptable given its rarity.

On a full-GET **miss** of a block-eligible object, the object is **whole-cached** (Option A): the whole object was fetched to satisfy the GET, so there is no range-miss amplification to avoid, and caching it whole is a single fetch (no re-download) that also handles ETag-less objects. It becomes a whole-mode entry (`BlockSize=0`); block mode is established only on the **range-read** path. A later range read of a whole-cached object serves its bytes efficiently from the whole body (`GetRangeStream` reads only the requested range). So an object's representation follows its first access: full-GET-first → whole; range-first → block. (Considered and rejected: re-fetching the object as blocks in the background to keep a strict `block-eligible == block-mode` invariant — it doubles upstream egress on every cold full-GET and can't cache ETag-less objects, for no benefit the range path doesn't already give.)

An entry is invalidated when a block fetch returns a definitive stale signal — an ETag mismatch (concurrent overwrite) or 404/403 (deleted / access revoked) — so a stale entry isn't retried on every read until TTL. A client-forced revalidation (`Cache-Control: no-cache`) of a block-mode entry likewise invalidates it and lets the miss path re-establish the current version.

### Configuration

The read-side boundary and the write-side warm cap are **separate knobs**; the former `write_through_max_size` and `block_cache_min_size` are both removed (the read boundary is now `block_size`, the write cap is `warm_on_write_max_size`):

| Key | Default | Meaning |
|---|---|---|
| `cache.block_caching_enabled` | `false` | Rollout flag. |
| `cache.block_size` | 4 MiB (must be < 64 MB) | Block granularity **and** the read-side whole↔block boundary: a read miss for an object **<** one block is whole-cached (blocking a sub-block object just stores one blob), **≥** one block is block-cached. |
| `cache.warm_on_write_max_size` | 25 MiB | Write-path cap: with `warm_on_write` on, only objects **≤** this are warm-cached whole. Independent of `block_size`. |
| `cache.size_threshold` | 1 GB | Unchanged overall cacheability ceiling. Block mode now lets TAG cache *parts* of objects up to this ceiling without downloading the whole thing. |

Why two knobs: block-caching a sub-block object is identical to whole-caching it, so `block_size` is the natural read boundary — no separate `block_cache_min_size` is needed. And "how big an object do we warm on write" is a distinct decision (bounded by the tee's in-memory buffer for small objects, a streaming read-back for larger), so it gets its own cap.

### Invalidation and consistency

- Blocks are ETag-scoped: an overwrite writes new keys under the new ETag; stale blocks age out by TTL (no explicit block deletion — same as bodies today).
- DELETE tombstone + `PutWithMetaStreamTombstoneAware` semantics apply per block; `writeStartTime` is stamped before each block fetch.
- Meta `Delete` orphans blocks (age out); the next read re-populates meta + touched blocks.
- The ETag guard on each block fetch prevents mixing versions under one ETag's keys.

## Distributed considerations (deferred)

ocache routes **per key** via consistent hashing, so appending `blockIdx` distributes an object's blocks across nodes. This is the standard sharded-cache design (ceph and most sharded stores work this way) and is often beneficial — it spreads load and allows parallel block fetches from multiple nodes. It is **not** treated as a problem here.

The only reason to revisit it would be TAG-specific: prod measured this deployment's cross-node gRPC *data plane* as expensive (a large share of burst CPU, and a warm-p95 tax in the 1-vs-2-node test). But that measurement was for **whole-object** fan-out (one key → one remote owner → whole object over gRPC); block-level transfers have a different profile and we don't yet know their cross-node impact — it may be neutral or better. That per-hop gRPC overhead is also an implementation cost being addressed separately, not a property of block distribution.

**Decision:** ship with block distribution as-is; measure block-level cross-node behavior in a cluster before deciding anything. Block caching defaults **off** (`block_caching_enabled`), and single-node deployments (Parseable's current shape) are unaffected regardless. *If* the data later shows a cluster problem, an optional block-locality routing mode — hashing on object identity `bucket|key|etag` so an object's blocks co-locate on one owner — is an **ocache-side** change (per the OCache-vs-TAG plan split) that can be added then. It is explicitly not a prerequisite for this work.

## Rollout and phasing

Back-compat: legacy whole-body entries (`BlockSize=0`) are served by the existing path; new large-object misses populate block-mode. Both coexist; readers dispatch on `meta.BlockSize`. Gated behind `block_caching_enabled` (default off).

1. `meta.BlockSize` + `MakeBlockKey` + config + reader dispatch (block vs whole).
2. Block-aware range serve: per-block probe + partial-hit fetch (coalesced, concurrent).
3. Warm-on-write gated to `≤ warm_on_write_max_size`, noop above; read boundary is `block_size`; remove `block_cache_min_size` and `write_through_max_size`.
4. Full-GET block assembly + metrics.
5. *(optional, later — may or may not do)* ocache block-locality routing for clusters.

Deferred: block-size-vs-footer-fit (single block size for now).

## Metrics

- Block hit/miss and partial-hit rate.
- Blocks fetched per request; block-fetch coalesce rate.
- Bytes populated vs bytes served (amplification ratio) — the headline KPI for this work.
- Watch existing `ocache_segment_size_bytes` for recompaction impact of the tier shift.

## Alternatives considered

- **Option B — ocache sparse blobs.** Store one body key as a sparse blob with a present-ranges map (`PutRange` + hole-aware `GetRangeStream`). Fewer keys and exact-byte population, but a larger change that lands in **ocache** (separate plan/repo). Kept in mind as a possible future evolution; Option A is chosen for phase 1 because it is self-contained in TAG and incremental.
- **Whole-object multipart write-through (#136).** Pre-warms *whole* bodies on write — right for manifests (read in full), wrong for Parquet (footer + selective ranges). Closed in favor of this RFC. Consequence for manifests: at 300–400 MB they are above the default `warm_on_write_max_size` (25 MiB), so warm-on-write is a noop for them — but because they are **read in full**, their first full-object read is a full-GET miss and lands on the whole-cache path (Option A), so they end up **whole-cached** (the right representation for a read-in-full object), and subsequent reads hit. The only change versus write-time pre-warming is that the first read after each manifest rewrite is a cold full fetch rather than a pre-warmed hit; if that proves material, raise `warm_on_write_max_size` to pre-warm manifests on write.
- **Optimize whole-object warm (#136 Approach B — parallel-range fetch / lazy warm).** Subsumed here: block-granular read-demand populate is a strictly better "warm on read-demand," and parallel-range fetch survives as the concurrent block-fill implementation detail.
