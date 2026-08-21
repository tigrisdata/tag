# RFC 0002: Parquet footer warming on write

- **Status:** Proposed
- **Related:** [RFC 0001](0001-block-aligned-caching.md) (block-aligned caching), [#174](https://github.com/tigrisdata/tag/pull/174) (read-triggered footer prefetch)

## Summary

Cache a parquet object's **metadata blocks at write time**, when TAG proxies the upload, instead of waiting for a reader to discover them as misses. For a workload where writers and readers overlap — an ingest pipeline feeding a dashboard that queries a recent time window — a newly written file is read within one refresh interval. TAG already has everything it needs at write time, so this schedules a fetch rather than speculating about one.

## Motivation

[#174](https://github.com/tigrisdata/tag/pull/174) added a read-triggered footer prefetch: when a read reaches a parquet object's trailer, TAG fetches the metadata blocks the reader is about to need. It is effective, but *reactive* by construction — the first read of a file is what fires it, so it can only help the second and later reads.

For a **sliding-window** workload, the first read is the only one that misses.

A dashboard querying `[now - W, now]` on a refresh interval `R` re-reads the same window every `R`. Two consequences:

- The window's files are read `W/R` times, and every read after the first finds them cached.
- The window **slides**: each interval, newly written files enter it and files older than `W` leave.

So the only cold footer reads are files written since the last refresh. A read-triggered prefetch cannot help them, because for those files the triggering read *is* the cold read.

This is a common shape for observability and analytics stacks: a columnar store where ingesters continuously write parquet and a dashboard repeatedly queries recent data. When the same TAG cluster proxies both the writes and the reads, the write is an exact predictor of the read.

### Why not the alternatives

Other prediction signals were considered:

| Approach | Cost to predict | Precision |
| --- | --- | --- |
| Parse the writer's catalog/manifest files | Manifests can reach tens of MB and are rewritten continuously; parsing lands on the serve path | Bounded by query-side file pruning TAG cannot observe |
| Prefix `LIST` of adjacent partitions | One request per partition | Requires the query's time extent, which TAG does not know |
| Adjacent-partition heuristic on read | none | Same extent problem |
| **Warm footer on write** | **none — TAG already has the object** | **Exact, whenever readers follow writers** |

Manifest parsing initially looks attractive: a catalog entry typically carries both the object key and its size, which is what is needed to locate a trailer. But manifests describe far more than any single query reads, so TAG would have to guess which entries matter — and the file pruning that narrows them happens on the query side, using predicates TAG never sees. Write-time warming needs no prediction at all.

## Goals

- Eliminate cold footer reads for objects written while a reader is active.
- Reuse the existing footer-prefetch machinery rather than adding a parallel path.
- Cost proportional to metadata size, not object size.
- Precision measurable from day one, on the same metrics as the read-triggered trigger.
- Off by default; shares the existing `cache.parquet_optimization` gate.

## Non-goals

- Warming whole objects. `cache.warm_on_write` already does that, and for large parquet files it moves two orders of magnitude more bytes than the metadata alone.
- Helping ad-hoc queries over data older than any recent write. Those still rely on the read-triggered prefetch from #174; a future RFC may add read-side adjacency prediction for them.
- Any change to the read path. This adds a trigger, not a serve mode.

## Background: what a footer costs

Parquet stores its metadata at the **end** of the object: the last 8 bytes are a 4-byte little-endian metadata length followed by the `PAR1` magic, with the metadata occupying the length bytes immediately before them. A reader cannot read any data before it reads that metadata.

Footer size scales with the number of row groups and columns, because the footer carries per-row-group, per-column statistics. For wide schemas it is a meaningful fraction of the object rather than a fixed small tail.

Measured on a production deployment with a wide schema (hundreds of columns), footers ran at roughly **1.25% of object size** — a few MB on objects of a few hundred MB. Against a 1 MiB `block_size` that metadata spans several blocks, so a reader opening such a file finds several blocks absent.

Two implications for sizing:

- Warming metadata costs on the order of 1% of what warming whole objects costs.
- The relevant comparison for "does the metadata fit the cached tail block?" is not `block_size` but the **remainder** block, `ContentLength mod block_size`, which averages half a block. Metadata exceeds it far more often than a naive `footer > block_size` test would suggest.

Deployments with narrow schemas will see much smaller footers and should expect this optimization to do correspondingly less; the histogram below is how to tell.

## Design

### Trigger

On a successful write of a key ending in `.parquet` — `PutObject`, `CompleteMultipartUpload`, and the copy path — schedule a background footer warm. Gated on `cache.parquet_optimization` and on block caching being enabled.

The trigger fires **after** the client's write response is committed, so it adds no latency to the write.

### Establishing the entry

At write time TAG has no cached metadata for the object, so unlike the read-triggered path there is no `meta` to start from. A single ranged request supplies everything:

```
GET <object>   Range: bytes=-8
```

The `206` response yields all three required inputs at once:

- **body** — the 8-byte trailer, parsed for the metadata length
- **`Content-Range: bytes X-Y/Z`** — `Z` is the object size, so no `HEAD` is needed
- **`ETag`** — keys the blocks and the meta entry

The **suffix** form is deliberate: it requires no prior knowledge of the object's size. That is the property that lets this run for an object TAG has never seen, without a second round trip and without consulting the writer's catalog.

The returned interval is validated to be exactly the object's last 8 bytes before any block index is derived from it. A server that answers a different interval — or ignores the range and returns `200` — is rejected; in the `200` case the body is closed **without** draining, since draining a whole object for connection reuse is precisely the cost this design exists to avoid.

### Populating

With `ContentLength`, `ETag` and the metadata length in hand, the covering block range is computed as in #174 and handed to the existing block-mode populate path, which fetches those blocks and writes the block-mode meta **last**, tombstone-aware. That ordering is the visibility gate from RFC 0001 and is preserved unchanged.

Objects whose metadata fits inside the tail block warm that one block, which is their entire footer.

### Bounds

Inherited from #174 rather than reinvented:

- Capped at a fixed maximum block count; blocks already cached are skipped.
- Coalesced per object, so a retried or duplicated write warms once.
- Fetches take the populate budget **non-blocking**, so warming is shed rather than queued when real reads are contending. Under load, serving wins.
- A malformed or non-parquet trailer aborts the warm silently: a `.parquet` suffix is a hint, not a guarantee.
- Anonymous writes are skipped — a public write implies nothing about read access.

### Interaction with eviction

Where the query window slides, objects older than the window stop being read. A FIFO eviction policy ages out oldest-first, which matches that access pattern. Under LRU the warmed-but-unread tail is reclaimed on pressure instead; either way an unused warm is bounded by the cache, not permanent.

## Metrics

- `tag_cache_block_prefetched_total{trigger="write_warm"}` — how much speculative work this trigger does
- `tag_cache_parquet_footer_bytes` — the footer-size distribution, which tells a deployment whether its schema makes this worthwhile at all
- `tag_cache_block_hits_total` / `tag_cache_block_misses_total` — the outcome

Judge it on the **hit ratio**, not on a prefetch-precision ratio. An earlier revision of
this RFC proposed a per-block "was it used" counter and a rule comparing precision between
triggers. That was dropped: attributing each serve back to a prefetch put a lookup on the
serve path, running thousands of times a second, to re-derive a number that is ~98% by
construction — a reader that has read a parquet trailer cannot do anything except read the
metadata region next. The hit ratio measures the outcome directly and costs nothing extra.

The decision rule is therefore: **prefetch volume rising without the block hit ratio
rising means the trigger is not landing where reads go, and should be turned off.**

## Rollout

1. Ship behind `cache.parquet_optimization`, shared with #174.
2. Compare `prefetched{trigger="write_warm"}` against `prefetch_used_total`, and watch block miss rate on first opens.
3. If precision holds, cold-footer reads should approach zero while a reader is active.

Reverting is a configuration change, not a rollback: clearing the flag disables both triggers.

## Alternatives considered

**Warm the whole object** (`cache.warm_on_write`). Already exists, and moves roughly 80× more bytes for the same benefit on a footer-driven workload. Rejected on cost.

**Parse the writer's catalog/manifest.** Would give every object's key and size without extra requests. Rejected because manifests are large, rewritten continuously, describe far more than any query reads, and would put parsing on the serve path — while write-time warming needs no prediction at all. It also couples TAG to one writer's catalog format, where the suffix-range approach depends only on the parquet spec.

**Tee the footer out of the upload body.** For multipart uploads the final part contains the trailer, so TAG could capture it without a second request. Rejected for phase 1: it couples the warm path to multipart internals and to whether the client uses multipart, to save one small ranged GET per object. Worth revisiting if that request ever shows up in the write-path budget.

**Read-triggered adjacent-partition prefetch.** Complementary rather than competing: it is the answer for ad-hoc queries over older data, where no recent write predicts the read. Left for a later RFC.
