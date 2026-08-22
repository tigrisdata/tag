# Parquet optimization

TAG can cache a parquet object's **metadata** ahead of the reader that needs it, so opening a file does not begin with a series of upstream round trips.

It is **off by default**. This page explains what it does, how to tell whether it will help your data, how to turn it on, and how to tell afterwards whether it did.

---

## Why parquet needs special handling

A parquet reader cannot read any data before it reads the file's metadata, and that metadata lives at the **end** of the object:

```
┌──────────────── object ────────────────┐
│  row groups …          │  metadata  │ L │ PAR1 │
└────────────────────────┴────────────┴───┴──────┘
                                        4B    4B
```

The last 8 bytes are a 4-byte little-endian metadata length followed by the `PAR1` magic. So a reader always:

1. reads the final 8 bytes to learn how long the metadata is, then
2. reads the metadata region, then
3. reads only the row groups that survive pruning.

Step 2 is on the critical path of every query touching that file. If those bytes are not cached, the reader waits for object storage before it can look at a single row.

## What TAG does about it

With [block-aligned caching](rfcs/0001-block-aligned-caching.md), an object is cached as fixed-size blocks (`cache.block_size`, 1 MiB by default). The read in step 1 caches only the block containing the trailer. If the metadata is larger than that block, the rest of it is **not** cached, and the reader discovers those blocks as misses.

When `cache.parquet_optimization` is on, TAG fills that gap from two directions:

| Trigger | Fires when | Helps |
| --- | --- | --- |
| `parquet_footer` | a read reaches an object's trailer | the **second and later** opens of a file |
| `write_warm` | a parquet object is written through TAG | the **first** open, before any read happens |

Both compute the metadata region from the length the file itself declares — never a guess — and fetch only the blocks it provably spans.

### Why the second trigger exists

The read trigger cannot help a file's first open, because the first read is what fires it. That matters when writers and readers overlap: an ingest pipeline writing parquet while a dashboard queries a recent time window re-reads the same window every refresh, so **every open after the first is already warm** and the only cold opens are files written since the last refresh. `write_warm` covers exactly those.

If nothing writes through TAG, or writes and reads are far apart in time, `write_warm` will do little and `parquet_footer` does the work.

---

## Will it help my data?

It depends on one thing: **is the metadata bigger than the block that already gets cached?**

Footer size scales with row groups and columns, because the footer carries per-column statistics for every row group. A wide schema can put several MB of metadata on a few-hundred-MB object; a narrow one keeps it under a hundred KB.

The comparison is *not* footer size against `block_size`. It is footer size against the **tail block**, which is usually a partial block:

```
tail_bytes = ContentLength - floor((ContentLength - 1) / block_size) * block_size
```

That is `ContentLength mod block_size` for most objects — anywhere from 1 byte to a full block — and exactly `block_size` for an object whose length happens to be a multiple of it. The prefetch does work whenever:

```
footer_bytes + 8  >  tail_bytes
```

For arbitrarily sized objects the tail averages half a block, so a footer of a few hundred KB against 1 MiB blocks still fires on a meaningful fraction of them. Objects that are exact multiples of `block_size` get a full block of coverage and fire least often.

**To find out for your data**, enable the flag on one node and read the histogram:

```promql
# Median parquet footer size. Compare against cache.block_size.
histogram_quantile(0.5, sum(rate(tag_cache_parquet_footer_bytes_bucket[1h])) by (le))
```

`tag_cache_parquet_footer_bytes` is recorded for **every** parquet object whose trailer is read — including ones that are not prefetched — so it describes the whole population, not just the part that was acted on. If the distribution sits well below your typical tail-block size, this optimization has nothing to do and should stay off.

---

## Enabling it

```yaml
cache:
  block_caching_enabled: true    # required — this builds on block mode
  block_size: 1048576            # the metadata is measured against this
  parquet_optimization: true
```

Or by environment:

```bash
TAG_CACHE_PARQUET_OPTIMIZATION=true
```

The value must be `true` or `1`. **Anything unrecognized leaves it off** — this is speculative work, so an ambiguous setting is not treated as consent.

Applies to keys ending in `.parquet` (case-insensitive). The suffix is a hint, not a guarantee: TAG validates the `PAR1` trailer and does nothing if it is absent, so a non-parquet file with that extension is simply ignored.

### Related settings

- **`cache.block_size`** — the single most important tuning knob. Size it to your reader's granularity; an oversized block over-fetches on every miss. It also sets what counts as "metadata too large to fit", as above.
- **`cache.meta_on_write`** — complementary, not an alternative. It caches an object's *metadata entry* on write so the first read does not spend a round trip discovering the object exists. Measured across a reader's opening sequence, it removes the discovery round trips while footer caching removes the block fetches; enabling both takes a cold first open to zero upstream requests.

---

## Checking whether it worked

Measure the **outcome**, not the speculation. The block hit ratio is what actually
matters and it already exists:

```promql
# Block cache hit ratio. Footer caching should raise it, most visibly on first opens.
sum(rate(tag_cache_block_hits_total[30m]))
/
(sum(rate(tag_cache_block_hits_total[30m])) + sum(rate(tag_cache_block_misses_total[30m])))
```

Alongside it, the cost side:

```promql
# How much speculative fetching each trigger is doing.
sum by (trigger) (rate(tag_cache_block_prefetched_total[30m]))
```

Rising hit ratio with modest prefetch volume is the shape you want. Prefetch volume
with no movement in hit ratio means the speculation is not landing where reads go, and
the trigger should be turned off.

There is deliberately **no per-block "was this prefetch used" counter**. Attributing
each later serve back to a prefetch costs a lookup on the serve path — hundreds of
times per full-object serve, thousands per second — to re-derive a number that is
~98% by construction, because a reader that has read a parquet trailer cannot do
anything except read the metadata region next. That is not a good trade for a hot
path, and the hit ratio answers the real question more directly.

Also worth watching:

- `tag_cache_parquet_footer_bytes` — the footer distribution. A shift here means a
  schema change, and it is worth re-checking that the optimization still applies.

## What it costs, and what bounds it

Fetching metadata is roughly 1% of the bytes that caching whole objects would move, but it is not free. The safeguards:

- **Bounded by the object.** The block range comes from the declared metadata length, validated against the object size. A corrupt or impossible length prefetches nothing.
- **Capped.** At most 32 blocks per object, so a pathological file cannot evict a working set.
- **Shed, not queued.** Fetches take the populate budget non-blocking, so under load prefetching is dropped rather than competing with reads that clients are waiting on.
- **Skips what is cached.** Blocks already present are never re-fetched.
- **Coalesced.** One scan per object version at a time, with a short cooldown afterwards, so a hot object is not re-examined on every read.
- **Off the request path.** Both triggers fire after the response is committed, so neither adds latency to the request that fired it.

## Turning it off

Set `cache.parquet_optimization: false` (or remove it). Both triggers stop; nothing needs to be rolled back, and blocks already cached remain valid and are served normally.

## Reference

- [RFC 0001 — Block-aligned caching](rfcs/0001-block-aligned-caching.md)
- [RFC 0002 — Parquet footer warming on write](rfcs/0002-parquet-footer-warming.md)
- [Configuration](configuration.md) · [Metrics](metrics.md)
