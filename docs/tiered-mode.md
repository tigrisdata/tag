# Tiered Store Mode

`mode: tiered` (or `TAG_MODE=tiered`) runs TAG as a two-tier cache in front of
an upstream that is a cheap, capacity-priced store — not the system of record.
The caller (typically another cache layer) treats TAG's 404 as "not cached" and
falls back to its own authoritative store.

## Semantics

**Local metadata is authoritative for existence.** TAG keeps metadata for every
object it holds, stamped with the tier the body lives in. A metadata miss
answers `NoSuchKey` immediately — no upstream request, in either tier.

**Small objects (declared size ≤ `cache.size_threshold`) are the local tier.**
PUT stores the whole object in the local cache (MD5 ETag over the decoded
body, honoring `If-Match`/`If-None-Match`); GET, HEAD, and DELETE are served
entirely locally. Reads and writes of small objects generate zero upstream
traffic.

**Large objects are the upstream tier.** The PUT passes through to upstream and
TAG stores a metadata marker locally. HEAD answers from the marker; GET
forwards for the body — the mode's only body traffic. DELETE forwards and
drops the marker.

**Cross-tier overwrites clean up the displaced version.** A small write over an
upstream-tier object deletes the upstream copy asynchronously (best-effort —
a failure leaves an orphan for the upstream bucket's own expiry to collect).
A large write over a local-tier object frees the local copy. With no prior
metadata, nothing is cached and nothing is cleaned.

**Population is by writes, plus one healing read-populate.** There is no
write-through of large bodies and no general read-populate: every unique read
costs at most one upstream GET, and never an upstream write. The one
exception is **re-tier-on-read**: a validated GET that hits an upstream-tier
marker whose size fits the local tier triggers a one-shot background move of
the body into the local tier. This heals objects mis-placed by the cold-start
window below, capping the damage at one extra upstream fetch per object
instead of one body forward per read until TTL. The displaced upstream copy
is left for the upstream bucket's own expiry.

## Authentication

The transparent-proxy flow, unchanged. Tiered semantics apply only to requests
whose SigV4 signature TAG validated locally; a request it cannot validate yet
(unknown key, anonymous) forwards to upstream exactly as in transparent mode,
and keys are learned from those responses. Until keys are learned, reads
behave like cache misses, and small writes land in the upstream tier (with a
marker, so they stay readable) — the first such write's 2xx is itself what
teaches the keys, and re-tier-on-read moves those objects into the local tier
on their first validated read.

## Configuration

```yaml
mode: tiered
upstream:
  endpoint: "https://t3.storage.dev"   # the cache bucket's endpoint
cache:
  size_threshold: 1073741824           # tier boundary (default 1GB)
  ttl: 24h                             # applies to both tiers' metadata
```

Startup is fatal when tiered mode is combined with:

- `cache.block_caching_enabled: true` — tiered mode is whole-object only
  (individual blocks expiring would break authoritative local misses). Block
  caching defaults to **off** in this mode.
- `cache.enabled: false` — the local metadata store is the mode.
- `upstream.transparent_proxy` set to anything — superseded by `mode`.

## Not implemented (v1)

Listings, multipart, copies, tagging, and ACL operations pass through to
upstream. Objects created upstream without TAG stamping a marker (a multipart
completion, a server-side copy) read as misses through TAG until written
again via a plain PUT. Client `Cache-Control` revalidation directives are not
consulted — the cache is the store.
