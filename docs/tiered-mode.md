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

**Population is by writes alone.** There is no read-populate and no
write-through of large bodies: every unique read costs at most one upstream
GET, and never an upstream write.

## Authentication

The transparent-proxy flow, unchanged. Tiered semantics apply only to requests
whose SigV4 signature TAG validated locally; a request it cannot validate yet
(unknown key, anonymous) forwards to upstream exactly as in transparent mode,
and keys are learned from those responses. Until keys are learned, requests
behave like cache misses.

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
