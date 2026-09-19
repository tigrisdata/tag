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
PUT STREAMS the object into the local cache — the body is never buffered in
memory, so the threshold can be sized to the workload without a per-request
memory cost. The MD5 ETag is computed over the decoded bytes as they stream;
because the ETag cannot exist before the last byte, the body is keyed by a
per-write id carried in the metadata (`BodyRef`), and the metadata commit is
what makes the entry visible. `If-Match`/`If-None-Match` are honored; GET,
HEAD, and DELETE are served entirely locally. Reads and writes of small
objects generate zero upstream traffic.

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
the body into the local tier — streamed like the engine's PUT, so a heal
costs fixed memory regardless of object size and the threshold is never
budget-capped. This heals objects mis-placed by the cold-start
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

**Forwarding flavor is decided by the endpoint.** Tiering is a caching
topology, not a forwarding flavor: a tiered deployment fronting a Tigris
domain (`*.tigris.dev`, `*.storage.dev`) forwards the transparent way (client
signature preserved, `X-Tigris-Proxy-*` identity headers, keys learned from
Tigris); one fronting any other endpoint — including `localhost`, which
transparent mode allows for local testing but which earns no trust claim
about the backend behind it — forwards the signing way (TAG validates the
caller against its credential store and re-signs). The startup log states
which flavor was selected.

**Credential requirement** (Tigris-backed tiered deployments): unlike proxy
mode's read-only guidance (which targets customer buckets), tiered mode's
upstream is the operator's own cache bucket, and the validated caller's key
must have **delete** permission there — the cross-tier cleanup DELETE is
signed with it. With read-only credentials every cleanup is rejected (visible
as `tag_tiered_cleanup_total{outcome="rejected"}`) and displaced upstream
copies accumulate until bucket expiry. On any other endpoint the cleanup is
never issued (next paragraph), so the credential-store key needs **read**
permission only — the re-tier fetch is a GET — and `rejected` cannot occur.

**Non-Tigris endpoints: cross-tier cleanup is disabled.** The cleanup DELETE
carries `If-Match` so a racing replacement is never deleted, and that safety
rests on the backend *enforcing* the precondition — verified on Tigris only.
A backend that accepts but ignores `If-Match` would let the delete remove a
replacement that landed after the pre-check, a durable loss no metadata repair
can undo. So on any other endpoint TAG does not issue the delete at all: the
displaced upstream copy ages out by bucket expiry (the same outcome as
read-only credentials), counted as
`tag_tiered_cleanup_total{outcome="unverified_backend"}`, and the startup log
says so. Deployments whose keys are never overwritten (content- or
version-addressed) never trigger cleanup in the first place.

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
- `cache.legacy_coordination: true` — tiered mode requires CAS-strength meta
  coordination (the cache is authoritative and the local tier holds the only
  copy, so ordering races that are transient staleness in proxy mode would be
  data loss here). CAS coordination is selected **automatically** when the
  setting is unset; every node running tiered is CAS-capable by construction,
  so the proxy modes' legacy default has no upgrade population to protect in
  this mode.
- `upstream.transparent_proxy` set to anything — superseded by `mode`.

## Not implemented (v1)

Listings, multipart transfers, copies, tagging, and ACL operations pass
through to upstream. A multipart **completion** stamps an upstream-tier
marker — the assembled object is upstream-tier by construction — so
multipart-written objects are immediately readable (HEAD from the marker,
GET forwarded); the marker's metadata comes from an upstream HEAD when the
completing request validated, and falls back to an ETag-only marker with
unknown length otherwise. Objects created upstream without a marker-stamping
operation (a server-side copy) read as misses through TAG until written
again via a plain PUT. Client `Cache-Control` revalidation directives are
not consulted — the cache is the store.

A validated GET/HEAD carrying a query parameter the local engine does not
implement (`versionId`, `partNumber`, `attributes`, presigned
`response-content-*` overrides) answers 501 NotImplemented when the object is
local-tier or uncached — the local store is authoritative there and cannot
forward without breaking that authority. The same request against an
upstream-tier object forwards normally. Sub-resource PUTs with no dedicated
route (`retention`, `legal-hold`) forward to upstream without touching the
object's local metadata.
