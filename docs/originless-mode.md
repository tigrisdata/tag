# Origin-less mode

TAG normally sits in front of an upstream: misses are fetched, writes are forwarded, and cached entries are revalidated against it. Origin-less mode removes the upstream entirely. TAG serves **only what its cache already holds** — a miss is the final answer, not something to go and fetch.

It exists for deployments where TAG is a cache *tier* inside a larger system, and that system owns the fallback: a caller that receives `NoSuchKey` reads from its own authoritative store instead. TAG then never needs to be durable — losing an entry costs the caller a slower read, never the data.

It is **off by default**. Enabling it is also the consent to its trust model — read [Trust model](#trust-model) before deploying it anywhere.

## Enabling it

```yaml
upstream:
  disabled: true
```

Or by environment:

```bash
TAG_UPSTREAM_DISABLED=true
```

`true`/`1` enable; an explicit `false`/`0` overrides a YAML `disabled: true`; anything unrecognized leaves the setting untouched — a typo must not silently flip a deployment's mode.

Two startup behaviors worth knowing:

- **Combining `disabled` with an `endpoint` is a startup error**, not something resolved silently. Honoring the endpoint would turn a cache-only tier into a proxy; honoring the flag would discard a configured origin. TAG refuses to guess.
- **Cross-node cache auth defaults off in this mode.** Cluster gRPC auth derives its token from the AWS credentials TAG uses upstream, and this mode has none — so the default requirement would force either a startup failure or dummy keys, and dummy keys look like authentication while proving nothing. An explicit `TAG_CACHE_GRPC_AUTH=true` is still honored (and still requires credentials). The deployment's trust boundary is the network: run origin-less TAG on an internal network segment, reachable only by its intended callers.

No AWS credentials are required. Startup logs a single line stating the mode and its limits.

## What it serves

Plain single-object operations, against the local cache alone:

- **`PUT`** stores the object under `cache.ttl` and returns its ETag. This is how the tier is populated — its callers write into it directly. Objects at or above `cache.block_size` are stored block-aligned when block caching is enabled (the default) — the same size boundary and the same writer as the proxying mode's populate path, so the entries are indistinguishable from proxy-populated ones.
- **`GET` / `HEAD`** serve from cache; a miss is `NoSuchKey`, the caller's cue to fall back to its authoritative store.
- **`DELETE`** invalidates the entry (not required for correctness — entries lapse by TTL — but explicit expiry gets prompt removal). **Multi-delete** (`POST ?delete`, up to 1000 keys) works the same way; deleting an absent key is a success, as on S3.
- **Conditional writes**: `If-None-Match: *` is put-if-absent in one request; `If-Match` guards overwrites (against a missing object it answers `NoSuchKey`). Check-then-store, not atomic — the right idiom for a cache tier, not a coordination primitive.
- **`ListObjects` / `ListObjectsV2`** enumerate the bucket's cached objects — prefix, delimiter rollup, pagination, `encoding-type=url` — so callers and operators can see what the tier holds. In cluster mode the listing is complete and ordered across all nodes (the storage layer K-way merges). The listing is **advisory**: it reflects metadata presence at scan time, and an entry mid-eviction can be listed yet answer `NoSuchKey` to the GET that follows — the read path stays the truth.

Any query parameter — `?versionId`, `?partNumber`, `?tagging`, and the rest — selects a representation or an operation this mode does not implement, and answers `501` rather than doing something silently wrong. The one exception is `x-id`, the no-op operation tag `aws-sdk-go-v2` appends to every request, which is ignored. Server-side copy (`X-Amz-Copy-Source`) is also `501`:

- **Hit** — served exactly as the proxying mode serves it: full objects, ranges (including block-mode entries), `If-None-Match` / `If-Modified-Since` conditionals, with the `X-Cache` header set.
- **Miss** — `NoSuchKey`, immediately. This is the signal the calling system uses to fall back to its authoritative store.
- **Bad range on a present object** — `416 InvalidRange`, as S3 semantics require. A present object with a wrong range is not a miss.

`Cache-Control: no-cache` and `no-store` from clients are **not consulted**. Both exist to reach past a cache to an origin, and there is no origin: honoring either would turn a healthy cached object into a 404. Nothing is served stale by ignoring them — with no origin, the cached copy is the only copy.

Everything else — `ListBuckets`, multipart, copies, tagging, ACLs — answers `501 NotImplemented`, rejected at the route table before touching anything.

### The visibility contract

One rule decides every answer:

> **An entry whose data is not fully present is invisible, on every request shape.**

An entry whose blocks were partly evicted, or whose body is gone leaving orphaned metadata, answers `NoSuchKey` to `GET`, `HEAD`, and conditionals alike — it never answers `HEAD 200` or `304 Not Modified` from metadata it cannot back with bytes. This matters because callers use `HEAD` as an existence check to decide whether to (re)populate the tier: a false "exists" would suppress exactly the healing that makes the entry servable again. A zero-length object is the boundary case: nothing can be missing from it, so it stays visible and servable.

One qualification: the completeness check runs immediately before serving, and an eviction can land in the moment between the two. A response that has already committed its status can then be cut short mid-body — the client sees an aborted transfer (a short read against the declared `Content-Length`), not a clean `NoSuchKey`. The window is a race of microseconds, not a steady state, but callers should treat an aborted transfer from this tier the same way they treat a miss: retry against the authoritative store. Detection is unambiguous, since the received byte count never matches the declared length.

## Trust model

**The network is the boundary.** Origin-less TAG cannot validate signatures — signature validation learns keys from an upstream, and there is none — so requests are served and accepted **regardless of authentication and regardless of any cached ACL**. An `Authorization` header is ignored, not evaluated. This is what typical callers require: S3 clients sign every request by default, and evaluating unverifiable signatures would reject them all.

The consequence is stated plainly: anything that can reach an origin-less TAG can read everything it holds and write into it. Deploy it only on a network segment reachable solely by its intended callers — a NetworkPolicy admitting only the gateway, not a convention. The explicit, contradiction-checked `upstream.disabled` switch is the deliberate consent for this trade; there is no separate flag to soften it, because a mode that half-trusts the network serves nothing useful.

## Observability

- Every response carries an `x-amz-request-id`, echoed in error bodies, so a failing request can be correlated across client and TAG logs.
- Unsupported operations are recorded under `tag_requests_total` with status `unsupported`, so a caller repeatedly issuing them shows up on the request dashboard instead of blending into the success rate.
- Writes record as `PutObject`/`DeleteObject` with the usual statuses.
- Hits and misses count in `tag_cache_hits_total` / `tag_cache_misses_total` as usual, and `X-Cache` reports `HIT`/`MISS` per response.

## Current limits

- **No multipart.** Objects arrive as single `PUT`s, bounded by `cache.size_threshold` (1 GiB default); larger bodies answer `EntityTooLarge`. Callers that upload objects as single PUTs — the intended shape for this tier — are unaffected.
- **No `ListBuckets`** (buckets are implicit; enumerating them would need a full scan), **no server-side copy, no tagging/ACL subresources.**
- Cluster mode works — cross-node serving goes through the cache layer, not the upstream — but multi-node sizing guidance will come with the deployment documentation.

## Turning it off

Remove `upstream.disabled` (or set `TAG_UPSTREAM_DISABLED=false`) and configure the upstream as usual. Cached entries remain valid; the proxying mode revalidates them against the origin on their normal terms.

## Reference

- [Configuration](configuration.md)
- [Cache control](cache-control.md) — client cache semantics in the proxying mode, for contrast
- [Metrics](metrics.md)
