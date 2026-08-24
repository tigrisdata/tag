# Origin-less mode

TAG normally sits in front of an upstream: misses are fetched, writes are forwarded, and cached entries are revalidated against it. Origin-less mode removes the upstream entirely. TAG serves **only what its cache already holds** — a miss is the final answer, not something to go and fetch.

It exists for deployments where TAG is a cache *tier* inside a larger system, and that system owns the fallback: a caller that receives `NoSuchKey` reads from its own authoritative store instead. TAG then never needs to be durable — losing an entry costs the caller a slower read, never the data.

It is **off by default**, and it is deliberately incremental: this first phase serves reads only. See [Current limits](#current-limits) before enabling it anywhere.

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

`GET` and `HEAD` of a single object, from cache alone:

- **Hit** — served exactly as the proxying mode serves it: full objects, ranges (including block-mode entries), `If-None-Match` / `If-Modified-Since` conditionals, with the `X-Cache` header set.
- **Miss** — `NoSuchKey`, immediately. This is the signal the calling system uses to fall back to its authoritative store.
- **Bad range on a present object** — `416 InvalidRange`, as S3 semantics require. A present object with a wrong range is not a miss.

`Cache-Control: no-cache` and `no-store` from clients are **not consulted**. Both exist to reach past a cache to an origin, and there is no origin: honoring either would turn a healthy cached object into a 404. Nothing is served stale by ignoring them — with no origin, the cached copy is the only copy.

Every other operation — writes, deletes, copies, multipart, listings, tagging, ACLs — answers `501 NotImplemented`. These are rejected before they touch anything, so a stray write cannot invalidate or delete cached data.

### The visibility contract

One rule decides every answer:

> **An entry whose data is not fully present is invisible, on every request shape.**

An entry whose blocks were partly evicted, or whose body is gone leaving orphaned metadata, answers `NoSuchKey` to `GET`, `HEAD`, and conditionals alike — it never answers `HEAD 200` or `304 Not Modified` from metadata it cannot back with bytes. This matters because callers use `HEAD` as an existence check to decide whether to (re)populate the tier: a false "exists" would suppress exactly the healing that makes the entry servable again. A zero-length object is the boundary case: nothing can be missing from it, so it stays visible and servable.

## Trust model (this phase)

Only **anonymous requests for objects cached with a `public-read` ACL** are served. TAG validates signatures by learning keys from its upstream, and there is no upstream — so signed requests cannot be validated and answer `NoSuchKey`, indistinguishable from absence. Serving regardless of ACL, for deployments whose network is the trust boundary, is a planned explicit switch — not a side effect of removing the origin.

## Observability

- Rejected operations are recorded under `tag_requests_total` with status `unsupported` — a caller persistently writing to an origin-less tier (a misconfigured client, for instance) shows up on the request dashboard instead of blending into the success rate.
- Hits and misses count in `tag_cache_hits_total` / `tag_cache_misses_total` as usual, and `X-Cache` reports `HIT`/`MISS` per response.

## Current limits

This phase is intentionally narrow. In particular, **nothing can write to an origin-less TAG yet** — mutations are rejected, so the cache can only serve entries it already holds. Local writes (letting callers `PUT` directly into the tier) and the network-trust read switch are subsequent phases. Until then this mode is for validating the read path, not for production population.

Cluster mode works — cross-node serving goes through the cache layer, not the upstream — but sizing and multi-node validation guidance will come with the deployment documentation once the mode is production-complete.

## Turning it off

Remove `upstream.disabled` (or set `TAG_UPSTREAM_DISABLED=false`) and configure the upstream as usual. Cached entries remain valid; the proxying mode revalidates them against the origin on their normal terms.

## Reference

- [Configuration](configuration.md)
- [Cache control](cache-control.md) — client cache semantics in the proxying mode, for contrast
- [Metrics](metrics.md)
