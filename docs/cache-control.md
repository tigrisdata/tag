# Cache Control & Revalidation

TAG supports RFC 7234-compliant cache revalidation. Clients can control caching behavior using standard `Cache-Control` headers, and TAG reports cache status via the `X-Cache` response header.

## X-Cache Header Reference

| Value         | Meaning                                                                          |
| ------------- | -------------------------------------------------------------------------------- |
| `HIT`         | A cache entry was selected to serve the response (including revalidation that confirmed the object is unchanged) |
| `MISS`        | Not in cache, fetched from upstream and now cached                               |
| `REVALIDATED` | Revalidation found a changed object; client validators may still yield a bodyless 304 |
| `BYPASS`      | Cache bypassed entirely (client requested `no-store`, or the request carries a representation selector: `versionId`, `partNumber`, `response-*`) |
| `DISABLED`    | Caching is disabled server-side (`TAG_CACHE_DISABLED=true`)                      |

In block mode, a committed stream can recover missing bytes from an upstream range only after validating the cached ETag and exact range. Because `HIT` is committed before the body streams, it describes the cache-backed request, not each byte's source or whether the transfer completed. See [S3 compatibility](s3-compatibility.md#caching-behavior) for incomplete cache-range behavior.

## Force Revalidation

Send `Cache-Control: no-cache` or `Cache-Control: max-age=0` to force TAG to check with upstream before serving a cached object. For GET, TAG sends a synthetic conditional request using the cached ETag. The origin reply checks cache freshness; TAG then evaluates the client's original validators against the confirmed representation. Multiple `If-None-Match` field values are treated as one comma-separated list, and `If-None-Match` takes precedence over `If-Modified-Since`. If they match, TAG returns a bodyless 304 without reading the cached body. Otherwise, an origin 304 serves the cached full object or requested range (`X-Cache: HIT`). If that 304 changes the cached ETag and the client still needs a body, TAG forwards the original request before committing so the body matches the new validator. An origin 200 or 206 returns the changed full or ranged response (`X-Cache: REVALIDATED`) unless the client's validator matches the new representation; in that case TAG returns a bodyless 304 while preserving the existing cache update or rewarm when eligible. This conditional-response behavior applies to GET; HEAD uses its separate revalidation path.

```bash
# Force revalidation on GET
curl -H "Cache-Control: no-cache" http://localhost:8080/my-bucket/my-key

# Force revalidation on HEAD
curl -I -H "Cache-Control: no-cache" http://localhost:8080/my-bucket/my-key

# Force revalidation on range request
curl -H "Cache-Control: no-cache" -H "Range: bytes=0-99" \
  http://localhost:8080/my-bucket/my-key
```

If the revalidation request to upstream fails, TAG serves the stale cached copy as a fallback. The client's validators are evaluated against that stale representation, but an origin failure remains a failed revalidation rather than a successful validation.

## Bypass Cache

Send `Cache-Control: no-store` to skip the cache entirely. TAG forwards the request directly to upstream and does not cache the response.

```bash
curl -H "Cache-Control: no-store" http://localhost:8080/my-bucket/my-key
```

For CopyObject requests, `Cache-Control: no-store` or `private` also skips TAG's
post-copy cache warming. The copy still uses the normal destination invalidation
path.

## Automatic Cache Invalidation

TAG automatically invalidates cached objects when they are modified through TAG:

- **PutObject** — Cache entry deleted before forwarding the upload
- **DeleteObject** — Cache entry deleted before forwarding the delete
- **DeleteObjects** (bulk) — In proxy mode, requested cache entries are invalidated before forwarding. For cache-enabled requests with a nonempty parsed key list, TAG collects the upstream response before publishing it and waits for the post-success per-key invalidation attempts to return before completing a 2xx response.

  The post-success pass targets keys with at least one successful requested delete; keys whose requested entries all fail are not re-invalidated. An unparseable result uses the conservative all-requested-keys fallback. Cache-disabled requests, unparseable request bodies, and empty key lists keep the direct streaming path. Cache backend failures remain best-effort and may leave stale metadata. The post-success pass runs under a shared 10-second deadline: once it passes, no further per-key invalidations start, the remaining keys are left to age out via TTL, and TAG logs a warning with the fenced and skipped key counts before still returning the upstream 2xx. In tiered mode each key's own cleanup keeps its per-key timeout, so one key already in progress can extend the wait by at most that much.
- **CopyObject** — Cache entry deleted for the destination key

For a cacheable whole-object GET miss, metadata becomes visible only after its cache write consumes the broadcast stream successfully. If the cache listener is dropped or the upstream body read fails, the partial body is not published as a new entry.

Objects modified directly on Tigris (bypassing TAG) remain in cache until they expire (default TTL: 24 hours, configurable via `TAG_CACHE_TTL`) or are revalidated via `Cache-Control: no-cache`.

## Verifying Cache Behavior

Check the `X-Cache` header to verify caching:

```bash
# Using curl to see cache headers
curl -I http://localhost:8080/my-bucket/my-key \
  -H "Authorization: AWS4-HMAC-SHA256 ..."

# Response will include:
# X-Cache: HIT    (served from cache)
# X-Cache: MISS   (fetched from upstream, now cached)
```
