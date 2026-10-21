# Cache Control & Revalidation

TAG supports RFC 7234-compliant cache revalidation. Clients can control caching behavior using standard `Cache-Control` headers, and TAG reports cache status via the `X-Cache` response header.

## X-Cache Header Reference

| Value         | Meaning                                                                          |
| ------------- | -------------------------------------------------------------------------------- |
| `HIT`         | A cache entry was selected to serve the response (including revalidation that confirmed the object is unchanged) |
| `MISS`        | Not in cache, fetched from upstream and now cached                               |
| `REVALIDATED` | Revalidated with upstream, object changed, new content returned                  |
| `BYPASS`      | Cache bypassed entirely (client requested `no-store`)                            |
| `DISABLED`    | Caching is disabled server-side (`TAG_CACHE_DISABLED=true`)                      |

In block mode, a committed stream can recover missing bytes from an upstream range only after validating the cached ETag and exact range. Because `HIT` is committed before the body streams, it describes the cache-backed request, not each byte's source or whether the transfer completed. See [S3 compatibility](s3-compatibility.md#caching-behavior) for incomplete cache-range behavior.

## Force Revalidation

Send `Cache-Control: no-cache` or `Cache-Control: max-age=0` to force TAG to check with upstream before serving a cached object. TAG sends a conditional request using the cached ETag. If the object hasn't changed, upstream returns 304 and TAG serves from cache (`X-Cache: HIT`). If changed, TAG streams the new content (`X-Cache: REVALIDATED`).

```bash
# Force revalidation on GET
curl -H "Cache-Control: no-cache" http://localhost:8080/my-bucket/my-key

# Force revalidation on HEAD
curl -I -H "Cache-Control: no-cache" http://localhost:8080/my-bucket/my-key

# Force revalidation on range request
curl -H "Cache-Control: no-cache" -H "Range: bytes=0-99" \
  http://localhost:8080/my-bucket/my-key
```

If the revalidation request to upstream fails, TAG serves the stale cached copy as a fallback.

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
