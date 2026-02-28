# Cache Control & Revalidation

TAG supports RFC 7234-compliant cache revalidation. Clients can control caching behavior using standard `Cache-Control` headers, and TAG reports cache status via the `X-Cache` response header.

## X-Cache Header Reference

| Value         | Meaning                                                                          |
| ------------- | -------------------------------------------------------------------------------- |
| `HIT`         | Served from cache (includes revalidation that confirmed the object is unchanged) |
| `MISS`        | Not in cache, fetched from upstream and now cached                               |
| `REVALIDATED` | Revalidated with upstream, object changed, new content returned                  |
| `BYPASS`      | Cache bypassed entirely (client requested `no-store`)                            |
| `DISABLED`    | Caching is disabled server-side (`TAG_CACHE_DISABLED=true`)                      |

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

## Automatic Cache Invalidation

TAG automatically invalidates cached objects when they are modified through TAG:

- **PutObject** — Cache entry deleted before forwarding the upload
- **DeleteObject** — Cache entry deleted before forwarding the delete
- **DeleteObjects** (bulk) — Cache entries deleted for all keys in the request
- **CopyObject** — Cache entry deleted for the destination key

Objects modified directly on Tigris (bypassing TAG) remain in cache until they expire (default TTL: 60 minutes) or are revalidated via `Cache-Control: no-cache`.

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
