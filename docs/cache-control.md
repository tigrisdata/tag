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

Send `Cache-Control: no-cache` or `Cache-Control: max-age=0` to force TAG to check with upstream before serving a cached object. TAG sends a conditional request using the cached ETag. If the object hasn't changed, upstream returns 304 and TAG serves from cache (`X-Cache: HIT`). If changed, TAG streams the new content (`X-Cache: REVALIDATED`). In proxy modes, a cacheable 200 revalidation is stored only when a follow-up HEAD confirms the same strong ETag and content length; otherwise TAG still returns the 200 body but does not cache that response.

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

In proxy modes, if TAG attempts a bulk delete but receives no upstream response,
it invalidates each requested key again because the origin may have applied the
delete before its reply was lost. An error before the HTTP request is attempted
does not trigger this second pass. A received 2xx response still preserves a
key's refill when every requested delete for that key reports `<Error>`. Tiered
mode removes local state only when its delete is confirmed successful; an error
without a response leaves it intact because the local copy may be the only copy.

Default legacy mode retains the v1.20 tombstone and plain metadata-key
operations so older TAG peers can keep reading and writing the same entries.
Current TAG writers also stamp metadata with a separate CAS generation. Current
TAG reads compare that generation before serving the body, so a late legacy
plain Put after the lost-response invalidation is not served by an updated TAG
reader. Rows written before generation tagging are treated as misses and rebuilt.
If a cache owner does not support the sidecar CAS operation, updated TAG readers
bypass the cache rather than trust unguarded metadata. A legacy miss creates a
small, TTL-bounded decision marker before fetching; if disk-cap eviction removes
it before commit, the populate is refused rather than recreating an old fence.

Older TAG readers do not check the generation sidecar. A v1.20 reader uses only
the plain metadata key, and its writer checks the timestamp tombstone before a
separate plain metadata Put. A writer that passed that check can publish after
a later delete, so an older reader can still serve a late plain Put during a
mixed-version rollout. This check-to-Put limit exists in the v1.20 protocol;
a newer TAG binary cannot change an older reader. The legacy setting preserves
wire compatibility for rollouts, not fleet-wide stale-read freedom while
unmodified v1.20 readers remain. The stale-refill guarantee here applies to
TAG readers running this source revision, not to unmodified v1.20 readers.
CAS mode fences metadata tokens atomically and should be enabled only after
every TAG peer in the cluster supports it.

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
