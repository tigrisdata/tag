# S3 API Compatibility

TAG implements all the commonly used bucket and object APIs from the Amazon S3 REST API, acting as a caching proxy between S3 clients and Tigris object storage. By default, TAG operates in transparent proxy mode where client requests are forwarded as-is with proxy headers. In signing mode, requests are validated, re-signed, and forwarded to the Tigris upstream endpoint.

## Supported Operations

### Service

| Operation   | Method | Path | Notes |
| ----------- | ------ | ---- | ----- |
| ListBuckets | GET    | `/`  |       |

### Bucket

All bucket operations support both `/{bucket}` and `/{bucket}/` path forms for compatibility with clients that send trailing slashes.

| Operation            | Method             | Path / Query            | Notes                                                |
| -------------------- | ------------------ | ----------------------- | ---------------------------------------------------- |
| CreateBucket         | PUT                | `/{bucket}`             |                                                      |
| DeleteBucket         | DELETE             | `/{bucket}`             |                                                      |
| HeadBucket           | HEAD               | `/{bucket}`             |                                                      |
| ListObjects V1       | GET                | `/{bucket}`             |                                                      |
| ListObjects V2       | GET                | `/{bucket}?list-type=2` |                                                      |
| ListMultipartUploads | GET                | `/{bucket}?uploads`     |                                                      |
| GetBucketLocation    | GET                | `/{bucket}?location`    | Returns configured region                            |
| GetBucketVersioning  | GET / PUT          | `/{bucket}?versioning`  | Forwarded to Tigris                                  |
| GetBucketACL         | GET / PUT          | `/{bucket}?acl`         | Forwarded to Tigris                                  |
| GetBucketPolicy      | GET / PUT / DELETE | `/{bucket}?policy`      | Forwarded to Tigris                                  |
| GetBucketCORS        | GET / PUT / DELETE | `/{bucket}?cors`        | Forwarded to Tigris                                  |
| GetBucketTagging     | GET / PUT / DELETE | `/{bucket}?tagging`     | Forwarded to Tigris                                  |
| GetBucketLifecycle   | GET / PUT / DELETE | `/{bucket}?lifecycle`   | Forwarded to Tigris                                  |
| DeleteObjects        | POST               | `/{bucket}?delete`      | Bulk delete with XML body; invalidates cache entries |

### Object

| Operation           | Method | Path / Query              | Notes                                                                  |
| ------------------- | ------ | ------------------------- | ---------------------------------------------------------------------- |
| GetObject           | GET    | `/{bucket}/{key}`         | Cache-first; supports Range and conditional headers                    |
| HeadObject          | HEAD   | `/{bucket}/{key}`         | Served from cached metadata when available                             |
| PutObject           | PUT    | `/{bucket}/{key}`         | Invalidates cache before forwarding                                    |
| DeleteObject        | DELETE | `/{bucket}/{key}`         | Invalidates cache before forwarding                                    |
| CopyObject          | PUT    | `/{bucket}/{key}`         | Detected via `X-Amz-Copy-Source` header; invalidates destination cache |
| GetObjectTagging    | GET    | `/{bucket}/{key}?tagging` |                                                                        |
| PutObjectTagging    | PUT    | `/{bucket}/{key}?tagging` |                                                                        |
| DeleteObjectTagging | DELETE | `/{bucket}/{key}?tagging` |                                                                        |
| GetObjectACL        | GET    | `/{bucket}/{key}?acl`     |                                                                        |
| PutObjectACL        | PUT    | `/{bucket}/{key}?acl`     |                                                                        |

### Multipart Upload

| Operation               | Method | Path / Query                            | Notes                                      |
| ----------------------- | ------ | --------------------------------------- | ------------------------------------------ |
| InitiateMultipartUpload | POST   | `/{bucket}/{key}?uploads`               |                                            |
| UploadPart              | PUT    | `/{bucket}/{key}?uploadId=&partNumber=` |                                            |
| UploadPartCopy          | PUT    | `/{bucket}/{key}?uploadId=&partNumber=` | Detected via `X-Amz-Copy-Source` header    |
| CompleteMultipartUpload | POST   | `/{bucket}/{key}?uploadId=`             |                                            |
| AbortMultipartUpload    | DELETE | `/{bucket}/{key}?uploadId=`             |                                            |
| ListParts               | GET    | `/{bucket}/{key}?uploadId=`             |                                            |

## Response Delivery

The direct-streaming path forwards the upstream status and headers before streaming the body. If copying a response body fails after headers are sent, TAG aborts the downstream response instead of completing its framing around the received prefix. Clients may see a transport error or body-read error. TAG does not append S3 error XML after commitment.

For directly streamed PUT and DELETE operations, an upstream 2xx still triggers cache invalidation or a tiered marker when caching is enabled, even if response delivery fails. A delivery error alone does not establish that the mutation failed.

This describes the direct-streaming path. Operations that capture an upstream body for XML processing use a separate response path.

## AWS Chunked Transfer Encoding

TAG supports AWS chunked transfer encoding (streaming SigV4), used by tools like [warp](https://github.com/minio/warp) and many AWS SDKs for large uploads.

In **transparent proxy mode** (default), chunked uploads are forwarded as-is since the original signatures remain valid (the Host header is not changed).

In **signing mode**, when a client sends a PUT with `X-Amz-Content-Sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD`, the body is wrapped in an AWS-proprietary chunked format where each chunk includes a signature chained from the request-level seed signature. Because TAG re-signs requests with the upstream Tigris hostname (changing the `Host` header), these per-chunk signatures become invalid. TAG handles this by decoding the chunked body on-the-fly — stripping the chunk framing and signatures — and forwarding the raw payload as `UNSIGNED-PAYLOAD`. The decoded content length is read from the `X-Amz-Decoded-Content-Length` header. This is a streaming operation with no buffering of the full object.

## Caching Behavior

TAG adds a caching layer for read operations. The `X-Cache` response header indicates cache status:

| Value      | Meaning                                                  |
| ---------- | -------------------------------------------------------- |
| `HIT`      | A cache entry was selected to serve the request; block streams may also recover a verified upstream remainder |
| `MISS`     | Fetched from Tigris and cached                           |
| `BYPASS`   | Caching skipped (e.g., conditional request returned 304) |
| `DISABLED` | Caching is turned off                                    |

Key caching behaviors:

- **Request coalescing** — Multiple concurrent GETs for the same object are coalesced into a single upstream fetch using a broadcast/subscriber pattern. For live requests, a fetch failure before response commitment returns TAG's existing S3 error response to waiting GETs; errors after commitment terminate the stream.
- **Range requests** — If the full object is cached, range requests are served from cache. On a range cache miss, the range is served directly from Tigris while a background fetch populates the cache with the full object.
- **Incomplete cached ranges** — TAG checks that a cached range supplies the requested byte count. A committed block GET can recover an unwritten suffix only when the upstream ETag and exact range match. `X-Cache: HIT` is committed before the body streams, so it describes the cache-backed request, not per-byte provenance or successful completion. If the ETag changes, TAG rejects the suffix without appending it; the already-sent 200 status and Content-Length remain, so the client may receive only the cached prefix while the handler reports an error. A whole-object cached Range response commits its 206 when the first nonempty cache chunk arrives and cannot restart upstream; a later positive short read returns a handler error while its 206 and Content-Length remain. Client write failures are not retried upstream.
- **Conditional requests** — `If-None-Match` and `If-Modified-Since` are evaluated against cached metadata and can return 304 without hitting Tigris.
- **Write-through invalidation** — In proxy mode, PUT, DELETE, CopyObject, and DeleteObjects invalidate affected cache entries before forwarding. For cache-enabled DeleteObjects requests with a nonempty parsed key list, cancellation during proxy-mode pre-forward invalidation or the tiered prior-version lookup stops later key preparation and prevents upstream dispatch. After an upstream success, TAG waits for post-success invalidation attempts before publishing a 2xx. The detached fence uses a shared 10-second deadline; a tiered cleanup already in progress may extend the wait by its per-key timeout. A timeout is logged as a warning and the 2xx is still returned.
- **Tombstone protection** — A short-lived tombstone is written on DELETE to prevent in-flight background cache writes from resurrecting deleted objects.
- **Multipart completion replay** — A fully captured successful response is cached for five seconds when TAG can identify a caller. A replay must match the exact bucket, object key, and upload ID and pass local validation for the saved access key; transparent mode also checks the caller's current bucket authorization. Entries without a saved caller identity are treated as misses; other requests follow normal forwarding and authorization.

## Addressing Style

TAG uses **path-style** addressing only (`http://host:port/bucket/key`). Virtual-hosted style (`http://bucket.host:port/key`) is not supported.

## Authentication

All requests must be signed with AWS Signature Version 4 (header-based).

In **transparent proxy mode** (default), TAG forwards the client's original Authorization header as-is and adds proxy headers (`X-Tigris-Forwarded-Host`, etc.) so Tigris validates the signature against the original host. No local credential store is needed.

In **signing mode**, TAG validates the incoming signature against its credential store, then re-signs the request with the same credentials for the Tigris upstream endpoint. During SigV4 signing, TAG trims surrounding whitespace and collapses runs of ASCII spaces in each signed header value. This normalization affects signature calculation only; forwarded metadata retains its interior spacing and tabs.

Presigned URL authentication (`X-Amz-Algorithm` query parameter) is supported for request validation.

## Not Supported

The following S3 features are not implemented by TAG and will be forwarded as-is to Tigris or may return errors:

- Object versioning (version-specific GET/DELETE)
- Server-side encryption (SSE-C, SSE-S3, SSE-KMS)
- Object Lock / WORM retention
- POST object (browser-based HTML form uploads)
- Public Access Block configuration
- Bucket ownership controls
- Multi-range requests (single Range header only)
- Virtual-hosted style addressing
- Website hosting configuration
- Replication configuration
- Inventory, analytics, and metrics configuration
- Storage class transitions (Glacier, Intelligent Tiering)

## S3 Compatibility Tests

TAG validates compatibility using a curated subset of 214 tests from the [ceph/s3-tests](https://github.com/ceph/s3-tests) suite. See [s3-compatibility-testing.md](s3-compatibility-testing.md) for how to run them.

| Category             | Tests | Coverage                                                |
| -------------------- | ----- | ------------------------------------------------------- |
| Header validation    | 48    | Content-Type, MD5, Content-Length, dates, authorization |
| Core list operations | 55    | Prefix, delimiter, max-keys, marker, continuation       |
| Object operations    | 34    | Read, write, metadata, ETags, ranges, conditionals      |
| Bucket operations    | 33    | Create, delete, list, naming rules                      |
| Multipart uploads    | 20    | Initiate, upload, complete, abort, copy parts           |
| Copy operations      | 9     | Same-bucket, cross-bucket, metadata handling            |
| Tagging              | 15    | Object and bucket tag CRUD                              |
