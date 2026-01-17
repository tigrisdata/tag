# TAG Architecture

This document describes the architecture of TAG (Tigris Access Gateway), a high-performance S3-compatible caching proxy.

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              TAG Proxy                                   │
│  ┌─────────┐  ┌──────────┐  ┌─────────┐  ┌─────────┐  ┌─────────────┐  │
│  │ Handler │──│   Auth   │──│  Proxy  │──│  Cache  │──│  Forwarder  │  │
│  │ Server  │  │ Validator│  │ Service │  │ Client  │  │   (Signer)  │  │
│  └────┬────┘  └──────────┘  └────┬────┘  └────┬────┘  └──────┬──────┘  │
│       │                          │            │               │         │
└───────┼──────────────────────────┼────────────┼───────────────┼─────────┘
        │                          │            │               │
        ▼                          │            ▼               ▼
   ┌─────────┐                     │       ┌─────────┐    ┌─────────┐
   │ Clients │                     │       │ ocache  │    │ Tigris  │
   │(S3 SDKs)│                     │       │ Cluster │    │ Storage │
   └─────────┘                     │       └─────────┘    └─────────┘
                                   │
                              ┌────┴────┐
                              │Broadcast│
                              │ Manager │
                              └─────────┘
```

## Components

### Handler Server (`handlers/`)

The HTTP server that receives incoming S3 requests.

- **server.go**: HTTP server setup, routing, and middleware
- **errors.go**: S3-compatible error responses

Routes requests to appropriate handlers based on HTTP method and path:
- `GET /{bucket}/{key}` → GetObject
- `PUT /{bucket}/{key}` → PutObject
- `DELETE /{bucket}/{key}` → DeleteObject
- `HEAD /{bucket}/{key}` → HeadObject
- `GET /health` → Health check
- `GET /metrics` → Prometheus metrics

### Authentication (`auth/`)

AWS Signature Version 4 authentication and request signing.

- **credentials.go**: Credential store for access/secret key pairs
- **validator.go**: Validates incoming request signatures
- **signer.go**: Re-signs requests for upstream Tigris
- **parser.go**: Parses Authorization headers and presigned URLs

**Authentication Flow:**
```
1. Extract access key from Authorization header or query params
2. Look up secret key from credential store
3. Compute expected signature using AWS SigV4 algorithm
4. Compare with provided signature
5. If valid, re-sign request for upstream with same credentials
```

### Proxy Service (`proxy/`)

Core proxy logic including caching and request coalescing.

- **service.go**: Main request handling, cache logic, broadcast coordination
- **forwarder.go**: Forwards requests to upstream Tigris
- **broadcast/**: Streaming broadcast pattern for request coalescing

### Cache Client (`cache/`)

Interface to the external ocache cluster.

- **cache.go**: ocache client wrapper with TAG-specific methods
- **object.go**: Cached object metadata structure

**Two-Key Storage Pattern:**
```
Object "bucket/key" is stored as:
  - Metadata key: "meta:bucket/key" → JSON with headers, size, etag, etc.
  - Body key: "body:bucket/key" → Raw object bytes
```

This pattern enables:
- Efficient HEAD requests (metadata only)
- Conditional request support (If-None-Match, If-Modified-Since)
- Range requests from cached full objects

### Metrics (`metrics/`)

Prometheus metrics for monitoring and alerting.

- **metrics.go**: Metric definitions and recording functions

## Request Flows

### GET Object (Cache Hit)

```
Client                 TAG                    ocache
  │                     │                       │
  │ GET /bucket/key     │                       │
  │────────────────────▶│                       │
  │                     │ Get meta:bucket/key   │
  │                     │──────────────────────▶│
  │                     │◀──────────────────────│ metadata
  │                     │ Get body:bucket/key   │
  │                     │──────────────────────▶│
  │                     │◀──────────────────────│ body (streaming)
  │◀────────────────────│                       │
  │  200 OK + body      │                       │
  │  X-Cache: HIT       │                       │
```

### GET Object (Cache Miss)

```
Client                 TAG                    ocache              Tigris
  │                     │                       │                    │
  │ GET /bucket/key     │                       │                    │
  │────────────────────▶│                       │                    │
  │                     │ Get meta:bucket/key   │                    │
  │                     │──────────────────────▶│                    │
  │                     │◀──────────────────────│ not found          │
  │                     │                       │                    │
  │                     │ GET /bucket/key (signed)                   │
  │                     │───────────────────────────────────────────▶│
  │                     │◀───────────────────────────────────────────│
  │                     │                       │       200 OK       │
  │                     │                       │                    │
  │                     │ Put meta + body       │                    │
  │                     │──────────────────────▶│                    │
  │◀────────────────────│                       │                    │
  │  200 OK + body      │                       │                    │
  │  X-Cache: MISS      │                       │                    │
```

### Request Coalescing (Multiple Concurrent Requests)

When multiple clients request the same uncached object simultaneously:

```
Client1                TAG                              Tigris
Client2                 │                                  │
Client3                 │                                  │
  │                     │                                  │
  │ GET /bucket/key     │                                  │
  │────────────────────▶│ (becomes fetcher)                │
  │     GET /bucket/key │                                  │
  │────────────────────▶│ (joins as listener)              │
  │         GET /bucket/key                                │
  │────────────────────▶│ (joins as listener)              │
  │                     │                                  │
  │                     │ GET /bucket/key (single request) │
  │                     │─────────────────────────────────▶│
  │                     │◀─────────────────────────────────│
  │                     │       200 OK (streaming)         │
  │                     │                                  │
  │◀────────────────────│ chunk 1 (broadcast to all)       │
  │◀────────────────────│                                  │
  │◀────────────────────│                                  │
  │                     │                                  │
  │◀────────────────────│ chunk 2 (broadcast to all)       │
  │◀────────────────────│                                  │
  │◀────────────────────│                                  │
  │                     │                                  │
  │◀────────────────────│ complete                         │
  │◀────────────────────│                                  │
  │◀────────────────────│                                  │
```

**Key Points:**
- First request becomes the "fetcher" and initiates upstream request
- Subsequent requests before streaming starts join as "listeners"
- All clients receive data simultaneously as chunks arrive
- Only ONE upstream request regardless of concurrent client count
- Memory usage scales with listeners, not object size

### Range Request Caching

Range requests use a background fetch pattern for optimal ML training workloads:

```
Client                 TAG                    ocache              Tigris
  │                     │                       │                    │
  │ GET /bucket/key     │                       │                    │
  │ Range: bytes=0-1023 │                       │                    │
  │────────────────────▶│                       │                    │
  │                     │ Get meta:bucket/key   │                    │
  │                     │──────────────────────▶│                    │
  │                     │◀──────────────────────│ not found          │
  │                     │                       │                    │
  │                     │ GET /bucket/key Range: bytes=0-1023        │
  │                     │───────────────────────────────────────────▶│
  │                     │◀───────────────────────────────────────────│
  │◀────────────────────│       206 Partial                          │
  │  206 Partial        │                                            │
  │                     │                                            │
  │                     │ (Background: fetch full object)            │
  │                     │───────────────────────────────────────────▶│
  │                     │◀───────────────────────────────────────────│
  │                     │       200 OK (full object)                 │
  │                     │ Put meta + body       │                    │
  │                     │──────────────────────▶│                    │
```

**Benefits:**
- Low latency: Client gets range immediately
- Future ranges served from cache
- Background fetches are coalesced (multiple range requests = single fetch)

## Cache Storage

### Metadata Structure

```go
type CachedObjectMeta struct {
    Bucket        string            // Source bucket
    Key           string            // Object key
    StatusCode    int               // HTTP status (usually 200)
    ContentLength int64             // Object size in bytes
    ContentType   string            // MIME type
    ETag          string            // Entity tag for conditional requests
    LastModified  time.Time         // Last modification time
    CacheControl  string            // Cache-Control header
    Headers       map[string]string // Additional headers to preserve
}
```

### Cache Keys

```
Metadata key:           "meta|bucket|key"
Body key:               "body|bucket|key"
```

### Cacheability Rules

Objects are cached when:
- Response status is 200 OK
- Size is within `size_threshold` (default 1GB)
- No `Cache-Control: no-store` or `private` headers

Objects are NOT cached when:
- Response is not 200 (errors, redirects)
- Size exceeds threshold
- Cache-Control prevents caching
- Cache is disabled

## Broadcast Manager

The broadcast manager coordinates request coalescing:

```go
type Manager struct {
    active     map[string]*Broadcaster  // Active broadcasts by key
    channelBuf int                      // Buffer size per listener
}

type Broadcaster struct {
    listeners []*Listener      // Subscribed listeners
    streaming bool             // True once first chunk sent
    done      bool             // Broadcast complete
}

type Listener struct {
    ch       chan Chunk       // Chunk delivery channel
    headers  http.Header      // Response headers
    status   int              // HTTP status code
}
```

**Policies:**
- **No Late Joiners**: Once streaming starts, new requests start their own broadcast
- **Slow Consumer Handling**: Listeners with full channels are disconnected
- **Memory Bounded**: `chunkSize × channelBuffer × numListeners` per broadcast

## Error Handling

### S3 Error Responses

TAG returns S3-compatible XML error responses:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>AccessDenied</Code>
    <Message>Access Denied</Message>
    <RequestId>request-id</RequestId>
</Error>
```

### Error Mapping

| Internal Error | S3 Error Code | HTTP Status |
|----------------|---------------|-------------|
| Invalid signature | SignatureDoesNotMatch | 403 |
| Unknown access key | InvalidAccessKeyId | 403 |
| Request expired | RequestTimeTooSkewed | 403 |
| Slow consumer | InternalError | 500 |
| Upstream error | InternalError | 502 |
