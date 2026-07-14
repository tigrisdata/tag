# Security & Access Control

This document describes TAG's authentication, authorization, and security architecture.

## Authentication Modes

TAG supports two authentication modes, controlled by the `transparent_proxy` configuration setting.

### Transparent Proxy (Default)

Clients sign requests with their own Tigris credentials. TAG forwards the request as-is, preserving the original `Authorization` header, and adds four proxy headers so Tigris can validate the client's signature against the original host:

- `X-Tigris-Forwarded-Host` - The original Host header from the client
- `X-Tigris-Proxy-Access-Key` - TAG's own access key
- `X-Tigris-Proxy-Timestamp` - Unix timestamp for the proxy signature
- `X-Tigris-Proxy-Signature` - HMAC-SHA256 signature proving TAG's identity

Tigris independently validates both the client's SigV4 signature and TAG's proxy signature. TAG's access key must belong to the same Tigris organization as the client's access key.

### Signing Mode

In signing mode, TAG terminates the client signature and re-issues the upstream one itself:

1. The client signs its request with its own SigV4 credentials.
2. TAG looks up the secret for the client's access key in its **local credential store** and cryptographically validates the incoming signature.
3. TAG re-signs the (possibly transformed) request for the upstream endpoint using standard AWS SigV4 with **the same access key and secret**, then streams it upstream.

TAG re-signs rather than forwarding the original signature because it may transform the request — for example decoding AWS chunked transfer encoding to `UNSIGNED-PAYLOAD` — which would otherwise invalidate the client's signature. The upstream sees the **same identity** as the client; this is not identity translation.

Because TAG must know the secret for every access key it serves, those credentials must be present in its local credential store. In production the store is populated only from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (a single pair), so **clients must authenticate with those same credentials**, and TAG re-signs upstream with them. If the store is empty, TAG rejects all requests.

Because it re-signs with standard SigV4, signing mode works against any S3-compatible service, not only Tigris (see [Endpoint Validation](#endpoint-validation)).

Set `TAG_TRANSPARENT_PROXY=false` or `upstream.transparent_proxy: false` in YAML to enable signing mode.

### Credential Handling by Mode

| Aspect | Transparent proxy (default) | Signing mode |
| --- | --- | --- |
| Upstream request signature | Client's original signature, forwarded unchanged | Re-signed by TAG with the client's own credentials |
| Does TAG need client secret keys? | No — cache hits are validated locally using signing keys learned from Tigris | Yes — the secret for each client access key must be in TAG's local store |
| Role of `AWS_*` credentials | TAG's own identity: signs `X-Tigris-Proxy-*` headers and background cache fetches (read-only) | The credential store's contents: clients present it and TAG re-signs every request (reads **and** writes) with it |
| Client credentials | Any credentials in the same Tigris org; secrets never stored | Must match a credential in TAG's store (by default the `AWS_*` pair) |
| Works with non-Tigris backends | No (Tigris only) | Yes (any S3-compatible service) |

## Local Authentication

In transparent proxy mode, TAG implements local SigV4 validation to serve cache hits without contacting Tigris on every request. This is enabled automatically when transparent proxy mode is active.

### How It Works

**First request (key learning):**

```text
Client                     TAG                          Tigris
  │                         │                             │
  │  GET /bucket/key        │                             │
  │  Authorization: ...     │                             │
  │────────────────────────▶│                             │
  │                         │  Forward + proxy headers    │
  │                         │────────────────────────────▶│
  │                         │◀────────────────────────────│
  │                         │  200 OK                     │
  │                         │  X-Tigris-Proxy-Signing-Keys│
  │                         │                             │
  │                         │  (unwrap & store keys)      │
  │                         │  (grant authz cache entry)  │
  │◀────────────────────────│                             │
  │  200 OK (key header     │                             │
  │  stripped from response)│                             │
```

**Subsequent requests (local validation):**

```text
Client                     TAG                          Tigris
  │                         │                             │
  │  GET /bucket/key        │                             │
  │  Authorization: ...     │                             │
  │────────────────────────▶│                             │
  │                         │  Validate SigV4 locally     │
  │                         │  Check authz cache          │
  │                         │  → AuthValidated            │
  │                         │                             │
  │                         │  Serve from cache           │
  │◀────────────────────────│                             │
  │  200 OK                 │                             │
  │  X-Cache: HIT           │                             │
```

### Components

| Component          | Purpose                                      | Capacity     | TTL                   |
| ------------------ | -------------------------------------------- | ------------ | --------------------- |
| `DerivedKeyStore`  | Pre-derived SigV4 signing keys               | 100K entries | 48 hours              |
| `AuthzCache`       | Per-bucket authorization decisions           | 1M entries   | 10 min (configurable) |
| `KeyUnwrapper`     | Decrypts signing keys from Tigris responses  | N/A          | N/A                   |
| `RequestValidator` | Validates SigV4 signatures using stored keys | N/A          | N/A                   |

**DerivedKeyStore** stores pre-derived SigV4 signing keys received from Tigris, indexed by `accessKey|date|region`. The 48-hour TTL covers today and yesterday to handle SigV4 clock skew tolerance.

**AuthzCache** tracks per-bucket authorization decisions, indexed by `accessKey|bucket`. Authorization for one bucket does not grant access to others. Configurable via `TAG_AUTHZ_CACHE_TTL` (default: 10 minutes).

**KeyUnwrapper** decrypts the `X-Tigris-Proxy-Signing-Keys` response header using AES-256-GCM with `SHA256(proxy_secret_key)` as the encryption key. The client's access key is used as Additional Authenticated Data (AAD) to prevent ciphertext transplant attacks between access keys.

**RequestValidator** performs cryptographic SigV4 validation using stored signing keys. Uses constant-time comparison (HMAC-Equal) to prevent timing attacks. Enforces a 15-minute clock skew tolerance.

## Access Control Flow

```text
Request arrives at TAG
    │
    ├─ No Authorization header (anonymous)
    │   └─ Forward to Tigris → Tigris decides (e.g., public bucket access)
    │
    ├─ Malformed Authorization header
    │   └─ Reject with 4xx error
    │
    ├─ Access key not in DerivedKeyStore (unknown key)
    │   └─ Forward to Tigris → learn keys on successful response
    │
    ├─ SigV4 signature mismatch
    │   └─ Forward to Tigris → re-learn keys if signature was stale
    │
    ├─ AuthzCache miss or expired
    │   └─ Forward to Tigris → re-authorize on successful response
    │
    └─ Valid signature + valid authz cache entry
        └─ AuthValidated → serve from cache if available
```

## Authorization Lifecycle

Authorization decisions are cached per `(accessKey, bucket)` pair:

| Event                                | Action                                    |
| ------------------------------------ | ----------------------------------------- |
| Tigris returns 2xx with signing keys | `AuthzCache.Grant(accessKey, bucket)`     |
| Tigris returns 403                   | `AuthzCache.Revoke(accessKey, bucket)`    |
| TTL expires (10 min default)         | Entry removed, next request re-authorizes |

Authorization is strictly per-bucket. A client may have access to some buckets but not others, and TAG enforces this at the cache level.

## Proxy Header Security

### Preventing Client Injection

Clients cannot forge proxy headers because TAG always overwrites them:

**Transparent mode**: TAG uses `Header.Set()` to overwrite any client-supplied proxy header values with TAG's own computed values. Even if a client sends `X-Tigris-Proxy-Signature`, it is replaced.

**Signing mode**: TAG strips all `X-Tigris-Proxy-*` and `X-Tigris-Forwarded-Host` headers from client requests entirely before forwarding.

### Proxy Signature Computation

TAG computes the proxy signature using its own secret key:

```text
canonical_string = forwarded_host + "\n" + timestamp + "\n" + method + "\n" + path
signature = HMAC-SHA256(TAG_secret_key, canonical_string)
```

Only TAG (and Tigris, which knows TAG's key) can produce a valid proxy signature.

## Internal Header Stripping

The `X-Tigris-Proxy-Signing-Keys` response header contains encrypted signing keys intended only for TAG. This header is **always stripped** from responses before they reach the client:

- Stripped in `ResponseInterceptor` before headers are written to the client
- Stripped regardless of whether local auth is enabled or disabled
- Stripped regardless of response status (success or error)
- Stripped regardless of forwarding mode (transparent or signing)

## Endpoint Validation

TAG validates the upstream endpoint at startup. In **every mode**, the endpoint must be a well-formed absolute `http://` or `https://` URL with a host; anything else (a hostless value like `s3.amazonaws.com`, a `host:port` string, a non-http scheme) is a fatal startup error.

**Transparent proxy mode** additionally restricts the host to the Tigris allowlist below, because the `X-Tigris-Proxy-*` identity headers TAG adds are only meaningful to Tigris:

| Pattern         | Example                          | Use Case                  |
| --------------- | -------------------------------- | ------------------------- |
| `localhost`     | `http://localhost:8080`          | Development and testing   |
| `*.tigris.dev`  | `https://fly.storage.tigris.dev` | Tigris production domains |
| `*.storage.dev` | `https://t3.storage.dev`         | Tigris storage domains    |

Any other host causes a fatal startup error in transparent mode.

**Signing mode** (`transparent_proxy: false`) does not apply the Tigris allowlist — it re-signs with standard SigV4 and can front any S3-compatible service. The upstream is a single, operator-configured value; TAG forwards only to that endpoint and never lets clients choose the upstream, so this is not an open proxy. The SSRF surface is limited to operator misconfiguration of `upstream.endpoint`, so treat that value as trusted configuration and set it explicitly. TAG logs a warning at startup when signing mode targets a non-Tigris host; third-party backends are community-supported.

## Credential Requirements

TAG loads credentials from environment variables at startup:

```bash
export AWS_ACCESS_KEY_ID=<access key>
export AWS_SECRET_ACCESS_KEY=<secret key>
```

How they are used, and what permissions they need, depends on the mode (see [Credential Handling by Mode](#credential-handling-by-mode)).

**Transparent proxy mode:** these are TAG's *own* Tigris credentials, used only to sign the `X-Tigris-Proxy-*` identity headers and to perform background cache fetches (fetching full objects after a range-request cache miss). Because TAG never re-signs client requests in this mode, **read-only** access to the cached buckets is sufficient. TAG's access key must belong to the same Tigris organization as client access keys; clients use their own credentials directly and TAG does not store their secret keys.

**Signing mode:** this pair populates TAG's local credential store — clients must authenticate with it, and TAG re-signs *every* forwarded request with it. Its permissions must therefore cover whatever operations clients perform: **read-only is not sufficient if clients issue writes** (PUT/DELETE) — grant the access needed for those operations. Set `upstream.region` to match the backend (the default `auto` is Tigris-specific); see the "Using TAG with other S3-compatible services" section in [configuration.md](configuration.md).

## Error Mapping

| Auth Error         | S3 Error Code         | HTTP Status | Action            |
| ------------------ | --------------------- | ----------- | ----------------- |
| Signature mismatch | SignatureDoesNotMatch | 403         | Forward to Tigris |
| Unknown access key | InvalidAccessKeyId    | 403         | Forward to Tigris |
| Expired request    | RequestTimeTooSkewed  | 403         | Forward to Tigris |
| Malformed auth     | MalformedAuth         | 400         | Reject at TAG     |
| Missing auth       | (none)                | (none)      | Forward to Tigris |
