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

TAG validates incoming request signatures against a local credential store, then re-signs requests for the upstream endpoint using standard AWS SigV4. This mode is used when TAG needs to perform credential translation (e.g., clients use different credentials than the upstream account). Because it re-signs with standard SigV4, signing mode works against any S3-compatible service, not only Tigris (see [Endpoint Validation](#endpoint-validation)).

Set `TAG_TRANSPARENT_PROXY=false` or `upstream.transparent_proxy: false` in YAML to enable signing mode.

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

TAG requires its own Tigris credentials via environment variables:

```bash
export AWS_ACCESS_KEY_ID=<TAG's access key>
export AWS_SECRET_ACCESS_KEY=<TAG's secret key>
```

These credentials must have **read-only access** to all buckets accessed through TAG. This is required for:

- Signing proxy headers (transparent mode)
- Background cache fetches (e.g., fetching full objects after a range request cache miss)
- Re-signing requests for upstream (signing mode)

In transparent proxy mode, TAG's access key must belong to the same Tigris organization as client access keys. Clients use their own credentials directly — TAG does not need or store client secret keys.

In signing mode, these are the credentials for the configured upstream — the Tigris account, or the third-party S3 account when running against another service. Set `upstream.region` to match that backend (the default `auto` is Tigris-specific); see the "Using TAG with other S3-compatible services" section in [configuration.md](configuration.md).

## Error Mapping

| Auth Error         | S3 Error Code         | HTTP Status | Action            |
| ------------------ | --------------------- | ----------- | ----------------- |
| Signature mismatch | SignatureDoesNotMatch | 403         | Forward to Tigris |
| Unknown access key | InvalidAccessKeyId    | 403         | Forward to Tigris |
| Expired request    | RequestTimeTooSkewed  | 403         | Forward to Tigris |
| Malformed auth     | MalformedAuth         | 400         | Reject at TAG     |
| Missing auth       | (none)                | (none)      | Forward to Tigris |
