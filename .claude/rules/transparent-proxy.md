# Transparent Proxy Architecture & Auth Patterns

## Forwarder Strategy Pattern

`RequestForwarder` interface with three implementations:
- `baseForwarder`: Shared HTTP execution, response streaming, cache interactions
- `signingForwarder`: Validates client sigs, re-signs for upstream
- `transparentForwarder`: Preserves client signatures, adds proxy headers

`NewForwarder()` factory selects based on config. Compile-time interface satisfaction checks.

## Configuration

- Transparent mode is default (`TAG_TRANSPARENT_PROXY` / `upstream.transparent_proxy`)
- `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` are TAG's own Tigris credentials with read-only access for all buckets (background cache fetches). Missing in transparent mode = fatal startup error.
- Local auth enabled implicitly when transparent proxy is active
- Endpoint validation: enforced only in transparent proxy mode (proxy HMAC headers are Tigris-specific) — only `localhost`, `*.tigris.dev`, `*.storage.dev` allowed, startup fatal on mismatch. Signing mode re-signs with standard SigV4 and permits any S3-compatible endpoint (`config.IsTigrisEndpoint` gates this; non-Tigris signing endpoints log a startup warning).

## Proxy Headers

TAG adds four proxy headers using HMAC-SHA256 over `forwarded_host\ntimestamp\nmethod\npath`:
- `X-Tigris-Forwarded-Host`: Original Host header from client
- `X-Tigris-Proxy-Access-Key`: TAG's access key
- `X-Tigris-Proxy-Timestamp`: Unix timestamp
- `X-Tigris-Proxy-Signature`: HMAC signature proving TAG's identity

## Header Security

- `r.Header.Clone()` to prevent mutation of original request headers
- `Header.Set()` to overwrite any client-injected proxy header values
- In signing mode, strip `X-Tigris-Proxy-*` and `X-Tigris-Forwarded-Host` from client requests
- `X-Tigris-Proxy-Signing-Keys` stripped from responses before reaching client — always, regardless of mode or error state

## Authentication Flow

- `AuthResult` enum: `AuthValidated`/`AuthNotValidated` + error (`nil` = forward to Tigris, non-nil = reject)
- Missing auth (anonymous) → `ErrMissingAuth` → forward to upstream (may be public bucket)
- Malformed auth → reject with error
- Unknown key / invalid sig → forward to Tigris (learns keys on response)
- Valid sig + valid authz → `AuthValidated` → serve from cache

## Per-Bucket Authorization

- `AuthzCache` keyed on `(accessKey, bucket)`. Auth for one bucket does not grant access to others.
- Granted when Tigris returns 2xx with signing keys
- Revoked when Tigris returns 403
- TTL: 10 minutes (configurable via `TAG_AUTHZ_CACHE_TTL`)

## Auth Parsing

- `auth/parser.go` provides `ExtractAccessKey()` and `ParseAuthInfo()` for SigV4 headers and query params
- SigV4 date parsing must handle `time.RFC1123Z` in addition to `RFC1123` and `TimeFormat`

## Security Configuration Parsing

- **Fail-closed**: Boolean security env vars (e.g., `TAG_CACHE_GRPC_AUTH`) must only disable on explicit "false" or "0". Any unrecognized value (typos, "True", "yes") defaults to enabled.
- **Unconditional auth**: If the gRPC server is exposed and authentication is enabled, apply auth unconditionally regardless of deployment mode (single-node vs cluster). Disabling must be explicit.
- **Missing credentials**: Missing AWS credentials when gRPC auth is enabled → `log.Fatal()` at startup.

## In-Memory Caches

- `AuthzCache`: Expirable LRU with TTL, per-bucket auth decisions
- `DerivedKeyStore`: Expirable LRU with TTL, pre-derived SigV4 signing keys indexed by `accessKey|date|region`
