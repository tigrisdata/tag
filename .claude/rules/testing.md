# Testing Patterns & Best Practices

## Test Environment Factories

Use dedicated `TestEnvironment` factories for different modes:
- `NewTestEnvironmentWithTransparentAuth`: Sets up `ProxySigner`, `DerivedKeyStore`, `RequestValidator`, `KeyUnwrapper`, `AuthzCache`

## Mocking Upstream

Use `httptest.NewServer` with custom `http.HandlerFunc` to simulate Tigris responses:
- e.g., `newSigningKeysUpstreamHandler` to inject `X-Tigris-Proxy-Signing-Keys`

## Header Inspection

Use raw `http.NewRequest` / `http.Client.Do` for verifying proxy behavior with specific HTTP headers (like `X-Cache` or internal headers). AWS SDK clients abstract these details.

## Synchronization

Never use `time.Sleep` for synchronization in concurrent tests. Use:
- `sync.WaitGroup`
- Channels
- `time.After` with polling

## Test Organization

Related S3 compatibility tests grouped under `tests/s3compat/`:
- `tests/s3compat/python/` — Python s3-tests (ceph)
- `tests/s3compat/sdk/` — Go SDK tests

## Integration Test Coverage Checklist

Cover these scenarios for transparent proxy:
- Happy path: key learning, local validation, cache hit
- Auth failures: unknown key, unauthenticated request → correct forwarding/rejection
- AuthZ revocation (Tigris 403 → cache entry removed)
- Internal header stripping (even on errors or cache hits)
- Multi-bucket auth enforcement (access to bucket A ≠ access to bucket B)

## Local Cluster Testing

New features involving cluster communication should have dedicated Makefile targets to spin up local multi-node clusters (e.g., `s3-test-local-cluster`) for running compatibility tests against a multi-node setup.

## Configuration Override Tests

Always add explicit tests for env var overrides (e.g., `TestHTTPPort_OverrideByEnv`, `TestGRPCAuth_UnrecognizedValueKeepsEnabled`) to validate configuration parsing logic.

## Incremental Verification

When updating complex dependencies or configuration logic, build and test after each change rather than batching all changes together.

## Cache Libraries

Prefer `hashicorp/golang-lru/v2/expirable` for bounded TTL caches over custom map implementations with manual cleanup logic.
