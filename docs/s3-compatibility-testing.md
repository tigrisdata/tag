# S3 Compatibility Tests

TAG includes S3 compatibility tests using the upstream [ceph/s3-tests](https://github.com/ceph/s3-tests) test suite. Test files are located in `tests/s3compat/`.

## Overview

The S3 compatibility tests validate that TAG correctly implements the S3 API by running a curated subset of the ceph/s3-tests against a real Tigris backend (t3.storage.dev). This ensures end-to-end compatibility for:

- **Header validation** - Content-Type, MD5, Content-Length handling
- **Bucket operations** - Create, delete, list, naming validation
- **Object operations** - Read, write, metadata, ETags
- **Multipart uploads** - Initiate, upload parts, complete, abort
- **Copy operations** - Same bucket, cross-bucket, metadata handling

## Prerequisites

1. **AWS Credentials** - Valid credentials for Tigris (t3.storage.dev):
   ```bash
   export AWS_ACCESS_KEY_ID=<your-tigris-access-key>
   export AWS_SECRET_ACCESS_KEY=<your-tigris-secret-key>
   ```

2. **Python 3** - Required for running the ceph/s3-tests suite

3. **Go 1.21+** - Required for building TAG (local mode only)

4. **Docker** - Required for containerized testing (CI mode)

## Running Tests Locally (Recommended)

The recommended approach runs TAG on your host machine with its embedded cache. This avoids needing a GitHub token for private module access.

```bash
# 1. Set credentials
export AWS_ACCESS_KEY_ID=<your-key>
export AWS_SECRET_ACCESS_KEY=<your-secret>

# 2. Start test infrastructure (builds and runs TAG with embedded cache)
make s3-test-local

# 3. Run S3 compatibility tests
make s3-tests

# 4. Cleanup when done
make s3-test-local-down
```

### Running a Single Test

To run a specific test, pass the test path as an argument:

```bash
cd tests/s3compat
./run-tests.sh test_s3.py::test_bucket_list_empty
```

## Running Tests in Docker (CI Mode)

For CI or fully containerized testing, TAG runs in Docker with its embedded cache. This requires a GitHub token for private Go module access.

```bash
# 1. Set credentials and GitHub token
export AWS_ACCESS_KEY_ID=<your-key>
export AWS_SECRET_ACCESS_KEY=<your-secret>
export GH_TOKEN=<your-github-pat>

# 2. Start infrastructure (builds and runs TAG in Docker)
make s3-test-infra

# 3. Run tests
make s3-tests

# 4. Cleanup
make s3-test-infra-down
```

## Test Categories

The test suite is organized into categories matching the curated test list from tigris-os:

| Category | Description | Test Count |
|----------|-------------|------------|
| `test_headers` | Header validation (MD5, Content-Type, etc.) | 18 |
| `test_s3` | Core S3 list operations (prefix, delimiter) | 58 |
| `test_objects` | Object read/write/metadata | 10 |
| `test_buckets` | Bucket operations and naming rules | 17 |
| `test_multipart` | Multipart upload operations | 8 |
| `test_copy` | Object copy operations | 9 |

## Architecture

```
┌─────────────────┐     ┌─────────────────────────────┐     ┌─────────────────┐
│   s3-tests      │────▶│            TAG              │────▶│     Tigris      │
│  (ceph/pytest)  │     │   ┌─────────────────────┐   │     │ (t3.storage.dev)│
│                 │     │   │  Embedded Cache     │   │     │                 │
└─────────────────┘     │   │  (RocksDB)          │   │     └─────────────────┘
                        │   └─────────────────────┘   │
                        └─────────────────────────────┘
```

## Configuration

The test configuration is in `tests/s3compat/s3tests.conf`. Key settings:

- **Host/Port**: `localhost:8080` (TAG endpoint)
- **Protocol**: HTTP (not HTTPS)
- **Bucket prefix**: `tag-test-{random}-` to avoid conflicts

Credentials are substituted at runtime via `run-tests.sh`.

## Troubleshooting

### Tests fail with "AWS credentials not set"

Ensure both environment variables are exported:
```bash
export AWS_ACCESS_KEY_ID=<your-key>
export AWS_SECRET_ACCESS_KEY=<your-secret>
```

### Tests fail with connection errors

Verify TAG is running:
```bash
curl http://localhost:8080/health  # TAG health check
```

### Cleaning up test artifacts

Remove the cloned s3-tests repository:
```bash
make s3-tests-clean
```

## Files

| File | Description |
|------|-------------|
| `tests/s3compat/run-tests.sh` | Test runner script (clones s3-tests, runs pytest via tox) |
| `tests/s3compat/s3tests.conf` | Test configuration template |
| `tests/s3compat/docker-compose.yml` | Docker setup for TAG |
