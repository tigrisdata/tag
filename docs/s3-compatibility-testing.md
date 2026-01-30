# S3 Compatibility Tests

TAG includes S3 compatibility tests using the upstream [ceph/s3-tests](https://github.com/ceph/s3-tests) test suite. Test files are located in `tests/s3compat/`.

## Overview

The S3 compatibility tests validate that TAG correctly implements the S3 API by running a curated subset of the ceph/s3-tests against a real Tigris backend (t3.storage.dev). This ensures end-to-end compatibility for:

- **Header validation** - Content-Type, MD5, Content-Length, authorization, date handling
- **Bucket operations** - Create, delete, list, naming validation, anonymous access
- **Object operations** - Read, write, metadata, ETags, range requests, conditional operations
- **Multipart uploads** - Initiate, upload parts, complete, abort, copy parts
- **Copy operations** - Same bucket, cross-bucket, metadata handling
- **Tagging** - Object and bucket tag operations

## Prerequisites

1. **AWS Credentials** - Valid credentials for Tigris (t3.storage.dev):

   ```bash
   export AWS_ACCESS_KEY_ID=<your-tigris-access-key>
   export AWS_SECRET_ACCESS_KEY=<your-tigris-secret-key>
   ```

2. **Python 3** - Required for running the ceph/s3-tests suite

3. **Go 1.21+** - Required for building TAG (local mode only)

4. **System dependencies** - RocksDB compression libraries (run `make install-deps`)

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

## Test Categories

The test suite is organized into categories based on the [ceph/s3-tests](https://github.com/ceph/s3-tests) test suite:

| Category         | Description                                                        | Test Count |
| ---------------- | ------------------------------------------------------------------ | ---------- |
| `test_headers`   | Header validation (MD5, Content-Type, authorization, dates)        | 48         |
| `test_s3`        | Core S3 list operations (prefix, delimiter, maxkeys)               | 55         |
| `test_objects`   | Object read/write/metadata, range requests, conditional operations | 34         |
| `test_buckets`   | Bucket operations, naming rules, anonymous access                  | 33         |
| `test_multipart` | Multipart upload, copy, error handling                             | 20         |
| `test_copy`      | Object copy operations                                             | 9          |
| `test_tagging`   | Object and bucket tagging operations                               | 15         |

**Total: 214 tests**

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

| File                                | Description                                               |
| ----------------------------------- | --------------------------------------------------------- |
| `tests/s3compat/run-tests.sh`       | Test runner script (clones s3-tests, runs pytest via tox) |
| `tests/s3compat/s3tests.conf`       | Test configuration template                               |
| `tests/s3compat/docker-compose.yml` | Docker setup for TAG                                      |

## Future Improvements

The following test categories from ceph/s3-tests are not currently enabled but could be added in the future:

| Category            | Description                                                    | Available Tests |
| ------------------- | -------------------------------------------------------------- | --------------- |
| Versioning          | Object versioning, delete markers, version listing             | ~25 tests       |
| SSE-C               | Server-side encryption with customer-provided keys             | ~17 tests       |
| SSE-S3              | Server-side encryption with S3-managed keys                    | ~16 tests       |
| SSE-KMS             | Server-side encryption with KMS                                | ~18 tests       |
| POST Object         | Browser-based uploads via HTML forms                           | ~30 tests       |
| Bucket Policy       | JSON-based access policies                                     | ~20 tests       |
| Object Lock         | WORM (Write Once Read Many) protection                         | ~30 tests       |
| Lifecycle           | Automatic object expiration and transitions                    | ~15 tests       |
| ACL                 | Access Control Lists for buckets and objects                   | ~18 tests       |
| Public Access Block | Block public access settings                                   | ~8 tests        |
| Bucket Ownership    | Object ownership controls                                      | ~6 tests        |
| CORS                | Cross-Origin Resource Sharing configuration and presigned URLs | ~10 tests       |
| Object Attributes   | GetObjectAttributes API and multipart part retrieval           | ~8 tests        |

**Note:** Some tests in ceph/s3-tests are marked with `@pytest.mark.fails_on_aws` indicating they test Ceph RGW-specific behavior that differs from AWS S3. These tests are intentionally excluded from our compatibility suite.
