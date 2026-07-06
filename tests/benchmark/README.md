# TAG Warp Benchmarks

Throughput/latency benchmarks for TAG's core S3 operations using
[MinIO `warp`](https://github.com/minio/warp). They run `warp` against a local
TAG instance (which proxies to Tigris) and exercise:

All operations run at concurrency 64.

| Operation | warp command                  | Object size                                  |
| --------- | ----------------------------- | -------------------------------------------- |
| GET       | `warp get`                    | 4 MiB                                        |
| GET RANGE | `warp get --range-size=64KiB` | 256 MiB object, 64 KiB ranges (SlateDB-like) |
| PUT       | `warp put`                    | 4 MiB                                        |
| HEAD      | `warp stat`                   | 4 MiB                                        |
| LIST V2   | `warp list`                   | 4 MiB                                        |

This is a **smoke benchmark**: the run fails if any operation errors. It does not
enforce performance thresholds — it captures numbers for inspection.

## Running locally

Requires Tigris credentials (TAG's own read-only creds, used for signing):

```bash
export AWS_ACCESS_KEY_ID=<your-key>
export AWS_SECRET_ACCESS_KEY=<your-secret>

make s3-test-local      # build + start TAG on :8080 (proxies to Tigris)
make bench-warp         # install warp (first run), run all 5 ops, write results/
make s3-test-local-down # stop TAG + cleanup
```

`make bench-warp-clean` removes the cached warp binary (`.bin/`) and `results/`.

## Results

Per operation, `results/` gets:

- `<op>.log` — full warp run output
- `<op>.csv.zst` — raw benchmark data (re-analyzable with `warp analyze`)
- `<op>.analyze.txt` — readable throughput / ops-s / latency-percentile summary

In CI the `results/` directory is uploaded as the `benchmark-results` artifact.

## Tuning

`run-warp.sh` reads env vars. Defaults run at moderate concurrency (64) and
shape the GET RANGE case to the SlateDB-fronting pattern (small ranges from
large objects); object sizes stay modest so bandwidth against real upstream
stays bounded. Override for heavier local runs:

| Var                                                              | Default                  | Meaning                                          |
| ---------------------------------------------------------------- | ------------------------ | ------------------------------------------------ |
| `WARP_VERSION`                                                   | `v1.5.0`                 | warp version installed via `go install`          |
| `WARP_HOST`                                                      | `localhost:8080`         | TAG host:port                                    |
| `WARP_BUCKET`                                                    | `tag-warp-benchmark`     | bucket for benchmark data (cleared each run)     |
| `WARP_REGION`                                                    | `auto`                   | SigV4 region (must match TAG's region)           |
| `WARP_DURATION`                                                  | `30s`                    | duration per operation                           |
| `WARP_CONCURRENT`                                                | `64`                     | concurrent operations                            |
| `WARP_OBJ_SIZE` / `WARP_OBJECTS`                                 | `4MiB` / `100`           | size / count for 4 MiB ops                       |
| `WARP_RANGE_OBJ_SIZE` / `WARP_RANGE_SIZE` / `WARP_RANGE_OBJECTS` | `256MiB` / `64KiB` / `8` | GET RANGE large object size / range size / count |

Example, a heavier local run:

```bash
WARP_DURATION=2m WARP_CONCURRENT=16 WARP_OBJECTS=500 make bench-warp
```

## CI

The `.github/workflows/benchmark.yaml` job runs nightly (and on demand via
**workflow_dispatch**). It starts TAG, runs `make bench-warp`, and uploads the
`results/` directory. It is intentionally **not** run on every PR.
