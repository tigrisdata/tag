# TAG Benchmarks

Benchmark results for a single TAG instance serving cached object reads. A single TAG node saturates a 100 Gbps NIC at ~85+ Gbps for objects 1 MiB and larger, delivers ~75K ops/sec for small objects, and maintains low single-digit millisecond TTFB — all while using around 12% of available CPU.

## Test Environment

Benchmarks were run on Amazon EC2 using [go-ycsb](https://github.com/pingcap/go-ycsb) and [warp](https://github.com/minio/warp).

| Role             | Instance      | vCPUs | Memory  | Storage   | Network  |
| ---------------- | ------------- | ----- | ------- | --------- | -------- |
| Benchmark client | c6in.16xlarge | 64    | 128 GiB | -         | 100 Gbps |
| TAG server       | i3en.24xlarge | 96    | 768 GiB | 60 TB SSD | 100 Gbps |

TAG was run as a single instance via `deploy/native/run.sh`. During the benchmarks, TAG CPU usage stayed under 1200% and memory usage remained around 24 GB, leaving significant headroom on the server.

## Results — warp

Benchmarks were run using [warp](https://github.com/minio/warp) on the same test environment. Each test ran GET operations for ~30 minutes.

| Object Size | Threads | OPS    | Throughput | p50 (ms) | p99 (ms) | TTFB p50 (ms) |
| ----------- | ------- | ------ | ---------- | -------- | -------- | ------------- |
| 1 KiB       | 16      | 38,724 | 37.8 MiB/s | 0.3      | 6.5      | < 1           |
| 1 KiB       | 32      | 58,535 | 57.2 MiB/s | 0.3      | 2.0      | < 1           |
| 1 KiB       | 64      | 75,717 | 73.9 MiB/s | 0.4      | 3.2      | < 1           |
| 100 KiB     | 16      | 19,543 | 1.9 GiB/s  | 0.6      | 10.7     | < 1           |
| 100 KiB     | 32      | 28,594 | 2.7 GiB/s  | 0.7      | 5.1      | 1             |
| 100 KiB     | 64      | 33,350 | 3.2 GiB/s  | 1.3      | 5.8      | 1             |
| 1 MiB       | 16      | 7,224  | 7.1 GiB/s  | 5.8      | 20.2     | 4             |
| 1 MiB       | 32      | 10,313 | 10.1 GiB/s | 2.9      | 12.7     | 1             |
| 1 MiB       | 64      | 10,955 | 10.7 GiB/s | 5.3      | 17.5     | 1             |
| 4 MiB       | 16      | 1,984  | 7.8 GiB/s  | 13.2     | 59.7     | 6             |
| 4 MiB       | 32      | 2,686  | 10.5 GiB/s | 10.3     | 38.1     | 1             |
| 4 MiB       | 64      | 2,775  | 10.8 GiB/s | 18.6     | 102.3    | 1             |

## Results — go-ycsb

Benchmarks were run using [go-ycsb](https://github.com/pingcap/go-ycsb) on the same test environment.

| Object Size | Threads | OPS    | p50 (us) | p99 (us) |
| ----------- | ------- | ------ | -------- | -------- |
| 1 KB        | 16      | 34,117 | 380      | 1,024    |
| 1 KB        | 32      | 47,743 | 484      | 2,275    |
| 1 KB        | 64      | 55,231 | 744      | 4,443    |
| 100 KB      | 16      | 7,906  | 1,842    | 4,015    |
| 100 KB      | 32      | 8,697  | 3,389    | 9,775    |
| 100 KB      | 64      | 9,726  | 4,411    | 25,999   |
| 1 MB        | 16      | 2,981  | 4,891    | 8,431    |
| 1 MB        | 32      | 4,816  | 6,523    | 11,223   |
| 1 MB        | 64      | 6,255  | 9,655    | 19,727   |

## Key Observations

- **1 KiB objects**: ~75K ops/sec at 64 threads with sub-millisecond p50.
- **100 KiB objects**: ~3.2 GiB/s at 64 threads (~26 Gbps).
- **1 MiB objects**: ~10.7 GiB/s (~86 Gbps) at 64 threads, saturating the 100 Gbps EC2 network link at 32+ threads.
- **4 MiB objects**: ~10.5 GiB/s (~84 Gbps) at 32 threads, saturating the 100 Gbps EC2 network link.
- TTFB remains sub-millisecond for small objects (1 KiB, 100 KiB) and stays in the low single-digit milliseconds for objects up to 4 MiB.
- Throughput scales with thread count for small objects and saturates the 100 Gbps NIC at ~85 Gbps for 1 MiB+ objects.

## Limitations

For 1 MiB+ objects, throughput is NIC-bound at ~85 Gbps — the practical maximum on a 100 Gbps EC2 link. For smaller objects (1 KiB, 100 KiB), throughput is client-bound; multiple benchmark clients would be needed to determine TAG's actual ceiling. A single go-ycsb instance does not scale well past ~20 Gbps and struggles with object sizes above 1 MB.
