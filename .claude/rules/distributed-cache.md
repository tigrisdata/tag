# Distributed Cache Considerations

## Embedded OCache Architecture

TAG uses embedded OCache for local storage:

- Each TAG node runs its own embedded cache
- No external cache server required
- Optional clustering for multi-node deployments

## Cluster Mode

When running multiple TAG nodes:

- **Discovery**: Memberlist gossip protocol (default port 7000)
- **Routing**: gRPC-based cache key routing (default port 9000)
- **Hashing**: Consistent hashing distributes keys across nodes
- **Local/Remote**: Requests for keys owned by other nodes forwarded via gRPC

## Fenced CAS Pattern for Cache Invalidation

Prevents stale async cache writes after invalidation (ocache v1.13.0, ocache#267):

```
1. Writer reads its DECISION-TIME token: GetMetaWithVersion — absent reads
   return a nonzero absence token; test found, never version==0
2. DELETE/invalidation → fenced CAS delete (DeleteIfVersion; on a live key the
   mismatch reports the current version for the retry) — bumps the fence
3. Writer commits with PutMetaIfVersion(expected = its token)
4. Any delete or write after the token → version mismatch → commit refused
5. Fences are retained by the store (fence-retention, default 6h)
```

Guarantees hold within the CAS op family only: never mix plain Put/Delete with
CAS ops on a managed key. On a lost commit, refetch before retrying — never
retry the same bytes with a fresh token.

## Coordination Modes (metaCoordinator)

Meta invalidation/populate ordering is a strategy selected once at construction
(`cache/coordinator.go`), never per-call if/else:

- **Legacy (default)**: `cache.legacy_coordination: true` — the pre-v1.21
  tombstone mechanism (plain Get/Put/Delete + timestamp tombstones). Safe for
  any cluster mix, including nodes upgrading straight from ≤v1.20.
- **CAS**: `legacy_coordination: false` — the fenced CAS pattern above.
  Requires every node in the cluster to be CAS-capable (≥v1.21) BEFORE the
  flip; flip via config + a brisk rolling restart (mixed-mechanism window is
  bounded by the restart).

The proxy layer is mode-blind: it carries an opaque uint64 token (store version
in CAS mode, wall-clock stamp in legacy mode) from decision to commit. Never
branch on the mode outside the coordinator implementations.

## Stream Multiplexing > Batching

For distributed caches, prefer stream multiplexing over explicit batching:

- Batching adds complexity: must route keys to correct nodes
- Stream multiplexing: one persistent stream per node connection
- Each request goes directly to the right stream
- No cross-node coordination needed

## Error Handling for Streaming RPCs

Robust error handling is critical:

- Detect stream disconnections promptly
- Fail pending requests immediately on disconnect
- Clear reconnection strategy
- Don't leave requests hanging indefinitely

## Common Mistakes to Avoid

### Ignoring Distributed Nature in Batching

`BatchGet` seems simple but is complex in distributed environments. Consider node routing before implementing.

### Underestimating Latency from Explicit Batching

While beneficial for throughput, explicit batching (waiting for timeout or N items) adds visible latency. Prefer zero-latency approaches.

### Missing Decision-Time Tokens in Async Writers

Background cache writers must capture their version token BEFORE fetching and
commit under it — a token read at commit time can postdate a delete and
resurrect stale data.

## Best Practices

- **Phased implementation**: Break optimization into phases (streaming, then batching, then OS-specific)
- **Industry pattern research**: Study how etcd, Redis handle similar problems
- **Focus on identified bottlenecks**: Target the specific issue (e.g., 42% syscalls)
