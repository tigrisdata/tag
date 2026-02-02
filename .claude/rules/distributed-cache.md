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

## Tombstone Pattern for Cache Invalidation

Prevents stale async cache writes after invalidation:

```
1. DELETE arrives → Write tombstone with timestamp
2. Delete meta and body keys
3. Async cache writer checks tombstone before writing metadata
4. If tombstone timestamp > write start time → skip metadata write
5. Tombstones expire after 60 seconds (short TTL)
```

This ensures in-flight background cache writes don't resurrect deleted objects.

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

### Missing Tombstone Checks in Async Writers

Background cache writers must check for tombstones before finalizing writes to prevent stale data.

## Best Practices

- **Phased implementation**: Break optimization into phases (streaming, then batching, then OS-specific)
- **Industry pattern research**: Study how etcd, Redis handle similar problems
- **Focus on identified bottlenecks**: Target the specific issue (e.g., 42% syscalls)
