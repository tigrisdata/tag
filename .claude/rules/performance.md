# Performance Optimization Patterns

## sync.Pool for Buffer Reuse

Reduce GC pressure by reusing buffers:

```go
var bufferPool = sync.Pool{
    New: func() interface{} {
        return new(bytes.Buffer)
    },
}

// Usage
buf := bufferPool.Get().(*bytes.Buffer)
buf.Reset()
defer bufferPool.Put(buf)
```

## Zero Added Latency Preference

Avoid explicit batching delays:

- Explicit batching (waiting for timeout or N items) adds latency
- Prefer implicit pipelining via HTTP/2 layer
- Let the transport handle batching naturally
