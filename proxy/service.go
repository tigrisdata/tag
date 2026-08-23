package proxy

import (
	"context"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/metrics"
	"github.com/tigrisdata/tag/proxy/broadcast"
)

const (
	// X-Cache header constants for indicating cache status.
	XCacheHeader      = "X-Cache"
	XCacheHit         = "HIT"
	XCacheMiss        = "MISS"
	XCacheBypass      = "BYPASS"      // Cache-Control: no-store bypassed cache
	XCacheDisabled    = "DISABLED"    // Cache is disabled
	XCacheRevalidated = "REVALIDATED" // Object revalidated with upstream
)

// writeCacheStatus sets the X-Cache response header and records the matching
// hit/miss counter, keeping the header and tag_cache_{hits,misses}_total in
// lockstep. Call it once per response, where the status is committed — never pair
// it with a separate RecordCacheHit/RecordCacheMiss for the same response.
//
// HIT increments hits; MISS increments misses. REVALIDATED, BYPASS, and DISABLED
// set the header but are neither hits nor misses: REVALIDATED (object changed on
// upstream) is tracked by the tag_revalidations_* metrics, and a bypassed/disabled
// request made no cache decision. Range-from-cache hits additionally call
// RecordRangeFromCacheHit for the specialized counter.
func writeCacheStatus(w http.ResponseWriter, status string) {
	w.Header().Set(XCacheHeader, status)
	switch status {
	case XCacheHit:
		metrics.RecordCacheHit()
	case XCacheMiss:
		metrics.RecordCacheMiss()
	}
}

// cacheMissStatus returns the X-Cache status for a request not served from cache:
// DISABLED when caching is off, BYPASS when the client opted out (Cache-Control:
// no-store), otherwise MISS. Only MISS counts as a cache miss.
func (s *Service) cacheMissStatus(bypassCache bool) string {
	switch {
	case !s.cache.IsEnabled():
		return XCacheDisabled
	case bypassCache:
		return XCacheBypass
	default:
		return XCacheMiss
	}
}

// Service provides the core caching proxy logic.
type Service struct {
	forwarder               RequestForwarder
	cache                   *cache.Cache
	config                  *config.Config
	cacheSemaphore          chan struct{}               // Count ceiling on concurrent cache-populate ops (nil = unlimited)
	populateBudget          *byteBudget                 // Byte budget bounding all cache buffering — populate + block-serve staging (nil = unlimited)
	perPopulateCap          int64                       // Max bytes a single populate can buffer (reservation ceiling)
	broadcastManager        *broadcast.Manager          // For streaming request coalescing
	activeBackgroundFetches sync.Map                    // Dedup for background full-object fetches (range caching)
	blockFetchMu            sync.Mutex                  // Guards blockFetches
	blockFetches            map[string]*blockFetchState // Coalesce block fetches while a detached remote write is pending
	// recentFooterWork suppresses repeat footer scans for an object version that was
	// already examined. Without it every tail read of a fully-warmed object re-probes
	// its metadata blocks, which in cluster mode are mostly remote.
	recentFooterWork *expirable.LRU[string, struct{}]
	// origin answers the questions that depend on having an upstream to fall back
	// to. See origin.go — it is policy, not I/O; the I/O is behind RequestForwarder.
	origin Origin
}

// revalidationRequested reports whether the client asked for revalidation AND
// this deployment can actually perform it.
//
// Both revalidation paths end at an upstream: the conditional-GET path directly,
// and the block-mode path by falling through to the miss handler. With no origin
// the fall-through would turn a client's Cache-Control: no-cache into a 404 for an
// object TAG is holding perfectly good bytes for. Ignoring the header is the
// correct degradation.
func (s *Service) revalidationRequested(r *http.Request) bool {
	return shouldForceRevalidate(r) && s.originPolicy().CanRevalidate()
}

// cacheBypassRequested reports whether the client asked to skip the cache AND this
// deployment can serve the request some other way.
//
// Cache-Control: no-store asks for a response that did not come from cache. With
// no upstream there is no other source, so honoring it turns a cached object into
// a 404 — the same failure revalidationRequested prevents, arriving by a different
// header. Nothing is being served stale by ignoring it: with no origin, the cached
// copy is the only copy.
func (s *Service) cacheBypassRequested(r *http.Request) bool {
	return shouldBypassCache(r) && s.originPolicy().CanFill()
}

// originPolicy returns the configured Origin, falling back to proxy behaviour when
// unset. NewService always sets it; the fallback covers Services built as literals
// (tests do this in a dozen places), where defaulting to "there is an upstream"
// keeps them describing the mode they were written for.
func (s *Service) originPolicy() Origin {
	if s.origin == nil {
		return proxyOrigin{}
	}
	return s.origin
}

// NewService creates a new proxy service.
func NewService(forwarder RequestForwarder, cache *cache.Cache, cfg *config.Config) *Service {
	channelBuf := cfg.Broadcast.ChannelBuffer
	if channelBuf <= 0 {
		channelBuf = broadcast.DefaultChannelBuffer
	}

	// Concurrent cache-populate operations are bounded two independent ways, and a
	// populate must pass both:
	//   1. A hard COUNT ceiling (MaxConcurrentWrites) — caps total in-flight populates.
	//   2. A BYTE budget (MaxPopulateMemoryBytes) — caps aggregate buffered memory.
	// The byte budget is what prevents the OOM failure mode: each populate reserves
	// min(object size, per-populate buffer ceiling), so many small objects populate
	// concurrently while a burst of large objects is throttled to keep buffered
	// memory bounded (a byte-unaware count alone can pin many GB — e.g. 256 large
	// populates × ~64–80MB ≈ 16–20GB).
	var cacheSem chan struct{}
	if cfg.Cache.MaxConcurrentWrites > 0 {
		cacheSem = make(chan struct{}, cfg.Cache.MaxConcurrentWrites)
	}

	perPopulateCap := perPopulateBufferBytes(cfg)

	var populateBudget *byteBudget
	// stagingCap is the share of the budget block-serve staging may hold; logged below.
	var stagingCap int64
	if cfg.Cache.MaxPopulateMemoryBytes > 0 {
		total := cfg.Cache.MaxPopulateMemoryBytes
		// Reserve a share of the budget for warm-on-write only when it's enabled;
		// otherwise there's no warm path to protect.
		reserveFraction := 0.0
		if cfg.Cache.WarmOnWrite {
			reserveFraction = cfg.Cache.WarmOnWriteReservedFraction
		}
		populateBudget = newByteBudget(total, reserveFraction, perPopulateCap)
		// Block-serve staging buffers (pre-commit range/first-block assembly, the streaming
		// pipeline's bounded prefetch window) draw from this SAME budget — so max_populate_memory_bytes is
		// an honest total, not a hidden 2x — but are capped at HALF of it. A warm multi-block
		// serve holds its staging bytes for the whole (possibly multi-second) response, and at
		// high concurrency those long holds would crowd cold-miss populates out of a shared,
		// uncapped budget (objects served but never cached, working set stays cold — a ~3x
		// warm-GET collapse; see #141). The half cap guarantees populates a constant floor of
		// total/2; staging still uses that whole half when populates are idle, and populates use
		// the whole budget when nothing is staging. The 2 GiB default dwarfs any realistic block,
		// so a staging buffer always fits; only a misconfigured budget of a few blocks or fewer
		// makes a buffer exceed the cap, and then that serve simply takes a degraded path
		// (probe-first / sequential / uncached remainder) — never a failed request.
		stagingCap = total / 2
		populateBudget.stagingCap = stagingCap
	}

	// The block buffer pool's retention cap must track the configured block size, or every
	// block staging buffer in a larger-block deployment would be allocated fresh and dropped.
	if bs := cfg.Cache.BlockSize; bs > maxPooledBlockBufBytes.Load() {
		maxPooledBlockBufBytes.Store(bs)
	}

	log.Info().
		Int("max_concurrent_writes", cfg.Cache.MaxConcurrentWrites).
		Int64("max_populate_memory_bytes", cfg.Cache.MaxPopulateMemoryBytes).
		Int64("per_populate_buffer_cap_bytes", perPopulateCap).
		Int64("serve_staging_cap_bytes", stagingCap).
		Msg("Cache-populate limits configured")

	svc := &Service{
		forwarder:        forwarder,
		cache:            cache,
		config:           cfg,
		cacheSemaphore:   cacheSem,
		populateBudget:   populateBudget,
		perPopulateCap:   perPopulateCap,
		broadcastManager: broadcast.NewManager(channelBuf),
		blockFetches:     make(map[string]*blockFetchState),
		origin:           originFor(cfg.Upstream.Endpoint),
	}
	// Allocated only when the feature that uses it is on.
	if cfg.Cache.ParquetOptimization {
		svc.recentFooterWork = expirable.NewLRU[string, struct{}](maxFooterWorkTracking, nil, footerWorkCooldown)
	}
	return svc
}

// perPopulateBufferBytes returns the maximum bytes a single cache-populate can
// buffer: the broadcast listener channel (channel_buffer chunks) plus the
// cache-write queue in setupCacheListener (channel_buffer/4 chunks, floored at 64).
// Each queued chunk retains at least a pooled DefaultChunkSize backing array
// (broadcast.GetChunkBuf pools DefaultChunkSize buffers), so a smaller configured
// chunk_size still holds that much per chunk — charge the larger of the two. This
// is the ceiling a populate ever reserves; smaller objects reserve their actual size.
func perPopulateBufferBytes(cfg *config.Config) int64 {
	chunkSize := int64(cfg.Broadcast.ChunkSize)
	if chunkSize < broadcast.DefaultChunkSize {
		chunkSize = broadcast.DefaultChunkSize
	}
	channelBuf := int64(cfg.Broadcast.ChannelBuffer)
	if channelBuf <= 0 {
		channelBuf = broadcast.DefaultChannelBuffer
	}
	queue := channelBuf / 4
	if queue < 64 {
		queue = 64 // matches setupCacheListener's cacheQueueSize floor
	}
	return (channelBuf + queue) * chunkSize
}

// populateWeight is the byte weight a populate reserves against the memory budget:
// the object's size, capped at the per-populate buffer ceiling (a populate never
// buffers more than the pipeline can hold). Unknown/negative sizes (e.g. chunked
// responses) reserve the full ceiling. It never exceeds the total budget, so an
// object larger than the whole budget can still populate one-at-a-time. Always ≥ 1.
func (s *Service) populateWeight(contentLength int64) int64 {
	w := s.perPopulateCap
	if contentLength > 0 && contentLength < w {
		w = contentLength
	}
	if w < 1 {
		w = 1
	}
	if budget := s.config.Cache.MaxPopulateMemoryBytes; budget > 0 && w > budget {
		w = budget
	}
	return w
}

const (
	// maxFooterWorkTracking bounds the set of recently scanned object versions.
	maxFooterWorkTracking = 65536

	// footerWorkCooldown is how long a footer scan is suppressed for one object
	// version after it completes. Short enough that an evicted footer is re-warmed
	// promptly, long enough that a hot object is not re-scanned per read.
	footerWorkCooldown = 5 * time.Minute
)

// stagingWeight is the byte weight a block-serve staging buffer reserves against the
// serve-staging budget: the buffer's ACTUAL size, clamped only by the total budget (so a
// buffer larger than the whole budget still serves one-at-a-time, mirroring populateWeight).
// Unlike populateWeight it is NOT capped at perPopulateCap — that ceiling models the
// broadcast pipeline's buffering, which has no relationship to raw block-sized staging
// buffers; clamping to it would let the budget admit more bytes than are actually allocated
// (e.g. 32 64 MiB pipeline buffers reserved as one 80 MiB cap), silently breaking the very
// bound the budget exists to enforce.
func (s *Service) stagingWeight(n int64) int64 {
	if n < 1 {
		n = 1
	}
	if budget := s.config.Cache.MaxPopulateMemoryBytes; budget > 0 && n > budget {
		n = budget
	}
	return n
}

// populatePriority classifies a cache-populate for admission against the byte
// budget. warmWrite populates (cache-warm-on-write, which pre-cache data about to
// be read) get a reserved, prioritized slice of the budget so the read-miss
// full-object warm flood can't starve them; readMiss populates use the rest.

type populatePriority int

const (
	priorityReadMiss populatePriority = iota
	priorityWarmWrite
)

func (p populatePriority) metricSource() string {
	if p == priorityWarmWrite {
		return metrics.PopulateSourceWarmOnWrite
	}
	return metrics.PopulateSourceReadMiss
}

// warmPopulateAcquireTimeout bounds how long a warm-on-write populate waits for
// budget before giving up (and being shed like any other populate). With the
// reservation active this wait is normally sub-second; the cap just prevents a warm
// goroutine from holding a slot indefinitely if the budget is disabled-reservation
// and permanently contended.
const warmPopulateAcquireTimeout = 30 * time.Second

// byteBudget is a priority-aware weighted semaphore bounding the aggregate bytes
// reserved by concurrent cache-populate operations. Read-miss populates acquire
// non-blocking and leave headroom for pending warm-on-write demand (elastic: they
// use the whole budget when no warm-on-write is pending, and back off only by what
// pending warm-on-writes need, capped at reserveCap). Warm-on-write populates wait
// (bounded) for budget and, by registering their pending demand, get priority as
// read-miss populates release — so warm-on-write is never starved by the read-miss
// flood while no budget is wasted.
type byteBudget struct {
	mu          sync.Mutex
	cond        *sync.Cond // signaled on release so waiting warm-on-write acquirers wake
	remaining   int64
	pendingWarm int64 // sum of weights of warm-on-write acquirers currently waiting
	reserveCap  int64 // max bytes read-miss/staging will hold back for pending warm-on-write
	// Block-serve staging buffers draw from this SAME budget (one honest total) but are bounded
	// to stagingCap, guaranteeing populates a floor of (remaining not below total-stagingCap) so
	// long-held serve buffers can never starve cold-miss populates (the #141 isolation). Staging
	// is unrestricted (stagingCap == total) unless a caller sets a smaller cap.
	stagingInUse int64 // bytes currently held by block-serve staging buffers
	stagingCap   int64 // max bytes staging may hold at once
}

// newByteBudget creates a budget of `total` bytes. reserveFraction (clamped to
// [0,1]) caps the share read-miss populates will yield to pending warm-on-write; 0
// disables the reservation (equal competition). When the reservation is enabled,
// reserveCap is floored at perPopulateCap (the largest weight a single populate
// reserves) so one warm-on-write populate can always fit in the reserve — otherwise
// a fraction*total smaller than a single populate would let read-miss oscillate just
// below the warm's required weight and starve it under a small budget.
func newByteBudget(total int64, reserveFraction float64, perPopulateCap int64) *byteBudget {
	// NaN is unordered, so it slips past the < / > clamps below; converting
	// total*NaN to int64 yields a garbage cap. Disable the reservation in that case.
	if math.IsNaN(reserveFraction) || reserveFraction < 0 {
		reserveFraction = 0
	} else if reserveFraction > 1 {
		reserveFraction = 1
	}
	cap := int64(float64(total) * reserveFraction)
	if reserveFraction > 0 && cap < perPopulateCap {
		cap = perPopulateCap
	}
	if cap > total {
		cap = total
	}
	// Default: staging may use the whole budget (no extra cap). Callers that run block-serve
	// staging (NewService) set a smaller stagingCap to guarantee populates a floor.
	b := &byteBudget{remaining: total, reserveCap: cap, stagingCap: total}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// tryAcquireReadMiss reserves n bytes for a read-miss populate without blocking. It
// succeeds only if it leaves room for what pending warm-on-write populates need
// (min(pendingWarm, reserveCap)), protecting warm-on-write, while using the whole
// budget when nothing is pending.
func (b *byteBudget) tryAcquireReadMiss(n int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	reserve := b.pendingWarm
	if reserve > b.reserveCap {
		reserve = b.reserveCap
	}
	if b.remaining-n < reserve {
		return false
	}
	b.remaining -= n
	return true
}

// acquireWarm reserves n bytes for a warm-on-write populate, waiting (bounded by
// ctx) for budget and taking priority over read-miss populates: while it waits it
// registers pendingWarm so read-miss acquirers yield, then parks on the condition
// variable until a release wakes it (or ctx is cancelled). Returns false only on
// cancellation.
func (b *byteBudget) acquireWarm(ctx context.Context, n int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pendingWarm += n
	defer func() { b.pendingWarm -= n }()

	// Wake this waiter if ctx is cancelled while it's parked in cond.Wait() — the
	// condition variable itself has no timeout. stop() cancels the hook once we're
	// done (whether we acquired or gave up).
	stop := context.AfterFunc(ctx, func() {
		b.mu.Lock()
		b.cond.Broadcast()
		b.mu.Unlock()
	})
	defer stop()

	for b.remaining < n {
		if ctx.Err() != nil {
			return false
		}
		b.cond.Wait()
	}
	b.remaining -= n
	return true
}

func (b *byteBudget) release(n int64) {
	b.mu.Lock()
	b.remaining += n
	b.cond.Broadcast() // wake warm-on-write waiters to re-check the freed budget
	b.mu.Unlock()
}

// tryAcquireStaging reserves n bytes for a block-serve staging buffer without blocking. Staging
// draws from the same pool as populates, so the configured budget is an honest total, but is
// additionally bounded to stagingCap — this guarantees populates a floor (the budget can't drop
// below total-stagingCap through staging alone), so long-held serve buffers never starve
// cold-miss populates (without this isolation, warm-GET throughput collapsed ~3x; see #141). Like
// read-miss it also leaves room for pending warm-on-write, so warm is never starved regardless of
// how stagingCap and the warm reserve are configured. Pair a true return with releaseStaging(n).
func (b *byteBudget) tryAcquireStaging(n int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stagingInUse+n > b.stagingCap {
		return false
	}
	reserve := b.pendingWarm
	if reserve > b.reserveCap {
		reserve = b.reserveCap
	}
	if b.remaining-n < reserve {
		return false
	}
	b.remaining -= n
	b.stagingInUse += n
	return true
}

// releaseStaging returns n staging bytes to the pool. It MUST pair with a successful
// tryAcquireStaging(n): using the plain release() would leave stagingInUse elevated, permanently
// shrinking the effective staging cap until restart.
func (b *byteBudget) releaseStaging(n int64) {
	b.mu.Lock()
	b.remaining += n
	b.stagingInUse -= n
	b.cond.Broadcast() // wake warm-on-write waiters to re-check the freed budget
	b.mu.Unlock()
}

// acquireCacheSlot tries to reserve a cache-populate slot without blocking,
// reserving `weight` bytes against the memory budget. It returns true (and must be
// paired with releaseCacheSlot passing the SAME weight) when both the count slot
// and the byte budget are reserved, or when a limiter is disabled (nil). It returns
// false when either the count limit or the memory budget is saturated, in which
// case the caller should skip caching rather than block or spawn unbounded work.
func (s *Service) acquireCacheSlot(ctx context.Context, weight int64, prio populatePriority) bool {
	// Warm-on-write may block waiting for its reserved budget, so it reserves the
	// byte budget FIRST and takes the count slot only once budget is secured.
	// Reversing this (count-then-blocking-budget) would let a waiting warm hold a
	// count slot for the whole wait, shedding read-miss populates that have budget
	// headroom and exhausting MaxConcurrentWrites under a burst of warms.
	if prio == priorityWarmWrite {
		if s.populateBudget != nil && !s.populateBudget.acquireWarm(ctx, weight) {
			return false
		}
		// The count slot is shared with read-miss populates, so protecting only the
		// byte budget still lets a read-miss flood that fills every slot shed a warm
		// that already holds its reserved bytes. Wait for a slot too, bounded by ctx
		// (the caller's warm-acquire deadline); free the held budget if it never comes.
		if s.cacheSemaphore != nil {
			select {
			case s.cacheSemaphore <- struct{}{}:
			case <-ctx.Done():
				if s.populateBudget != nil {
					s.populateBudget.release(weight) // hand back budget we can't use
				}
				return false
			}
		}
		return true
	}

	// Read-miss acquires non-blocking: count slot then byte budget.
	if s.cacheSemaphore != nil {
		select {
		case s.cacheSemaphore <- struct{}{}:
		default:
			return false
		}
	}
	if s.populateBudget != nil && !s.populateBudget.tryAcquireReadMiss(weight) {
		if s.cacheSemaphore != nil {
			<-s.cacheSemaphore // hand back the count slot we just took
		}
		return false
	}
	return true
}

// releaseCacheSlot releases a slot reserved by acquireCacheSlot. `weight` must
// match the value passed to the paired acquireCacheSlot.
func (s *Service) releaseCacheSlot(weight int64) {
	if s.populateBudget != nil {
		s.populateBudget.release(weight)
	}
	if s.cacheSemaphore != nil {
		<-s.cacheSemaphore
	}
}

// statusRecorder wraps http.ResponseWriter to capture the response status code
// written while forwarding an upstream response. Forward() returns nil even when
// upstream responds 4xx/5xx (the response streamed successfully), so mutating
// handlers use this to gate post-forward cache re-invalidation on an actual 2xx —
// otherwise a rejected PUT/DELETE/COPY would still tombstone the destination and
// discard a valid racing refill, causing later reads to miss unnecessarily.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (rec *statusRecorder) WriteHeader(code int) {
	rec.status = code
	rec.ResponseWriter.WriteHeader(code)
}

func (rec *statusRecorder) Write(b []byte) (int, error) {
	if rec.status == 0 {
		rec.status = http.StatusOK
	}
	return rec.ResponseWriter.Write(b)
}

// Flush delegates to the underlying writer when it supports flushing, so wrapping
// never disables streaming for handlers that rely on http.Flusher.
func (rec *statusRecorder) Flush() {
	if f, ok := rec.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// wroteSuccess reports whether upstream returned a 2xx status.
func (rec *statusRecorder) wroteSuccess() bool {
	return rec.status >= 200 && rec.status < 300
}

// HandlePutObject handles PUT requests for objects.
// Invalidates cache BEFORE forwarding to ensure consistency.
func (s *Service) HandlePutObject(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	bucket, key := ParseBucketKey(r)

	log.Debug().Str("bucket", bucket).Str("key", key).Msg("HandlePutObject")

	// Invalidate cache BEFORE forwarding to ensure consistency
	// This prevents stale data from being served if forwarding succeeds but cache invalidation fails
	s.invalidateObject(context.Background(), bucket, key)

	// Forward to Tigris, recording the upstream status. When eligible, forwardPutMaybeTee
	// tees the decoded body so we can populate the cache directly (write-through) instead of
	// a read-back warm-on-write GET; teed is non-nil only when a tee was attempted.
	// requestRejectsCache marks a no-store/private PUT that was plain-forwarded before tee
	// admission and must not start a read-back warm after a successful write.
	rec := &statusRecorder{ResponseWriter: w}
	teed, requestRejectsCache, err := s.forwardPutMaybeTee(r.Context(), rec, r, bucket, key)

	// Re-invalidate AFTER upstream confirms the write. A GET that raced the
	// in-flight PUT may have fetched the pre-PUT object and begun re-caching it;
	// this second invalidation writes a tombstone newer than that write's start
	// time, so the tombstone-aware cache write skips the stale repopulation —
	// restoring read-after-write semantics.
	// Gated on a 2xx: a rejected PUT leaves the object unchanged, so re-invalidating
	// would only discard a valid racing refill and cause an unnecessary later miss.
	// Routed through invalidateObject (like the pre-forward call) so a failure of this
	// read-after-write-critical invalidation is recorded and logged, not discarded.
	if err == nil && rec.wroteSuccess() && s.cache.IsEnabled() {
		s.invalidateObject(context.Background(), bucket, key)
		teeHandled := requestRejectsCache
		if teed != nil {
			// writeThroughCache takes ownership of the reserved populate budget.
			teeHandled = s.writeThroughCache(bucket, key, teed)
			teed = nil
		}
		if !teeHandled {
			s.warmOnWrite(r, bucket, key)
		}
		// Independent of warmOnWrite: that caches whole objects and is off here,
		// while this caches only the metadata region (RFC 0002).
		s.warmParquetFooterOnWrite(r, bucket, key)
	}
	// Forward errored or upstream rejected the write: nothing cached, so release any budget
	// reserved for the tee.
	if teed != nil {
		s.releaseCacheSlot(teed.weight)
	}

	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.RecordRequest("PutObject", status, time.Since(start).Seconds())

	return err
}

// HandleDeleteObject handles DELETE requests for objects.
// Invalidates cache BEFORE forwarding to ensure consistency.
func (s *Service) HandleDeleteObject(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	bucket, key := ParseBucketKey(r)

	log.Debug().Str("bucket", bucket).Str("key", key).Msg("HandleDeleteObject")

	// Invalidate cache BEFORE forwarding to ensure consistency
	// This prevents stale data from being served if forwarding succeeds but cache invalidation fails
	s.invalidateObject(context.Background(), bucket, key)

	// Forward to upstream, recording the upstream status.
	rec := &statusRecorder{ResponseWriter: w}
	err := s.forwarder.Forward(r.Context(), rec, r)

	// Re-invalidate AFTER upstream confirms the delete, for the same
	// read-after-write reason as HandlePutObject: a GET racing the in-flight
	// DELETE may have re-cached the not-yet-deleted object; this second
	// tombstone blocks that stale repopulation.
	// Gated on a 2xx: a rejected DELETE leaves the object present, so re-invalidating
	// would only discard a valid racing refill and cause an unnecessary later miss.
	// Routed through invalidateObject (like the pre-forward call) so a failure of this
	// read-after-write-critical invalidation is recorded and logged, not discarded.
	if err == nil && rec.wroteSuccess() && s.cache.IsEnabled() {
		s.invalidateObject(context.Background(), bucket, key)
	}

	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.RecordRequest("DeleteObject", status, time.Since(start).Seconds())

	return err
}

// HandleHeadObject handles HEAD requests for objects.
// Serves from cached metadata when available (no body fetch needed).
// Supports cache revalidation via Cache-Control: no-cache/max-age=0.
func (s *Service) HandleHeadObject(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	ctx := r.Context()
	bucket, key := ParseBucketKey(r)
	forceRevalidate := s.revalidationRequested(r)
	bypassCache := s.cacheBypassRequested(r)

	log.Debug().Str("bucket", bucket).Str("key", key).Msg("HandleHeadObject")

	// Validate credentials before serving from cache
	result, accessKey, secretKey, err := s.forwarder.ValidateAndGetCredentials(r)
	if err != nil {
		metrics.RecordRequest("HeadObject", "auth_error", time.Since(start).Seconds())
		return err
	}

	// Try to serve from cached metadata (no body needed)
	// Anonymous requests can also read from cache if the object's ACL is public-read.
	isAnonymous := isAnonymousRequest(r, result, err)

	if (result == AuthValidated || isAnonymous) && !bypassCache && s.cache.IsEnabled() {
		meta, found, cacheErr := s.cache.GetMeta(ctx, bucket, key)
		cacheHit := cacheErr == nil && found && meta != nil
		if cacheHit && isAnonymous && !meta.IsPublicRead() {
			cacheHit = false
			log.Debug().Str("bucket", bucket).Str("key", key).Msg("Skipping HEAD cache for anonymous request - object not public")
		}
		if cacheHit {
			// Client-triggered revalidation: Cache-Control: no-cache/max-age=0
			if forceRevalidate && meta.ETag != "" {
				log.Debug().Str("bucket", bucket).Str("key", key).Msg("HEAD cache hit requires revalidation (client-triggered)")
				return s.revalidateAndServeHead(ctx, w, bucket, key, accessKey, secretKey, meta, start)
			}

			if !forceRevalidate {
				log.Debug().Str("bucket", bucket).Str("key", key).Msg("HEAD served from cache")
				meta.WriteHeaders(w)
				writeCacheStatus(w, XCacheHit)
				w.WriteHeader(meta.StatusCode)
				metrics.RecordRequest("HeadObject", "success", time.Since(start).Seconds())
				return nil
			}
			// forceRevalidate but no ETag — fall through to upstream
		}
	}

	// Cache miss - forward to upstream. Set the X-Cache header (and the matching
	// hit/miss counter) before forwarding; a disabled/bypassed request is neither.
	if !s.cache.IsEnabled() {
		writeCacheStatus(w, XCacheDisabled)
	} else if bypassCache {
		writeCacheStatus(w, XCacheBypass)
	} else {
		writeCacheStatus(w, XCacheMiss)
	}
	err = s.forwarder.Forward(ctx, w, r)

	status := "success"
	if err != nil {
		status = "error"
	}
	metrics.RecordRequest("HeadObject", status, time.Since(start).Seconds())
	return err
}

// HandleCopyObject handles copy object requests.
// Invalidates cache BEFORE forwarding to ensure consistency.
func (s *Service) HandleCopyObject(w http.ResponseWriter, r *http.Request) error {
	bucket, key := ParseBucketKey(r)

	log.Debug().Str("bucket", bucket).Str("key", key).Msg("HandleCopyObject")

	// Invalidate cache for destination object BEFORE forwarding to ensure consistency
	// This prevents stale data from being served if forwarding succeeds but cache invalidation fails
	if s.cache.IsEnabled() {
		s.invalidateObject(context.Background(), bucket, key)
	}

	// Forward to upstream, capturing the response so we can confirm the copy
	// actually succeeded. CopyObject signals failure either with a non-2xx status
	// or — famously — a 200 OK carrying an <Error> body, and Forward would report
	// neither.
	capture, err := s.forwarder.ForwardWithCapture(r.Context(), w, r)

	// Re-invalidate the destination AFTER upstream confirms the copy, for the same
	// read-after-write reason as HandlePutObject: a GET racing the in-flight copy
	// may have re-cached the pre-copy destination object; this second tombstone
	// blocks that stale repopulation.
	// Gated on a confirmed-successful copy: a rejected copy leaves the destination
	// unchanged, so re-invalidating would only discard a valid racing refill.
	if err == nil && s3WriteSucceeded(capture) && s.cache.IsEnabled() {
		s.invalidateObject(context.Background(), bucket, key)
		s.warmOnWrite(r, bucket, key)
		s.warmParquetFooterOnWrite(r, bucket, key)
	}

	return err
}

// s3WriteSucceeded reports whether a captured mutation response indicates the write
// actually happened: a 2xx status whose body is not an S3 <Error> document. Both
// CopyObject and CompleteMultipartUpload can return 200 OK with an error body for an
// operation that failed mid-flight, so status alone is not enough.
//
// It deliberately does NOT gate on capture.Complete. The two ways to be wrong are
// not symmetric: treating a failed write as successful only re-invalidates an object
// that didn't change (an extra cache miss), while treating a successful write as
// failed skips the invalidation and can leave a racing refill of the OLD object
// cached — serving stale data. So an unconfirmable outcome must be assumed
// successful. An incomplete capture still detects a failure whenever the <Error>
// root element was captured (isS3ErrorBody only reads the first element); only a body
// truncated before that root reads as success, which is the safe side.
func s3WriteSucceeded(capture *ResponseCapture) bool {
	if capture == nil || capture.StatusCode < 200 || capture.StatusCode >= 300 {
		return false
	}
	return !isS3ErrorBody(capture.Body)
}

// invalidateObject removes an object's cached metadata (writing a tombstone) and
// records the true outcome of the attempt. A failed backend invalidation is recorded
// as an error rather than success: a false-green delete metric would hide the very
// read-after-write hazard the invalidation exists to prevent, since the stale entry
// is still in place. It is a no-op when the cache is disabled.
func (s *Service) invalidateObject(ctx context.Context, bucket, key string) {
	if !s.cache.IsEnabled() {
		return
	}
	if err := s.cache.Delete(ctx, bucket, key); err != nil {
		metrics.RecordCacheOperation("delete", "error")
		log.Debug().Err(err).Str("bucket", bucket).Str("key", key).Msg("Cache invalidation failed")
		return
	}
	metrics.RecordCacheOperation("delete", "success")
}

// warmOnWrite repopulates the cache after a successful write by triggering a
// background full-object fetch, so a read soon after the write hits cache
// (cache-warm-on-write; see cache.warm_on_write). It is best-effort and fully
// detached: triggerBackgroundCacheFetch deduplicates concurrent warms, sheds under
// the populate byte budget, and stamps its own writeStartTime before the GET so an
// invalidation racing the warm is provably newer and blocks it. Safe to call on
// every successful write.
//
// Credentials and public-read handling depend on how the write was authenticated,
// because a write proves nothing about who may READ the object:
//
//   - Authenticated write (anonymous=false): warm with the write's credentials
//     (TAG's own in transparent mode, the client's in signing mode) via a signed
//     fetch, which is never marked public-read. The cached entry serves authenticated
//     reads; a signing-mode write-only client's warm will fail, exactly as its own
//     read would.
//   - Anonymous write (anonymous=true): warm with an UNSIGNED fetch. A successful
//     anonymous write only proves public-WRITE, so we must not infer public-read from
//     it. Instead the anonymous fetch itself is the probe: upstream returns 200 only if
//     the object is genuinely publicly READABLE (then it is cached public-read, so
//     anonymous reads hit) and 403 otherwise (nothing is cached, so a private object in
//     a public-write bucket is never exposed). This mirrors the read path, which
//     likewise caches public-read only after a successful anonymous read.
//
// Best-effort caveat: warms are keyed by bucket/key for dedup, so if any fetch for
// this key is already in flight — a concurrent read-path warm, or the warm from a
// rapid prior write to the same key — this warm coalesces into that one and is
// dropped. When it coalesces into a fetch that predates this write, that fetch's own
// populate is tombstone-blocked (its writeStartTime is older than this write's
// invalidation), so it writes nothing either: the key is simply left absent, not
// left stale. The next read then misses and inline-populates the current object.
// This can never serve a stale object — the same tombstone that blocks the racing
// populate is the read-after-write guard.
func (s *Service) warmOnWrite(r *http.Request, bucket, key string) {
	if !s.config.Cache.WarmOnWrite || !s.cache.IsEnabled() {
		return
	}

	// Anonymous write → anonymous warm (unsigned fetch, public-read learned from the
	// probe). See the doc comment: never infer public-read from a public write.
	if hasNoAuthCredentials(r) {
		metrics.WarmOnWriteTriggered.Inc()
		s.triggerBackgroundCacheFetch(bucket, key, "", "", true /*anonymous*/, priorityWarmWrite)
		return
	}

	_, accessKey, secretKey, err := s.forwarder.ValidateAndGetCredentials(r)
	if err != nil || accessKey == "" || secretKey == "" {
		return
	}
	metrics.WarmOnWriteTriggered.Inc()
	s.triggerBackgroundCacheFetch(bucket, key, accessKey, secretKey, false /*anonymous*/, priorityWarmWrite)
}

// HandlePassthrough handles requests that are passed through without caching.
func (s *Service) HandlePassthrough(w http.ResponseWriter, r *http.Request) error {
	return s.forwarder.Forward(r.Context(), w, r)
}

// HandleCompleteMultipartUpload handles CompleteMultipartUpload with idempotency caching.
// This caches successful completion responses in ocache to support idempotent calls,
// matching tigris-os behavior where a second CompleteMultipartUpload call returns success.
func (s *Service) HandleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request) error {
	bucket, key := ParseBucketKey(r)
	uploadId := r.URL.Query().Get("uploadId")
	ctx := r.Context()

	log.Debug().Str("bucket", bucket).Str("key", key).Str("uploadId", uploadId).Msg("HandleCompleteMultipartUpload")

	// Check ocache first for idempotent completion (works across TAG pods).
	// A replay returns the already-completed response without touching upstream and
	// without changing the object, so it needs no read-cache invalidation — the first
	// completion (below) already invalidated it.
	if s.cache.IsEnabled() {
		entry, found, err := s.cache.GetCompletion(ctx, bucket, key, uploadId)
		if err == nil && found {
			log.Debug().Str("uploadId", uploadId).Msg("CompleteMultipartUpload cache hit - returning cached response")
			for k, v := range entry.Headers {
				w.Header().Set(k, v)
			}
			w.WriteHeader(entry.StatusCode)
			_, _ = w.Write(entry.Body)
			return nil
		}
	}

	// Invalidate the object's read cache BEFORE forwarding. Completing a multipart
	// upload overwrites the object, so any previously cached version is now stale;
	// like PutObject/DeleteObject/CopyObject, invalidate up front so a forward that
	// succeeds but whose post-invalidation fails can't leave stale data served.
	s.invalidateObject(context.Background(), bucket, key)

	// Forward to upstream with response capture
	capture, err := s.forwarder.ForwardWithCapture(ctx, w, r)
	if err != nil {
		return err
	}

	// Re-invalidate AFTER upstream confirms the completion, for the same
	// read-after-write reason as HandlePutObject: a GET racing the in-flight
	// completion may have re-cached the pre-overwrite object; this second tombstone
	// blocks that stale repopulation. Gated on a confirmed-successful completion
	// (2xx and not a 200-with-<Error> body) so a failed completion, which leaves the
	// object unchanged, doesn't discard a valid racing refill.
	completed := s3WriteSucceeded(capture)
	if completed && s.cache.IsEnabled() {
		s.invalidateObject(context.Background(), bucket, key)
		// Warm-on-write is the only way to make a multipart-completed object hot:
		// TAG never sees its assembled body, so a write-through tee is impossible.
		s.warmOnWrite(r, bucket, key)
		// The path that matters for parquet: ingestors write via multipart, so this
		// is where a freshly written file's metadata gets warmed (RFC 0002).
		s.warmParquetFooterOnWrite(r, bucket, key)
		// Prototype: establish the metadata entry so the first read does not pay an
		// upstream round trip to discover it. Multipart is the gap — write-through
		// cannot tee a body TAG never sees.
		s.cacheBlockMetaOnWrite(r, bucket, key, completedMultipartETag(capture))
	}

	// Cache successful completions in ocache for idempotent replays. Only cache a
	// genuine success (not a 200-with-<Error> body) that was fully captured, so we
	// never replay a corrupted or error response as a successful completion.
	if completed && capture.Complete {
		if cacheErr := s.cache.PutCompletion(ctx, bucket, key, uploadId, capture.StatusCode, capture.Headers, capture.Body); cacheErr != nil {
			log.Debug().Err(cacheErr).Msg("Failed to cache completion response")
			// Don't fail the request if caching fails
		}
	}

	return nil
}

// ParseBucketKey extracts bucket and key from request path.
func ParseBucketKey(r *http.Request) (bucket, key string) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) >= 1 {
		bucket = parts[0]
	}
	if len(parts) >= 2 {
		key = parts[1]
	}
	return
}

// hasNoAuthCredentials returns true if the request carries no SigV4 credentials
// (no Authorization header and no X-Amz-Credential query parameter).
func hasNoAuthCredentials(r *http.Request) bool {
	return r.Header.Get("Authorization") == "" &&
		r.URL.Query().Get("X-Amz-Credential") == ""
}

// isAnonymousRequest returns true if the request has no SigV4 authentication
// and the auth result indicates it was not validated (AuthNotValidated with no error).
func isAnonymousRequest(r *http.Request, result AuthResult, authErr error) bool {
	return authErr == nil && result == AuthNotValidated && hasNoAuthCredentials(r)
}

// parseContentRange parses a "bytes START-END/TOTAL" Content-Range header. total is the object
// size (0 if the header is absent, the total is "*", or it is unparseable — total extraction is
// prefix-independent to preserve historical behavior). hasBounds is true only when the "bytes "
// prefix and numeric START-END are present and parsed, so the unsatisfied-range form
// "bytes */TOTAL" yields total>0 with hasBounds=false.
func parseContentRange(contentRange string) (start, end, total int64, hasBounds bool) {
	if contentRange == "" {
		return 0, 0, 0, false
	}
	slashIdx := strings.LastIndex(contentRange, "/")
	if slashIdx == -1 {
		return 0, 0, 0, false
	}
	if totalStr := contentRange[slashIdx+1:]; totalStr != "*" {
		if t, err := strconv.ParseInt(totalStr, 10, 64); err == nil {
			total = t
		}
	}
	const prefix = "bytes "
	if !strings.HasPrefix(contentRange, prefix) {
		return 0, 0, total, false
	}
	rangePart := contentRange[len(prefix):slashIdx] // "START-END" or "*"
	dashIdx := strings.IndexByte(rangePart, '-')
	if dashIdx == -1 {
		return 0, 0, total, false
	}
	s, err1 := strconv.ParseInt(rangePart[:dashIdx], 10, 64)
	e, err2 := strconv.ParseInt(rangePart[dashIdx+1:], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, total, false
	}
	return s, e, total, true
}

// GetRegion returns the configured upstream region.
func (s *Service) GetRegion() string {
	return s.config.Upstream.Region
}
