// Package metrics provides Prometheus metrics for TAG.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// RequestsTotal counts total requests by operation and status.
	RequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tag_requests_total",
			Help: "Total number of requests processed by TAG",
		},
		[]string{"operation", "status"},
	)

	// RequestDuration tracks request latency by operation.
	RequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "tag_request_duration_seconds",
			Help:    "Request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation"},
	)

	// CacheHits counts cache hits.
	CacheHits = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "tag_cache_hits_total",
			Help: "Total number of cache hits",
		},
	)

	// CacheMisses counts cache misses.
	CacheMisses = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "tag_cache_misses_total",
			Help: "Total number of cache misses",
		},
	)

	// CacheOperations counts cache operations by type and result.
	CacheOperations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tag_cache_operations_total",
			Help: "Total number of cache operations",
		},
		[]string{"operation", "result"},
	)

	// UpstreamRequestDuration tracks upstream (Tigris) request latency.
	UpstreamRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "tag_upstream_request_duration_seconds",
			Help:    "Upstream request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)

	// UpstreamErrors counts upstream request errors.
	UpstreamErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tag_upstream_errors_total",
			Help: "Total number of upstream errors",
		},
		[]string{"method"},
	)

	// AuthFailures counts authentication/signature validation failures.
	AuthFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tag_auth_failures_total",
			Help: "Total number of authentication failures",
		},
		[]string{"reason"},
	)

	// ActiveConnections tracks the number of active connections.
	ActiveConnections = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "tag_active_connections",
			Help: "Number of active connections",
		},
	)

	// BytesTransferred tracks bytes transferred.
	BytesTransferred = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tag_bytes_transferred_total",
			Help: "Total bytes transferred",
		},
		[]string{"direction"}, // "in" or "out"
	)

	// BroadcastShared counts requests that joined an existing broadcast stream.
	BroadcastShared = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "tag_broadcast_shared_total",
			Help: "Number of requests that joined an existing broadcast stream",
		},
	)

	// BroadcastFetches counts upstream fetches (broadcast initiators).
	BroadcastFetches = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "tag_broadcast_fetches_total",
			Help: "Number of upstream fetches (broadcast initiators)",
		},
	)

	// BroadcastSlowConsumers counts listeners disconnected for being too slow.
	BroadcastSlowConsumers = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "tag_broadcast_slow_consumers_total",
			Help: "Number of listeners disconnected for being too slow",
		},
	)

	// ActiveBroadcasts tracks the number of active broadcasts.
	ActiveBroadcasts = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "tag_active_broadcasts",
			Help: "Number of active broadcasts",
		},
	)

	// BackgroundFetchesTriggered counts background fetch triggers from range requests.
	BackgroundFetchesTriggered = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "tag_background_fetches_triggered_total",
			Help: "Number of background full-object fetches triggered by range requests",
		},
	)

	// BackgroundFetchesSucceeded counts successful background fetches.
	BackgroundFetchesSucceeded = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "tag_background_fetches_succeeded_total",
			Help: "Number of background fetches that completed successfully",
		},
	)

	// BackgroundFetchesFailed counts failed background fetches.
	BackgroundFetchesFailed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "tag_background_fetches_failed_total",
			Help: "Number of background fetches that failed",
		},
	)

	// RangeFromCacheHits counts ranges served from cached full objects.
	RangeFromCacheHits = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "tag_range_from_cache_hits_total",
			Help: "Number of range requests served from cached full objects",
		},
	)

	// ActiveBackgroundFetches tracks the number of active background fetches.
	ActiveBackgroundFetches = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "tag_active_background_fetches",
			Help: "Number of active background fetches",
		},
	)
)

// RecordRequest records a request with its duration and status.
func RecordRequest(operation, status string, durationSeconds float64) {
	RequestsTotal.WithLabelValues(operation, status).Inc()
	RequestDuration.WithLabelValues(operation).Observe(durationSeconds)
}

// RecordCacheHit records a cache hit.
func RecordCacheHit() {
	CacheHits.Inc()
	CacheOperations.WithLabelValues("get", "hit").Inc()
}

// RecordCacheMiss records a cache miss.
func RecordCacheMiss() {
	CacheMisses.Inc()
	CacheOperations.WithLabelValues("get", "miss").Inc()
}

// RecordCacheOperation records a cache operation.
func RecordCacheOperation(operation, result string) {
	CacheOperations.WithLabelValues(operation, result).Inc()
}

// RecordUpstreamRequest records an upstream request.
func RecordUpstreamRequest(method string, durationSeconds float64, err error) {
	UpstreamRequestDuration.WithLabelValues(method).Observe(durationSeconds)
	if err != nil {
		UpstreamErrors.WithLabelValues(method).Inc()
	}
}

// RecordAuthFailure records an authentication failure.
func RecordAuthFailure(reason string) {
	AuthFailures.WithLabelValues(reason).Inc()
}

// RecordBroadcastShared records a request that joined an existing broadcast.
func RecordBroadcastShared() {
	BroadcastShared.Inc()
}

// RecordBroadcastFetch records a new upstream fetch (broadcast initiator).
func RecordBroadcastFetch() {
	BroadcastFetches.Inc()
}

// RecordBroadcastSlowConsumer records a listener disconnected for being too slow.
func RecordBroadcastSlowConsumer() {
	BroadcastSlowConsumers.Inc()
}

// SetActiveBroadcasts sets the number of active broadcasts.
func SetActiveBroadcasts(count int) {
	ActiveBroadcasts.Set(float64(count))
}

// RecordBackgroundFetchTriggered records a background fetch trigger.
func RecordBackgroundFetchTriggered() {
	BackgroundFetchesTriggered.Inc()
}

// RecordBackgroundFetchSucceeded records a successful background fetch.
func RecordBackgroundFetchSucceeded() {
	BackgroundFetchesSucceeded.Inc()
}

// RecordBackgroundFetchFailed records a failed background fetch.
func RecordBackgroundFetchFailed() {
	BackgroundFetchesFailed.Inc()
}

// RecordRangeFromCacheHit records a range request served from cache.
func RecordRangeFromCacheHit() {
	RangeFromCacheHits.Inc()
}

// SetActiveBackgroundFetches sets the number of active background fetches.
func SetActiveBackgroundFetches(count int) {
	ActiveBackgroundFetches.Set(float64(count))
}
