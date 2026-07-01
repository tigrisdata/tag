package handlers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/metrics"
	"github.com/tigrisdata/tag/proxy"
	"github.com/tigrisdata/tag/s3err"
)

// Server is the HTTP server for S3-compatible API.
type Server struct {
	service      *proxy.Service
	router       *mux.Router
	httpServer   *http.Server
	bindAddr     string
	pprofEnabled bool
	tlsCertFile  string
	tlsKeyFile   string
	// admissionSem bounds concurrently-served S3 requests. nil disables admission
	// control (unlimited).
	admissionSem chan struct{}
}

// NewServer creates a new HTTP server.
func NewServer(service *proxy.Service, bindIP string, port int, pprofEnabled bool, maxInflight int) *Server {
	s := &Server{
		service:      service,
		bindAddr:     fmt.Sprintf("%s:%d", bindIP, port),
		pprofEnabled: pprofEnabled,
	}

	if maxInflight > 0 {
		s.admissionSem = make(chan struct{}, maxInflight)
	}

	s.router = s.setupRouter()
	return s
}

// SetTLS configures TLS certificate and key files.
// When both are set, the server will use HTTPS instead of HTTP.
func (s *Server) SetTLS(certFile, keyFile string) {
	s.tlsCertFile = certFile
	s.tlsKeyFile = keyFile
}

// connectionTrackingMiddleware tracks active HTTP connections.
func (s *Server) connectionTrackingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		metrics.ActiveConnections.Inc()
		defer metrics.ActiveConnections.Dec()
		next.ServeHTTP(w, r)
	})
}

// admissionMiddleware bounds the number of concurrently-served S3 requests. When
// the limit is saturated it sheds with 503 SlowDown so that overload becomes
// backpressure instead of unbounded goroutine/OS-thread/memory growth (the
// thread-exhaustion / OOM failure mode). Operational endpoints (health, metrics,
// pprof) are exempt so probes and observability keep working under load.
func (s *Server) admissionMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.admissionSem == nil || isExemptFromAdmission(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		select {
		case s.admissionSem <- struct{}{}:
			metrics.InflightRequests.Inc()
			defer func() {
				<-s.admissionSem
				metrics.InflightRequests.Dec()
			}()
			next.ServeHTTP(w, r)
		default:
			metrics.AdmissionShed.Inc()
			s3err.WriteError(w, r, s3err.ErrSlowDown)
		}
	})
}

// isExemptFromAdmission reports whether a path is a non-S3 operational endpoint
// that must not be subject to admission control.
func isExemptFromAdmission(path string) bool {
	return path == "/health" || path == "/metrics" || strings.HasPrefix(path, "/debug/")
}

// setupRouter configures the S3-compatible routes.
func (s *Server) setupRouter() *mux.Router {
	r := mux.NewRouter()
	r.SkipClean(true) // Preserve path for S3 compatibility

	// Apply connection tracking middleware
	r.Use(s.connectionTrackingMiddleware)

	// Bound concurrently-served S3 requests (sheds with 503 SlowDown when full)
	r.Use(s.admissionMiddleware)

	// Health check endpoint
	r.HandleFunc("/health", s.handleHealth).Methods("GET")

	// Metrics endpoint for Prometheus
	r.Handle("/metrics", promhttp.Handler()).Methods("GET")

	// pprof endpoints for profiling (if enabled)
	if s.pprofEnabled {
		r.HandleFunc("/debug/pprof/", pprof.Index)
		r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		r.HandleFunc("/debug/pprof/profile", pprof.Profile)
		r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		r.HandleFunc("/debug/pprof/trace", pprof.Trace)
		r.Handle("/debug/pprof/heap", pprof.Handler("heap"))
		r.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
		r.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
		r.Handle("/debug/pprof/block", pprof.Handler("block"))
		r.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
		r.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
		log.Info().Msg("pprof endpoints enabled at /debug/pprof/")
	}

	// S3 API routes - path style
	// The order matters - more specific routes should come first

	// CompleteMultipartUpload - POST with uploadId but no partNumber
	// Must be registered before generic handleObjectWithQuery to cache completion responses
	r.HandleFunc("/{bucket}/{object:.+}", s.handleCompleteMultipartUpload).
		Queries("uploadId", "{uploadId}").
		Methods("POST").
		MatcherFunc(func(r *http.Request, rm *mux.RouteMatch) bool {
			// Only match if partNumber is NOT present (CompleteMultipartUpload)
			// UploadPart has both uploadId and partNumber
			return r.URL.Query().Get("partNumber") == ""
		})

	// Object operations with query parameters (UploadPart, ListParts, AbortMultipartUpload, etc.)
	r.HandleFunc("/{bucket}/{object:.+}", s.handleObjectWithQuery).
		Queries("uploadId", "{uploadId}").
		Methods("PUT", "POST", "DELETE", "GET")

	// Multipart upload initiation
	r.HandleFunc("/{bucket}/{object:.+}", s.handleInitiateMultipart).
		Queries("uploads", "").
		Methods("POST")

	// Object tagging
	r.HandleFunc("/{bucket}/{object:.+}", s.handleObjectTagging).
		Queries("tagging", "").
		Methods("GET", "PUT", "DELETE")

	// Object ACL
	r.HandleFunc("/{bucket}/{object:.+}", s.handleObjectACL).
		Queries("acl", "").
		Methods("GET", "PUT")

	// Basic object operations
	r.HandleFunc("/{bucket}/{object:.+}", s.handleObject).Methods("GET", "HEAD", "PUT", "DELETE")

	// Copy object (PUT with X-Amz-Copy-Source header)
	// Handled in handleObject based on header presence

	// Bucket operations with query parameters
	// Each route is registered for both /{bucket} and /{bucket}/ because S3 clients
	// like warp send bucket-level requests with trailing slashes. We cannot strip
	// trailing slashes via middleware because that would break SigV4 signature
	// validation (the client signs the request with the original path).
	for _, prefix := range []string{"/{bucket}", "/{bucket}/"} {
		r.HandleFunc(prefix, s.handleBucketMultipartUploads).
			Queries("uploads", "").
			Methods("GET")

		r.HandleFunc(prefix, s.handleListObjectsV2).
			Queries("list-type", "2").
			Methods("GET")

		r.HandleFunc(prefix, s.handleBucketVersioning).
			Queries("versioning", "").
			Methods("GET", "PUT")

		r.HandleFunc(prefix, s.handleBucketACL).
			Queries("acl", "").
			Methods("GET", "PUT")

		r.HandleFunc(prefix, s.handleBucketLifecycle).
			Queries("lifecycle", "").
			Methods("GET", "PUT", "DELETE")

		r.HandleFunc(prefix, s.handleBucketPolicy).
			Queries("policy", "").
			Methods("GET", "PUT", "DELETE")

		r.HandleFunc(prefix, s.handleBucketCORS).
			Queries("cors", "").
			Methods("GET", "PUT", "DELETE")

		r.HandleFunc(prefix, s.handleBucketTagging).
			Queries("tagging", "").
			Methods("GET", "PUT", "DELETE")

		r.HandleFunc(prefix, s.handleBucketLocation).
			Queries("location", "").
			Methods("GET")

		// DeleteObjects (multi-object delete)
		r.HandleFunc(prefix, s.handleDeleteObjects).
			Queries("delete", "").
			Methods("POST")

		// Basic bucket operations (ListObjects V1, CreateBucket, DeleteBucket, HeadBucket)
		r.HandleFunc(prefix, s.handleBucket).Methods("GET", "HEAD", "PUT", "DELETE")
	}

	// List buckets (service level)
	r.HandleFunc("/", s.handleListBuckets).Methods("GET")

	return r
}

// Start starts the HTTP server.
// If TLS is configured via SetTLS, the server listens with HTTPS.
func (s *Server) Start() error {
	s.httpServer = &http.Server{
		Addr:         s.bindAddr,
		Handler:      s.router,
		ReadTimeout:  5 * time.Minute,
		WriteTimeout: 5 * time.Minute,
		IdleTimeout:  120 * time.Second,
	}

	if s.tlsCertFile != "" && s.tlsKeyFile != "" {
		log.Info().Str("addr", s.bindAddr).Msg("Starting HTTPS server (TLS enabled)")
		return s.httpServer.ListenAndServeTLS(s.tlsCertFile, s.tlsKeyFile)
	}

	log.Info().Str("addr", s.bindAddr).Msg("Starting HTTP server")
	return s.httpServer.ListenAndServe()
}

// Router returns the HTTP router for testing.
func (s *Server) Router() http.Handler {
	return s.router
}

// Stop gracefully stops the HTTP server.
func (s *Server) Stop(ctx context.Context) error {
	if s.httpServer == nil {
		return nil
	}

	log.Info().Msg("Stopping HTTP server")
	return s.httpServer.Shutdown(ctx)
}

// responseTracker wraps http.ResponseWriter to track whether headers have been
// committed. This prevents handleError from writing error XML after a handler
// has already started writing a response (which would corrupt the HTTP stream
// and break keep-alive connections).
type responseTracker struct {
	http.ResponseWriter
	committed bool
}

func (rt *responseTracker) WriteHeader(code int) {
	rt.committed = true
	rt.ResponseWriter.WriteHeader(code)
}

func (rt *responseTracker) Write(b []byte) (int, error) {
	rt.committed = true // implicit 200 if WriteHeader not called
	return rt.ResponseWriter.Write(b)
}

func (rt *responseTracker) Flush() {
	if f, ok := rt.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// handleWithError calls a handler function and handles any returned error.
// If headers have already been committed (e.g., the handler started streaming
// a response before encountering an error), the error is logged but no error
// response is written to avoid corrupting the HTTP stream.
func handleWithError(w http.ResponseWriter, r *http.Request, handler func(http.ResponseWriter, *http.Request) error) {
	rt := &responseTracker{ResponseWriter: w}
	err := handler(rt, r)
	if err != nil {
		if rt.committed {
			log.Warn().Err(err).Str("path", r.URL.Path).
				Msg("Error after response headers committed, cannot send error response")
		} else {
			handleError(rt, r, err)
		}
	}
}

// handleHealth handles health check requests.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("OK")); err != nil {
		log.Debug().Err(err).Msg("Failed to write health response")
	}
}

// handleError handles errors from service methods, mapping auth errors to S3 error codes.
func handleError(w http.ResponseWriter, r *http.Request, err error) {
	if authErr, ok := proxy.IsAuthError(err); ok {
		WriteAuthError(w, r, int(authErr.Code), err)
		return
	}
	s3err.WriteInternalError(w, r, err)
}

// getBucketName extracts the bucket name from request path variables.
func getBucketName(r *http.Request) string {
	vars := mux.Vars(r)
	return vars["bucket"]
}

// validBucketNameRegex validates basic S3 bucket name format:
// - Must start with a lowercase letter or number
// - Can contain lowercase letters, numbers, hyphens, and dots
// - Must end with a lowercase letter or number
var validBucketNameRegex = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*[a-z0-9]$`)

// validateBucketName checks if bucket name follows S3 naming rules.
// Returns true if valid, false if invalid (error already written to response).
// S3 bucket naming rules:
// - 3-63 characters
// - Lowercase letters, numbers, hyphens, dots only
// - Must start and end with letter or number
// - Cannot contain consecutive dots (..)
// - Cannot contain dot-dash (.-) or dash-dot (-.) patterns
// - Cannot contain underscores
func validateBucketName(w http.ResponseWriter, r *http.Request) bool {
	bucket := getBucketName(r)
	if bucket == "" {
		log.Error().Str("path", r.URL.Path).Msg("Empty bucket name")
		s3err.WriteError(w, r, s3err.ErrInvalidArgument)
		return false
	}

	// Length check: 3-63 characters
	if len(bucket) < 3 || len(bucket) > 63 {
		log.Debug().Str("bucket", bucket).Msg("Bucket name length invalid")
		s3err.WriteError(w, r, s3err.ErrInvalidBucketName)
		return false
	}

	// Must match valid pattern (start/end with letter or number)
	if !validBucketNameRegex.MatchString(bucket) {
		log.Debug().Str("bucket", bucket).Msg("Bucket name format invalid")
		s3err.WriteError(w, r, s3err.ErrInvalidBucketName)
		return false
	}

	// Cannot contain consecutive dots (..)
	if strings.Contains(bucket, "..") {
		log.Debug().Str("bucket", bucket).Msg("Bucket name contains consecutive dots")
		s3err.WriteError(w, r, s3err.ErrInvalidBucketName)
		return false
	}

	// Cannot contain dot-dash (.-) or dash-dot (-.)
	if strings.Contains(bucket, ".-") || strings.Contains(bucket, "-.") {
		log.Debug().Str("bucket", bucket).Msg("Bucket name contains invalid dot-dash pattern")
		s3err.WriteError(w, r, s3err.ErrInvalidBucketName)
		return false
	}

	// Cannot contain underscore
	if strings.Contains(bucket, "_") {
		log.Debug().Str("bucket", bucket).Msg("Bucket name contains underscore")
		s3err.WriteError(w, r, s3err.ErrInvalidBucketName)
		return false
	}

	return true
}

// validateContentLength validates the Content-Length header for PUT requests.
// Returns true if valid, false if invalid (error already written to response).
// For AWS chunked transfer encoding (streaming SigV4), the decoded content length
// is provided in X-Amz-Decoded-Content-Length instead of Content-Length.
func validateContentLength(w http.ResponseWriter, r *http.Request) bool {
	// AWS chunked encoding: Content-Length reflects wire size (with chunk framing),
	// and the actual payload size is in X-Amz-Decoded-Content-Length.
	if proxy.IsStreamingPayload(r.Header.Get("X-Amz-Content-Sha256")) {
		v := r.Header.Get("X-Amz-Decoded-Content-Length")
		if v == "" {
			s3err.WriteError(w, r, s3err.ErrMissingContentLength)
			return false
		}
		decodedLen, err := strconv.ParseInt(v, 10, 64)
		if err != nil || decodedLen < 0 {
			s3err.WriteError(w, r, s3err.ErrBadContentLength)
			return false
		}
		return true
	}

	v := r.Header.Get("Content-Length")
	if v == "" {
		s3err.WriteError(w, r, s3err.ErrMissingContentLength)
		return false
	}
	contentLength, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		s3err.WriteError(w, r, s3err.ErrBadContentLength)
		return false
	}
	if contentLength < 0 {
		s3err.WriteError(w, r, s3err.ErrBadContentLength)
		return false
	}
	return true
}

// handleObject handles basic object operations (GET, HEAD, PUT, DELETE).
func (s *Server) handleObject(w http.ResponseWriter, r *http.Request) {
	// Validate bucket name
	if !validateBucketName(w, r) {
		return
	}

	// Check for copy operation (PUT with X-Amz-Copy-Source header)
	if r.Method == http.MethodPut && r.Header.Get("X-Amz-Copy-Source") != "" {
		handleWithError(w, r, s.service.HandleCopyObject)
		return
	}

	// Validate Content-Length for PUT (non-copy)
	if r.Method == http.MethodPut {
		if !validateContentLength(w, r) {
			return
		}
	}

	var handler func(http.ResponseWriter, *http.Request) error
	switch r.Method {
	case http.MethodGet:
		handler = s.service.HandleGetObject
	case http.MethodHead:
		handler = s.service.HandleHeadObject
	case http.MethodPut:
		handler = s.service.HandlePutObject
	case http.MethodDelete:
		handler = s.service.HandleDeleteObject
	default:
		s3err.WriteError(w, r, s3err.ErrMethodNotAllowed)
		return
	}

	handleWithError(w, r, handler)
}

// handleBucket handles basic bucket operations.
func (s *Server) handleBucket(w http.ResponseWriter, r *http.Request) {
	if !validateBucketName(w, r) {
		return
	}

	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete:
		handleWithError(w, r, s.service.HandlePassthrough)
	default:
		s3err.WriteError(w, r, s3err.ErrMethodNotAllowed)
	}
}

// handleListBuckets handles ListBuckets operation.
func (s *Server) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	handleWithError(w, r, s.service.HandlePassthrough)
}

// handleListObjectsV2 handles ListObjectsV2 operation.
func (s *Server) handleListObjectsV2(w http.ResponseWriter, r *http.Request) {
	if !validateBucketName(w, r) {
		return
	}
	handleWithError(w, r, s.service.HandlePassthrough)
}

// handleCompleteMultipartUpload handles CompleteMultipartUpload with idempotency caching.
// This caches successful completion responses in ocache to support idempotent calls,
// matching tigris-os behavior where a second CompleteMultipartUpload returns success.
func (s *Server) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request) {
	if !validateBucketName(w, r) {
		return
	}
	handleWithError(w, r, s.service.HandleCompleteMultipartUpload)
}

// handleObjectWithQuery handles object operations with query parameters (multipart).
func (s *Server) handleObjectWithQuery(w http.ResponseWriter, r *http.Request) {
	if !validateBucketName(w, r) {
		return
	}
	handleWithError(w, r, s.service.HandlePassthrough)
}

// handleInitiateMultipart handles InitiateMultipartUpload.
func (s *Server) handleInitiateMultipart(w http.ResponseWriter, r *http.Request) {
	if !validateBucketName(w, r) {
		return
	}
	handleWithError(w, r, s.service.HandlePassthrough)
}

// handleBucketMultipartUploads handles ListMultipartUploads.
func (s *Server) handleBucketMultipartUploads(w http.ResponseWriter, r *http.Request) {
	if !validateBucketName(w, r) {
		return
	}
	handleWithError(w, r, s.service.HandlePassthrough)
}

// handleObjectTagging handles object tagging operations.
func (s *Server) handleObjectTagging(w http.ResponseWriter, r *http.Request) {
	if !validateBucketName(w, r) {
		return
	}
	handleWithError(w, r, s.service.HandlePassthrough)
}

// handleObjectACL handles object ACL operations.
func (s *Server) handleObjectACL(w http.ResponseWriter, r *http.Request) {
	if !validateBucketName(w, r) {
		return
	}
	handleWithError(w, r, s.service.HandlePassthrough)
}

// handleBucketVersioning handles bucket versioning operations.
func (s *Server) handleBucketVersioning(w http.ResponseWriter, r *http.Request) {
	if !validateBucketName(w, r) {
		return
	}
	handleWithError(w, r, s.service.HandlePassthrough)
}

// handleBucketACL handles bucket ACL operations.
func (s *Server) handleBucketACL(w http.ResponseWriter, r *http.Request) {
	if !validateBucketName(w, r) {
		return
	}
	handleWithError(w, r, s.service.HandlePassthrough)
}

// handleBucketLifecycle handles bucket lifecycle operations.
func (s *Server) handleBucketLifecycle(w http.ResponseWriter, r *http.Request) {
	if !validateBucketName(w, r) {
		return
	}
	handleWithError(w, r, s.service.HandlePassthrough)
}

// handleBucketPolicy handles bucket policy operations.
func (s *Server) handleBucketPolicy(w http.ResponseWriter, r *http.Request) {
	if !validateBucketName(w, r) {
		return
	}
	handleWithError(w, r, s.service.HandlePassthrough)
}

// handleBucketCORS handles bucket CORS operations.
func (s *Server) handleBucketCORS(w http.ResponseWriter, r *http.Request) {
	if !validateBucketName(w, r) {
		return
	}
	handleWithError(w, r, s.service.HandlePassthrough)
}

// handleBucketTagging handles bucket tagging operations.
func (s *Server) handleBucketTagging(w http.ResponseWriter, r *http.Request) {
	if !validateBucketName(w, r) {
		return
	}
	handleWithError(w, r, s.service.HandlePassthrough)
}

// handleBucketLocation handles GetBucketLocation operation.
// Returns the configured region directly instead of proxying to upstream.
func (s *Server) handleBucketLocation(w http.ResponseWriter, r *http.Request) {
	if !validateBucketName(w, r) {
		return
	}

	region := s.service.GetRegion()

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">%s</LocationConstraint>`, region)
}

// handleDeleteObjects handles DeleteObjects (multi-object delete) operation.
func (s *Server) handleDeleteObjects(w http.ResponseWriter, r *http.Request) {
	if !validateBucketName(w, r) {
		return
	}
	handleWithError(w, r, s.service.HandleDeleteObjects)
}
