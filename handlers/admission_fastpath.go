package handlers

import (
	"net/http"
	"net/http/pprof"
	"sort"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/tigrisdata/tag/metrics"
	"github.com/tigrisdata/tag/s3err"
)

const (
	admissionHealthPath       = "/health"
	admissionMetricsPath      = "/metrics"
	admissionObjectPath       = "/{bucket}/{object:.+}"
	admissionBucketPath       = "/{bucket}"
	admissionBucketSlashPath  = "/{bucket}/"
	admissionServicePath      = "/"
	admissionUploadIDQuery    = "uploadId"
	admissionUploadsQuery     = "uploads"
	admissionDeleteQuery      = "delete"
	admissionPprofPath        = "/debug/pprof/"
	admissionPprofCmdlinePath = "/debug/pprof/cmdline"
	admissionPprofProfilePath = "/debug/pprof/profile"
	admissionPprofSymbolPath  = "/debug/pprof/symbol"
	admissionPprofTracePath   = "/debug/pprof/trace"
	admissionPprofHeapPath    = "/debug/pprof/heap"
	admissionPprofGoroutine   = "/debug/pprof/goroutine"
	admissionPprofAllocs      = "/debug/pprof/allocs"
	admissionPprofBlock       = "/debug/pprof/block"
	admissionPprofMutex       = "/debug/pprof/mutex"
	admissionPprofThread      = "/debug/pprof/threadcreate"
)

var (
	admissionBucketPaths = [...]string{admissionBucketPath, admissionBucketSlashPath}
	admissionPprofPaths  = [...]string{
		admissionPprofPath,
		admissionPprofCmdlinePath,
		admissionPprofProfilePath,
		admissionPprofSymbolPath,
		admissionPprofTracePath,
		admissionPprofHeapPath,
		admissionPprofGoroutine,
		admissionPprofAllocs,
		admissionPprofBlock,
		admissionPprofMutex,
		admissionPprofThread,
	}
	admissionBasicMethods = [...]string{
		http.MethodGet,
		http.MethodHead,
		http.MethodPut,
		http.MethodDelete,
	}
)

type admissionRouteKind uint8

const (
	admissionRouteOther admissionRouteKind = iota
	admissionRouteOperational
	admissionRouteS3
)

// The classifier uses these priorities to keep the common fallback routes
// ahead of detailed query routes without changing setupRouter's route order.
const (
	admissionFastOperational = iota
	admissionFastObject
	admissionFastObjectQuery
	admissionFastBucket
	admissionFastBucketQuery
	admissionFastService

	// Query-specific bucket definitions use this shared rank. Object query
	// definitions override it because they precede bucket fallbacks.
	admissionFastQuery = admissionFastBucketQuery
)

// admissionRouteDefinition is the single description of a route registered by
// setupRouter. The classifier registers the same definitions with marker
// handlers, so adding a route cannot silently leave saturated requests on the
// mux walk or classify an operational route as S3.
type admissionRouteDefinition struct {
	name        string
	path        string
	queries     []string
	methods     []string
	kind        admissionRouteKind
	fastPath    bool
	fastMethods []string
	fastOrder   int
	matcher     mux.MatcherFunc
	handler     func(*Server) http.Handler
}

type admissionRouteMarker struct {
	kind admissionRouteKind
}

func (*admissionRouteMarker) ServeHTTP(http.ResponseWriter, *http.Request) {}

// admissionRouteDefinitions is shared by setupRouter and the saturated
// classifier. Keep the order of this slice equal to the production mux order:
// query-specific S3 handlers must precede their generic fallbacks.
func admissionRouteDefinitions(pprofEnabled bool) []admissionRouteDefinition {
	routes := []admissionRouteDefinition{
		{
			name:      "health",
			path:      admissionHealthPath,
			methods:   []string{http.MethodGet},
			kind:      admissionRouteOperational,
			fastOrder: admissionFastOperational,
			handler: func(s *Server) http.Handler {
				return http.HandlerFunc(s.handleHealth)
			},
		},
		{
			name:      "metrics",
			path:      admissionMetricsPath,
			methods:   []string{http.MethodGet},
			kind:      admissionRouteOperational,
			fastOrder: admissionFastOperational,
			handler: func(*Server) http.Handler {
				return promhttp.Handler()
			},
		},
	}

	if pprofEnabled {
		routes = append(routes,
			admissionRouteDefinition{
				name:      "pprof-index",
				path:      admissionPprofPath,
				kind:      admissionRouteOperational,
				fastOrder: admissionFastOperational,
				handler: func(*Server) http.Handler {
					return http.HandlerFunc(pprof.Index)
				},
			},
			admissionRouteDefinition{
				name:      "pprof-cmdline",
				path:      admissionPprofCmdlinePath,
				kind:      admissionRouteOperational,
				fastOrder: admissionFastOperational,
				handler: func(*Server) http.Handler {
					return http.HandlerFunc(pprof.Cmdline)
				},
			},
			admissionRouteDefinition{
				name:      "pprof-profile",
				path:      admissionPprofProfilePath,
				kind:      admissionRouteOperational,
				fastOrder: admissionFastOperational,
				handler: func(*Server) http.Handler {
					return http.HandlerFunc(pprof.Profile)
				},
			},
			admissionRouteDefinition{
				name:      "pprof-symbol",
				path:      admissionPprofSymbolPath,
				kind:      admissionRouteOperational,
				fastOrder: admissionFastOperational,
				handler: func(*Server) http.Handler {
					return http.HandlerFunc(pprof.Symbol)
				},
			},
			admissionRouteDefinition{
				name:      "pprof-trace",
				path:      admissionPprofTracePath,
				kind:      admissionRouteOperational,
				fastOrder: admissionFastOperational,
				handler: func(*Server) http.Handler {
					return http.HandlerFunc(pprof.Trace)
				},
			},
			admissionRouteDefinition{
				name:      "pprof-heap",
				path:      admissionPprofHeapPath,
				kind:      admissionRouteOperational,
				fastOrder: admissionFastOperational,
				handler: func(*Server) http.Handler {
					return pprof.Handler("heap")
				},
			},
			admissionRouteDefinition{
				name:      "pprof-goroutine",
				path:      admissionPprofGoroutine,
				kind:      admissionRouteOperational,
				fastOrder: admissionFastOperational,
				handler: func(*Server) http.Handler {
					return pprof.Handler("goroutine")
				},
			},
			admissionRouteDefinition{
				name:      "pprof-allocs",
				path:      admissionPprofAllocs,
				kind:      admissionRouteOperational,
				fastOrder: admissionFastOperational,
				handler: func(*Server) http.Handler {
					return pprof.Handler("allocs")
				},
			},
			admissionRouteDefinition{
				name:      "pprof-block",
				path:      admissionPprofBlock,
				kind:      admissionRouteOperational,
				fastOrder: admissionFastOperational,
				handler: func(*Server) http.Handler {
					return pprof.Handler("block")
				},
			},
			admissionRouteDefinition{
				name:      "pprof-mutex",
				path:      admissionPprofMutex,
				kind:      admissionRouteOperational,
				fastOrder: admissionFastOperational,
				handler: func(*Server) http.Handler {
					return pprof.Handler("mutex")
				},
			},
			admissionRouteDefinition{
				name:      "pprof-threadcreate",
				path:      admissionPprofThread,
				kind:      admissionRouteOperational,
				fastOrder: admissionFastOperational,
				handler: func(*Server) http.Handler {
					return pprof.Handler("threadcreate")
				},
			},
		)
	}

	routes = append(routes,
		admissionRouteDefinition{
			name:      "object-complete",
			path:      admissionObjectPath,
			queries:   []string{admissionUploadIDQuery, "{uploadId}"},
			methods:   []string{http.MethodPost},
			kind:      admissionRouteS3,
			fastOrder: admissionFastQuery,
			matcher: func(r *http.Request, _ *mux.RouteMatch) bool {
				// CompleteMultipartUpload has uploadId but no partNumber.
				return r.URL.Query().Get("partNumber") == ""
			},
			handler: func(s *Server) http.Handler {
				return http.HandlerFunc(s.handleCompleteMultipartUpload)
			},
		},
		admissionRouteDefinition{
			name:        "object-query",
			path:        admissionObjectPath,
			queries:     []string{admissionUploadIDQuery, "{uploadId}"},
			methods:     []string{http.MethodPut, http.MethodPost, http.MethodDelete, http.MethodGet},
			kind:        admissionRouteS3,
			fastPath:    true,
			fastMethods: []string{http.MethodPost},
			fastOrder:   admissionFastObjectQuery,
			handler: func(s *Server) http.Handler {
				return http.HandlerFunc(s.handleObjectWithQuery)
			},
		},
		admissionRouteDefinition{
			name:      "object-initiate-multipart",
			path:      admissionObjectPath,
			queries:   []string{admissionUploadsQuery, ""},
			methods:   []string{http.MethodPost},
			kind:      admissionRouteS3,
			fastPath:  true,
			fastOrder: admissionFastObjectQuery,
			handler: func(s *Server) http.Handler {
				return http.HandlerFunc(s.handleInitiateMultipart)
			},
		},
		admissionRouteDefinition{
			name:      "object-tagging",
			path:      admissionObjectPath,
			queries:   []string{"tagging", ""},
			methods:   []string{http.MethodGet, http.MethodPut, http.MethodDelete},
			kind:      admissionRouteS3,
			fastOrder: admissionFastQuery,
			handler: func(s *Server) http.Handler {
				return http.HandlerFunc(s.handleObjectTagging)
			},
		},
		admissionRouteDefinition{
			name:      "object-acl",
			path:      admissionObjectPath,
			queries:   []string{"acl", ""},
			methods:   []string{http.MethodGet, http.MethodPut},
			kind:      admissionRouteS3,
			fastOrder: admissionFastQuery,
			handler: func(s *Server) http.Handler {
				return http.HandlerFunc(s.handleObjectACL)
			},
		},
		admissionRouteDefinition{
			name:      "object-basic",
			path:      admissionObjectPath,
			methods:   admissionBasicMethods[:],
			kind:      admissionRouteS3,
			fastPath:  true,
			fastOrder: admissionFastObject,
			handler: func(s *Server) http.Handler {
				return http.HandlerFunc(s.handleObject)
			},
		},
	)

	for i, prefix := range admissionBucketPaths {
		suffix := ""
		if i == 1 {
			suffix = "-slash"
		}
		routes = append(routes,
			admissionRouteDefinition{
				name:      "bucket-uploads" + suffix,
				path:      prefix,
				queries:   []string{admissionUploadsQuery, ""},
				methods:   []string{http.MethodGet},
				kind:      admissionRouteS3,
				fastOrder: admissionFastQuery,
				handler: func(s *Server) http.Handler {
					return http.HandlerFunc(s.handleBucketMultipartUploads)
				},
			},
			admissionRouteDefinition{
				name:      "bucket-list-v2" + suffix,
				path:      prefix,
				queries:   []string{"list-type", "2"},
				methods:   []string{http.MethodGet},
				kind:      admissionRouteS3,
				fastOrder: admissionFastQuery,
				handler: func(s *Server) http.Handler {
					return http.HandlerFunc(s.handleListObjectsV2)
				},
			},
			admissionRouteDefinition{
				name:      "bucket-versioning" + suffix,
				path:      prefix,
				queries:   []string{"versioning", ""},
				methods:   []string{http.MethodGet, http.MethodPut},
				kind:      admissionRouteS3,
				fastOrder: admissionFastQuery,
				handler: func(s *Server) http.Handler {
					return http.HandlerFunc(s.handleBucketVersioning)
				},
			},
			admissionRouteDefinition{
				name:      "bucket-acl" + suffix,
				path:      prefix,
				queries:   []string{"acl", ""},
				methods:   []string{http.MethodGet, http.MethodPut},
				kind:      admissionRouteS3,
				fastOrder: admissionFastQuery,
				handler: func(s *Server) http.Handler {
					return http.HandlerFunc(s.handleBucketACL)
				},
			},
			admissionRouteDefinition{
				name:      "bucket-lifecycle" + suffix,
				path:      prefix,
				queries:   []string{"lifecycle", ""},
				methods:   []string{http.MethodGet, http.MethodPut, http.MethodDelete},
				kind:      admissionRouteS3,
				fastOrder: admissionFastQuery,
				handler: func(s *Server) http.Handler {
					return http.HandlerFunc(s.handleBucketLifecycle)
				},
			},
			admissionRouteDefinition{
				name:      "bucket-policy" + suffix,
				path:      prefix,
				queries:   []string{"policy", ""},
				methods:   []string{http.MethodGet, http.MethodPut, http.MethodDelete},
				kind:      admissionRouteS3,
				fastOrder: admissionFastQuery,
				handler: func(s *Server) http.Handler {
					return http.HandlerFunc(s.handleBucketPolicy)
				},
			},
			admissionRouteDefinition{
				name:      "bucket-cors" + suffix,
				path:      prefix,
				queries:   []string{"cors", ""},
				methods:   []string{http.MethodGet, http.MethodPut, http.MethodDelete},
				kind:      admissionRouteS3,
				fastOrder: admissionFastQuery,
				handler: func(s *Server) http.Handler {
					return http.HandlerFunc(s.handleBucketCORS)
				},
			},
			admissionRouteDefinition{
				name:      "bucket-tagging" + suffix,
				path:      prefix,
				queries:   []string{"tagging", ""},
				methods:   []string{http.MethodGet, http.MethodPut, http.MethodDelete},
				kind:      admissionRouteS3,
				fastOrder: admissionFastQuery,
				handler: func(s *Server) http.Handler {
					return http.HandlerFunc(s.handleBucketTagging)
				},
			},
			admissionRouteDefinition{
				name:      "bucket-location" + suffix,
				path:      prefix,
				queries:   []string{"location", ""},
				methods:   []string{http.MethodGet},
				kind:      admissionRouteS3,
				fastOrder: admissionFastQuery,
				handler: func(s *Server) http.Handler {
					return http.HandlerFunc(s.handleBucketLocation)
				},
			},
			admissionRouteDefinition{
				name:      "bucket-delete-objects" + suffix,
				path:      prefix,
				queries:   []string{admissionDeleteQuery, ""},
				methods:   []string{http.MethodPost},
				kind:      admissionRouteS3,
				fastPath:  true,
				fastOrder: admissionFastQuery,
				handler: func(s *Server) http.Handler {
					return http.HandlerFunc(s.handleDeleteObjects)
				},
			},
			admissionRouteDefinition{
				name:      "bucket-basic" + suffix,
				path:      prefix,
				methods:   admissionBasicMethods[:],
				kind:      admissionRouteS3,
				fastPath:  true,
				fastOrder: admissionFastBucket,
				handler: func(s *Server) http.Handler {
					return http.HandlerFunc(s.handleBucket)
				},
			},
		)
	}

	routes = append(routes, admissionRouteDefinition{
		name:      "service-list-buckets",
		path:      admissionServicePath,
		methods:   []string{http.MethodGet},
		kind:      admissionRouteS3,
		fastPath:  true,
		fastOrder: admissionFastService,
		handler: func(s *Server) http.Handler {
			return http.HandlerFunc(s.handleListBuckets)
		},
	})

	return routes
}

func registerAdmissionRoute(router *mux.Router, route admissionRouteDefinition, handler http.Handler) {
	r := router.Handle(route.path, handler)
	if len(route.queries) > 0 {
		r.Queries(route.queries...)
	}
	if len(route.methods) > 0 {
		r.Methods(route.methods...)
	}
	if route.matcher != nil {
		r.MatcherFunc(route.matcher)
	}
	r.Name(route.name)
}

type admissionRouteClassifier struct {
	router *mux.Router
}

func newAdmissionRouteClassifier(pprofEnabled bool) *admissionRouteClassifier {
	routes := admissionRouteDefinitions(pprofEnabled)
	sort.SliceStable(routes, func(i, j int) bool {
		return routes[i].fastOrder < routes[j].fastOrder
	})

	r := mux.NewRouter()
	r.SkipClean(true)
	for _, route := range routes {
		if !route.fastPath && route.kind != admissionRouteOperational {
			continue
		}
		fastRoute := route
		if route.fastMethods != nil {
			fastRoute.methods = route.fastMethods
		}
		registerAdmissionRoute(r, fastRoute, &admissionRouteMarker{kind: route.kind})
	}
	return &admissionRouteClassifier{router: r}
}

func (c *admissionRouteClassifier) classify(r *http.Request) admissionRouteKind {
	var match mux.RouteMatch
	if !c.router.Match(r, &match) || match.Route == nil {
		return admissionRouteOther
	}
	marker, ok := match.Route.GetHandler().(*admissionRouteMarker)
	if !ok {
		return admissionRouteOther
	}
	return marker.kind
}

func isOperationalRouteTemplate(template string) bool {
	return template == admissionHealthPath ||
		template == admissionMetricsPath ||
		(len(template) >= len(admissionPprofPath) && template[:len(admissionPprofPath)] == admissionPprofPath)
}

func (s *Server) admissionFastPath(next http.Handler) http.Handler {
	classifier := newAdmissionRouteClassifier(s.pprofEnabled)
	shed := s.connectionTrackingMiddleware(http.HandlerFunc(s.shedAdmission))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sem := s.admissionSem
		if sem == nil || len(sem) != cap(sem) || classifier.classify(r) != admissionRouteS3 {
			next.ServeHTTP(w, r)
			return
		}

		// The length check above is only a snapshot. Confirm saturation without
		// changing the admission result if a slot opened concurrently.
		select {
		case sem <- struct{}{}:
			<-sem
			next.ServeHTTP(w, r)
		default:
			shed.ServeHTTP(w, r)
		}
	})
}

func (s *Server) shedAdmission(w http.ResponseWriter, r *http.Request) {
	metrics.AdmissionShed.Inc()
	s3err.WriteError(w, r, s3err.ErrSlowDown)
}
