package handlers

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

type admissionRouteSignature struct {
	name    string
	path    string
	queries []string
	methods []string
}

func signatureForAdmissionRoute(route *mux.Route) admissionRouteSignature {
	signature := admissionRouteSignature{name: route.GetName()}
	signature.path, _ = route.GetPathTemplate()
	signature.queries, _ = route.GetQueriesTemplates()
	signature.methods, _ = route.GetMethods()
	if len(signature.queries) == 0 {
		signature.queries = nil
	}
	if len(signature.methods) == 0 {
		signature.methods = nil
	}
	return signature
}

func signatureForAdmissionDefinition(route admissionRouteDefinition) admissionRouteSignature {
	var queries []string
	for i := 0; i < len(route.queries); i += 2 {
		queries = append(queries, route.queries[i]+"="+route.queries[i+1])
	}
	return admissionRouteSignature{
		name:    route.name,
		path:    route.path,
		queries: queries,
		methods: route.methods,
	}
}

func TestAdmissionRouteTableMatchesRegisteredRoutes(t *testing.T) {
	for _, pprofEnabled := range []bool{false, true} {
		router := (&Server{pprofEnabled: pprofEnabled}).setupRouter()
		definitions := admissionRouteDefinitions(pprofEnabled)
		want := make([]admissionRouteSignature, 0, len(definitions))
		for _, definition := range definitions {
			want = append(want, signatureForAdmissionDefinition(definition))
		}

		var got []admissionRouteSignature
		if err := router.Walk(func(route *mux.Route, _ *mux.Router, _ []*mux.Route) error {
			got = append(got, signatureForAdmissionRoute(route))
			return nil
		}); err != nil {
			t.Fatalf("pprof=%v walk setup router: %v", pprofEnabled, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("pprof=%v registered routes differ from shared definitions:\n got: %#v\nwant: %#v", pprofEnabled, got, want)
		}

		classifier := newAdmissionRouteClassifier(pprofEnabled)
		classifierNames := make(map[string]struct{}, len(definitions))
		if err := classifier.router.Walk(func(route *mux.Route, _ *mux.Router, _ []*mux.Route) error {
			classifierNames[route.GetName()] = struct{}{}
			return nil
		}); err != nil {
			t.Fatalf("pprof=%v walk classifier: %v", pprofEnabled, err)
		}
		wantClassifierRoutes := 0
		for _, definition := range definitions {
			if definition.fastPath || definition.kind == admissionRouteOperational {
				wantClassifierRoutes++
				if _, ok := classifierNames[definition.name]; !ok {
					t.Errorf("pprof=%v classifier is missing %q", pprofEnabled, definition.name)
				}
			}
		}
		if len(classifierNames) != wantClassifierRoutes {
			t.Fatalf("pprof=%v classifier has %d routes, want %d", pprofEnabled, len(classifierNames), wantClassifierRoutes)
		}

		for _, definition := range definitions {
			methods := definition.methods
			if len(methods) == 0 {
				methods = []string{http.MethodGet}
			}
			for _, method := range methods {
				req := admissionRouteSampleRequest(definition, method)
				if got := classifier.classify(req); got != definition.kind {
					t.Fatalf("pprof=%v %s %s: classifier kind = %d, want %d", pprofEnabled, method, req.URL.RequestURI(), got, definition.kind)
				}
			}
		}
	}
}

func admissionRouteSampleRequest(route admissionRouteDefinition, method string) *http.Request {
	path := strings.Replace(route.path, "{bucket}", "bucket", 1)
	path = strings.Replace(path, "{object:.+}", "object", 1)
	var queryParts []string
	for i := 0; i < len(route.queries); i += 2 {
		value := route.queries[i+1]
		if strings.HasPrefix(value, "{") && strings.HasSuffix(value, "}") {
			value = "value"
		}
		queryParts = append(queryParts, route.queries[i]+"="+value)
	}
	if len(queryParts) > 0 {
		path += "?" + strings.Join(queryParts, "&")
	}
	return httptest.NewRequest(method, "http://localhost"+path, nil)
}

func classifyRegisteredAdmissionRoute(router *mux.Router, r *http.Request) admissionRouteKind {
	var match mux.RouteMatch
	if !router.Match(r, &match) || match.Route == nil {
		return admissionRouteOther
	}
	template, err := match.Route.GetPathTemplate()
	if err != nil {
		return admissionRouteOther
	}
	if isOperationalRouteTemplate(template) {
		return admissionRouteOperational
	}
	return admissionRouteS3
}

func TestAdmissionRouteClassifierMatchesRegisteredRoutes(t *testing.T) {
	cases := []struct {
		name   string
		method string
		path   string
	}{
		{name: "object get", method: http.MethodGet, path: "/bucket/key"},
		{name: "object head", method: http.MethodHead, path: "/bucket/key"},
		{name: "object put", method: http.MethodPut, path: "/bucket/key?x=y"},
		{name: "object delete", method: http.MethodDelete, path: "/bucket/key"},
		{name: "object upload part", method: http.MethodPut, path: "/bucket/key?uploadId=upload&partNumber=1"},
		{name: "object complete", method: http.MethodPost, path: "/bucket/key?uploadId=upload"},
		{name: "object complete with empty part", method: http.MethodPost, path: "/bucket/key?uploadId=upload&partNumber="},
		{name: "object initiate", method: http.MethodPost, path: "/bucket/key?uploads="},
		{name: "object initiate arbitrary value", method: http.MethodPost, path: "/bucket/key?uploads=anything"},
		{name: "object tagging", method: http.MethodGet, path: "/bucket/key?tagging="},
		{name: "object acl", method: http.MethodPut, path: "/bucket/key?acl="},
		{name: "bucket", method: http.MethodGet, path: "/bucket"},
		{name: "bucket slash", method: http.MethodGet, path: "/bucket/"},
		{name: "bucket arbitrary query", method: http.MethodDelete, path: "/bucket?unregistered=value"},
		{name: "bucket versioning", method: http.MethodPut, path: "/bucket/?versioning="},
		{name: "bucket delete objects", method: http.MethodPost, path: "/bucket?delete="},
		{name: "bucket delete objects arbitrary value", method: http.MethodPost, path: "/bucket/?delete=anything"},
		{name: "list buckets", method: http.MethodGet, path: "/"},
		{name: "list buckets query", method: http.MethodGet, path: "/?x=y"},
		{name: "health", method: http.MethodGet, path: "/health"},
		{name: "health query", method: http.MethodGet, path: "/health?x=y"},
		{name: "metrics", method: http.MethodGet, path: "/metrics"},
		{name: "pprof-looking object", method: http.MethodGet, path: "/debug/pprof/object"},
		{name: "wrong object method", method: http.MethodPatch, path: "/bucket/key"},
		{name: "wrong bucket method", method: http.MethodPost, path: "/bucket"},
		{name: "wrong root method", method: http.MethodHead, path: "/"},
		{name: "wrong health method", method: http.MethodPost, path: "/health"},
		{name: "wrong metrics method", method: http.MethodPost, path: "/metrics"},
		{name: "unknown path", method: http.MethodGet, path: "/unknown"},
		{name: "double slash object", method: http.MethodGet, path: "/bucket//key"},
		{name: "dot path object", method: http.MethodGet, path: "/bucket/../key"},
	}

	for _, pprofEnabled := range []bool{false, true} {
		registered := (&Server{pprofEnabled: pprofEnabled}).setupRouter()
		classifier := newAdmissionRouteClassifier(pprofEnabled)

		for _, tc := range cases {
			t.Run(func() string {
				if pprofEnabled {
					return "pprof-enabled/" + tc.name
				}
				return "pprof-disabled/" + tc.name
			}(), func(t *testing.T) {
				req := httptest.NewRequest(tc.method, "http://localhost"+tc.path, nil)
				want := classifyRegisteredAdmissionRoute(registered, req)
				got := classifier.classify(req)
				if got != want {
					t.Fatalf("classifier kind = %d, registered router kind = %d", got, want)
				}
			})
		}

		for _, path := range admissionPprofPaths {
			for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPatch} {
				req := httptest.NewRequest(method, "http://localhost"+path, nil)
				want := classifyRegisteredAdmissionRoute(registered, req)
				if got := classifier.classify(req); got != want {
					t.Errorf("%s %s: classifier kind = %d, registered router kind = %d", method, path, got, want)
				}
			}
		}
	}
}

func TestAdmissionRouteClassifierMatchesQueryAndPathVariations(t *testing.T) {
	methods := []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodPost, http.MethodDelete, http.MethodPatch, http.MethodOptions}
	paths := []string{"/", "/bucket", "/bucket/", "/bucket/key", "/bucket//key", "/bucket/../key", "//bucket", "/debug/pprof/", "/debug/pprof/heap", "/debug/pprof/object", "/health", "/metrics"}
	queries := []string{"", "uploadId=x", "uploadId=", "uploads", "uploads=x", "delete", "delete=x", "tagging=", "acl=", "list-type=2", "x", "x=1&x=2", "uploadId=x&partNumber=1", "partNumber=1&uploadId=x", "%zz", "foo=bar;baz"}

	for _, pprofEnabled := range []bool{false, true} {
		registered := (&Server{pprofEnabled: pprofEnabled}).setupRouter()
		classifier := newAdmissionRouteClassifier(pprofEnabled)
		for _, method := range methods {
			for _, path := range paths {
				for _, query := range queries {
					req := &http.Request{
						Method: method,
						URL:    &url.URL{Path: path, RawQuery: query},
						Header: make(http.Header),
					}
					want := classifyRegisteredAdmissionRoute(registered, req)
					if got := classifier.classify(req); got != want {
						t.Fatalf("pprof=%v %s %s?%s: classifier kind = %d, registered router kind = %d", pprofEnabled, method, path, query, got, want)
					}
				}
			}
		}
	}
}

func TestAdmissionFastPathPreservesShedResponse(t *testing.T) {
	cases := []struct {
		name   string
		method string
		path   string
	}{
		{name: "object", method: http.MethodGet, path: "/bucket/key"},
		{name: "bucket", method: http.MethodGet, path: "/bucket"},
		{name: "bucket trailing slash", method: http.MethodGet, path: "/bucket/"},
		{name: "multipart", method: http.MethodPost, path: "/bucket/key?uploadId=upload&partNumber=1"},
		{name: "list buckets", method: http.MethodGet, path: "/"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := &Server{admissionSem: make(chan struct{}, 1)}
			base.admissionSem <- struct{}{}
			candidate := &Server{admissionSem: make(chan struct{}, 1)}
			candidate.admissionSem <- struct{}{}

			baseRouter := base.setupRouter()
			candidateRouter := candidate.admissionFastPath(candidate.setupRouter())
			baseResponse := httptest.NewRecorder()
			candidateResponse := httptest.NewRecorder()
			baseRouter.ServeHTTP(baseResponse, httptest.NewRequest(tc.method, "http://localhost"+tc.path, nil))
			candidateRouter.ServeHTTP(candidateResponse, httptest.NewRequest(tc.method, "http://localhost"+tc.path, nil))

			if candidateResponse.Code != baseResponse.Code {
				t.Fatalf("status = %d, base status = %d", candidateResponse.Code, baseResponse.Code)
			}
			if !bytes.Equal(candidateResponse.Body.Bytes(), baseResponse.Body.Bytes()) {
				t.Fatalf("response body differs from base:\n candidate: %s\n base: %s", candidateResponse.Body, baseResponse.Body)
			}
			if candidateResponse.Header().Get("Content-Type") != baseResponse.Header().Get("Content-Type") {
				t.Fatalf("content type = %q, base content type = %q", candidateResponse.Header().Get("Content-Type"), baseResponse.Header().Get("Content-Type"))
			}
		})
	}
}

func TestAdmissionFastPathDelegatesNonS3AndOperationalRequests(t *testing.T) {
	cases := []struct {
		name   string
		method string
		path   string
	}{
		{name: "health", method: http.MethodGet, path: "/health"},
		{name: "metrics", method: http.MethodGet, path: "/metrics"},
		{name: "enabled pprof", method: http.MethodGet, path: "/debug/pprof/heap"},
		{name: "pprof-like object", method: http.MethodGet, path: "/debug/pprof/object"},
		{name: "method not allowed", method: http.MethodPost, path: "/bucket"},
		{name: "not found", method: http.MethodGet, path: "//unknown"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{admissionSem: make(chan struct{}, 1), pprofEnabled: true}
			s.admissionSem <- struct{}{}
			delegated := 0
			handler := s.admissionFastPath(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				delegated++
				w.WriteHeader(http.StatusTeapot)
			}))

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(tc.method, "http://localhost"+tc.path, nil))
			if tc.name == "pprof-like object" {
				if rec.Code != http.StatusServiceUnavailable {
					t.Fatalf("pprof-like object status = %d, want 503", rec.Code)
				}
				if delegated != 0 {
					t.Fatalf("pprof-like object delegated %d times", delegated)
				}
				return
			}
			if rec.Code != http.StatusTeapot || delegated != 1 {
				t.Fatalf("status = %d, delegated = %d, want 418 and one delegation", rec.Code, delegated)
			}
		})
	}
}

func TestAdmissionFastPathKeepsOperationalAndFallbackRoutesAvailable(t *testing.T) {
	s := NewServer(nil, "127.0.0.1", 0, true, 1)
	s.admissionSem <- struct{}{}

	for _, path := range []string{admissionHealthPath, admissionMetricsPath, admissionPprofPath} {
		rec := httptest.NewRecorder()
		s.Router().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://localhost"+path, nil))
		if rec.Code == http.StatusServiceUnavailable {
			t.Fatalf("operational route %s was shed", path)
		}
	}

	for _, tc := range []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/debug/pprof/object"},
		{method: http.MethodGet, path: "//unknown"},
		{method: http.MethodPost, path: "/bucket"},
	} {
		rec := httptest.NewRecorder()
		s.Router().ServeHTTP(rec, httptest.NewRequest(tc.method, "http://localhost"+tc.path, nil))
		if tc.path == "/debug/pprof/object" {
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("pprof-looking object status = %d, want 503", rec.Code)
			}
		} else if rec.Code == http.StatusServiceUnavailable {
			t.Fatalf("fallback route %s %s was shed", tc.method, tc.path)
		}
	}
}

func TestAdmissionFastPathDelegatesWhenCapacityIsAvailable(t *testing.T) {
	s := &Server{admissionSem: make(chan struct{}, 1)}
	called := 0
	handler := s.admissionFastPath(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called++
		w.WriteHeader(http.StatusNoContent)
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://localhost/bucket/key", nil))
	if rec.Code != http.StatusNoContent || called != 1 {
		t.Fatalf("status = %d, called = %d, want 204 and one delegation", rec.Code, called)
	}
	if got := len(s.admissionSem); got != 0 {
		t.Fatalf("admission semaphore length = %d, want 0", got)
	}
}
