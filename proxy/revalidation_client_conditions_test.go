package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/metrics"
)

const (
	revalidationTestBucket = "revalidation-bucket"
	revalidationTestKey    = "object"
)

type revalidationTestOrigin struct {
	body         []byte
	etag         string
	lastModified time.Time
}

func (o *revalidationTestOrigin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if o.etag != "" {
		w.Header().Set("ETag", o.etag)
	}
	if !o.lastModified.IsZero() {
		w.Header().Set("Last-Modified", o.lastModified.UTC().Format(http.TimeFormat))
	}
	w.Header().Set("Content-Type", "application/octet-stream")

	// A wildcard represents an existing object even when that representation
	// has no ETag. ServeContent supplies the origin's normal ETag/IMS precedence
	// and byte-range behavior for all other conditionals.
	writer := &revalidationTestResponseWriter{ResponseWriter: w, lastModified: o.lastModified}
	if strings.TrimSpace(r.Header.Get("If-None-Match")) == "*" {
		writer.WriteHeader(http.StatusNotModified)
		return
	}
	http.ServeContent(writer, r, "object", o.lastModified, bytes.NewReader(o.body))
}

type revalidationTestResponseWriter struct {
	http.ResponseWriter
	lastModified time.Time
}

func (w *revalidationTestResponseWriter) WriteHeader(status int) {
	if status == http.StatusNotModified && !w.lastModified.IsZero() {
		w.Header().Set("Last-Modified", w.lastModified.UTC().Format(http.TimeFormat))
	}
	w.ResponseWriter.WriteHeader(status)
}

type revalidationTestCacheClient struct {
	cacheclient.CacheClient
	bodyReads  atomic.Int64
	rangeReads atomic.Int64
	readPrefix []byte
	readErr    error
}

func (c *revalidationTestCacheClient) GetStream(ctx context.Context, key string, w io.Writer) error {
	c.bodyReads.Add(1)
	if c.readErr != nil {
		if len(c.readPrefix) != 0 {
			if _, err := w.Write(c.readPrefix); err != nil {
				return err
			}
		}
		return c.readErr
	}
	return c.CacheClient.GetStream(ctx, key, w)
}

func (c *revalidationTestCacheClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	c.rangeReads.Add(1)
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

type revalidationHTTPForwarder struct {
	*mockForwarder
	client              *http.Client
	originURL           string
	conditionalErr      error
	conditionalResponse *http.Response
	forwardCalls        atomic.Int64
	fullObjectCalls     atomic.Int64
	forwardedINM        string
	forwardedIMS        string
}

func (f *revalidationHTTPForwarder) DoConditionalGetRequest(ctx context.Context, bucket, key, accessKey, secretKey, etag string, lastModified int64, rangeHeader string) (*http.Response, error) {
	if f.conditionalErr != nil {
		return nil, f.conditionalErr
	}
	if f.conditionalResponse != nil {
		return f.conditionalResponse, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.originURL+"/"+bucket+"/"+key, nil)
	if err != nil {
		return nil, err
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	if lastModified > 0 {
		req.Header.Set("If-Modified-Since", time.Unix(lastModified, 0).UTC().Format(http.TimeFormat))
	}
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	return f.client.Do(req)
}

func (f *revalidationHTTPForwarder) Forward(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	f.forwardCalls.Add(1)
	f.forwardedINM = r.Header.Get("If-None-Match")
	f.forwardedIMS = r.Header.Get("If-Modified-Since")
	req, err := http.NewRequestWithContext(ctx, r.Method, f.originURL+r.URL.RequestURI(), r.Body)
	if err != nil {
		return err
	}
	req.Header = r.Header.Clone()
	req.ContentLength = r.ContentLength
	resp, err := f.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	n, err := io.Copy(w, resp.Body)
	metrics.BytesTransferred.WithLabelValues("out").Add(float64(n))
	return err
}

func (f *revalidationHTTPForwarder) DoFullObjectRequest(ctx context.Context, bucket, key, accessKey, secretKey string) (*http.Response, error) {
	f.fullObjectCalls.Add(1)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.originURL+"/"+bucket+"/"+key, nil)
	if err != nil {
		return nil, err
	}
	return f.client.Do(req)
}

type revalidationTestFixture struct {
	service      *Service
	cache        *cache.Cache
	memory       *cacheclient.MemoryCache
	cacheClient  *revalidationTestCacheClient
	forwarder    *revalidationHTTPForwarder
	originServer *httptest.Server
	gateway      *httptest.Server
	client       *http.Client
}

func newRevalidationTestFixture(t *testing.T, origin *revalidationTestOrigin, meta *cache.CachedObjectMeta, body []byte) *revalidationTestFixture {
	t.Helper()
	originServer := httptest.NewServer(origin)
	originClient := &http.Client{Transport: &http.Transport{}, Timeout: 5 * time.Second}
	mock := &mockForwarder{}
	forwarder := &revalidationHTTPForwarder{mockForwarder: mock, client: originClient, originURL: originServer.URL}
	memory := cacheclient.NewMemoryCache()
	cacheClient := &revalidationTestCacheClient{CacheClient: memory}
	cfg := config.NewDefault()
	cacheStore := cache.NewCacheWithClient(cacheClient, &cfg.Cache)
	service := NewService(forwarder, cacheStore, cfg)
	if meta != nil {
		if err := cacheStore.PutWithMeta(context.Background(), meta.Bucket, meta.Key, meta, body, 0); err != nil {
			t.Fatalf("seed cached object: %v", err)
		}
	}
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := service.HandleGetObject(w, r); err != nil {
			panic(http.ErrAbortHandler)
		}
	}))
	client := &http.Client{Transport: &http.Transport{}, Timeout: 5 * time.Second}
	t.Cleanup(func() {
		gateway.Close()
		service.Stop()
		client.CloseIdleConnections()
		originClient.CloseIdleConnections()
		originServer.Close()
	})
	return &revalidationTestFixture{
		service: service, cache: cacheStore, memory: memory, cacheClient: cacheClient,
		forwarder: forwarder, originServer: originServer, gateway: gateway, client: client,
	}
}

type revalidationTestResponse struct {
	status int
	header http.Header
	body   []byte
}

func readRevalidationTestResponse(t *testing.T, client *http.Client, target string, headers http.Header) revalidationTestResponse {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header = headers.Clone()
	if req.Header.Get("Authorization") == "" {
		// The mock forwarder accepts this marker; it keeps the 206 rewarm on
		// the authenticated branch rather than the anonymous public-read path.
		req.Header.Set("Authorization", "test")
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", target, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read GET %s body: %v", target, err)
	}
	return revalidationTestResponse{status: resp.StatusCode, header: resp.Header.Clone(), body: body}
}

func compareRevalidationTestResponses(t *testing.T, direct, gateway revalidationTestResponse) {
	t.Helper()
	if direct.status != gateway.status {
		t.Errorf("gateway status = %d, direct origin status = %d", gateway.status, direct.status)
	}
	for _, name := range []string{"ETag", "Last-Modified", "Content-Type", "Content-Length", "Content-Range"} {
		if direct.status == http.StatusNotModified && name == "Content-Type" {
			continue
		}
		if got, want := gateway.header.Get(name), direct.header.Get(name); got != want {
			t.Errorf("gateway %s = %q, direct origin %s = %q", name, got, name, want)
		}
	}
	if !bytes.Equal(gateway.body, direct.body) {
		t.Errorf("gateway body = %q (%d bytes), direct origin body = %q (%d bytes)", gateway.body, len(gateway.body), direct.body, len(direct.body))
	}
}

func TestRevalidationClientConditionsMatchOrigin(t *testing.T) {
	oldModified := time.Date(2026, 10, 27, 3, 4, 5, 0, time.UTC)
	newModified := oldModified.Add(time.Hour)
	currentBody := []byte("current object body")
	oldBody := []byte("older object bytes")
	weakCachedBody := []byte(`{"n":1}`)
	weakCurrentBody := []byte(`{ "n": 1 }`)
	futureIMS := oldModified.Add(24 * time.Hour).Format(http.TimeFormat)

	tests := []struct {
		name             string
		cachedETag       string
		cachedModified   time.Time
		cachedBody       []byte
		originETag       string
		originModified   time.Time
		originBody       []byte
		cacheControl     string
		ifNoneMatch      string
		ifModifiedSince  string
		rangeHeader      string
		wantStatus       int
		wantBody         []byte
		wantXCache       string
		wantFullReads    int64
		wantRangeReads   int64
		wantForwardCalls int64
		wantOutBytes     int64
		wantRewarm       bool
		wantCachedETag   string
		wantCachedBody   []byte
		wantStoredETag   string
	}{
		{
			name: "unchanged matching ETag list avoids cache read", cachedETag: `"current"`, cachedModified: oldModified,
			originETag: `"current"`, originModified: oldModified, originBody: currentBody,
			ifNoneMatch: `"other", W/"current"`, wantStatus: http.StatusNotModified, wantXCache: XCacheHit, wantFullReads: 0,
		},
		{
			name: "weak-to-strong match returns bodyless 304", cachedETag: `W/"current"`, cachedModified: oldModified,
			originETag: `"current"`, originModified: oldModified, originBody: weakCurrentBody, cachedBody: weakCachedBody,
			ifNoneMatch: `"current"`, wantStatus: http.StatusNotModified, wantXCache: XCacheHit,
			wantStoredETag: `W/"current"`,
		},
		{
			name: "weak-to-strong ETag forwards when client needs exact body", cachedETag: `W/"current"`, cachedModified: oldModified,
			originETag: `"current"`, originModified: oldModified, originBody: weakCurrentBody, cachedBody: weakCachedBody,
			ifNoneMatch: `"other"`, wantStatus: http.StatusOK, wantBody: weakCurrentBody, wantXCache: XCacheMiss,
			wantForwardCalls: 1, wantOutBytes: int64(len(weakCurrentBody)), wantStoredETag: `W/"current"`,
		},
		{
			name: "weak-to-strong ETag forwards requested range", cachedETag: `W/"current"`, cachedModified: oldModified,
			originETag: `"current"`, originModified: oldModified, originBody: weakCurrentBody, cachedBody: weakCachedBody,
			ifNoneMatch: `"other"`, rangeHeader: "bytes=0-3", wantStatus: http.StatusPartialContent,
			wantBody: weakCurrentBody[:4], wantXCache: XCacheMiss, wantForwardCalls: 1, wantOutBytes: 4,
			wantStoredETag: `W/"current"`,
		},
		{
			name: "mismatching If-None-Match takes precedence over matching IMS", cachedETag: `"current"`, cachedModified: oldModified,
			originETag: `"current"`, originModified: oldModified, originBody: currentBody,
			ifNoneMatch: `"other"`, ifModifiedSince: futureIMS, wantStatus: http.StatusOK, wantBody: currentBody, wantXCache: XCacheHit, wantFullReads: 1, wantOutBytes: int64(len(currentBody)),
		},
		{
			name: "max-age zero matching ETag avoids cache read", cachedETag: `"current"`, cachedModified: oldModified,
			originETag: `"current"`, originModified: oldModified, originBody: currentBody,
			cacheControl: "max-age=0", ifNoneMatch: `"current"`, wantStatus: http.StatusNotModified, wantXCache: XCacheHit,
		},
		{
			name: "unchanged matching If-Modified-Since avoids cache read", cachedETag: `"current"`, cachedModified: oldModified,
			originETag: `"current"`, originModified: oldModified, originBody: currentBody,
			ifModifiedSince: oldModified.Format(http.TimeFormat), wantStatus: http.StatusNotModified, wantXCache: XCacheHit, wantFullReads: 0,
		},
		{
			name: "unchanged older If-Modified-Since serves cached body", cachedETag: `"current"`, cachedModified: oldModified,
			originETag: `"current"`, originModified: oldModified, originBody: currentBody,
			ifModifiedSince: oldModified.Add(-time.Hour).Format(http.TimeFormat), wantStatus: http.StatusOK, wantBody: currentBody, wantXCache: XCacheHit, wantFullReads: 1, wantOutBytes: int64(len(currentBody)),
		},
		{
			name: "matching ETag wins over Range", cachedETag: `"current"`, cachedModified: oldModified,
			originETag: `"current"`, originModified: oldModified, originBody: currentBody,
			ifNoneMatch: `"current"`, rangeHeader: "bytes=0-4", wantStatus: http.StatusNotModified, wantXCache: XCacheHit, wantFullReads: 0,
		},
		{
			name: "mismatching validator still serves requested range", cachedETag: `"current"`, cachedModified: oldModified,
			originETag: `"current"`, originModified: oldModified, originBody: currentBody,
			ifNoneMatch: `"other"`, ifModifiedSince: futureIMS, rangeHeader: "bytes=0-4",
			wantStatus: http.StatusPartialContent, wantBody: currentBody[:5], wantXCache: XCacheHit, wantRangeReads: 1, wantOutBytes: 5,
		},
		{
			name: "old client validator does not match changed origin", cachedETag: `"old"`, cachedModified: oldModified,
			originETag: `"new"`, originModified: newModified, originBody: currentBody,
			ifNoneMatch: `"old"`, wantStatus: http.StatusOK, wantBody: currentBody, wantXCache: XCacheRevalidated, wantOutBytes: int64(len(currentBody)), wantCachedETag: `"new"`, wantCachedBody: currentBody,
		},
		{
			name: "older IMS does not match changed 200", cachedETag: `"old"`, cachedModified: oldModified,
			originETag: `"new"`, originModified: newModified, originBody: currentBody,
			ifModifiedSince: oldModified.Format(http.TimeFormat), wantStatus: http.StatusOK, wantBody: currentBody,
			wantXCache: XCacheRevalidated, wantOutBytes: int64(len(currentBody)), wantCachedETag: `"new"`, wantCachedBody: currentBody,
		},
		{
			name: "new IMS matches changed 200 while cache is refreshed", cachedETag: `"old"`, cachedModified: oldModified,
			originETag: `"new"`, originModified: newModified, originBody: currentBody,
			ifModifiedSince: newModified.Format(http.TimeFormat), wantStatus: http.StatusNotModified,
			wantXCache: XCacheRevalidated, wantCachedETag: `"new"`, wantCachedBody: currentBody,
		},
		{
			name: "new client validator matches changed 200 while cache is refreshed", cachedETag: `"old"`, cachedModified: oldModified,
			originETag: `"new"`, originModified: newModified, originBody: currentBody,
			ifNoneMatch: `"new"`, wantStatus: http.StatusNotModified, wantXCache: XCacheRevalidated, wantCachedETag: `"new"`, wantCachedBody: currentBody,
		},
		{
			name: "old client validator does not match changed 206", cachedETag: `"old"`, cachedModified: oldModified,
			originETag: `"new"`, originModified: newModified, originBody: currentBody,
			ifNoneMatch: `"old"`, rangeHeader: "bytes=0-4", wantStatus: http.StatusPartialContent,
			wantBody: currentBody[:5], wantXCache: XCacheRevalidated, wantOutBytes: 5, wantRewarm: true, wantCachedETag: `"new"`, wantCachedBody: currentBody,
		},
		{
			name: "new client validator matches changed 206 while full object is rewarmed", cachedETag: `"old"`, cachedModified: oldModified,
			originETag: `"new"`, originModified: newModified, originBody: currentBody,
			ifNoneMatch: `"new"`, rangeHeader: "bytes=0-4", wantStatus: http.StatusNotModified,
			wantXCache: XCacheRevalidated, wantRewarm: true, wantCachedETag: `"new"`, wantCachedBody: currentBody,
		},
		{
			name: "origin 304 Last-Modified update is used for client IMS", cachedETag: `"current"`, cachedModified: oldModified,
			originETag: `"current"`, originModified: newModified, originBody: currentBody,
			ifModifiedSince: oldModified.Add(30 * time.Minute).Format(http.TimeFormat),
			wantStatus:      http.StatusOK, wantBody: currentBody, wantXCache: XCacheHit, wantFullReads: 1, wantOutBytes: int64(len(currentBody)),
		},
		{
			name: "wildcard matches changed representation without an ETag", cachedETag: `"old"`, cachedModified: oldModified,
			originModified: newModified, originBody: currentBody,
			ifNoneMatch: "*", wantStatus: http.StatusNotModified, wantXCache: XCacheRevalidated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			origin := &revalidationTestOrigin{body: tt.originBody, etag: tt.originETag, lastModified: tt.originModified}
			cachedBody := tt.cachedBody
			if cachedBody == nil {
				cachedBody = oldBody
				if tt.cachedETag != "" && tt.cachedETag == tt.originETag {
					cachedBody = tt.originBody
				}
			}
			cachedMeta := &cache.CachedObjectMeta{
				Bucket: revalidationTestBucket, Key: revalidationTestKey, ETag: tt.cachedETag,
				ContentType: "application/octet-stream", ContentLength: int64(len(cachedBody)),
				LastModified: tt.cachedModified.Unix(), StatusCode: http.StatusOK,
			}
			fixture := newRevalidationTestFixture(t, origin, cachedMeta, cachedBody)
			headers := make(http.Header)
			cacheControl := tt.cacheControl
			if cacheControl == "" {
				cacheControl = "no-cache"
			}
			headers.Set("Cache-Control", cacheControl)
			if tt.ifNoneMatch != "" {
				headers.Set("If-None-Match", tt.ifNoneMatch)
			}
			if tt.ifModifiedSince != "" {
				headers.Set("If-Modified-Since", tt.ifModifiedSince)
			}
			if tt.rangeHeader != "" {
				headers.Set("Range", tt.rangeHeader)
			}
			direct := readRevalidationTestResponse(t, fixture.client, fixture.originServer.URL+"/"+revalidationTestBucket+"/"+revalidationTestKey, headers)
			outBytesBefore := testutil.ToFloat64(metrics.BytesTransferred.WithLabelValues("out"))
			gateway := readRevalidationTestResponse(t, fixture.client, fixture.gateway.URL+"/"+revalidationTestBucket+"/"+revalidationTestKey, headers)
			if direct.status != tt.wantStatus {
				t.Errorf("direct origin status = %d, want %d", direct.status, tt.wantStatus)
			}
			if !bytes.Equal(direct.body, tt.wantBody) {
				t.Errorf("direct origin body = %q, want %q", direct.body, tt.wantBody)
			}
			compareRevalidationTestResponses(t, direct, gateway)
			if got := gateway.header.Get(XCacheHeader); got != tt.wantXCache {
				t.Errorf("gateway X-Cache = %q, want %q", got, tt.wantXCache)
			}
			if got := fixture.cacheClient.bodyReads.Load(); got != tt.wantFullReads {
				t.Errorf("cache full-body reads = %d, want %d", got, tt.wantFullReads)
			}
			if got := fixture.cacheClient.rangeReads.Load(); got != tt.wantRangeReads {
				t.Errorf("cache range reads = %d, want %d", got, tt.wantRangeReads)
			}
			if got := fixture.forwarder.forwardCalls.Load(); got != tt.wantForwardCalls {
				t.Errorf("direct fallback calls = %d, want %d", got, tt.wantForwardCalls)
			}
			if got := testutil.ToFloat64(metrics.BytesTransferred.WithLabelValues("out")) - outBytesBefore; got != float64(tt.wantOutBytes) {
				t.Errorf("downstream byte metric delta = %v, want %d", got, tt.wantOutBytes)
			}
			if tt.wantRewarm {
				waitFor(t, func() bool {
					meta, found, err := fixture.cache.GetMeta(context.Background(), revalidationTestBucket, revalidationTestKey)
					return err == nil && found && meta != nil && meta.ETag == tt.wantCachedETag
				})
				if got := fixture.forwarder.fullObjectCalls.Load(); got != 1 {
					t.Errorf("full-object rewarm calls = %d, want 1", got)
				}
			}
			if tt.wantCachedETag != "" {
				meta, found, err := fixture.cache.GetMeta(context.Background(), revalidationTestBucket, revalidationTestKey)
				if err != nil || !found || meta.ETag != tt.wantCachedETag {
					t.Fatalf("cache metadata = (%+v, found=%v, err=%v), want ETag %q", meta, found, err, tt.wantCachedETag)
				}
				if meta.StatusCode != http.StatusOK || meta.ContentLength != int64(len(tt.wantCachedBody)) {
					t.Errorf("refreshed metadata status/length = %d/%d, want 200/%d", meta.StatusCode, meta.ContentLength, len(tt.wantCachedBody))
				}
				var cached bytes.Buffer
				if err := fixture.memory.GetStream(context.Background(), cache.MakeBodyKey(revalidationTestBucket, revalidationTestKey, tt.wantCachedETag), &cached); err != nil {
					t.Fatalf("read refreshed cache body: %v", err)
				}
				if !bytes.Equal(cached.Bytes(), tt.wantCachedBody) {
					t.Errorf("refreshed cache body = %q, want %q", cached.Bytes(), tt.wantCachedBody)
				}
			}
			if tt.wantStoredETag != "" {
				meta, found, err := fixture.cache.GetMeta(context.Background(), revalidationTestBucket, revalidationTestKey)
				if err != nil || !found || meta.ETag != tt.wantStoredETag {
					t.Errorf("stored ETag after origin 304 = (%+v, found=%v, err=%v), want %q", meta, found, err, tt.wantStoredETag)
				}
			}
			if tt.name == "origin 304 Last-Modified update is used for client IMS" {
				if got := gateway.header.Get("Last-Modified"); got != tt.originModified.UTC().Format(http.TimeFormat) {
					t.Errorf("gateway Last-Modified = %q, want origin's updated %q", got, tt.originModified.UTC().Format(http.TimeFormat))
				}
				meta, found, err := fixture.cache.GetMeta(context.Background(), revalidationTestBucket, revalidationTestKey)
				if err != nil || !found || meta.LastModified != tt.cachedModified.Unix() {
					t.Errorf("304 changed stored metadata: meta=%+v found=%v err=%v", meta, found, err)
				}
			}
		})
	}
}

func TestRevalidationBodyMissForwardsOriginalClientConditions(t *testing.T) {
	modified := time.Date(2026, 10, 27, 3, 4, 5, 0, time.UTC)
	body := []byte("current object body")
	origin := &revalidationTestOrigin{body: body, etag: `"current"`, lastModified: modified}
	meta := &cache.CachedObjectMeta{
		Bucket: revalidationTestBucket, Key: revalidationTestKey, ETag: `"current"`,
		ContentType: "application/octet-stream", ContentLength: int64(len(body)),
		LastModified: modified.Unix(), StatusCode: http.StatusOK,
	}
	fixture := newRevalidationTestFixture(t, origin, meta, body)
	if err := fixture.cache.DeleteBody(context.Background(), revalidationTestBucket, revalidationTestKey, meta.ETag); err != nil {
		t.Fatalf("remove cached body: %v", err)
	}
	headers := http.Header{"Cache-Control": []string{"no-cache"}, "If-None-Match": []string{`"other"`}}
	direct := readRevalidationTestResponse(t, fixture.client, fixture.originServer.URL+"/"+revalidationTestBucket+"/"+revalidationTestKey, headers)
	gateway := readRevalidationTestResponse(t, fixture.client, fixture.gateway.URL+"/"+revalidationTestBucket+"/"+revalidationTestKey, headers)
	compareRevalidationTestResponses(t, direct, gateway)
	if got := gateway.header.Get(XCacheHeader); got != XCacheMiss {
		t.Errorf("pre-commit body-miss fallback X-Cache = %q, want %q", got, XCacheMiss)
	}
	if direct.status != http.StatusOK || !bytes.Equal(direct.body, body) {
		t.Fatalf("direct origin response = %d %q, want 200 %q", direct.status, direct.body, body)
	}
	if got := fixture.forwarder.forwardCalls.Load(); got != 1 {
		t.Errorf("direct fallback calls = %d, want 1", got)
	}
	if fixture.forwarder.forwardedINM != `"other"` {
		t.Errorf("fallback If-None-Match = %q, want original client validator", fixture.forwarder.forwardedINM)
	}
	if got := fixture.cacheClient.bodyReads.Load(); got != 1 {
		t.Errorf("pre-commit cache body reads = %d, want 1", got)
	}
}

func TestRevalidationStaleFallbackUsesClientConditionsWithoutSuccessValidation(t *testing.T) {
	modified := time.Date(2026, 10, 27, 3, 4, 5, 0, time.UTC)
	body := []byte("stale object body")
	origin := &revalidationTestOrigin{body: body, etag: `"current"`, lastModified: modified}
	meta := &cache.CachedObjectMeta{
		Bucket: revalidationTestBucket, Key: revalidationTestKey, ETag: `"current"`,
		ContentType: "application/octet-stream", ContentLength: int64(len(body)),
		LastModified: modified.Unix(), StatusCode: http.StatusOK,
	}
	fixture := newRevalidationTestFixture(t, origin, meta, body)
	fixture.forwarder.conditionalErr = errors.New("origin unavailable")
	beforeFailed := testutil.ToFloat64(metrics.RevalidationsFailed)
	beforeNotModified := testutil.ToFloat64(metrics.RevalidationsNotModified)
	beforeStale := testutil.ToFloat64(metrics.RevalidationsStaleServed)
	headers := http.Header{"Cache-Control": []string{"no-cache"}, "If-None-Match": []string{`"current"`}}
	response := readRevalidationTestResponse(t, fixture.client, fixture.gateway.URL+"/"+revalidationTestBucket+"/"+revalidationTestKey, headers)
	if response.status != http.StatusNotModified || len(response.body) != 0 {
		t.Fatalf("stale conditional response = %d body %q, want bodyless 304", response.status, response.body)
	}
	if got := response.header.Get(XCacheHeader); got != XCacheHit {
		t.Errorf("stale fallback X-Cache = %q, want %q", got, XCacheHit)
	}
	if got := fixture.cacheClient.bodyReads.Load(); got != 0 {
		t.Errorf("matching stale conditional performed %d cache body reads, want 0", got)
	}
	if got := fixture.forwarder.forwardCalls.Load(); got != 0 {
		t.Errorf("stale fallback made %d direct forwards, want 0", got)
	}
	if got := testutil.ToFloat64(metrics.RevalidationsFailed) - beforeFailed; got != 1 {
		t.Errorf("revalidation failures delta = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.RevalidationsNotModified) - beforeNotModified; got != 0 {
		t.Errorf("successful 304 validation delta = %v after origin failure, want 0", got)
	}
	if got := testutil.ToFloat64(metrics.RevalidationsStaleServed) - beforeStale; got != 1 {
		t.Errorf("stale served delta = %v, want 1", got)
	}
}

func TestRevalidationCommittedCacheReadFailureDoesNotForwardAgain(t *testing.T) {
	modified := time.Date(2026, 10, 27, 3, 4, 5, 0, time.UTC)
	body := bytes.Repeat([]byte("x"), int(smallObjectThreshold)+1024)
	origin := &revalidationTestOrigin{body: body, etag: `"large"`, lastModified: modified}
	meta := &cache.CachedObjectMeta{
		Bucket: revalidationTestBucket, Key: revalidationTestKey, ETag: `"large"`,
		ContentType: "application/octet-stream", ContentLength: int64(len(body)),
		LastModified: modified.Unix(), StatusCode: http.StatusOK,
	}
	fixture := newRevalidationTestFixture(t, origin, meta, body)
	fixture.cacheClient.readPrefix = []byte("partial")
	fixture.cacheClient.readErr = errors.New("cache stream failed")
	req, err := http.NewRequest(http.MethodGet, fixture.gateway.URL+"/"+revalidationTestBucket+"/"+revalidationTestKey, nil)
	if err != nil {
		t.Fatalf("create gateway GET: %v", err)
	}
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Authorization", "test")
	resp, err := fixture.client.Do(req)
	if err != nil {
		t.Fatalf("gateway GET: %v", err)
	}
	defer resp.Body.Close()
	got, readErr := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(got, fixture.cacheClient.readPrefix) || readErr == nil {
		t.Fatalf("committed cache failure = status %d body length %d read error %v, want incomplete 200 prefix", resp.StatusCode, len(got), readErr)
	}
	if got := fixture.forwarder.forwardCalls.Load(); got != 0 {
		t.Errorf("post-commit cache failure forwarded %d times, want 0", got)
	}
	if got := fixture.cacheClient.bodyReads.Load(); got != 1 {
		t.Errorf("cache full-body reads = %d, want 1", got)
	}
}

func TestRevalidationChangedBodyDrainFailureStillCompletesMatching304(t *testing.T) {
	modified := time.Date(2026, 10, 27, 3, 4, 5, 0, time.UTC)
	oldBody := []byte("old body")
	newBody := []byte("partial new body")
	origin := &revalidationTestOrigin{body: newBody, etag: `"new"`, lastModified: modified.Add(time.Minute)}
	meta := &cache.CachedObjectMeta{
		Bucket: revalidationTestBucket, Key: revalidationTestKey, ETag: `"old"`,
		ContentType: "application/octet-stream", ContentLength: int64(len(oldBody)),
		LastModified: modified.Unix(), StatusCode: http.StatusOK,
	}
	fixture := newRevalidationTestFixture(t, origin, meta, oldBody)
	bodyErr := errors.New("origin body truncated")
	responseHeaders := make(http.Header)
	responseHeaders.Set("ETag", `"new"`)
	responseHeaders.Set("Last-Modified", origin.lastModified.UTC().Format(http.TimeFormat))
	responseHeaders.Set("Content-Length", strconv.Itoa(len(newBody)+10))
	fixture.forwarder.conditionalResponse = &http.Response{
		StatusCode:    http.StatusOK,
		Header:        responseHeaders,
		ContentLength: int64(len(newBody) + 10),
		Body:          &revalidationErrorAfterBytes{body: newBody, err: bodyErr},
	}
	headers := http.Header{"Cache-Control": []string{"no-cache"}, "If-None-Match": []string{`"new"`}}
	outBytesBefore := testutil.ToFloat64(metrics.BytesTransferred.WithLabelValues("out"))
	response := readRevalidationTestResponse(t, fixture.client, fixture.gateway.URL+"/"+revalidationTestBucket+"/"+revalidationTestKey, headers)
	if response.status != http.StatusNotModified || len(response.body) != 0 {
		t.Fatalf("response = %d body %q, want complete 304 with no body", response.status, response.body)
	}
	if got := testutil.ToFloat64(metrics.BytesTransferred.WithLabelValues("out")) - outBytesBefore; got != 0 {
		t.Errorf("downstream byte metric delta = %v, want 0", got)
	}
	if fixture.forwarder.forwardCalls.Load() != 0 {
		t.Fatal("origin-body drain failure caused a second direct request")
	}
	fixture.service.Stop()
	if _, found, err := fixture.cache.GetMeta(context.Background(), revalidationTestBucket, revalidationTestKey); err != nil || found {
		t.Errorf("truncated changed body was published: found=%v err=%v", found, err)
	}
}

func TestRevalidationChangedRangeDrainFailureStillCompletesMatching304(t *testing.T) {
	modified := time.Date(2026, 10, 27, 3, 4, 5, 0, time.UTC)
	oldBody := []byte("old range object")
	newBody := []byte("new range object body")
	origin := &revalidationTestOrigin{body: newBody, etag: `"new"`, lastModified: modified.Add(time.Minute)}
	meta := &cache.CachedObjectMeta{
		Bucket: revalidationTestBucket, Key: revalidationTestKey, ETag: `"old"`,
		ContentType: "application/octet-stream", ContentLength: int64(len(oldBody)),
		LastModified: modified.Unix(), StatusCode: http.StatusOK,
	}
	fixture := newRevalidationTestFixture(t, origin, meta, oldBody)
	responseHeaders := make(http.Header)
	responseHeaders.Set("ETag", `"new"`)
	responseHeaders.Set("Last-Modified", origin.lastModified.UTC().Format(http.TimeFormat))
	responseHeaders.Set("Content-Range", fmt.Sprintf("bytes 0-4/%d", len(newBody)))
	responseHeaders.Set("Content-Length", "10")
	fixture.forwarder.conditionalResponse = &http.Response{
		StatusCode:    http.StatusPartialContent,
		Header:        responseHeaders,
		ContentLength: 10,
		Body:          &revalidationErrorAfterBytes{body: []byte("new b"), err: errors.New("range body truncated")},
	}
	headers := http.Header{
		"Authorization": []string{"test"},
		"Cache-Control": []string{"no-cache"},
		"If-None-Match": []string{`"new"`},
		"Range":         []string{"bytes=0-4"},
	}
	direct := readRevalidationTestResponse(t, fixture.client, fixture.originServer.URL+"/"+revalidationTestBucket+"/"+revalidationTestKey, headers)
	outBytesBefore := testutil.ToFloat64(metrics.BytesTransferred.WithLabelValues("out"))
	gateway := readRevalidationTestResponse(t, fixture.client, fixture.gateway.URL+"/"+revalidationTestBucket+"/"+revalidationTestKey, headers)
	compareRevalidationTestResponses(t, direct, gateway)
	if direct.status != http.StatusNotModified || len(direct.body) != 0 {
		t.Fatalf("direct origin response = %d body %q, want bodyless 304", direct.status, direct.body)
	}
	if got := testutil.ToFloat64(metrics.BytesTransferred.WithLabelValues("out")) - outBytesBefore; got != 0 {
		t.Errorf("downstream byte metric delta = %v, want 0", got)
	}
	waitFor(t, func() bool {
		current, found, err := fixture.cache.GetMeta(context.Background(), revalidationTestBucket, revalidationTestKey)
		return err == nil && found && current != nil && current.ETag == `"new"`
	})
	if got := fixture.forwarder.fullObjectCalls.Load(); got != 1 {
		t.Errorf("full-object rewarm calls = %d, want 1", got)
	}
	var cached bytes.Buffer
	if err := fixture.memory.GetStream(context.Background(), cache.MakeBodyKey(revalidationTestBucket, revalidationTestKey, `"new"`), &cached); err != nil {
		t.Fatalf("read rewarmed body: %v", err)
	}
	if !bytes.Equal(cached.Bytes(), newBody) {
		t.Errorf("rewarmed body length = %d, want %d", cached.Len(), len(newBody))
	}
}

type revalidationErrorAfterBytes struct {
	body []byte
	err  error
}

func (r *revalidationErrorAfterBytes) Read(p []byte) (int, error) {
	if len(r.body) != 0 {
		n := copy(p, r.body)
		r.body = r.body[n:]
		return n, nil
	}
	return 0, r.err
}

func (r *revalidationErrorAfterBytes) Close() error { return nil }
