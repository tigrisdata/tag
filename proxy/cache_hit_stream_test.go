package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/metrics"
)

type testStreamCacheClient struct {
	cacheclient.CacheClient
	stream      func(context.Context, string, io.Writer) error
	rangeStream func(context.Context, string, int64, int64, io.Writer) error
}

type commitTrackingResponseWriter struct {
	*httptest.ResponseRecorder
	committed bool
}

func (w *commitTrackingResponseWriter) WriteHeader(statusCode int) {
	w.committed = true
	w.ResponseRecorder.WriteHeader(statusCode)
}

func (w *commitTrackingResponseWriter) Write(p []byte) (int, error) {
	w.committed = true
	return w.ResponseRecorder.Write(p)
}

func (w *commitTrackingResponseWriter) Flush() {
	w.committed = true
	w.ResponseRecorder.Flush()
}

func (c *testStreamCacheClient) GetStream(ctx context.Context, key string, w io.Writer) error {
	return c.stream(ctx, key, w)
}

func (c *testStreamCacheClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if c.rangeStream != nil {
		return c.rangeStream(ctx, key, start, end, w)
	}
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

func newLargeStreamCacheService(t *testing.T, body []byte, stream func(context.Context, string, io.Writer) error) (*Service, *cache.Cache, *cache.CachedObjectMeta) {
	t.Helper()

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	cfg.Cache.SetLegacyCoordination(true)
	base := cacheclient.NewMemoryCache()
	client := &testStreamCacheClient{CacheClient: base, stream: stream}
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	meta := &cache.CachedObjectMeta{
		Bucket:        "bucket",
		Key:           "key",
		ETag:          `"etag"`,
		ContentType:   "text/plain",
		ContentLength: int64(len(body)),
		StatusCode:    http.StatusOK,
	}
	if err := store.PutWithMeta(context.Background(), meta.Bucket, meta.Key, meta, body, 0); err != nil {
		t.Fatalf("seed cache: %v", err)
	}

	return NewService(&mockForwarder{}, store, cfg), store, meta
}

func newWholeObjectCacheServiceWithCoordinator(t *testing.T, objectBody, cacheBody []byte, legacy bool) (*Service, *cache.CachedObjectMeta) {
	t.Helper()

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	cfg.Cache.SetLegacyCoordination(legacy)
	store := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	meta := &cache.CachedObjectMeta{
		Bucket:        "bucket",
		Key:           "key",
		ETag:          `"etag"`,
		ContentType:   "text/plain",
		ContentLength: int64(len(objectBody)),
		StatusCode:    http.StatusOK,
	}
	if err := store.PutWithMeta(context.Background(), meta.Bucket, meta.Key, meta, cacheBody, 0); err != nil {
		t.Fatalf("seed whole-object cache: %v", err)
	}
	return NewService(&mockForwarder{}, store, cfg), meta
}

func TestBodyLengthMatches(t *testing.T) {
	ctx := context.Background()
	cfg := config.NewDefault()
	store := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	svc := NewService(&mockForwarder{}, store, cfg)
	tests := []struct {
		name      string
		body      []byte
		length    int64
		stored    bool
		wantMatch bool
	}{
		{name: "one-byte-exact", body: []byte("a"), length: 1, stored: true, wantMatch: true},
		{name: "exact", body: []byte("abc"), length: 3, stored: true, wantMatch: true},
		{name: "short", body: []byte("ab"), length: 3, stored: true},
		{name: "empty", length: 3, stored: true},
		{name: "overlong", body: []byte("abcd"), length: 3, stored: true},
		{name: "absent", length: 3},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, etag := "b", "key-"+strconv.Itoa(i), `"etag-`+strconv.Itoa(i)+`"`
			meta := &cache.CachedObjectMeta{Bucket: bucket, Key: key, ETag: etag, ContentLength: tt.length}
			if tt.stored {
				if err := store.PutWithMeta(ctx, bucket, key, meta, tt.body, 60); err != nil {
					t.Fatalf("PutWithMeta: %v", err)
				}
			}
			got, err := svc.bodyLengthMatches(ctx, bucket, key, meta)
			if err != nil || got != tt.wantMatch {
				t.Fatalf("bodyLengthMatches() = (%v, %v), want (%v, nil)", got, err, tt.wantMatch)
			}
		})
	}
}

func TestBodyLengthMatchesPropagatesTransientRangeError(t *testing.T) {
	wantErr := errors.New("peer unavailable")
	cfg := config.NewDefault()
	client := &testStreamCacheClient{
		CacheClient: cacheclient.NewMemoryCache(),
		rangeStream: func(context.Context, string, int64, int64, io.Writer) error {
			return wantErr
		},
	}
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	svc := NewService(&mockForwarder{}, store, cfg)
	meta := &cache.CachedObjectMeta{Bucket: "b", Key: "k", ETag: `"etag"`, ContentLength: 8}

	got, err := svc.bodyLengthMatches(context.Background(), meta.Bucket, meta.Key, meta)
	if got || !errors.Is(err, wantErr) {
		t.Fatalf("bodyLengthMatches() = (%v, %v), want (false, %v)", got, err, wantErr)
	}
}

func TestServeFromCache_LargeObject(t *testing.T) {
	body := bytes.Repeat([]byte("cached body"), 8192)
	stream := func(_ context.Context, _ string, w io.Writer) error {
		if _, err := w.Write(nil); err != nil {
			return err
		}
		for _, chunk := range [][]byte{body[:1], body[1:32768], body[32768:]} {
			if _, err := w.Write(chunk); err != nil {
				return err
			}
		}
		return nil
	}
	svc, _, meta := newLargeStreamCacheService(t, body, stream)

	w := httptest.NewRecorder()
	if err := svc.serveFromCache(context.Background(), w, meta.Bucket, meta.Key, meta, time.Now(), func() {}); err != nil {
		t.Fatalf("serveFromCache() error = %v", err)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Fatalf("%s = %q, want %q", XCacheHeader, got, XCacheHit)
	}
	if !bytes.Equal(w.Body.Bytes(), body) {
		t.Fatalf("body mismatch: got %d bytes, want %d", w.Body.Len(), len(body))
	}
}

func TestServeFromCache_LargeObjectUncommittedBeforeFirstByte(t *testing.T) {
	body := bytes.Repeat([]byte("cached body"), 8192)
	wantErr := errors.New("cache stream failed before first byte")
	stream := func(_ context.Context, _ string, _ io.Writer) error {
		return wantErr
	}
	svc, _, meta := newLargeStreamCacheService(t, body, stream)

	w := httptest.NewRecorder()
	err := svc.serveFromCache(context.Background(), w, meta.Bucket, meta.Key, meta, time.Now(), func() {})
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("serveFromCache() error = %v, want wrapped %v", err, wantErr)
	}
	if got := w.Header().Get(XCacheHeader); got != "" {
		t.Fatalf("%s = %q, want no cache status before first byte", XCacheHeader, got)
	}
	if w.Body.Len() != 0 {
		t.Fatalf("body length = %d, want 0", w.Body.Len())
	}
}

func TestHandleGetObject_LargeEmptyCacheHitFallsBackBeforeHeaders(t *testing.T) {
	body := bytes.Repeat([]byte("cached body"), 8192)
	stream := func(_ context.Context, _ string, _ io.Writer) error {
		return nil
	}
	svc, _, _ := newLargeStreamCacheService(t, body, stream)

	var upstreamCalls atomic.Int32
	forwarder := &mockForwarder{
		doRequestFunc: func(context.Context, *http.Request, string, string) (*http.Response, error) {
			upstreamCalls.Add(1)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header: http.Header{
					"Content-Length": []string{"14"},
					"Content-Type":   []string{"text/plain"},
					"ETag":           []string{`"upstream"`},
				},
				Body: io.NopCloser(bytes.NewReader([]byte("upstream body"))),
			}, nil
		},
	}
	svc.forwarder = forwarder

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	if err := svc.HandleGetObject(w, r); err != nil {
		t.Fatalf("HandleGetObject() error = %v", err)
	}
	if got := upstreamCalls.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1", got)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheMiss {
		t.Fatalf("%s = %q, want %q", XCacheHeader, got, XCacheMiss)
	}
	if got := w.Body.String(); got != "upstream body" {
		t.Fatalf("body = %q, want %q", got, "upstream body")
	}
}

func TestHandleGetObject_LargeCacheHitPostCommitErrorDoesNotFallBack(t *testing.T) {
	body := bytes.Repeat([]byte("cached body"), 8192)
	beforeErrors := requestCount(t, "error")
	beforeSuccesses := requestCount(t, "success")
	firstChunk := body[:32768]
	streamErr := errors.New("cache stream failed after first byte")
	stream := func(_ context.Context, _ string, w io.Writer) error {
		if _, err := w.Write(firstChunk); err != nil {
			return err
		}
		return streamErr
	}
	svc, _, _ := newLargeStreamCacheService(t, body, stream)

	var upstreamCalls atomic.Int32
	svc.forwarder = &mockForwarder{
		doRequestFunc: func(context.Context, *http.Request, string, string) (*http.Response, error) {
			upstreamCalls.Add(1)
			return nil, errors.New("unexpected upstream request")
		},
	}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	if err := svc.HandleGetObject(w, r); err != nil {
		t.Fatalf("HandleGetObject() error = %v", err)
	}
	if got := upstreamCalls.Load(); got != 0 {
		t.Fatalf("upstream calls = %d, want 0 after committed cache response", got)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get(XCacheHeader); got != XCacheHit {
		t.Fatalf("%s = %q, want %q", XCacheHeader, got, XCacheHit)
	}
	if !bytes.Equal(w.Body.Bytes(), firstChunk) {
		t.Fatalf("body mismatch: got %d bytes, want %d", w.Body.Len(), len(firstChunk))
	}
	if got := requestCount(t, "error"); got != beforeErrors+1 {
		t.Fatalf("error request count = %v, want %v", got, beforeErrors+1)
	}
	if got := requestCount(t, "success"); got != beforeSuccesses {
		t.Fatalf("success request count = %v, want %v", got, beforeSuccesses)
	}
	if _, found, err := svc.cache.GetMeta(context.Background(), "bucket", "key"); err != nil || !found {
		t.Errorf("transient post-commit stream error removed valid metadata: found=%v err=%v", found, err)
	}
}

func TestServeFromCache_OverlongBodyIsNotSuccessfulInCAS(t *testing.T) {
	smallObject := []byte("small object")
	largeObject := cacheTestBodyAboveSmallObjectThreshold("cached body")
	tests := []struct {
		name              string
		objectBody        []byte
		cacheBody         []byte
		wantErr           bool
		wantBody          []byte
		wantCacheStatus   string
		wantContentLength int
		wantRequestStatus string
	}{
		{
			name:       "small",
			objectBody: smallObject,
			cacheBody:  append(append([]byte(nil), smallObject...), []byte(" extra")...),
			wantErr:    true,
		},
		{
			name:              "large",
			objectBody:        largeObject,
			cacheBody:         append(append([]byte(nil), largeObject...), []byte(" extra")...),
			wantBody:          append(append([]byte(nil), largeObject...), []byte(" extra")...),
			wantCacheStatus:   XCacheHit,
			wantContentLength: len(largeObject),
			wantRequestStatus: "error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, meta := newWholeObjectCacheServiceWithCoordinator(t, tt.objectBody, tt.cacheBody, false)
			_, metaVersion, found, versionErr := svc.cache.GetMetaWithVersion(context.Background(), meta.Bucket, meta.Key)
			if versionErr != nil || !found {
				t.Fatalf("read seed metadata version: found=%v err=%v", found, versionErr)
			}
			beforeLocalSuccess := requestCountFrom(t, "success", metrics.SourceLocal)
			beforeLocalErrors := requestCountFrom(t, "error", metrics.SourceLocal)
			w := &commitTrackingResponseWriter{ResponseRecorder: httptest.NewRecorder()}
			err := svc.serveFromCache(context.Background(), w, meta.Bucket, meta.Key, meta, time.Now(), func() {
				svc.invalidateMetaIfVersion(context.Background(), meta.Bucket, meta.Key, metaVersion)
			})
			if (err != nil) != tt.wantErr {
				t.Fatalf("serveFromCache() error = %v, want error %v", err, tt.wantErr)
			}
			if _, found, err := svc.cache.GetMeta(context.Background(), meta.Bucket, meta.Key); err != nil || found {
				t.Errorf("CAS cleanup left overlong metadata: found=%v err=%v", found, err)
			}
			if tt.wantErr {
				if w.committed || w.Body.Len() != 0 || w.Header().Get(XCacheHeader) != "" {
					t.Errorf("short-path response committed: headers=%v body=%q", w.Header(), w.Body.String())
				}
				if got := requestCountFrom(t, "success", metrics.SourceLocal); got != beforeLocalSuccess {
					t.Errorf("local success count delta = %v, want 0", got-beforeLocalSuccess)
				}
				return
			}
			if !w.committed {
				t.Fatal("large-path response was not committed")
			}
			if got := w.Header().Get(XCacheHeader); got != tt.wantCacheStatus {
				t.Errorf("%s = %q, want %q", XCacheHeader, got, tt.wantCacheStatus)
			}
			if got := w.Header().Get("Content-Length"); got != strconv.Itoa(tt.wantContentLength) {
				t.Errorf("Content-Length = %q, want %d", got, tt.wantContentLength)
			}
			if !bytes.Equal(w.Body.Bytes(), tt.wantBody) {
				t.Errorf("body length = %d, want %d", w.Body.Len(), len(tt.wantBody))
			}
			if got := requestCountFrom(t, tt.wantRequestStatus, metrics.SourceLocal); got != beforeLocalErrors+1 {
				t.Errorf("local error count delta = %v, want 1", got-beforeLocalErrors)
			}
			if got := requestCountFrom(t, "success", metrics.SourceLocal); got != beforeLocalSuccess {
				t.Errorf("local success count delta = %v, want 0", got-beforeLocalSuccess)
			}
		})
	}
}

func TestHandleGetObject_ShortBodyCleanupPreservesSameETagReplacement(t *testing.T) {
	body := cacheTestBodyAboveSmallObjectThreshold("cached body")
	prefix := body[:shortCacheBodyPrefixSize]
	for _, tc := range []struct {
		name   string
		legacy bool
	}{
		{name: "legacy default", legacy: true},
		{name: "CAS coordinator", legacy: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseStream := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseStream()
			cfg := config.NewDefault()
			cfg.Cache.SetBlockCachingEnabled(false)
			cfg.Cache.SetLegacyCoordination(tc.legacy)
			base := cacheclient.NewMemoryCache()
			var streamOnce sync.Once
			stream := func(ctx context.Context, key string, w io.Writer) error {
				first := false
				var streamErr error
				streamOnce.Do(func() {
					first = true
					if _, err := w.Write(prefix); err != nil {
						streamErr = err
						close(started)
						return
					}
					close(started)
					select {
					case <-release:
					case <-ctx.Done():
						streamErr = ctx.Err()
					}
				})
				if first {
					return streamErr
				}
				return base.GetStream(ctx, key, w)
			}
			client := &testStreamCacheClient{CacheClient: base, stream: stream}
			store := cache.NewCacheWithClient(client, &cfg.Cache)
			meta := &cache.CachedObjectMeta{
				Bucket:        "bucket",
				Key:           "key",
				ETag:          `"etag"`,
				ContentType:   "text/plain",
				ContentLength: int64(len(body)),
				StatusCode:    http.StatusOK,
			}
			if err := store.PutWithMeta(context.Background(), meta.Bucket, meta.Key, meta, body, 0); err != nil {
				t.Fatalf("seed cache: %v", err)
			}
			svc := NewService(&mockForwarder{}, store, cfg)
			var upstreamCalls atomic.Int32
			svc.forwarder = &mockForwarder{
				doRequestFunc: func(context.Context, *http.Request, string, string) (*http.Response, error) {
					upstreamCalls.Add(1)
					return nil, errors.New("unexpected upstream request after commitment")
				},
			}
			beforeErrors := requestCountFrom(t, "error", metrics.SourceLocal)
			beforeSuccesses := requestCountFrom(t, "success", metrics.SourceLocal)
			w := httptest.NewRecorder()
			done := make(chan error, 1)
			go func() {
				done <- svc.HandleGetObject(w, httptest.NewRequest(http.MethodGet, "/bucket/key", nil))
			}()
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("old cache stream did not reach its first committed chunk")
			}

			replacement := &cache.CachedObjectMeta{
				Bucket:        meta.Bucket,
				Key:           meta.Key,
				ETag:          meta.ETag,
				ContentType:   meta.ContentType,
				ContentLength: meta.ContentLength,
				StatusCode:    meta.StatusCode,
			}
			if err := store.PutWithMeta(context.Background(), meta.Bucket, meta.Key, replacement, body, 0); err != nil {
				t.Fatalf("commit same-ETag replacement: %v", err)
			}
			releaseStream()
			if err := <-done; err != nil {
				t.Fatalf("HandleGetObject() error = %v, want nil after committed stream", err)
			}

			if w.Code != http.StatusOK || w.Header().Get(XCacheHeader) != XCacheHit || w.Header().Get("Content-Length") != strconv.Itoa(len(body)) {
				t.Errorf("older response = %d headers=%v, want committed HIT with metadata length", w.Code, w.Header())
			}
			if !bytes.Equal(w.Body.Bytes(), prefix) {
				t.Errorf("older response body length = %d, want short prefix length %d", w.Body.Len(), len(prefix))
			}
			if got := upstreamCalls.Load(); got != 0 {
				t.Errorf("post-commit upstream calls = %d, want 0", got)
			}
			if got := requestCountFrom(t, "error", metrics.SourceLocal); got != beforeErrors+1 {
				t.Errorf("local error count delta = %v, want 1", got-beforeErrors)
			}
			if got := requestCountFrom(t, "success", metrics.SourceLocal); got != beforeSuccesses {
				t.Errorf("local success count delta = %v, want 0", got-beforeSuccesses)
			}

			current, found, err := store.GetMeta(context.Background(), meta.Bucket, meta.Key)
			if err != nil || !found || current == nil || current.ETag != meta.ETag {
				t.Fatalf("same-ETag replacement metadata was removed: current=%+v found=%v err=%v", current, found, err)
			}
			var currentBody bytes.Buffer
			if err := store.GetBodyStream(context.Background(), meta.Bucket, meta.Key, current.BodyDiscriminator(), &currentBody); err != nil {
				t.Fatalf("read replacement body: %v", err)
			}
			if !bytes.Equal(currentBody.Bytes(), body) {
				t.Errorf("replacement body length = %d, want %d", currentBody.Len(), len(body))
			}
		})
	}
}

func TestHandleGetObject_EmptyCacheBodyPreservesSameETagReplacement(t *testing.T) {
	largeBody := cacheTestBodyAboveSmallObjectThreshold("cached body")
	shortBody := []byte("cached replacement body")
	for _, tc := range []struct {
		name          string
		legacy        bool
		revalidate    bool
		stale         bool
		unexpectedEOF bool
		prefixBytes   int
		body          []byte
	}{
		{name: "legacy default", legacy: true, body: largeBody},
		{name: "CAS coordinator", legacy: false, body: largeBody},
		{name: "legacy revalidation fallback", legacy: true, revalidate: true, body: largeBody},
		{name: "legacy short read error", legacy: true, unexpectedEOF: true, prefixBytes: 1, body: shortBody},
		{name: "legacy large read error before commit", legacy: true, unexpectedEOF: true, body: largeBody},
		{name: "legacy revalidation short read error", legacy: true, revalidate: true, unexpectedEOF: true, prefixBytes: 1, body: shortBody},
		{name: "legacy stale fallback short read error", legacy: true, revalidate: true, stale: true, unexpectedEOF: true, prefixBytes: 1, body: shortBody},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := tc.body
			started := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseStream := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseStream()
			cfg := config.NewDefault()
			cfg.Cache.SetBlockCachingEnabled(false)
			cfg.Cache.SetLegacyCoordination(tc.legacy)
			base := cacheclient.NewMemoryCache()
			var streamOnce sync.Once
			stream := func(ctx context.Context, key string, w io.Writer) error {
				first := false
				var streamErr error
				streamOnce.Do(func() {
					first = true
					if tc.prefixBytes > 0 {
						_, streamErr = w.Write(body[:tc.prefixBytes])
					}
					close(started)
					select {
					case <-release:
					case <-ctx.Done():
						streamErr = ctx.Err()
					}
					if tc.unexpectedEOF && streamErr == nil {
						streamErr = io.ErrUnexpectedEOF
					}
				})
				if first {
					return streamErr
				}
				return base.GetStream(ctx, key, w)
			}
			client := &testStreamCacheClient{CacheClient: base, stream: stream}
			store := cache.NewCacheWithClient(client, &cfg.Cache)
			meta := &cache.CachedObjectMeta{
				Bucket:        "bucket",
				Key:           "key",
				ETag:          `"etag"`,
				ContentType:   "text/plain",
				ContentLength: int64(len(body)),
				StatusCode:    http.StatusOK,
			}
			if err := store.PutWithMeta(context.Background(), meta.Bucket, meta.Key, meta, body, 0); err != nil {
				t.Fatalf("seed cache: %v", err)
			}
			svc := NewService(&mockForwarder{}, store, cfg)
			var upstreamCalls atomic.Int32
			forwarder := &mockForwarder{
				doRequestFunc: func(context.Context, *http.Request, string, string) (*http.Response, error) {
					upstreamCalls.Add(1)
					return &http.Response{
						StatusCode: http.StatusOK,
						Header: http.Header{
							"Content-Length": []string{strconv.Itoa(len(body))},
							"Cache-Control":  []string{"no-store"},
							"Content-Type":   []string{"text/plain"},
							"ETag":           []string{meta.ETag},
						},
						Body:          io.NopCloser(bytes.NewReader(body)),
						ContentLength: int64(len(body)),
					}, nil
				},
			}
			if tc.revalidate {
				if tc.stale {
					forwarder.conditionalErr = errors.New("upstream revalidation failed")
				} else {
					forwarder.conditionalResp = &http.Response{
						StatusCode: http.StatusNotModified,
						Header:     make(http.Header),
						Body:       io.NopCloser(bytes.NewReader(nil)),
					}
				}
				forwarder.forwardFunc = func(_ context.Context, w http.ResponseWriter, _ *http.Request) error {
					upstreamCalls.Add(1)
					w.Header().Set("Content-Length", strconv.Itoa(len(body)))
					w.Header().Set("Cache-Control", "no-store")
					w.Header().Set("Content-Type", "text/plain")
					w.Header().Set("ETag", meta.ETag)
					w.WriteHeader(http.StatusOK)
					_, err := w.Write(body)
					return err
				}
			}
			svc.forwarder = forwarder
			w := httptest.NewRecorder()
			done := make(chan error, 1)
			go func() {
				r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
				if tc.revalidate {
					r.Header.Set("Cache-Control", "no-cache")
				}
				done <- svc.HandleGetObject(w, r)
			}()
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("cache stream did not start")
			}

			replacement := &cache.CachedObjectMeta{
				Bucket:        meta.Bucket,
				Key:           meta.Key,
				ETag:          meta.ETag,
				ContentType:   meta.ContentType,
				ContentLength: meta.ContentLength,
				StatusCode:    meta.StatusCode,
			}
			if err := store.PutWithMeta(context.Background(), meta.Bucket, meta.Key, replacement, body, 0); err != nil {
				t.Fatalf("commit same-ETag replacement: %v", err)
			}
			releaseStream()
			if err := <-done; err != nil {
				t.Fatalf("HandleGetObject() error = %v, want successful upstream fallback", err)
			}
			if tc.revalidate && !forwarder.conditionalCalled {
				t.Fatal("cache-body fallback did not take the revalidation path")
			}
			if w.Code != http.StatusOK || w.Header().Get(XCacheHeader) != XCacheMiss || w.Header().Get("Content-Length") != strconv.Itoa(len(body)) || !bytes.Equal(w.Body.Bytes(), body) {
				t.Fatalf("cache-body fallback = %d headers=%v body_bytes=%d, want full MISS", w.Code, w.Header(), w.Body.Len())
			}
			if got := upstreamCalls.Load(); got != 1 {
				t.Fatalf("cache-body fallback upstream calls = %d, want 1", got)
			}

			current, found, err := store.GetMeta(context.Background(), meta.Bucket, meta.Key)
			if err != nil || !found || current == nil || current.ETag != meta.ETag {
				t.Fatalf("same-ETag replacement metadata was removed: current=%+v found=%v err=%v", current, found, err)
			}
			var currentBody bytes.Buffer
			if err := store.GetBodyStream(context.Background(), meta.Bucket, meta.Key, current.BodyDiscriminator(), &currentBody); err != nil {
				t.Fatalf("read replacement body: %v", err)
			}
			if !bytes.Equal(currentBody.Bytes(), body) {
				t.Errorf("replacement body length = %d, want %d", currentBody.Len(), len(body))
			}
			second := httptest.NewRecorder()
			if err := svc.HandleGetObject(second, httptest.NewRequest(http.MethodGet, "/bucket/key", nil)); err != nil {
				t.Fatalf("second GET error = %v", err)
			}
			if second.Code != http.StatusOK || second.Header().Get(XCacheHeader) != XCacheHit || !bytes.Equal(second.Body.Bytes(), body) {
				t.Errorf("replacement GET = %d X-Cache=%q body_bytes=%d, want full HIT", second.Code, second.Header().Get(XCacheHeader), second.Body.Len())
			}
			if got := upstreamCalls.Load(); got != 1 {
				t.Errorf("replacement GET caused %d upstream calls, want 1", got)
			}
		})
	}
}

func TestHandleGetObject_LargeShortCacheBodyInvalidatesBeforeNextGetInCAS(t *testing.T) {
	body := cacheTestBodyAboveSmallObjectThreshold("cached body")
	prefix := body[:shortCacheBodyPrefixSize]
	svc, meta := newWholeObjectCacheServiceWithCoordinator(t, body, prefix, false)
	var upstreamCalls atomic.Int32
	svc.forwarder = &mockForwarder{
		doRequestFunc: func(context.Context, *http.Request, string, string) (*http.Response, error) {
			upstreamCalls.Add(1)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header: http.Header{
					"Content-Length": []string{strconv.Itoa(len(body))},
					"Content-Type":   []string{"text/plain"},
					"ETag":           []string{`"refetched"`},
				},
				Body:          io.NopCloser(bytes.NewReader(body)),
				ContentLength: int64(len(body)),
			}, nil
		},
	}

	beforeLocalErrors := requestCountFrom(t, "error", metrics.SourceLocal)
	beforeLocalSuccesses := requestCountFrom(t, "success", metrics.SourceLocal)
	beforeUpstreamSuccesses := requestCountFrom(t, "success", metrics.SourceUpstream)
	first := httptest.NewRecorder()
	if err := svc.HandleGetObject(first, httptest.NewRequest(http.MethodGet, "/bucket/key", nil)); err != nil {
		t.Fatalf("first HandleGetObject() error = %v", err)
	}
	if first.Code != http.StatusOK || first.Header().Get(XCacheHeader) != XCacheHit {
		t.Fatalf("first response = %d X-Cache %q, want 200 HIT", first.Code, first.Header().Get(XCacheHeader))
	}
	if first.Header().Get("Content-Length") != strconv.Itoa(len(body)) || !bytes.Equal(first.Body.Bytes(), prefix) {
		t.Fatalf("first committed response length=%q body_bytes=%d, want length %d and prefix %d", first.Header().Get("Content-Length"), first.Body.Len(), len(body), len(prefix))
	}
	if got := upstreamCalls.Load(); got != 0 {
		t.Fatalf("first GET made %d upstream calls, want 0 after commitment", got)
	}
	if _, found, err := svc.cache.GetMeta(context.Background(), meta.Bucket, meta.Key); err != nil || found {
		t.Fatalf("mismatched cache metadata remains after committed response: found=%v err=%v", found, err)
	}
	if got := requestCountFrom(t, "error", metrics.SourceLocal); got != beforeLocalErrors+1 {
		t.Errorf("first local error count delta = %v, want 1", got-beforeLocalErrors)
	}
	if got := requestCountFrom(t, "success", metrics.SourceLocal); got != beforeLocalSuccesses {
		t.Errorf("first local success count delta = %v, want 0", got-beforeLocalSuccesses)
	}

	second := httptest.NewRecorder()
	if err := svc.HandleGetObject(second, httptest.NewRequest(http.MethodGet, "/bucket/key", nil)); err != nil {
		t.Fatalf("second HandleGetObject() error = %v", err)
	}
	if second.Code != http.StatusOK || second.Header().Get(XCacheHeader) != XCacheMiss {
		t.Fatalf("second response = %d X-Cache %q, want 200 MISS", second.Code, second.Header().Get(XCacheHeader))
	}
	if second.Header().Get("Content-Length") != strconv.Itoa(len(body)) || !bytes.Equal(second.Body.Bytes(), body) {
		t.Fatalf("second response length=%q body_bytes=%d, want %d complete bytes", second.Header().Get("Content-Length"), second.Body.Len(), len(body))
	}
	if got := upstreamCalls.Load(); got != 1 {
		t.Errorf("total upstream calls = %d, want 1", got)
	}
	if got := requestCountFrom(t, "success", metrics.SourceUpstream); got != beforeUpstreamSuccesses+1 {
		t.Errorf("upstream success count delta = %v, want 1", got-beforeUpstreamSuccesses)
	}
}

func TestHandleGetObject_RevalidationLargeShortBodyInvalidatesMetadataInCAS(t *testing.T) {
	body := cacheTestBodyAboveSmallObjectThreshold("cached body")
	prefix := body[:shortCacheBodyPrefixSize]
	tests := []struct {
		name            string
		conditionalResp *http.Response
		conditionalErr  error
	}{
		{
			name: "not-modified",
			conditionalResp: &http.Response{
				StatusCode: http.StatusNotModified,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader(nil)),
			},
		},
		{
			name:           "stale-after-error",
			conditionalErr: errors.New("upstream revalidation failed"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, meta := newWholeObjectCacheServiceWithCoordinator(t, body, prefix, false)
			var fallbackCalls atomic.Int32
			forwarder := &mockForwarder{
				conditionalResp: tt.conditionalResp,
				conditionalErr:  tt.conditionalErr,
				forwardFunc: func(context.Context, http.ResponseWriter, *http.Request) error {
					fallbackCalls.Add(1)
					return errors.New("unexpected fallback after committed cache response")
				},
			}
			svc.forwarder = forwarder
			beforeErrors := requestCountFrom(t, "error", metrics.SourceLocal)
			beforeSuccesses := requestCountFrom(t, "success", metrics.SourceLocal)
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
			r.Header.Set("Cache-Control", "no-cache")
			if err := svc.HandleGetObject(w, r); err != nil {
				t.Fatalf("HandleGetObject() error = %v", err)
			}
			if !forwarder.conditionalCalled {
				t.Fatal("HandleGetObject did not take the revalidation path")
			}
			if got := fallbackCalls.Load(); got != 0 {
				t.Errorf("post-commit fallback calls = %d, want 0", got)
			}
			if w.Code != http.StatusOK || w.Header().Get(XCacheHeader) != XCacheHit || w.Header().Get("Content-Length") != strconv.Itoa(len(body)) || !bytes.Equal(w.Body.Bytes(), prefix) {
				t.Errorf("committed response = %d headers=%v body_bytes=%d, want HIT with short prefix and recorded length", w.Code, w.Header(), w.Body.Len())
			}
			if _, found, err := svc.cache.GetMeta(context.Background(), meta.Bucket, meta.Key); err != nil || found {
				t.Errorf("mismatched metadata remains after revalidation serve: found=%v err=%v", found, err)
			}
			if got := requestCountFrom(t, "error", metrics.SourceLocal); got != beforeErrors+1 {
				t.Errorf("local error count delta = %v, want 1", got-beforeErrors)
			}
			if got := requestCountFrom(t, "success", metrics.SourceLocal); got != beforeSuccesses {
				t.Errorf("local success count delta = %v, want 0", got-beforeSuccesses)
			}
		})
	}
}

func TestLegacyOverlongHintPreventsIncompleteReplay(t *testing.T) {
	body := cacheTestBodyAboveSmallObjectThreshold("cached body")
	overlong := append(append([]byte(nil), body...), []byte(" extra")...)
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	base := cacheclient.NewMemoryCache()
	var bodyStreams atomic.Int32
	var rangeReads atomic.Int32
	client := &testStreamCacheClient{
		CacheClient: base,
		stream: func(ctx context.Context, key string, w io.Writer) error {
			bodyStreams.Add(1)
			return base.GetStream(ctx, key, w)
		},
		rangeStream: func(ctx context.Context, key string, start, end int64, w io.Writer) error {
			rangeReads.Add(1)
			return base.GetRangeStream(ctx, key, start, end, w)
		},
	}
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	meta := &cache.CachedObjectMeta{
		Bucket:        "bucket",
		Key:           "key",
		ETag:          `"etag"`,
		ContentType:   "text/plain",
		ContentLength: int64(len(body)),
		StatusCode:    http.StatusOK,
	}
	if err := store.PutWithMeta(context.Background(), meta.Bucket, meta.Key, meta, overlong, 0); err != nil {
		t.Fatalf("seed overlong legacy entry: %v", err)
	}
	svc := NewService(&mockForwarder{}, store, cfg)
	var upstreamCalls atomic.Int32
	forwarder := &mockForwarder{
		conditionalResp: &http.Response{
			StatusCode: http.StatusNotModified,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader(nil)),
		},
		forwardFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request) error {
			upstreamCalls.Add(1)
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("ETag", meta.ETag)
			w.WriteHeader(http.StatusOK)
			_, err := w.Write(body)
			return err
		},
	}
	svc.forwarder = forwarder

	beforeErrors := requestCountFrom(t, "error", metrics.SourceLocal)
	beforeSuccesses := requestCountFrom(t, "success", metrics.SourceLocal)
	handlerErrors := make(chan error, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerErrors <- svc.HandleGetObject(w, r)
	}))
	defer server.Close()
	clientHTTP := &http.Client{Timeout: 5 * time.Second}

	first, err := clientHTTP.Get(server.URL + "/bucket/key")
	if err != nil {
		t.Fatalf("first GET: %v", err)
	}
	firstBody, firstReadErr := io.ReadAll(first.Body)
	_ = first.Body.Close()
	if first.StatusCode != http.StatusOK || first.Header.Get("Content-Length") != strconv.Itoa(len(body)) {
		t.Fatalf("first response = %d headers=%v, want 200 with advertised length", first.StatusCode, first.Header)
	}
	if len(firstBody) != 0 || !errors.Is(firstReadErr, io.ErrUnexpectedEOF) {
		t.Fatalf("first response read = %d bytes, %v; want empty body and unexpected EOF", len(firstBody), firstReadErr)
	}
	if err := <-handlerErrors; err != nil {
		t.Fatalf("first HandleGetObject: %v", err)
	}
	if got := bodyStreams.Load(); got != 1 {
		t.Fatalf("cache body streams after first GET = %d, want 1", got)
	}
	if got := requestCountFrom(t, "error", metrics.SourceLocal); got != beforeErrors+1 {
		t.Errorf("first local error count delta = %v, want 1", got-beforeErrors)
	}
	if got := requestCountFrom(t, "success", metrics.SourceLocal); got != beforeSuccesses {
		t.Errorf("first local success count delta = %v, want 0", got-beforeSuccesses)
	}
	if hinted, err := store.HasBodyLengthMismatch(context.Background(), meta.Bucket, meta.Key, meta.BodyDiscriminator(), meta.ContentLength); err != nil || !hinted {
		t.Fatalf("legacy length hint = (%v, %v), want (true, nil)", hinted, err)
	}

	secondRequest, err := http.NewRequest(http.MethodGet, server.URL+"/bucket/key", nil)
	if err != nil {
		t.Fatalf("second request: %v", err)
	}
	secondRequest.Header.Set("Cache-Control", "no-cache")
	second, err := clientHTTP.Do(secondRequest)
	if err != nil {
		t.Fatalf("second revalidation GET: %v", err)
	}
	secondBody, secondReadErr := io.ReadAll(second.Body)
	_ = second.Body.Close()
	if err := <-handlerErrors; err != nil {
		t.Fatalf("second revalidation HandleGetObject: %v", err)
	}
	if !forwarder.conditionalCalled {
		t.Fatal("second GET did not take the revalidation path")
	}
	if secondReadErr != nil || second.StatusCode != http.StatusOK || second.Header.Get(XCacheHeader) != XCacheMiss || second.Header.Get("Content-Length") != strconv.Itoa(len(body)) || !bytes.Equal(secondBody, body) {
		t.Fatalf("second revalidation response = %d headers=%v body_bytes=%d read_error=%v, want complete 200 MISS", second.StatusCode, second.Header, len(secondBody), secondReadErr)
	}
	if got := bodyStreams.Load(); got != 1 {
		t.Errorf("cache body streams after quarantined retry = %d, want 1", got)
	}
	if got := rangeReads.Load(); got != 1 {
		t.Errorf("range probes after quarantined retry = %d, want 1", got)
	}
	if got := upstreamCalls.Load(); got != 1 {
		t.Errorf("upstream calls after quarantined retry = %d, want 1", got)
	}
	if _, found, err := store.GetMeta(context.Background(), meta.Bucket, meta.Key); err != nil || !found {
		t.Errorf("legacy metadata should remain until TTL: found=%v err=%v", found, err)
	}
}

func TestHandleGetObject_ShortCacheBodyUpstreamFailureBeforeCommit(t *testing.T) {
	objectBody := []byte("complete object body")
	cacheBody := objectBody[:5]
	svc, _ := newWholeObjectCacheServiceWithCoordinator(t, objectBody, cacheBody, true)
	wantErr := errors.New("upstream request failed")
	var upstreamCalls atomic.Int32
	svc.forwarder = &mockForwarder{
		doRequestFunc: func(context.Context, *http.Request, string, string) (*http.Response, error) {
			upstreamCalls.Add(1)
			return nil, wantErr
		},
	}

	beforeLocalSuccess := requestCountFrom(t, "success", metrics.SourceLocal)
	beforeUpstreamErrors := requestCountFrom(t, "error", metrics.SourceUpstream)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w := &commitTrackingResponseWriter{ResponseRecorder: httptest.NewRecorder()}
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil).WithContext(ctx)
	err := svc.HandleGetObject(w, r)

	if !errors.Is(err, wantErr) {
		t.Fatalf("HandleGetObject() error = %v, want %v", err, wantErr)
	}
	if got := upstreamCalls.Load(); got != 1 {
		t.Errorf("upstream calls = %d, want 1", got)
	}
	if w.committed || w.Body.Len() != 0 || w.Header().Get(XCacheHeader) != "" {
		t.Errorf("response committed before the upstream error: committed=%v code=%d headers=%v body=%q", w.committed, w.Code, w.Header(), w.Body.String())
	}
	if got := requestCountFrom(t, "success", metrics.SourceLocal); got != beforeLocalSuccess {
		t.Errorf("local success count delta = %v, want 0", got-beforeLocalSuccess)
	}
	if got := requestCountFrom(t, "error", metrics.SourceUpstream); got != beforeUpstreamErrors+1 {
		t.Errorf("upstream error count delta = %v, want 1", got-beforeUpstreamErrors)
	}
}

func TestHandleGetObject_CompletedPreHeaderBroadcastReturnsError(t *testing.T) {
	wantErr := errors.New("upstream request failed")
	var upstreamCalls atomic.Int32
	forwarder := &mockForwarder{
		doRequestFunc: func(context.Context, *http.Request, string, string) (*http.Response, error) {
			upstreamCalls.Add(1)
			return nil, wantErr
		},
	}
	svc, _ := newTestService(forwarder, true)
	broadcastKey := makeBroadcastKey("bucket", "key", "")
	broadcaster, first := svc.broadcastManager.GetOrCreate(broadcastKey)
	if !first {
		t.Fatal("initial GetOrCreate did not create a broadcaster")
	}
	broadcaster.Complete(wantErr)
	defer svc.broadcastManager.Remove(broadcastKey)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w := &commitTrackingResponseWriter{ResponseRecorder: httptest.NewRecorder()}
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil).WithContext(ctx)
	err := svc.HandleGetObject(w, r)

	if !errors.Is(err, wantErr) {
		t.Fatalf("HandleGetObject() error = %v, want %v", err, wantErr)
	}
	if got := upstreamCalls.Load(); got != 0 {
		t.Errorf("late listener started %d upstream fetches, want 0", got)
	}
	if w.committed || w.Body.Len() != 0 || w.Header().Get(XCacheHeader) != "" {
		t.Errorf("response committed before the broadcast error: committed=%v code=%d headers=%v body=%q", w.committed, w.Code, w.Header(), w.Body.String())
	}
}

func requestCount(t *testing.T, status string) float64 {
	return requestCountFrom(t, status, metrics.SourceLocal)
}

func requestCountFrom(t *testing.T, status, source string) float64 {
	t.Helper()

	var metric dto.Metric
	// mode defaults to "unknown" in tests (SetMode is a main.go concern).
	if err := metrics.RequestsTotal.WithLabelValues("GetObject", status, "unknown", source).Write(&metric); err != nil {
		t.Fatalf("read %s request count: %v", status, err)
	}
	return metric.GetCounter().GetValue()
}
