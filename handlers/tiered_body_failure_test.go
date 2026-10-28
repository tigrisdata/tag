package handlers

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	cacheclient "github.com/tigrisdata/ocache/client"
	ocachecoordinator "github.com/tigrisdata/ocache/coordinator"
	ocacheerrors "github.com/tigrisdata/ocache/storage/errors"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
	"github.com/tigrisdata/tag/s3err"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type tieredBodyReadClient struct {
	cacheclient.CacheClient
	getStream      func(context.Context, string, io.Writer) error
	getRangeStream func(context.Context, string, int64, int64, io.Writer) error
}

func (c *tieredBodyReadClient) GetStream(ctx context.Context, key string, w io.Writer) error {
	if c.getStream != nil {
		return c.getStream(ctx, key, w)
	}
	return c.CacheClient.GetStream(ctx, key, w)
}

func (c *tieredBodyReadClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if c.getRangeStream != nil {
		return c.getRangeStream(ctx, key, start, end, w)
	}
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

type tieredBodyReadForwarder struct {
	proxy.RequestForwarder
}

func (tieredBodyReadForwarder) ValidateAndGetCredentials(*http.Request) (proxy.AuthResult, string, string, error) {
	return proxy.AuthValidated, "access", "secret", nil
}

type tieredBodyReadFixture struct {
	client *tieredBodyReadClient
	store  *cache.Cache
	router http.Handler
	key    string
	etag   string
}

func newTieredBodyReadFixture(t *testing.T, body []byte, contentLength int64) *tieredBodyReadFixture {
	return newTieredBodyReadFixtureForKey(t, body, contentLength, "object")
}

func newTieredBodyReadFixtureForKey(t *testing.T, body []byte, contentLength int64, key string) *tieredBodyReadFixture {
	t.Helper()

	cfg := config.NewDefault()
	cfg.Mode = config.ModeTiered
	cfg.Cache.SetBlockCachingEnabled(false)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate tiered config: %v", err)
	}

	client := &tieredBodyReadClient{CacheClient: cacheclient.NewMemoryCache()}
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	service := proxy.NewService(&tieredBodyReadForwarder{}, store, cfg)
	t.Cleanup(service.Stop)
	sum := md5.Sum(body)
	etag := `"` + hex.EncodeToString(sum[:]) + `"`
	fixture := &tieredBodyReadFixture{
		client: client,
		store:  store,
		router: NewServer(service, "127.0.0.1", 0, false, 0).Router(),
		key:    key,
		etag:   etag,
	}

	meta := &cache.CachedObjectMeta{
		Bucket:        "bucket",
		Key:           key,
		ETag:          etag,
		ContentLength: contentLength,
		StatusCode:    http.StatusOK,
	}
	if err := store.PutWithMeta(context.Background(), meta.Bucket, meta.Key, meta, body, 60); err != nil {
		t.Fatalf("seed local-tier object: %v", err)
	}
	return fixture
}

func (f *tieredBodyReadFixture) get() *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	f.router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/bucket/"+url.PathEscape(f.key), nil))
	return response
}

func (f *tieredBodyReadFixture) getRange(value string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/bucket/"+url.PathEscape(f.key), nil)
	request.Header.Set("Range", value)
	f.router.ServeHTTP(response, request)
	return response
}

func (f *tieredBodyReadFixture) put(body string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPut, "/bucket/"+url.PathEscape(f.key), strings.NewReader(body))
	request.Header.Set("Content-Length", strconv.Itoa(len(body)))
	f.router.ServeHTTP(response, request)
	return response
}

func errorCode(t *testing.T, response *httptest.ResponseRecorder) s3err.ErrorCode {
	t.Helper()
	var body s3err.ErrorResponse
	if err := xml.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode S3 error response %q: %v", response.Body.String(), err)
	}
	return body.Code
}

func TestTieredBodyReadFailureReturnsRetryableHTTPError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "internal cache error", err: status.Error(codes.Internal, "backend read failed")},
		{name: "ordinary I/O error", err: errors.New("disk read failed")},
		{name: "embedded backend I/O error", err: ocacheerrors.NewIOError("Get", "body|bucket|object", errors.New("disk read failed"))},
		{name: "cluster peer unavailable", err: status.Error(codes.Unavailable, "peer unavailable")},
		{name: "OCache owner-routing error", err: ocachecoordinator.NewNodeNotFoundError("owner", "body|bucket/object/body-ref")},
		{name: "OCache retry-exhausted owner-routing error", err: ocachecoordinator.NewMaxRetriesExceededError("owner", "body|bucket/object/body-ref", 1, ocachecoordinator.NewNodeNotFoundError("owner", "body|bucket/object/body-ref"))},
		{name: "OCache forwarded owner-routing error", err: status.Error(codes.Unavailable, "routing error: node not found in ring")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte("body remains readable")
			fixture := newTieredBodyReadFixture(t, body, int64(len(body)))
			_, beforeVersion, found, err := fixture.store.GetMetaWithVersion(context.Background(), "bucket", "object")
			if err != nil || !found {
				t.Fatalf("read seeded metadata: found=%v err=%v", found, err)
			}

			fixture.client.getStream = func(context.Context, string, io.Writer) error { return tc.err }
			response := fixture.get()
			if response.Code != http.StatusInternalServerError {
				t.Errorf("HTTP status = %d, want %d; body=%q", response.Code, http.StatusInternalServerError, response.Body.String())
			}
			if got := errorCode(t, response); got != s3err.ErrInternalError {
				t.Errorf("S3 error code = %q, want %q", got, s3err.ErrInternalError)
			}
			if got := response.Header().Get(proxy.XCacheHeader); got != "" {
				t.Errorf("%s = %q, want no authoritative cache-miss response", proxy.XCacheHeader, got)
			}

			meta, afterVersion, found, err := fixture.store.GetMetaWithVersion(context.Background(), "bucket", "object")
			if err != nil || !found || meta == nil {
				t.Errorf("metadata after failed read: meta=%+v found=%v err=%v, want original metadata", meta, found, err)
			} else if afterVersion != beforeVersion || meta.ETag != fixture.etag {
				t.Errorf("metadata after failed read: version=%d ETag=%q, want version=%d and original ETag", afterVersion, meta.ETag, beforeVersion)
			}

			fixture.client.getStream = nil
			recovered := fixture.get()
			if recovered.Code != http.StatusOK || recovered.Body.String() != string(body) {
				t.Errorf("GET after the cache read recovers: status=%d body=%q, want 200 and original body", recovered.Code, recovered.Body.String())
			}
		})
	}
}

func TestTieredBodyReadIOErrorKeyContainingNotFoundRemainsRetryable(t *testing.T) {
	body := []byte("body remains readable")
	fixture := newTieredBodyReadFixtureForKey(t, body, int64(len(body)), "not found")
	_, beforeVersion, found, err := fixture.store.GetMetaWithVersion(context.Background(), "bucket", fixture.key)
	if err != nil || !found {
		t.Fatalf("read seeded metadata: found=%v err=%v", found, err)
	}

	wantBodyKey := cache.MakeBodyKey("bucket", fixture.key, fixture.etag)
	var readKey string
	readError := func(key string) error {
		readKey = key
		return ocacheerrors.NewIOError("Get", key, errors.New("disk read failed"))
	}
	checkRetryable := func(stage string, response *httptest.ResponseRecorder) {
		t.Helper()
		if readKey != wantBodyKey {
			t.Errorf("%s body key = %q, want %q", stage, readKey, wantBodyKey)
		}
		if response.Code != http.StatusInternalServerError || errorCode(t, response) != s3err.ErrInternalError {
			t.Errorf("%s response = %d %q, want 500 InternalError", stage, response.Code, response.Body.String())
		}
		if got := response.Header().Get(proxy.XCacheHeader); got != "" {
			t.Errorf("%s %s = %q, want no authoritative cache-miss response", stage, proxy.XCacheHeader, got)
		}
		if _, version, found, err := fixture.store.GetMetaWithVersion(context.Background(), "bucket", fixture.key); err != nil || !found || version != beforeVersion {
			t.Errorf("metadata after %s: found=%v version=%d err=%v, want unchanged version %d", stage, found, version, err, beforeVersion)
		}
	}

	fixture.client.getStream = func(_ context.Context, key string, _ io.Writer) error { return readError(key) }
	checkRetryable("full GET", fixture.get())
	fixture.client.getStream = nil
	if recovered := fixture.get(); recovered.Code != http.StatusOK || recovered.Body.String() != string(body) {
		t.Errorf("GET after full-read recovery = %d %q, want 200 and original body", recovered.Code, recovered.Body.String())
	}

	fixture.client.getRangeStream = func(ctx context.Context, key string, start, end int64, w io.Writer) error {
		if start == 0 && end == 1 {
			return fixture.client.CacheClient.GetRangeStream(ctx, key, start, end, w)
		}
		return readError(key)
	}
	checkRetryable("Range GET", fixture.getRange("bytes=2-5"))
	fixture.client.getRangeStream = nil
	if recovered := fixture.getRange("bytes=2-5"); recovered.Code != http.StatusPartialContent || recovered.Body.String() != string(body[2:6]) {
		t.Errorf("Range GET after read recovery = %d %q, want 206 and %q", recovered.Code, recovered.Body.String(), body[2:6])
	}

	var bodyReads int
	fixture.client.getRangeStream = func(_ context.Context, key string, _, _ int64, _ io.Writer) error {
		return readError(key)
	}
	fixture.client.getStream = func(context.Context, string, io.Writer) error {
		bodyReads++
		return nil
	}
	checkRetryable("body-presence probe", fixture.get())
	if bodyReads != 0 {
		t.Errorf("body reads after failed presence probe = %d, want 0", bodyReads)
	}
	fixture.client.getRangeStream = nil
	fixture.client.getStream = nil
	if recovered := fixture.get(); recovered.Code != http.StatusOK || recovered.Body.String() != string(body) {
		t.Errorf("GET after probe recovery = %d %q, want 200 and original body", recovered.Code, recovered.Body.String())
	}
}

func TestTieredRangeBodyReadFailureReturnsRetryableHTTPError(t *testing.T) {
	body := []byte("range body remains readable")
	fixture := newTieredBodyReadFixture(t, body, int64(len(body)))
	_, beforeVersion, found, err := fixture.store.GetMetaWithVersion(context.Background(), "bucket", "object")
	if err != nil || !found {
		t.Fatalf("read seeded metadata: found=%v err=%v", found, err)
	}

	for _, readErr := range []error{
		status.Error(codes.Internal, "backend range read failed"),
		ocachecoordinator.NewNodeNotFoundError("owner", "body|bucket/object/body-ref"),
		ocachecoordinator.NewMaxRetriesExceededError("owner", "body|bucket/object/body-ref", 1, ocachecoordinator.NewNodeNotFoundError("owner", "body|bucket/object/body-ref")),
		status.Error(codes.Unavailable, "routing error: node not found in ring"),
	} {
		fixture.client.getRangeStream = func(ctx context.Context, key string, start, end int64, w io.Writer) error {
			if start == 0 && end == 1 {
				return fixture.client.CacheClient.GetRangeStream(ctx, key, start, end, w)
			}
			return readErr
		}
		response := fixture.getRange("bytes=2-5")
		if response.Code != http.StatusInternalServerError || errorCode(t, response) != s3err.ErrInternalError {
			t.Errorf("range GET response = %d %q, want 500 InternalError", response.Code, response.Body.String())
		}
		if got := response.Header().Get(proxy.XCacheHeader); got != "" {
			t.Errorf("%s = %q, want no authoritative cache-miss response", proxy.XCacheHeader, got)
		}
		if _, afterVersion, found, err := fixture.store.GetMetaWithVersion(context.Background(), "bucket", "object"); err != nil || !found || afterVersion != beforeVersion {
			t.Errorf("metadata after range read failure: found=%v version=%d err=%v, want unchanged version %d", found, afterVersion, err, beforeVersion)
		}
	}

	fixture.client.getRangeStream = nil
	recovered := fixture.getRange("bytes=2-5")
	if recovered.Code != http.StatusPartialContent || recovered.Body.String() != string(body[2:6]) {
		t.Errorf("range GET after the cache read recovers: status=%d body=%q, want 206 and %q", recovered.Code, recovered.Body.String(), body[2:6])
	}
}

func TestTieredPresenceProbeOwnerRoutingErrorIsRetryable(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{
			name: "direct",
			err:  ocachecoordinator.NewNodeNotFoundError("owner", "body|bucket/object/body-ref"),
		},
		{
			name: "retry-exhausted",
			err: ocachecoordinator.NewMaxRetriesExceededError(
				"owner",
				"body|bucket/object/body-ref",
				1,
				ocachecoordinator.NewNodeNotFoundError("owner", "body|bucket/object/body-ref"),
			),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newTieredBodyReadFixture(t, []byte("body remains readable"), int64(len("body remains readable")))
			_, beforeVersion, found, err := fixture.store.GetMetaWithVersion(context.Background(), "bucket", "object")
			if err != nil || !found {
				t.Fatalf("read seeded metadata: found=%v err=%v", found, err)
			}

			var bodyReads int
			fixture.client.getRangeStream = func(context.Context, string, int64, int64, io.Writer) error {
				return tc.err
			}
			fixture.client.getStream = func(context.Context, string, io.Writer) error {
				bodyReads++
				return nil
			}
			response := fixture.get()
			if response.Code != http.StatusInternalServerError || errorCode(t, response) != s3err.ErrInternalError {
				t.Errorf("GET response = %d %q, want 500 InternalError", response.Code, response.Body.String())
			}
			if bodyReads != 0 {
				t.Errorf("body reads = %d, want 0 after presence probe failure", bodyReads)
			}
			if _, afterVersion, found, err := fixture.store.GetMetaWithVersion(context.Background(), "bucket", "object"); err != nil || !found || afterVersion != beforeVersion {
				t.Errorf("metadata after failed presence probe: found=%v version=%d err=%v, want unchanged version %d", found, afterVersion, err, beforeVersion)
			}
		})
	}
}

func TestTieredPresenceProbeUnavailableWithNotFoundTextIsRetryable(t *testing.T) {
	fixture := newTieredBodyReadFixture(t, []byte("body remains readable"), int64(len("body remains readable")))
	_, beforeVersion, found, err := fixture.store.GetMetaWithVersion(context.Background(), "bucket", "object")
	if err != nil || !found {
		t.Fatalf("read seeded metadata: found=%v err=%v", found, err)
	}

	var bodyReads int
	fixture.client.getRangeStream = func(context.Context, string, int64, int64, io.Writer) error {
		return status.Error(codes.Unavailable, "routing error: node not found in ring")
	}
	fixture.client.getStream = func(context.Context, string, io.Writer) error {
		bodyReads++
		return nil
	}
	response := fixture.get()
	if response.Code != http.StatusInternalServerError || errorCode(t, response) != s3err.ErrInternalError {
		t.Errorf("GET response = %d %q, want 500 InternalError", response.Code, response.Body.String())
	}
	if bodyReads != 0 {
		t.Errorf("body reads = %d, want 0 after presence probe failure", bodyReads)
	}
	if _, afterVersion, found, err := fixture.store.GetMetaWithVersion(context.Background(), "bucket", "object"); err != nil || !found || afterVersion != beforeVersion {
		t.Errorf("metadata after failed presence probe: found=%v version=%d err=%v, want unchanged version %d", found, afterVersion, err, beforeVersion)
	}
}

func TestTieredShortRangeBeyondBodyRemainsAuthoritativeMiss(t *testing.T) {
	fixture := newTieredBodyReadFixture(t, []byte("short"), int64(len("complete original body")))
	response := fixture.getRange("bytes=8-9")
	if response.Code != http.StatusNotFound || errorCode(t, response) != s3err.ErrNoSuchKey {
		t.Errorf("range GET response = %d %q, want 404 NoSuchKey", response.Code, response.Body.String())
	}
	if meta, _, found, err := fixture.store.GetMetaWithVersion(context.Background(), "bucket", "object"); err != nil || found {
		t.Errorf("metadata after confirmed short-range failure: meta=%+v found=%v err=%v, want cleanup", meta, found, err)
	}
}

func TestTieredConfirmedBodyFailureStillMissesAndCleansMetadata(t *testing.T) {
	for _, tc := range []struct {
		name       string
		body       []byte
		readError  error
		deleteBody bool
		emptyRead  bool
		corrupt    bool
	}{
		{
			name:       "body absent after presence probe",
			body:       []byte("present at probe"),
			readError:  status.Error(codes.NotFound, "key not found"),
			deleteBody: true,
		},
		{
			name:      "successful empty stream after presence probe",
			body:      []byte("present at probe"),
			emptyRead: true,
		},
		{
			name:    "verified length mismatch",
			body:    []byte("short"),
			corrupt: true,
		},
		{
			name:      "backend reports data corruption",
			body:      []byte("present at probe"),
			readError: status.Error(codes.DataLoss, "data corruption detected"),
		},
		{
			name:      "embedded backend reports data corruption",
			body:      []byte("present at probe"),
			readError: ocacheerrors.NewCorruptionError("Get", "body|bucket|object", errors.New("corrupt value")),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metaLength := int64(len(tc.body))
			if tc.corrupt {
				metaLength = int64(len("complete original body"))
			}
			fixture := newTieredBodyReadFixture(t, tc.body, metaLength)
			if tc.readError != nil {
				fixture.client.getStream = func(ctx context.Context, key string, w io.Writer) error {
					if tc.deleteBody {
						if err := fixture.client.CacheClient.Delete(ctx, key); err != nil {
							return err
						}
					}
					return tc.readError
				}
			} else if tc.emptyRead {
				fixture.client.getStream = func(ctx context.Context, key string, w io.Writer) error {
					// The embedded client returns nil with no bytes for an absent
					// stream; model that after eviction following the successful probe.
					if err := fixture.client.CacheClient.Delete(ctx, key); err != nil {
						return err
					}
					err := fixture.client.CacheClient.GetStream(ctx, key, w)
					if status.Code(err) == codes.NotFound {
						return nil
					}
					return err
				}
			}

			response := fixture.get()
			if response.Code != http.StatusNotFound || errorCode(t, response) != s3err.ErrNoSuchKey {
				t.Errorf("GET response = %d %q, want 404 NoSuchKey", response.Code, response.Body.String())
			}
			if meta, _, found, err := fixture.store.GetMetaWithVersion(context.Background(), "bucket", "object"); err != nil || found {
				t.Errorf("metadata after confirmed body failure: meta=%+v found=%v err=%v, want metadata cleanup", meta, found, err)
			}
		})
	}
}

func TestTieredConfirmedBodyAbsencePreservesNewerPut(t *testing.T) {
	body := []byte("same contents")
	fixture := newTieredBodyReadFixture(t, body, int64(len(body)))
	_, beforeVersion, found, err := fixture.store.GetMetaWithVersion(context.Background(), "bucket", "object")
	if err != nil || !found {
		t.Fatalf("read seeded metadata: found=%v err=%v", found, err)
	}

	var putStatus int
	fixture.client.getStream = func(ctx context.Context, key string, w io.Writer) error {
		// The GET has already read its metadata snapshot and passed the presence
		// probe. Commit an identical-content PUT before the old body disappears;
		// the new version has the same ETag but a distinct metadata version.
		put := fixture.put(string(body))
		putStatus = put.Code
		if putStatus != http.StatusOK {
			return errors.New("concurrent PUT did not commit")
		}
		if err := fixture.client.CacheClient.Delete(ctx, key); err != nil {
			return err
		}
		return fixture.client.CacheClient.GetStream(ctx, key, w)
	}

	response := fixture.get()
	if putStatus != http.StatusOK {
		t.Fatalf("same-content PUT status = %d, want %d", putStatus, http.StatusOK)
	}
	if response.Code != http.StatusNotFound || errorCode(t, response) != s3err.ErrNoSuchKey {
		t.Errorf("GET of the older snapshot = %d %q, want 404 NoSuchKey", response.Code, response.Body.String())
	}
	meta, afterVersion, found, err := fixture.store.GetMetaWithVersion(context.Background(), "bucket", "object")
	if err != nil || !found || meta == nil {
		t.Fatalf("new metadata after stale-body cleanup: meta=%+v found=%v err=%v", meta, found, err)
	}
	if afterVersion == beforeVersion || meta.ETag != fixture.etag {
		t.Fatalf("new metadata version=%d ETag=%q; want newer version with same ETag %q", afterVersion, meta.ETag, fixture.etag)
	}

	fixture.client.getStream = nil
	recovered := fixture.get()
	if recovered.Code != http.StatusOK || recovered.Body.String() != string(body) {
		t.Errorf("GET after same-content PUT: status=%d body=%q, want 200 and current body", recovered.Code, recovered.Body.String())
	}
}
