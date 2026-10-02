package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
)

// A GET that races an in-flight PUT can fetch the pre-PUT object and begin
// re-caching it. HandlePutObject must re-invalidate AFTER forwarding so that
// stale repopulation does not survive (read-after-write).
func TestHandlePutObject_ReinvalidatesAfterForward(t *testing.T) {
	var c *cache.Cache

	// Simulate the racing GET writing a stale entry mid-PUT (during Forward).
	repopulate := func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		b, k := ParseBucketKey(r)
		meta := &cache.CachedObjectMeta{Bucket: b, Key: k, ETag: `"stale"`, ContentLength: 5, StatusCode: 200}
		_ = c.PutWithMeta(context.Background(), b, k, meta, []byte("stale"), 60)
		w.WriteHeader(http.StatusOK)
		return nil
	}

	var svc *Service
	svc, c = newTestService(&mockForwarder{forwardFunc: repopulate}, true)

	r := httptest.NewRequest(http.MethodPut, "/test-bucket/test-key", strings.NewReader("new body"))
	w := httptest.NewRecorder()
	if err := svc.HandlePutObject(w, r); err != nil {
		t.Fatalf("HandlePutObject: %v", err)
	}

	b, k := ParseBucketKey(r)
	if _, found, _ := c.GetMeta(context.Background(), b, k); found {
		t.Error("stale entry still cached after PUT — post-forward re-invalidation missing")
	}
}

type earlySuccessRoundTripper struct {
	prefixAccepted chan<- []byte
	allowResponse  <-chan struct{}
}

func (transport earlySuccessRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	var prefix [1]byte
	n, err := io.ReadFull(req.Body, prefix[:])
	if err != nil {
		return nil, err
	}
	transport.prefixAccepted <- append([]byte(nil), prefix[:n]...)
	<-transport.allowResponse
	if trace := httptrace.ContextClientTrace(req.Context()); trace != nil && trace.WroteRequest != nil {
		trace.WroteRequest(httptrace.WroteRequestInfo{})
	}
	return &http.Response{
		StatusCode:    http.StatusOK,
		Header:        http.Header{"Content-Length": []string{"6"}},
		Body:          io.NopCloser(strings.NewReader("stored")),
		ContentLength: 6,
		Request:       req,
	}, nil
}

type releaseBodyOnDeadlineWriter struct {
	*httptest.ResponseRecorder
	releaseBody  chan struct{}
	releaseOnce  *sync.Once
	deadlineSet  chan struct{}
	deadlineOnce sync.Once
}

func (w *releaseBodyOnDeadlineWriter) SetReadDeadline(time.Time) error {
	w.releaseOnce.Do(func() { close(w.releaseBody) })
	w.deadlineOnce.Do(func() { close(w.deadlineSet) })
	return nil
}

func TestHandlePutObject_ReinvalidatesAfterUpstreamSuccessAndValidationError(t *testing.T) {
	const (
		accessKey = "AKIAIOSDNN7EXAMPLE"
		secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
		region    = "us-east-1"
		bucket    = "bucket"
		key       = "chunked-object"
	)
	const wireBody = "8\r\nABCDEFGH\r\n5\r\nFAIL!\r\n0\r\n\r\n"

	bodyBlocked := make(chan struct{})
	releaseBody := make(chan struct{})
	var releaseBodyOnce sync.Once
	defer releaseBodyOnce.Do(func() { close(releaseBody) })
	allowResponse := make(chan struct{})
	var allowResponseOnce sync.Once
	defer allowResponseOnce.Do(func() { close(allowResponse) })
	prefixAccepted := make(chan []byte, 1)

	service, c := newTestService(&mockForwarder{}, true)
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(accessKey, secretKey)
	base := newBaseForwarder("http://upstream.example.com", region, 1)
	base.httpClient = &http.Client{
		Transport: earlySuccessRoundTripper{
			prefixAccepted: prefixAccepted,
			allowResponse:  allowResponse,
		},
		Timeout: 5 * time.Second,
	}
	service.forwarder = &signingForwarder{
		baseForwarder: base,
		credStore:     credentials,
		validator:     auth.NewRequestValidator(credentials),
	}
	service.config.Cache.WarmOnWrite = false

	chunkHeader := "8\r\n"
	requestBody := &chunkExtentBodyGate{
		reader:    strings.NewReader(wireBody),
		stopAfter: int64(len(chunkHeader) + 1),
		blocked:   bodyBlocked,
		release:   releaseBody,
	}
	headers := make(http.Header)
	headers.Set("Content-Encoding", "aws-chunked")
	headers.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
	headers.Set("X-Amz-Decoded-Content-Length", "12")
	r, err := auth.NewRequestSigner("http://client.example.com", region).SignRequest(
		context.Background(),
		http.MethodPut,
		"/"+bucket+"/"+key,
		requestBody,
		"STREAMING-UNSIGNED-PAYLOAD-TRAILER",
		accessKey,
		secretKey,
		headers,
	)
	if err != nil {
		t.Fatal(err)
	}
	r.ContentLength = int64(len(wireBody))

	writer := &releaseBodyOnDeadlineWriter{
		ResponseRecorder: httptest.NewRecorder(),
		releaseBody:      releaseBody,
		releaseOnce:      &releaseBodyOnce,
		deadlineSet:      make(chan struct{}),
	}
	forwardDone := make(chan error, 1)
	go func() { forwardDone <- service.HandlePutObject(writer, r) }()
	select {
	case prefix := <-prefixAccepted:
		if string(prefix) != "A" {
			t.Fatalf("upstream prefix = %q, want the first valid-frame byte", prefix)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("upstream did not accept a prefix of the valid frame")
	}

	// A GET that began before the PUT completed can repopulate stale bytes after
	// the pre-forward invalidation. Model that commit before the upstream 2xx.
	staleMeta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          `"stale"`,
		ContentLength: 5,
		StatusCode:    http.StatusOK,
	}
	if err := c.PutWithMeta(context.Background(), bucket, key, staleMeta, []byte("stale"), 60); err != nil {
		t.Fatalf("seed racing stale refill: %v", err)
	}
	allowResponseOnce.Do(func() { close(allowResponse) })

	select {
	case err := <-forwardDone:
		if !errors.Is(err, errAWSChunkExceedsDecodedLength) {
			t.Fatalf("HandlePutObject error = %v, want the overlong-frame error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("PUT did not finish after the overlong frame was validated")
	}
	select {
	case <-bodyBlocked:
	default:
		t.Fatal("post-response validation did not reach the unread client body")
	}
	select {
	case <-writer.deadlineSet:
	default:
		t.Fatal("post-response validation did not set a read deadline")
	}
	if writer.Body.Len() != 0 {
		t.Fatalf("upstream success body reached the client despite validation failure: %q", writer.Body.String())
	}
	if _, found, err := c.GetMeta(context.Background(), bucket, key); err != nil {
		t.Fatalf("read cache metadata: %v", err)
	} else if found {
		t.Fatal("racing stale refill survived the upstream-success validation error")
	}
}

// A rejected CopyObject leaves the destination unchanged, so the post-forward
// re-invalidation must NOT fire — otherwise a racing refill of the still-valid
// destination is discarded, causing an unnecessary later miss.
func TestHandleCopyObject_FailedCopyKeepsRacingRefill(t *testing.T) {
	var c *cache.Cache
	repopulateThenFail := func(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
		b, k := ParseBucketKey(r)
		meta := &cache.CachedObjectMeta{Bucket: b, Key: k, ETag: `"refill"`, ContentLength: 6, StatusCode: 200}
		_ = c.PutWithMeta(context.Background(), b, k, meta, []byte("refill"), 60)
		body := []byte(`<Error><Code>AccessDenied</Code></Error>`)
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write(body)
		return &ResponseCapture{StatusCode: http.StatusForbidden, Body: body, Complete: true}, nil
	}

	var svc *Service
	svc, c = newTestService(&mockForwarder{captureFunc: repopulateThenFail}, true)

	r := httptest.NewRequest(http.MethodPut, "/dst-bucket/dst-key", nil)
	r.Header.Set("X-Amz-Copy-Source", "/src-bucket/src-key")
	w := httptest.NewRecorder()
	if err := svc.HandleCopyObject(w, r); err != nil {
		t.Fatalf("HandleCopyObject: %v", err)
	}

	b, k := ParseBucketKey(r)
	if _, found, _ := c.GetMeta(context.Background(), b, k); !found {
		t.Error("racing refill discarded after a FAILED copy — re-invalidation must be gated on success")
	}
}

// A CopyObject that returns 200 OK but an <Error> body actually failed, so the
// destination is unchanged and must not be re-invalidated.
func TestHandleCopyObject_200ErrorBodyKeepsRacingRefill(t *testing.T) {
	var c *cache.Cache
	repopulateThen200Error := func(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
		b, k := ParseBucketKey(r)
		meta := &cache.CachedObjectMeta{Bucket: b, Key: k, ETag: `"refill"`, ContentLength: 6, StatusCode: 200}
		_ = c.PutWithMeta(context.Background(), b, k, meta, []byte("refill"), 60)
		body := []byte(`<Error><Code>InternalError</Code></Error>`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return &ResponseCapture{StatusCode: http.StatusOK, Body: body, Complete: true}, nil
	}

	var svc *Service
	svc, c = newTestService(&mockForwarder{captureFunc: repopulateThen200Error}, true)

	r := httptest.NewRequest(http.MethodPut, "/dst-bucket/dst-key", nil)
	w := httptest.NewRecorder()
	if err := svc.HandleCopyObject(w, r); err != nil {
		t.Fatalf("HandleCopyObject: %v", err)
	}

	b, k := ParseBucketKey(r)
	if _, found, _ := c.GetMeta(context.Background(), b, k); !found {
		t.Error("racing refill discarded after a 200-with-error-body copy — that copy did not succeed")
	}
}

// A successful CopyObject changed the destination, so the post-forward
// re-invalidation must fire to drop any stale racing refill.
func TestHandleCopyObject_SuccessReinvalidates(t *testing.T) {
	var c *cache.Cache
	repopulateThenSucceed := func(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
		b, k := ParseBucketKey(r)
		meta := &cache.CachedObjectMeta{Bucket: b, Key: k, ETag: `"stale"`, ContentLength: 5, StatusCode: 200}
		_ = c.PutWithMeta(context.Background(), b, k, meta, []byte("stale"), 60)
		body := []byte(`<CopyObjectResult><ETag>"new"</ETag></CopyObjectResult>`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return &ResponseCapture{StatusCode: http.StatusOK, Body: body, Complete: true}, nil
	}

	var svc *Service
	svc, c = newTestService(&mockForwarder{captureFunc: repopulateThenSucceed}, true)

	r := httptest.NewRequest(http.MethodPut, "/dst-bucket/dst-key", nil)
	w := httptest.NewRecorder()
	if err := svc.HandleCopyObject(w, r); err != nil {
		t.Fatalf("HandleCopyObject: %v", err)
	}

	b, k := ParseBucketKey(r)
	if _, found, _ := c.GetMeta(context.Background(), b, k); found {
		t.Error("stale refill survived a SUCCESSFUL copy — post-forward re-invalidation should have removed it")
	}
}

// A bulk delete with a per-object failure must re-invalidate only the keys that
// were actually deleted; a racing refill of a FAILED key must survive.
func TestHandleDeleteObjects_PartialFailureKeepsFailedKeyRefill(t *testing.T) {
	var c *cache.Cache
	const okKey = "ok-key"
	const failKey = "fail-key"

	repopulateThenPartial := func(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
		b, _ := ParseBucketKey(r)
		for _, k := range []string{okKey, failKey} {
			meta := &cache.CachedObjectMeta{Bucket: b, Key: k, ETag: `"refill"`, ContentLength: 6, StatusCode: 200}
			_ = c.PutWithMeta(context.Background(), b, k, meta, []byte("refill"), 60)
		}
		body := []byte(`<DeleteResult><Deleted><Key>ok-key</Key></Deleted><Error><Key>fail-key</Key><Code>AccessDenied</Code></Error></DeleteResult>`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return &ResponseCapture{StatusCode: http.StatusOK, Body: body, Complete: true}, nil
	}

	var svc *Service
	svc, c = newTestService(&mockForwarder{captureFunc: repopulateThenPartial}, true)

	reqBody := `<Delete><Object><Key>ok-key</Key></Object><Object><Key>fail-key</Key></Object></Delete>`
	r := httptest.NewRequest(http.MethodPost, "/bulk-bucket?delete", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	if err := svc.HandleDeleteObjects(w, r); err != nil {
		t.Fatalf("HandleDeleteObjects: %v", err)
	}

	b, _ := ParseBucketKey(r)
	if _, found, _ := c.GetMeta(context.Background(), b, okKey); found {
		t.Error("deleted key's stale refill survived — it should have been re-invalidated")
	}
	if _, found, _ := c.GetMeta(context.Background(), b, failKey); !found {
		t.Error("failed key's valid refill was discarded — failed keys must not be re-invalidated")
	}
}

// A DeleteObjects request can list the same key under multiple version IDs. If one
// version's delete succeeds and another fails, the key's current object may have
// changed, so a racing refill must be dropped. Matching failures by key alone would
// wrongly treat the key as fully failed and keep the stale refill.
func TestHandleDeleteObjects_MixedVersionSuccessReinvalidatesKey(t *testing.T) {
	var c *cache.Cache
	const key = "versioned-key"

	repopulateThenMixed := func(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
		b, _ := ParseBucketKey(r)
		meta := &cache.CachedObjectMeta{Bucket: b, Key: key, ETag: `"refill"`, ContentLength: 6, StatusCode: 200}
		_ = c.PutWithMeta(context.Background(), b, key, meta, []byte("refill"), 60)
		body := []byte(`<DeleteResult><Deleted><Key>versioned-key</Key><VersionId>v1</VersionId></Deleted><Error><Key>versioned-key</Key><VersionId>v2</VersionId><Code>AccessDenied</Code></Error></DeleteResult>`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return &ResponseCapture{StatusCode: http.StatusOK, Body: body, Complete: true}, nil
	}

	var svc *Service
	svc, c = newTestService(&mockForwarder{captureFunc: repopulateThenMixed}, true)

	reqBody := `<Delete><Object><Key>versioned-key</Key><VersionId>v1</VersionId></Object><Object><Key>versioned-key</Key><VersionId>v2</VersionId></Object></Delete>`
	r := httptest.NewRequest(http.MethodPost, "/bulk-bucket?delete", strings.NewReader(reqBody))
	w := httptest.NewRecorder()
	if err := svc.HandleDeleteObjects(w, r); err != nil {
		t.Fatalf("HandleDeleteObjects: %v", err)
	}

	b, _ := ParseBucketKey(r)
	if _, found, _ := c.GetMeta(context.Background(), b, key); found {
		t.Error("key with a succeeded version-delete kept its stale refill — should be re-invalidated")
	}
}

// A failed unqualified delete may be reported with a VersionId the request never
// sent ("null" on a version-suspended bucket, or a concrete id the server
// resolved). Counting errors by key (not matching version tuples) keeps such a
// failed key's valid refill instead of over-invalidating it.
func TestHandleDeleteObjects_VersionIdMismatchFailureKeepsRefill(t *testing.T) {
	for _, respVersion := range []string{"null", "concrete-v3"} {
		t.Run(respVersion, func(t *testing.T) {
			var c *cache.Cache
			const key = "unqualified-key"

			repopulateThenFail := func(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
				b, _ := ParseBucketKey(r)
				meta := &cache.CachedObjectMeta{Bucket: b, Key: key, ETag: `"refill"`, ContentLength: 6, StatusCode: 200}
				_ = c.PutWithMeta(context.Background(), b, key, meta, []byte("refill"), 60)
				// Request sent no VersionId; response errors with a different representation.
				body := []byte(`<DeleteResult><Error><Key>unqualified-key</Key><VersionId>` + respVersion + `</VersionId><Code>AccessDenied</Code></Error></DeleteResult>`)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(body)
				return &ResponseCapture{StatusCode: http.StatusOK, Body: body, Complete: true}, nil
			}

			var svc *Service
			svc, c = newTestService(&mockForwarder{captureFunc: repopulateThenFail}, true)

			// Request omits VersionId entirely.
			reqBody := `<Delete><Object><Key>unqualified-key</Key></Object></Delete>`
			r := httptest.NewRequest(http.MethodPost, "/bulk-bucket?delete", strings.NewReader(reqBody))
			w := httptest.NewRecorder()
			if err := svc.HandleDeleteObjects(w, r); err != nil {
				t.Fatalf("HandleDeleteObjects: %v", err)
			}

			b, _ := ParseBucketKey(r)
			if _, found, _ := c.GetMeta(context.Background(), b, key); !found {
				t.Errorf("failed delete (response VersionId=%q) over-invalidated the key — valid refill discarded", respVersion)
			}
		})
	}
}
