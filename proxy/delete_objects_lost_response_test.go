package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

type deleteObjectsAttemptForwarder struct {
	*mockForwarder
	attempted bool
}

func (f *deleteObjectsAttemptForwarder) forwardWithCaptureAttempted(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) (*ResponseCapture, bool, error) {
	capture, err := f.ForwardWithCapture(ctx, w, r)
	return capture, f.attempted, err
}

func newDeleteObjectsCoordinationService(forwarder RequestForwarder, legacy bool) (*Service, *cache.Cache) {
	cfg := config.NewDefault()
	cfg.Cache.SetLegacyCoordination(legacy)
	store := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	return NewService(forwarder, store, cfg), store
}

func seedDeleteObjectsCache(t *testing.T, c *cache.Cache, bucket string, keys ...string) {
	t.Helper()
	for _, key := range keys {
		meta := &cache.CachedObjectMeta{
			Bucket:        bucket,
			Key:           key,
			ETag:          `"before-delete-` + key + `"`,
			ContentLength: int64(len("old body")),
			StatusCode:    http.StatusOK,
		}
		if err := c.PutWithMeta(context.Background(), bucket, key, meta, []byte("old body"), 60); err != nil {
			t.Fatalf("seed cache %s/%s: %v", bucket, key, err)
		}
	}
}

func deleteObjectsRefillTokens(t *testing.T, ctx context.Context, c *cache.Cache, bucket string, keys ...string) map[string]func() bool {
	t.Helper()
	refills := make(map[string]func() bool, len(keys))
	for _, key := range keys {
		key := key
		_, token, found, err := c.GetMetaWithVersion(ctx, bucket, key)
		if err != nil {
			t.Fatalf("read decision token for %s/%s: %v", bucket, key, err)
		}
		if found {
			t.Fatalf("pre-forward invalidation left %s/%s readable", bucket, key)
		}
		refills[key] = func() bool {
			meta := &cache.CachedObjectMeta{
				Bucket:        bucket,
				Key:           key,
				ETag:          `"old-refill-` + key + `"`,
				ContentLength: int64(len("old body")),
				StatusCode:    http.StatusOK,
			}
			wrote, err := c.PutWithMetaStreamIfVersion(context.Background(), bucket, key, meta, strings.NewReader("old body"), 60, token)
			if err != nil {
				t.Fatalf("publish version-guarded refill for %s/%s: %v", bucket, key, err)
			}
			return wrote
		}
	}
	return refills
}

func checkDeleteObjectsRefill(t *testing.T, c *cache.Cache, bucket, key string, refill func() bool, wantVisible bool) {
	t.Helper()
	wrote := refill()
	if wrote != wantVisible {
		t.Errorf("version-guarded refill for %s/%s wrote=%t, want %t", bucket, key, wrote, wantVisible)
	}
	meta, found, err := c.GetMeta(context.Background(), bucket, key)
	if err != nil {
		t.Fatalf("read post-refill metadata for %s/%s: %v", bucket, key, err)
	}
	if found != wantVisible {
		t.Errorf("post-refill metadata for %s/%s found=%t, want %t", bucket, key, found, wantVisible)
	} else if found && meta.ETag != `"old-refill-`+key+`"` {
		t.Errorf("post-refill ETag for %s/%s = %q, want old refill", bucket, key, meta.ETag)
	}
}

func TestDeleteObjectsLostResponseRefill(t *testing.T) {
	modes := []struct {
		name   string
		legacy bool
	}{
		{name: "legacy-tombstones", legacy: true},
		{name: "cas-fences", legacy: false},
	}

	for _, mode := range modes {
		mode := mode
		t.Run("lost-after-apply/"+mode.name, func(t *testing.T) {
			const bucket = "bulk-bucket"
			keys := []string{"deleted-key", "second-key"}
			requestBody := `<Delete><Object><Key>deleted-key</Key><VersionId>v1</VersionId></Object><Object><Key>second-key</Key></Object><Object><Key>deleted-key</Key><VersionId>v2</VersionId></Object></Delete>`
			accepted := make(chan []byte, 1)
			applyAndLoseReply := make(chan struct{})
			applied := make(chan struct{})
			originErr := make(chan error, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.RawQuery != "delete" {
					originErr <- errors.New("origin did not receive DeleteObjects")
					return
				}
				body, err := io.ReadAll(r.Body)
				if err != nil {
					originErr <- err
					return
				}
				accepted <- body
				<-applyAndLoseReply
				close(applied)
				hijacker, ok := w.(http.Hijacker)
				if !ok {
					originErr <- errors.New("origin response writer cannot hijack connection")
					return
				}
				conn, _, err := hijacker.Hijack()
				if err != nil {
					originErr <- err
					return
				}
				_ = conn.Close()
			}))
			defer server.Close()
			defer func() {
				select {
				case <-applyAndLoseReply:
				default:
					close(applyAndLoseReply)
				}
			}()

			base := newBaseForwarder(server.URL, "us-east-1", 10)
			base.httpClient = server.Client()
			forwarder := &transparentForwarder{
				baseForwarder:    base,
				proxySigner:      auth.NewProxySigner("test-access-key", "test-secret-key"),
				upstreamEndpoint: server.URL,
			}
			forwarder.initInterceptor()
			svc, c := newDeleteObjectsCoordinationService(forwarder, mode.legacy)
			seedDeleteObjectsCache(t, c, bucket, keys...)

			r := httptest.NewRequest(http.MethodPost, "/"+bucket+"?delete", strings.NewReader(requestBody))
			w := httptest.NewRecorder()
			forwardDone := make(chan error, 1)
			go func() { forwardDone <- svc.HandleDeleteObjects(w, r) }()

			select {
			case got := <-accepted:
				if string(got) != requestBody {
					t.Fatalf("origin request body = %q, want %q", got, requestBody)
				}
			case err := <-originErr:
				t.Fatalf("origin setup: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("origin did not receive DeleteObjects")
			}

			// The GET begins after the service's first invalidation and obtains the
			// same decision token as streamFromUpstream. Its old-object response is
			// held until after the origin applies the delete and loses its reply.
			tokens := deleteObjectsRefillTokens(t, context.Background(), c, bucket, keys...)
			close(applyAndLoseReply)
			select {
			case <-applied:
			case err := <-originErr:
				t.Fatalf("origin apply/response-loss setup: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("origin did not apply DeleteObjects before losing its reply")
			}

			select {
			case err := <-forwardDone:
				if err == nil || !errors.Is(err, io.EOF) {
					t.Errorf("HandleDeleteObjects error = %v, want the original lost-response EOF", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("HandleDeleteObjects did not return after the origin lost its reply")
			}
			for _, key := range keys {
				checkDeleteObjectsRefill(t, c, bucket, key, tokens[key], false)
			}
		})

		t.Run("canceled-client-context/"+mode.name, func(t *testing.T) {
			const bucket = "bulk-bucket"
			const key = "cancelled-key"
			seededErr := errors.New("upstream response unavailable")
			var svc *Service
			var c *cache.Cache
			var refill func() bool
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			mock := &mockForwarder{captureFunc: func(ctx context.Context, _ http.ResponseWriter, _ *http.Request) (*ResponseCapture, error) {
				refill = deleteObjectsRefillTokens(t, ctx, c, bucket, key)[key]
				cancel()
				return nil, seededErr
			}}
			forwarder := &deleteObjectsAttemptForwarder{mockForwarder: mock, attempted: true}
			svc, c = newDeleteObjectsCoordinationService(forwarder, mode.legacy)
			seedDeleteObjectsCache(t, c, bucket, key)

			r := httptest.NewRequest(http.MethodPost, "/"+bucket+"?delete", strings.NewReader(`<Delete><Object><Key>cancelled-key</Key></Object></Delete>`)).WithContext(ctx)
			err := svc.HandleDeleteObjects(httptest.NewRecorder(), r)
			if refill == nil {
				t.Fatal("capture racing GET token did not prepare a refill")
			}
			if ctx.Err() != context.Canceled {
				t.Fatalf("test request context error = %v, want canceled", ctx.Err())
			}
			if err != seededErr {
				t.Errorf("handler error = %v, want original error object %v", err, seededErr)
			}
			checkDeleteObjectsRefill(t, c, bucket, key, refill, false)
		})

		t.Run("pre-dispatch-error/"+mode.name, func(t *testing.T) {
			const bucket = "bulk-bucket"
			const key = "pre-dispatch-key"
			forwardErr := errors.New("request construction failed")
			var c *cache.Cache
			var refill func() bool
			mock := &mockForwarder{captureFunc: func(ctx context.Context, _ http.ResponseWriter, _ *http.Request) (*ResponseCapture, error) {
				refill = deleteObjectsRefillTokens(t, ctx, c, bucket, key)[key]
				return nil, forwardErr
			}}
			forwarder := &deleteObjectsAttemptForwarder{mockForwarder: mock, attempted: false}
			svc, c := newDeleteObjectsCoordinationService(forwarder, mode.legacy)
			seedDeleteObjectsCache(t, c, bucket, key)

			r := httptest.NewRequest(http.MethodPost, "/"+bucket+"?delete", strings.NewReader(`<Delete><Object><Key>pre-dispatch-key</Key></Object></Delete>`))
			err := svc.HandleDeleteObjects(httptest.NewRecorder(), r)
			if refill == nil {
				t.Fatal("capture racing GET token did not prepare a refill")
			}
			if err != forwardErr {
				t.Errorf("handler error = %v, want original error object %v", err, forwardErr)
			}
			checkDeleteObjectsRefill(t, c, bucket, key, refill, true)
		})

		t.Run("partial-2xx/"+mode.name, func(t *testing.T) {
			const bucket = "bulk-bucket"
			keys := []string{"fully-failed", "deleted", "mixed-version"}
			requestBody := `<Delete><Object><Key>fully-failed</Key></Object><Object><Key>deleted</Key></Object><Object><Key>mixed-version</Key><VersionId>v1</VersionId></Object><Object><Key>mixed-version</Key><VersionId>v2</VersionId></Object></Delete>`
			responseBody := []byte(`<DeleteResult><Deleted><Key>deleted</Key></Deleted><Error><Key>fully-failed</Key><Code>AccessDenied</Code></Error><Error><Key>mixed-version</Key><VersionId>v2</VersionId><Code>AccessDenied</Code></Error></DeleteResult>`)
			var c *cache.Cache
			mock := &mockForwarder{captureFunc: func(_ context.Context, w http.ResponseWriter, _ *http.Request) (*ResponseCapture, error) {
				tokens := deleteObjectsRefillTokens(t, context.Background(), c, bucket, keys...)
				for _, key := range keys {
					if !tokens[key]() {
						t.Fatalf("could not establish the racing refill for %s", key)
					}
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(responseBody)
				return &ResponseCapture{StatusCode: http.StatusOK, Body: responseBody, Complete: true}, nil
			}}
			svc, store := newDeleteObjectsCoordinationService(mock, mode.legacy)
			c = store
			seedDeleteObjectsCache(t, c, bucket, keys...)

			r := httptest.NewRequest(http.MethodPost, "/"+bucket+"?delete", strings.NewReader(requestBody))
			if err := svc.HandleDeleteObjects(httptest.NewRecorder(), r); err != nil {
				t.Errorf("HandleDeleteObjects error for a 2xx response = %v", err)
			}
			if _, found, err := c.GetMeta(context.Background(), bucket, "fully-failed"); err != nil {
				t.Fatalf("read fully failed key: %v", err)
			} else if !found {
				t.Errorf("refill for a key whose only entry failed was discarded")
			}
			for _, key := range []string{"deleted", "mixed-version"} {
				if _, found, err := c.GetMeta(context.Background(), bucket, key); err != nil {
					t.Fatalf("read deleted key %s: %v", key, err)
				} else if found {
					t.Errorf("refill for key %s with a successful delete entry was retained", key)
				}
			}
		})
	}
}
