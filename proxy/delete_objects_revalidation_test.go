package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

func TestDeleteObjectsLostResponseRevalidation(t *testing.T) {
	const bucket, key = "revalidation-delete", "old-key"
	const oldBody = "old body"
	requestBody := `<Delete><Object><Key>` + key + `</Key></Object></Delete>`

	for _, mode := range []struct {
		name   string
		legacy bool
	}{
		{name: "legacy-tombstones", legacy: true},
		{name: "cas-fences", legacy: false},
	} {
		mode := mode
		t.Run(mode.name, func(t *testing.T) {
			revalidationStarted := make(chan struct{}, 1)
			releaseRevalidation := make(chan struct{})
			deleteAccepted := make(chan []byte, 1)
			releaseDelete := make(chan struct{})
			deleteResponseLost := make(chan struct{})
			originErrors := make(chan error, 4)
			var deleteApplied atomic.Bool

			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost && r.URL.RawQuery == "delete":
					body, err := io.ReadAll(r.Body)
					if err != nil {
						originErrors <- err
						return
					}
					deleteAccepted <- body
					<-releaseDelete
					deleteApplied.Store(true)
					hijacker, ok := w.(http.Hijacker)
					if !ok {
						originErrors <- errors.New("origin response writer cannot hijack connection")
						return
					}
					conn, _, err := hijacker.Hijack()
					if err != nil {
						originErrors <- err
						return
					}
					_ = conn.Close()
					close(deleteResponseLost)
				case r.Method == http.MethodGet && r.URL.Path == "/"+bucket+"/"+key && r.Header.Get("If-None-Match") == `"old"`:
					// This conditional GET captured the old representation before the
					// delete. Its response is held until the delete's error-path fence
					// has completed, then arrives too late to be authoritative.
					revalidationStarted <- struct{}{}
					<-releaseRevalidation
					w.Header().Set("Content-Length", "8")
					w.Header().Set("Content-Type", "text/plain")
					w.Header().Set("ETag", `"old"`)
					w.WriteHeader(http.StatusOK)
					_, _ = io.WriteString(w, oldBody)
				case r.Method == http.MethodHead && r.URL.Path == "/"+bucket+"/"+key:
					if deleteApplied.Load() {
						w.Header().Set("Content-Length", "9")
						w.WriteHeader(http.StatusNotFound)
						return
					}
					w.Header().Set("Content-Length", "8")
					w.Header().Set("Content-Type", "text/plain")
					w.Header().Set("ETag", `"old"`)
					w.WriteHeader(http.StatusOK)
				case r.Method == http.MethodGet && r.URL.Path == "/"+bucket+"/"+key:
					if deleteApplied.Load() {
						w.Header().Set("Content-Length", "9")
						w.WriteHeader(http.StatusNotFound)
						_, _ = io.WriteString(w, "NoSuchKey")
						return
					}
					w.Header().Set("Content-Length", "8")
					w.Header().Set("Content-Type", "text/plain")
					w.Header().Set("ETag", `"old"`)
					w.WriteHeader(http.StatusOK)
					_, _ = io.WriteString(w, oldBody)
				default:
					originErrors <- errors.New("unexpected origin request: " + r.Method + " " + r.URL.String())
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer origin.Close()
			defer func() {
				select {
				case <-releaseDelete:
				default:
					close(releaseDelete)
				}
				select {
				case <-releaseRevalidation:
				default:
					close(releaseRevalidation)
				}
			}()

			cfg := config.NewDefault()
			cfg.Cache.SetLegacyCoordination(mode.legacy)
			store := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
			seed := &cache.CachedObjectMeta{
				Bucket:        bucket,
				Key:           key,
				ETag:          `"old"`,
				ContentLength: int64(len(oldBody)),
				ContentType:   "text/plain",
				StatusCode:    http.StatusOK,
			}
			if err := store.PutWithMeta(context.Background(), bucket, key, seed, []byte(oldBody), 60); err != nil {
				t.Fatalf("seed old cache entry: %v", err)
			}

			base := newBaseForwarder(origin.URL, "us-east-1", 10)
			base.httpClient = origin.Client()
			forwarder := &transparentForwarder{
				baseForwarder:    base,
				proxySigner:      auth.NewProxySigner("test-access-key", "test-secret-key"),
				upstreamEndpoint: origin.URL,
			}
			forwarder.initInterceptor()
			svc := NewService(forwarder, store, cfg)

			getReq := httptest.NewRequest(http.MethodGet, "http://gateway/"+bucket+"/"+key, nil)
			getReq.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test")
			getReq.Header.Set("Cache-Control", "no-cache")
			getRecorder := httptest.NewRecorder()
			getDone := make(chan error, 1)
			go func() { getDone <- svc.HandleGetObject(getRecorder, getReq) }()
			select {
			case <-revalidationStarted:
			case err := <-originErrors:
				t.Fatalf("origin revalidation setup: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("conditional GET did not reach the origin")
			}

			deleteReq := httptest.NewRequest(http.MethodPost, "http://gateway/"+bucket+"?delete", strings.NewReader(requestBody))
			deleteReq.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test")
			deleteDone := make(chan error, 1)
			go func() { deleteDone <- svc.HandleDeleteObjects(httptest.NewRecorder(), deleteReq) }()
			select {
			case body := <-deleteAccepted:
				if string(body) != requestBody {
					t.Fatalf("origin DeleteObjects body=%q, want %q", body, requestBody)
				}
			case err := <-originErrors:
				t.Fatalf("origin DeleteObjects setup: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("origin did not receive DeleteObjects after the first invalidation")
			}

			close(releaseDelete)
			select {
			case <-deleteResponseLost:
			case err := <-originErrors:
				t.Fatalf("origin response-loss setup: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("origin did not apply DeleteObjects before losing its reply")
			}
			select {
			case err := <-deleteDone:
				if err == nil || !errors.Is(err, io.EOF) {
					t.Fatalf("HandleDeleteObjects error=%v; want original transport EOF", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("DeleteObjects handler did not return after its error-path invalidation")
			}

			close(releaseRevalidation)
			select {
			case err := <-getDone:
				if err != nil {
					t.Fatalf("revalidation GET error=%v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("revalidation GET did not finish")
			}
			if getRecorder.Code != http.StatusOK || getRecorder.Body.String() != oldBody {
				t.Fatalf("in-flight revalidation response=(%d,%q), want the already-received old body", getRecorder.Code, getRecorder.Body.String())
			}

			if _, found, err := store.GetMeta(context.Background(), bucket, key); err != nil {
				t.Fatalf("read metadata after delayed revalidation: %v", err)
			} else if found {
				t.Fatal("late revalidation metadata remained readable after the lost DeleteObjects reply")
			}

			laterRecorder := httptest.NewRecorder()
			laterReq := httptest.NewRequest(http.MethodGet, "http://gateway/"+bucket+"/"+key, nil)
			laterReq.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test")
			if err := svc.HandleGetObject(laterRecorder, laterReq); err != nil {
				t.Fatalf("later GET error=%v", err)
			}
			if laterRecorder.Header().Get(XCacheHeader) == XCacheHit || laterRecorder.Body.String() == oldBody {
				t.Fatalf("later GET served stale data: X-Cache=%q body=%q", laterRecorder.Header().Get(XCacheHeader), laterRecorder.Body.String())
			}
		})
	}
}
