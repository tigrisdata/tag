package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

const (
	revalidationBenchmarkBucket = "benchmark-bucket"
	revalidationBenchmarkKey    = "object"
	revalidationBenchmarkETag   = `"revalidation-benchmark"`
	revalidationBenchmarkSize   = 512 << 10 // streamed whole-body path, below the default 1 MiB block boundary
	revalidationBenchmarkRegion = "auto"
	revalidationBenchmarkAccess = "revalidation-benchmark-access"
	revalidationBenchmarkSecret = "revalidation-benchmark-secret"
)

type revalidationBenchmarkOrigin struct {
	body         []byte
	lastModified time.Time
}

func (o *revalidationBenchmarkOrigin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("ETag", revalidationBenchmarkETag)
	w.Header().Set("Last-Modified", o.lastModified.UTC().Format(http.TimeFormat))
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, "object", o.lastModified, bytes.NewReader(o.body))
}

// BenchmarkRevalidationClientMatch measures the default transparent-proxy GET
// route for a 512 KiB whole-object cache hit forced through origin revalidation.
// The client validator matches the unchanged object; the origin round trip is
// real loopback HTTP and the cache client is in memory. The benchmark consumes
// the complete gateway response so its body transfer is included in elapsed time.
func BenchmarkRevalidationClientMatch(b *testing.B) {
	oldLogger := log.Logger
	log.Logger = zerolog.New(io.Discard)
	defer func() { log.Logger = oldLogger }()

	body := bytes.Repeat([]byte{'x'}, revalidationBenchmarkSize)
	modified := time.Now().UTC().Truncate(time.Second)
	origin := httptest.NewServer(&revalidationBenchmarkOrigin{body: body, lastModified: modified})

	cfg := config.NewDefault()
	memory := cacheclient.NewMemoryCache()
	cacheStore := cache.NewCacheWithClient(memory, &cfg.Cache)
	meta := &cache.CachedObjectMeta{
		Bucket:        revalidationBenchmarkBucket,
		Key:           revalidationBenchmarkKey,
		ETag:          revalidationBenchmarkETag,
		ContentType:   "application/octet-stream",
		ContentLength: int64(len(body)),
		LastModified:  modified.Unix(),
		StatusCode:    http.StatusOK,
	}
	if err := cacheStore.PutWithMeta(context.Background(), revalidationBenchmarkBucket, revalidationBenchmarkKey, meta, body, 0); err != nil {
		origin.Close()
		b.Fatalf("seed whole-object cache hit: %v", err)
	}

	var service *Service
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := service.HandleGetObject(w, r); err != nil {
			panic(http.ErrAbortHandler)
		}
	}))

	credentials := auth.NewCredentialStore()
	credentials.AddCredential(revalidationBenchmarkAccess, revalidationBenchmarkSecret)
	derivedKeys := auth.NewDerivedKeyStore(auth.DefaultDerivedKeyTTL)
	authz := auth.NewAuthzCache(time.Hour)
	authz.Grant(revalidationBenchmarkAccess, revalidationBenchmarkBucket)
	clientSigner := auth.NewRequestSigner(gateway.URL, revalidationBenchmarkRegion)
	extraHeaders := http.Header{}
	extraHeaders.Set("Cache-Control", "no-cache")
	extraHeaders.Set("If-None-Match", revalidationBenchmarkETag)
	signedRequest, err := clientSigner.SignObjectRequest(
		context.Background(), http.MethodGet, revalidationBenchmarkBucket, revalidationBenchmarkKey,
		nil, "UNSIGNED-PAYLOAD", revalidationBenchmarkAccess, revalidationBenchmarkSecret, extraHeaders,
	)
	if err != nil {
		gateway.Close()
		origin.Close()
		b.Fatalf("sign benchmark request: %v", err)
	}
	authInfo, err := auth.ParseAuthInfo(signedRequest)
	if err != nil {
		gateway.Close()
		origin.Close()
		b.Fatalf("parse benchmark authorization: %v", err)
	}
	derivedKey, err := credentials.GetSigningKey(authInfo.AccessKey, authInfo.Date, authInfo.Region)
	if err != nil {
		gateway.Close()
		origin.Close()
		b.Fatalf("derive benchmark signing key: %v", err)
	}
	derivedKeys.Store(authInfo.AccessKey, authInfo.Date, authInfo.Region, derivedKey)
	localAuth := &LocalAuthConfig{
		DerivedKeyStore: derivedKeys,
		Validator:       auth.NewRequestValidator(derivedKeys),
		AuthzCache:      authz,
	}
	forwarder := NewForwarder(
		nil,
		origin.URL,
		revalidationBenchmarkRegion,
		4,
		auth.NewProxySigner("revalidation-proxy", "revalidation-proxy-secret"),
		localAuth,
	)
	service = NewService(forwarder, cacheStore, cfg)
	client := gateway.Client()
	b.Cleanup(func() {
		gateway.Close()
		service.Stop()
		client.CloseIdleConnections()
		origin.Close()
	})

	b.ResetTimer()
	for b.Loop() {
		request := signedRequest.Clone(context.Background())
		response, err := client.Do(request)
		if err != nil {
			b.Fatalf("GET %s: %v", request.URL, err)
		}
		read, readErr := io.Copy(io.Discard, response.Body)
		closeErr := response.Body.Close()
		if readErr != nil {
			b.Fatalf("read response body: %v", readErr)
		}
		if closeErr != nil {
			b.Fatalf("close response body: %v", closeErr)
		}
		switch response.StatusCode {
		case http.StatusOK:
			if read != int64(len(body)) {
				b.Fatalf("baseline 200 body bytes = %d, want %d", read, len(body))
			}
		case http.StatusNotModified:
			if read != 0 {
				b.Fatalf("conditional 304 body bytes = %d, want 0", read)
			}
		default:
			b.Fatalf("gateway status = %d, want baseline 200 or conditional 304", response.StatusCode)
		}
	}
}
