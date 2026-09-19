package auth

import (
	"bytes"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestSigningKeyCacheSeparatesCredentialAndDate(t *testing.T) {
	signer := NewRequestSigner("https://upstream.example.com", "us-east-1")
	date := time.Now().UTC().Format(shortTimeFormat)
	otherDate := time.Now().UTC().AddDate(0, 0, -1).Format(shortTimeFormat)

	keys := []struct {
		accessKey string
		secretKey string
		date      string
	}{
		{accessKey: "access-a", secretKey: "secret-a", date: date},
		{accessKey: "access-b", secretKey: "secret-a", date: date},
		{accessKey: "access-a", secretKey: "secret-b", date: date},
		{accessKey: "access-a", secretKey: "secret-a", date: otherDate},
	}

	for _, tt := range keys {
		got := signer.deriveSigningKey(tt.accessKey, tt.secretKey, tt.date)
		want := deriveSigningKey(tt.secretKey, tt.date, signer.region)
		wantArray := signingKey(want)
		if !bytes.Equal(got, wantArray) {
			t.Errorf("derived key for %#v = %x, want %x", tt, got, wantArray)
		}
	}

	if got := signer.signingKeyCache.cache.Len(); got != len(keys) {
		t.Fatalf("signing key cache length = %d, want %d", got, len(keys))
	}
}

func TestSigningKeyCacheConcurrentReuse(t *testing.T) {
	cache := newSigningKeyCache()
	key := signingKeyCacheKey{
		accessKey: "access-key",
		secretKey: "secret-key",
		date:      "20260907",
		region:    "us-east-1",
		service:   service,
	}
	wantBytes := deriveSigningKey(key.secretKey, key.date, key.region)
	want := signingKey(wantBytes)

	const workers = 32
	start := make(chan struct{})
	results := make(chan signingKey, workers)
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			<-start
			results <- cache.getOrDerive(key, key.secretKey, key.date, key.region)
		}()
	}
	close(start)
	group.Wait()
	close(results)

	for got := range results {
		if !bytes.Equal(got, want) {
			t.Errorf("concurrent derived key = %x, want %x", got, want)
		}
	}
	if got := cache.cache.Len(); got != 1 {
		t.Fatalf("signing key cache length = %d, want 1", got)
	}
}

func TestSigningKeyCacheIsBounded(t *testing.T) {
	cache := newSigningKeyCache()
	keys := make([]signingKeyCacheKey, maxSigningKeyCacheSize+1)
	for i := range keys {
		keys[i] = signingKeyCacheKey{
			accessKey: fmt.Sprintf("access-%d", i),
			secretKey: fmt.Sprintf("secret-%d", i),
			date:      "20260907",
			region:    "us-east-1",
			service:   service,
		}
		cache.getOrDerive(keys[i], keys[i].secretKey, keys[i].date, keys[i].region)
	}

	if got := cache.cache.Len(); got != maxSigningKeyCacheSize {
		t.Fatalf("signing key cache length = %d, want %d", got, maxSigningKeyCacheSize)
	}
	if _, ok := cache.cache.Get(keys[0]); ok {
		t.Fatalf("oldest signing key remained after cache capacity was exceeded")
	}
}

func TestRequestSignerCachedSigningPreservesRequestSignatures(t *testing.T) {
	const (
		accessKey = "AKIAIOSFODNN7EXAMPLE"
		secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	)
	signer := NewRequestSigner("https://upstream.example.com", "us-east-1")
	store := NewCredentialStore()
	store.AddCredential(accessKey, secretKey)
	validator := NewRequestValidator(store)
	signingTime := time.Now().UTC().Truncate(time.Second).Add(-time.Second)

	paths := []string{
		"/bucket/object-a?prefix=one%2Ftwo",
		"/bucket/object-b?prefix=one%2Ftwo",
	}
	requests := make([]*http.Request, len(paths))
	for i, path := range paths {
		req, err := http.NewRequest(http.MethodGet, "https://upstream.example.com"+path, nil)
		if err != nil {
			t.Fatalf("create request: %v", err)
		}
		req.Header.Set("Range", "bytes=0-1048575")
		req.Header.Set("X-Amz-Date", signingTime.Format(TimeFormat))
		req.Header.Set("X-Amz-Content-Sha256", emptyBodyHash)
		if err := signer.signHTTP(req, accessKey, secretKey, emptyBodyHash, signingTime); err != nil {
			t.Fatalf("sign request %q: %v", path, err)
		}
		if _, err := validator.ValidateRequest(req); err != nil {
			t.Fatalf("ValidateRequest(%q): %v", path, err)
		}
		requests[i] = req
	}

	if got, want := requests[0].Header.Get("Authorization"), requests[1].Header.Get("Authorization"); got == want {
		t.Fatalf("request-specific signatures are equal: %q", got)
	}
	if got := signer.signingKeyCache.cache.Len(); got != 1 {
		t.Fatalf("signing key cache length = %d, want 1", got)
	}
}

func TestRequestSignerCachedSigningConcurrentRequests(t *testing.T) {
	const (
		accessKey = "AKIAIOSFODNN7EXAMPLE"
		secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	)
	signer := NewRequestSigner("https://upstream.example.com", "us-east-1")
	store := NewCredentialStore()
	store.AddCredential(accessKey, secretKey)
	validator := NewRequestValidator(store)
	signingTime := time.Now().UTC().Truncate(time.Second).Add(-time.Second)

	const workers = 32
	start := make(chan struct{})
	errs := make(chan error, workers)
	var group sync.WaitGroup
	group.Add(workers)
	for i := range workers {
		go func(i int) {
			defer group.Done()
			<-start
			path := fmt.Sprintf("/bucket/object-%d?part=%d", i, i)
			req, err := http.NewRequest(http.MethodGet, "https://upstream.example.com"+path, nil)
			if err != nil {
				errs <- fmt.Errorf("create request %d: %w", i, err)
				return
			}
			req.Header.Set("Range", "bytes=0-1048575")
			req.Header.Set("X-Amz-Date", signingTime.Format(TimeFormat))
			req.Header.Set("X-Amz-Content-Sha256", emptyBodyHash)
			if err := signer.signHTTP(req, accessKey, secretKey, emptyBodyHash, signingTime); err != nil {
				errs <- fmt.Errorf("sign request %d: %w", i, err)
				return
			}
			if _, err := validator.ValidateRequest(req); err != nil {
				errs <- fmt.Errorf("validate request %d: %w", i, err)
			}
		}(i)
	}
	close(start)
	group.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if got := signer.signingKeyCache.cache.Len(); got != 1 {
		t.Fatalf("signing key cache length = %d, want 1", got)
	}
}

func TestRequestSignerCachedSigningHandlesUTCDateBoundary(t *testing.T) {
	const (
		accessKey = "AKIAIOSFODNN7EXAMPLE"
		secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	)
	signer := NewRequestSigner("https://upstream.example.com", "us-east-1")
	validator := NewRequestValidator(NewCredentialStore())
	boundary := time.Now().UTC().Truncate(24 * time.Hour)
	times := []time.Time{boundary.Add(-time.Second), boundary}
	var signatures []string

	for _, signingTime := range times {
		req, err := http.NewRequest(http.MethodGet, "https://upstream.example.com/bucket/object", nil)
		if err != nil {
			t.Fatalf("create request: %v", err)
		}
		req.Header.Set("X-Amz-Date", signingTime.Format(TimeFormat))
		req.Header.Set("X-Amz-Content-Sha256", emptyBodyHash)
		if err := signer.signHTTP(req, accessKey, secretKey, emptyBodyHash, signingTime); err != nil {
			t.Fatalf("sign request at %s: %v", signingTime, err)
		}

		authInfo, err := ParseAuthInfo(req)
		if err != nil {
			t.Fatalf("ParseAuthInfo(): %v", err)
		}
		want := validator.computeSignature(req, authInfo, deriveSigningKey(secretKey, signingTime.Format(shortTimeFormat), signer.region), emptyBodyHash, signingTime)
		if got := authInfo.Signature; got != want {
			t.Errorf("signature at %s = %q, want %q", signingTime, got, want)
		}
		signatures = append(signatures, authInfo.Signature)
	}

	if signatures[0] == signatures[1] {
		t.Fatalf("signatures across UTC date boundary are equal: %q", signatures[0])
	}
	if got := signer.signingKeyCache.cache.Len(); got != len(times) {
		t.Fatalf("signing key cache length = %d, want %d", got, len(times))
	}
}
