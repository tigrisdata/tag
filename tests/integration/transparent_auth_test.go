package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/proxy"
)

// TestTransparentAuth_SigningKeyLearning_ThenCacheHit verifies the core transparent
// auth flow: first request learns signing keys from Tigris, second request validates
// locally and is served from cache.
func TestTransparentAuth_SigningKeyLearning_ThenCacheHit(t *testing.T) {
	backend := s3mem.New()
	env := NewTestEnvironmentWithTransparentAuth(t, newSigningKeysUpstreamHandler(t, backend))
	env.S3Backend = backend
	defer env.Close()

	bucket := "tp-learn-bucket"
	key := "test-object.txt"
	content := []byte("transparent proxy test content")

	require.NoError(t, env.PutTestObject(bucket, key, content))

	// Request 1: unknown access key → forwarded to Tigris → signing keys learned
	resp1, err := env.DoSignedRequest("GET", "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	defer resp1.Body.Close()

	body1, err := io.ReadAll(resp1.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp1.StatusCode)
	assert.Equal(t, content, body1)
	assert.Equal(t, int32(1), env.GetUpstreamRequestCount(), "First request should go to upstream")

	// Internal header must NOT leak to client
	assert.Empty(t, resp1.Header.Get("X-Tigris-Proxy-Signing-Keys"), "Internal signing keys header must not leak to client")

	// Signing keys should have been learned
	assert.True(t, env.DerivedKeyStore.HasKey(TestAccessKey), "DerivedKeyStore should have learned keys for TestAccessKey")
	assert.True(t, env.AuthzCache.IsAuthorized(TestAccessKey, bucket), "AuthzCache should have granted access for TestAccessKey+bucket")

	// Wait for cache population
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second), "Object should be cached after first GET")

	// Request 2: local auth succeeds + cache hit
	resp2, err := env.DoSignedRequest("GET", "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	defer resp2.Body.Close()

	body2, err := io.ReadAll(resp2.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	assert.Equal(t, content, body2)
	env.AssertXCacheHit(t)
	assert.Equal(t, int32(1), env.GetUpstreamRequestCount(), "Second request should be served from cache, upstream count should not increase")
}

func TestTransparentAuth_PresignedURL_KeyLearning_ThenCacheHit(t *testing.T) {
	backend := s3mem.New()
	env := NewTestEnvironmentWithTransparentAuth(t, newSigningKeysUpstreamHandler(t, backend))
	env.S3Backend = backend
	defer env.Close()

	bucket := "tp-presigned-bucket"
	key := "test-object.txt"
	content := []byte("presigned cache hit content")
	require.NoError(t, env.PutTestObject(bucket, key, content))

	req1, err := env.PresignedGetRequest(t.Context(), bucket, key)
	require.NoError(t, err)
	resp1, err := http.DefaultClient.Do(req1)
	require.NoError(t, err)
	body1, err := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp1.StatusCode)
	require.Equal(t, content, body1)
	require.Equal(t, int32(1), env.GetUpstreamRequestCount())
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second))

	req2, err := env.PresignedGetRequest(t.Context(), bucket, key)
	require.NoError(t, err)
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	body2, err := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	require.NoError(t, err)
	require.Equal(t, content, body2)
	require.Equal(t, proxy.XCacheHit, resp2.Header.Get(proxy.XCacheHeader))
	require.Equal(t, int32(1), env.GetUpstreamRequestCount())

	headReq, err := env.PresignedHeadRequest(t.Context(), bucket, key)
	require.NoError(t, err)
	headResp, err := http.DefaultClient.Do(headReq)
	require.NoError(t, err)
	headResp.Body.Close()
	require.Equal(t, http.StatusOK, headResp.StatusCode)
	require.Equal(t, proxy.XCacheHit, headResp.Header.Get(proxy.XCacheHeader))
	require.Equal(t, int32(1), env.GetUpstreamRequestCount())
}

func TestTransparentAuth_PresignedURLWithProxyCredentialsHitsCache(t *testing.T) {
	content := []byte("proxy credential presigned content")
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		w.Header().Set("ETag", `"proxy-credential-etag"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
	})
	env := NewTestEnvironmentWithTransparentAuth(t, handler)
	defer env.Close()

	bucket := "proxy-credential-bucket"
	key := "object.bin"
	client := env.GetS3ClientWithCreds(TestProxyAccessKey, TestProxySecretKey)

	req1, err := presignedGetRequest(t.Context(), client, bucket, key)
	require.NoError(t, err)
	resp1, err := http.DefaultClient.Do(req1)
	require.NoError(t, err)
	body1, err := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	require.NoError(t, err)
	require.Equal(t, content, body1)
	require.Equal(t, proxy.XCacheMiss, resp1.Header.Get(proxy.XCacheHeader))
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second))
	require.True(t, env.AuthzCache.IsAuthorized(TestProxyAccessKey, bucket))
	require.False(t, env.DerivedKeyStore.HasKey(TestProxyAccessKey))

	req2, err := presignedGetRequest(t.Context(), client, bucket, key)
	require.NoError(t, err)
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	body2, err := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	require.NoError(t, err)
	require.Equal(t, content, body2)
	require.Equal(t, proxy.XCacheHit, resp2.Header.Get(proxy.XCacheHeader))
	require.Equal(t, int32(1), env.GetUpstreamRequestCount())
}

func TestTransparentAuth_DifferentKeyAuthorizesCachedObjectUpstream(t *testing.T) {
	content := []byte("upstream-authorized cached content")
	var probeCount int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"upstream-authorized-etag"`)
		if r.Header.Get("Range") == "bytes=0-0" {
			atomic.AddInt32(&probeCount, 1)
			w.Header().Set("Content-Length", "1")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-0/%d", len(content)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(content[:1])
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
	})
	env := NewTestEnvironmentWithTransparentAuth(t, handler)
	defer env.Close()

	bucket := "different-key-bucket"
	key := "object.bin"

	req1, err := env.PresignedGetRequest(t.Context(), bucket, key)
	require.NoError(t, err)
	resp1, err := http.DefaultClient.Do(req1)
	require.NoError(t, err)
	body1, err := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	require.NoError(t, err)
	require.Equal(t, content, body1)
	require.Equal(t, proxy.XCacheMiss, resp1.Header.Get(proxy.XCacheHeader))
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second))
	require.False(t, env.DerivedKeyStore.HasKey(TestAccessKey))

	req2, err := env.PresignedGetRequest(t.Context(), bucket, key)
	require.NoError(t, err)
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	body2, err := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	require.NoError(t, err)
	require.Equal(t, content, body2)
	require.Equal(t, proxy.XCacheHit, resp2.Header.Get(proxy.XCacheHeader))
	require.Equal(t, int32(2), env.GetUpstreamRequestCount(), "second request should use one-byte auth probe")
	require.Equal(t, int32(1), atomic.LoadInt32(&probeCount))
}

// A probe that reports the object gone must invalidate the entry. Otherwise a later read
// that validates locally is still served from cache after the object was deleted upstream.
func TestTransparentAuth_DifferentKeyProbeNotFoundInvalidates(t *testing.T) {
	content := []byte("object that gets deleted upstream")
	var deleted atomic.Bool
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if deleted.Load() {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("<Error><Code>NoSuchKey</Code></Error>"))
			return
		}
		w.Header().Set("ETag", `"deleted-object-etag"`)
		if r.Header.Get("Range") == "bytes=0-0" {
			w.Header().Set("Content-Length", "1")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-0/%d", len(content)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(content[:1])
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
	})
	env := NewTestEnvironmentWithTransparentAuth(t, handler)
	defer env.Close()

	bucket := "probe-404-bucket"
	key := "object.bin"

	req1, err := env.PresignedGetRequest(t.Context(), bucket, key)
	require.NoError(t, err)
	resp1, err := http.DefaultClient.Do(req1)
	require.NoError(t, err)
	_, err = io.ReadAll(resp1.Body)
	resp1.Body.Close()
	require.NoError(t, err)
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second))

	deleted.Store(true)

	req2, err := env.PresignedGetRequest(t.Context(), bucket, key)
	require.NoError(t, err)
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	resp2.Body.Close()

	require.Equal(t, http.StatusNotFound, resp2.StatusCode)
	require.True(t, env.WaitForNotCached(bucket, key, 2*time.Second),
		"entry for an object deleted upstream must not survive the probe")
}

// A block-mode entry must not enter the unknown-key authorization path. That path serves
// through the whole-body helpers, which cannot read block-mode bodies: they would report the
// body missing and delete a valid entry. The request falls through to the miss path instead,
// so no authorization probe is issued.
func TestTransparentAuth_DifferentKeyBlockModeEntrySkipsProbe(t *testing.T) {
	content := []byte("block-mode cached content")
	etag := `"block-mode-etag"`
	var probeCount int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", etag)
		if r.Header.Get("Range") == "bytes=0-0" {
			atomic.AddInt32(&probeCount, 1)
			w.Header().Set("Content-Length", "1")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-0/%d", len(content)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(content[:1])
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
	})
	env := NewTestEnvironmentWithTransparentAuth(t, handler)
	defer env.Close()

	ctx := context.Background()
	bucket := "block-mode-bucket"
	key := "object.bin"
	const blockSize = int64(4 * 1024 * 1024)

	// Pre-store a block-mode entry whose identity matches upstream, so the only thing that
	// can keep it out of the authorization path is the block-mode guard itself.
	require.NoError(t, env.Cache.PutBlockStream(ctx, bucket, key, etag, blockSize, 0, bytes.NewReader(content), 300))
	meta := &cache.CachedObjectMeta{
		Bucket: bucket, Key: key, ETag: etag,
		ContentType:   "application/octet-stream",
		ContentLength: int64(len(content)),
		StatusCode:    http.StatusOK,
		BlockSize:     blockSize,
		LastModified:  time.Now().Unix(),
	}
	wrote, err := env.Cache.PutMetaTombstoneAware(ctx, bucket, key, meta, 300, time.Now().UnixNano())
	require.NoError(t, err)
	require.True(t, wrote, "block-mode meta write skipped")

	req, err := env.PresignedGetRequest(t.Context(), bucket, key)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, content, body)
	require.Equal(t, int32(0), atomic.LoadInt32(&probeCount),
		"block-mode entry must not be authorized through the whole-body serve path")
}

func TestTransparentAuth_DifferentKeyChecksumModeHitsCache(t *testing.T) {
	content := []byte("checksum-mode cached content")
	const checksum = "dGVzdC1jaGVja3N1bQ=="
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"checksum-mode-etag"`)
		w.Header().Set("X-Amz-Checksum-Crc32c", checksum)
		if r.Header.Get("Range") == "bytes=0-0" {
			w.Header().Set("Content-Length", "1")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-0/%d", len(content)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(content[:1])
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
	})
	env := NewTestEnvironmentWithTransparentAuth(t, handler)
	defer env.Close()

	bucket := "checksum-mode-bucket"
	key := "object.bin"
	withChecksumMode := func(input *s3.GetObjectInput) {
		input.ChecksumMode = s3types.ChecksumModeEnabled
	}

	req1, err := env.PresignedGetRequest(t.Context(), bucket, key, withChecksumMode)
	require.NoError(t, err)
	require.Equal(t, "ENABLED", req1.URL.Query().Get("X-Amz-Checksum-Mode"))
	resp1, err := http.DefaultClient.Do(req1)
	require.NoError(t, err)
	body1, err := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	require.NoError(t, err)
	require.Equal(t, content, body1)
	require.Equal(t, proxy.XCacheMiss, resp1.Header.Get(proxy.XCacheHeader))
	require.Equal(t, checksum, resp1.Header.Get("X-Amz-Checksum-Crc32c"))
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second))

	req2, err := env.PresignedGetRequest(t.Context(), bucket, key, withChecksumMode)
	require.NoError(t, err)
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	body2, err := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	require.NoError(t, err)
	require.Equal(t, content, body2)
	require.Equal(t, proxy.XCacheHit, resp2.Header.Get(proxy.XCacheHeader))
	require.Equal(t, checksum, resp2.Header.Get("X-Amz-Checksum-Crc32c"))
	require.Equal(t, int32(2), env.GetUpstreamRequestCount())
}

func TestTransparentAuth_DifferentKeyProbeDenialIsReturned(t *testing.T) {
	content := []byte("private cached content")
	var denyProbe int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-0" && atomic.LoadInt32(&denyProbe) == 1 {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("<Error><Code>AccessDenied</Code></Error>"))
			return
		}
		w.Header().Set("ETag", `"private-etag"`)
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
	})
	env := NewTestEnvironmentWithTransparentAuth(t, handler)
	defer env.Close()

	bucket := "denied-key-bucket"
	key := "object.bin"
	req1, err := env.PresignedGetRequest(t.Context(), bucket, key)
	require.NoError(t, err)
	resp1, err := http.DefaultClient.Do(req1)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, resp1.Body)
	resp1.Body.Close()
	require.NoError(t, err)
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second))

	atomic.StoreInt32(&denyProbe, 1)
	req2, err := env.PresignedGetRequest(t.Context(), bucket, key)
	require.NoError(t, err)
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	body2, err := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp2.StatusCode)
	require.Equal(t, proxy.XCacheMiss, resp2.Header.Get(proxy.XCacheHeader))
	require.Contains(t, string(body2), "AccessDenied")
	require.Equal(t, int32(2), env.GetUpstreamRequestCount())
}

func TestTransparentAuth_DifferentKeyZeroByteObject(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"zero-byte-etag"`)
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
	})
	env := NewTestEnvironmentWithTransparentAuth(t, handler)
	defer env.Close()

	bucket := "zero-byte-bucket"
	key := "empty.bin"
	req1, err := env.PresignedGetRequest(t.Context(), bucket, key)
	require.NoError(t, err)
	resp1, err := http.DefaultClient.Do(req1)
	require.NoError(t, err)
	resp1.Body.Close()
	require.Equal(t, http.StatusOK, resp1.StatusCode)
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second))

	req2, err := env.PresignedGetRequest(t.Context(), bucket, key)
	require.NoError(t, err)
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	body2, err := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	require.NoError(t, err)
	require.Empty(t, body2)
	require.Equal(t, proxy.XCacheHit, resp2.Header.Get(proxy.XCacheHeader))
	require.Equal(t, int32(2), env.GetUpstreamRequestCount())
}

func TestTransparentAuth_DifferentKeyProbe416RefetchesFullObject(t *testing.T) {
	var objectBecameEmpty int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-0" && atomic.LoadInt32(&objectBecameEmpty) == 1 {
			w.Header().Set("Content-Range", "bytes */0")
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if atomic.LoadInt32(&objectBecameEmpty) == 1 {
			w.Header().Set("ETag", `"empty-etag"`)
			w.Header().Set("Content-Length", "0")
			w.WriteHeader(http.StatusOK)
			return
		}
		content := []byte("old nonempty content")
		w.Header().Set("ETag", `"old-etag"`)
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
	})
	env := NewTestEnvironmentWithTransparentAuth(t, handler)
	defer env.Close()

	bucket := "range-416-bucket"
	key := "object.bin"
	req1, err := env.PresignedGetRequest(t.Context(), bucket, key)
	require.NoError(t, err)
	resp1, err := http.DefaultClient.Do(req1)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, resp1.Body)
	resp1.Body.Close()
	require.NoError(t, err)
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second))

	atomic.StoreInt32(&objectBecameEmpty, 1)
	req2, err := env.PresignedGetRequest(t.Context(), bucket, key)
	require.NoError(t, err)
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	body2, err := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp2.StatusCode)
	require.Empty(t, body2)
	require.Equal(t, proxy.XCacheMiss, resp2.Header.Get(proxy.XCacheHeader))
	require.Equal(t, int32(3), env.GetUpstreamRequestCount())
}

func TestTransparentAuth_PresignedSemanticQueryBypassesCache(t *testing.T) {
	backend := s3mem.New()
	baseHandler := newSigningKeysUpstreamHandler(t, backend)
	variantContent := []byte("semantic query response")
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("response-content-type") != "" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(variantContent)
			return
		}
		baseHandler.ServeHTTP(w, r)
	})

	env := NewTestEnvironmentWithTransparentAuth(t, handler)
	env.S3Backend = backend
	defer env.Close()

	bucket := "tp-presigned-query-bucket"
	key := "test-object.txt"
	defaultContent := []byte("default cached content")
	require.NoError(t, env.PutTestObject(bucket, key, defaultContent))

	warmReq, err := env.PresignedGetRequest(t.Context(), bucket, key)
	require.NoError(t, err)
	warmResp, err := http.DefaultClient.Do(warmReq)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, warmResp.Body)
	warmResp.Body.Close()
	require.NoError(t, err)
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second))

	countBeforeVariant := env.GetUpstreamRequestCount()
	variantReq, err := env.PresignedGetRequest(t.Context(), bucket, key, func(input *s3.GetObjectInput) {
		input.ResponseContentType = aws.String("text/plain")
	})
	require.NoError(t, err)
	variantResp, err := http.DefaultClient.Do(variantReq)
	require.NoError(t, err)
	variantBody, err := io.ReadAll(variantResp.Body)
	variantResp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, variantContent, variantBody)
	require.Equal(t, proxy.XCacheBypass, variantResp.Header.Get(proxy.XCacheHeader))
	require.Equal(t, countBeforeVariant+1, env.GetUpstreamRequestCount())

	cachedReq, err := env.PresignedGetRequest(t.Context(), bucket, key)
	require.NoError(t, err)
	cachedResp, err := http.DefaultClient.Do(cachedReq)
	require.NoError(t, err)
	cachedBody, err := io.ReadAll(cachedResp.Body)
	cachedResp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, defaultContent, cachedBody)
	require.Equal(t, proxy.XCacheHit, cachedResp.Header.Get(proxy.XCacheHeader))
}

func TestTransparentAuth_PresignedTemporaryCredentialsBypassCache(t *testing.T) {
	backend := s3mem.New()
	env := NewTestEnvironmentWithTransparentAuth(t, newSigningKeysUpstreamHandler(t, backend))
	env.S3Backend = backend
	defer env.Close()

	bucket := "tp-presigned-temporary-bucket"
	key := "test-object.txt"
	content := []byte("temporary credential content")
	require.NoError(t, env.PutTestObject(bucket, key, content))

	sessionReq, err := presignedGetRequest(
		t.Context(),
		env.GetS3ClientWithSessionToken("temporary-session-token"),
		bucket,
		key,
	)
	require.NoError(t, err)
	sessionResp, err := http.DefaultClient.Do(sessionReq)
	require.NoError(t, err)
	sessionResp.Body.Close()
	require.Equal(t, http.StatusOK, sessionResp.StatusCode)
	require.Equal(t, proxy.XCacheBypass, sessionResp.Header.Get(proxy.XCacheHeader))
	require.False(t, env.DerivedKeyStore.HasKey(TestAccessKey))
	require.False(t, env.AuthzCache.IsAuthorized(TestAccessKey, bucket))
	require.False(t, env.IsCached(bucket, key))
}

// TestTransparentAuth_UnknownAccessKey_ForwardsToTigris verifies that requests
// signed with an unknown access key bypass the cache and are forwarded to Tigris.
func TestTransparentAuth_UnknownAccessKey_ForwardsToTigris(t *testing.T) {
	backend := s3mem.New()
	env := NewTestEnvironmentWithTransparentAuth(t, newSigningKeysUpstreamHandler(t, backend))
	env.S3Backend = backend
	defer env.Close()

	bucket := "tp-unknown-key-bucket"
	key := "test-object.txt"
	content := []byte("content for unknown key test")

	require.NoError(t, env.PutTestObject(bucket, key, content))

	// Initial request with TestAccessKey to populate cache + learn keys
	resp1, err := env.DoSignedRequest("GET", "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	io.Copy(io.Discard, resp1.Body)
	resp1.Body.Close()
	require.Equal(t, http.StatusOK, resp1.StatusCode)

	require.True(t, env.WaitForCached(bucket, key, 2*time.Second), "Object should be cached")

	// Sanity check: TestAccessKey request now gets cache hit
	resp2, err := env.DoSignedRequest("GET", "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()
	require.Equal(t, http.StatusOK, resp2.StatusCode)
	env.AssertXCacheHit(t)

	countBefore := env.GetUpstreamRequestCount()

	// Request with unknown access key — should bypass cache, forward to Tigris
	unknownSigner := auth.NewRequestSigner(env.TAGServer.URL, TestRegion)
	unknownAccessKey := "AKID_UNKNOWN_EXAMPLE1234"
	unknownSecretKey := "unknown-secret-key-for-testing"

	path := "/" + bucket + "/" + key
	req, err := unknownSigner.SignRequest(
		context.Background(), "GET", path, nil, "",
		unknownAccessKey, unknownSecretKey, http.Header{},
	)
	require.NoError(t, err)

	resp3, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	io.Copy(io.Discard, resp3.Body)
	resp3.Body.Close()

	// Should have gone to upstream (unknown key → AuthNotValidated → forwarded)
	assert.Greater(t, env.GetUpstreamRequestCount(), countBefore, "Unknown access key request should be forwarded to upstream")
}

// TestTransparentAuth_UnauthenticatedRequest_ForwardsToTigris verifies that
// requests without an Authorization header are forwarded to Tigris (not rejected).
func TestTransparentAuth_UnauthenticatedRequest_ForwardsToTigris(t *testing.T) {
	publicContent := []byte("public bucket content")

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		w.Write(publicContent)
	})

	env := NewTestEnvironmentWithTransparentAuth(t, handler)
	defer env.Close()

	// Send unsigned request (no Authorization header)
	req, err := http.NewRequest("GET", env.TAGServer.URL+"/public-bucket/test.txt", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "Unauthenticated request should be forwarded, not rejected")
	assert.Equal(t, publicContent, body)
	assert.Equal(t, int32(1), env.GetUpstreamRequestCount(), "Request should have been forwarded to upstream")
}

// TestTransparentAuth_AuthzRevocationOn403 verifies that when Tigris returns 403,
// the authz cache entry is revoked and subsequent requests are forwarded.
func TestTransparentAuth_AuthzRevocationOn403(t *testing.T) {
	backend := s3mem.New()

	// Stateful handler: returns 403 when forbidden flag is set
	var returnForbidden int32
	baseHandler := newSigningKeysUpstreamHandler(t, backend)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.LoadInt32(&returnForbidden) == 1 {
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`))
			return
		}
		baseHandler.ServeHTTP(w, r)
	})

	env := NewTestEnvironmentWithTransparentAuth(t, handler)
	env.S3Backend = backend
	defer env.Close()

	bucket := "tp-revoke-bucket"
	key := "test-object.txt"
	content := []byte("content for revocation test")

	require.NoError(t, env.PutTestObject(bucket, key, content))

	// Request 1: succeeds, keys learned, authz granted
	resp1, err := env.DoSignedRequest("GET", "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	io.Copy(io.Discard, resp1.Body)
	resp1.Body.Close()
	require.Equal(t, http.StatusOK, resp1.StatusCode)
	assert.True(t, env.AuthzCache.IsAuthorized(TestAccessKey, bucket))

	require.True(t, env.WaitForCached(bucket, key, 2*time.Second))

	// Request 2: cache hit (sanity check)
	resp2, err := env.DoSignedRequest("GET", "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()
	require.Equal(t, http.StatusOK, resp2.StatusCode)
	env.AssertXCacheHit(t)

	// Revoke authz and enable 403 from upstream
	env.AuthzCache.Revoke(TestAccessKey, bucket)
	atomic.StoreInt32(&returnForbidden, 1)

	// Request 3: authz expired → forwarded → gets 403 → revocation
	resp3, err := env.DoSignedRequest("GET", "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	io.Copy(io.Discard, resp3.Body)
	resp3.Body.Close()

	assert.Equal(t, http.StatusForbidden, resp3.StatusCode, "Should return 403 from Tigris")
	assert.False(t, env.AuthzCache.IsAuthorized(TestAccessKey, bucket), "AuthzCache should have revoked access on 403")

	// Request 4: still not authorized → forwarded → 403
	resp4, err := env.DoSignedRequest("GET", "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	io.Copy(io.Discard, resp4.Body)
	resp4.Body.Close()

	assert.Equal(t, http.StatusForbidden, resp4.StatusCode)
}

// TestTransparentAuth_InternalHeaderAlwaysStripped verifies that the
// X-Tigris-Proxy-Signing-Keys header never reaches the client on any request type.
func TestTransparentAuth_InternalHeaderAlwaysStripped(t *testing.T) {
	backend := s3mem.New()

	// Upstream always sets the internal header, even on PUTs
	faker := newSigningKeysUpstreamHandler(t, backend)
	alwaysHeaderHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := httptest.NewRecorder()
		faker.ServeHTTP(rec, r)

		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		// Force header on all responses regardless of status
		if rec.Header().Get("X-Tigris-Proxy-Signing-Keys") == "" {
			w.Header().Set("X-Tigris-Proxy-Signing-Keys", "should-be-stripped")
		}
		w.WriteHeader(rec.Code)
		w.Write(rec.Body.Bytes())
	})

	env := NewTestEnvironmentWithTransparentAuth(t, alwaysHeaderHandler)
	env.S3Backend = backend
	defer env.Close()

	bucket := "tp-strip-bucket"
	key := "test-object.txt"
	content := []byte("content for header stripping test")

	require.NoError(t, env.PutTestObject(bucket, key, content))

	// GET (cache miss) — header stripped
	resp1, err := env.DoSignedRequest("GET", "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	io.Copy(io.Discard, resp1.Body)
	resp1.Body.Close()
	assert.Equal(t, http.StatusOK, resp1.StatusCode)
	assert.Empty(t, resp1.Header.Get("X-Tigris-Proxy-Signing-Keys"), "Internal header must be stripped on cache miss GET")

	// Wait for cache
	require.True(t, env.WaitForCached(bucket, key, 2*time.Second))

	// GET (cache hit) — header should not appear at all (response from cache)
	resp2, err := env.DoSignedRequest("GET", "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	env.AssertXCacheHit(t)
	assert.Empty(t, resp2.Header.Get("X-Tigris-Proxy-Signing-Keys"), "Internal header must not appear on cache hit")

	// PUT request — header stripped
	putBody := []byte("new content")
	h := sha256.Sum256(putBody)
	bodyHash := hex.EncodeToString(h[:])
	putReq, err := env.Signer.SignRequest(
		context.Background(), "PUT", "/"+bucket+"/"+key,
		bytes.NewReader(putBody), bodyHash,
		TestAccessKey, TestSecretKey, http.Header{},
	)
	require.NoError(t, err)

	resp3, err := http.DefaultClient.Do(putReq)
	require.NoError(t, err)
	io.Copy(io.Discard, resp3.Body)
	resp3.Body.Close()
	assert.Empty(t, resp3.Header.Get("X-Tigris-Proxy-Signing-Keys"), "Internal header must be stripped on PUT")
}

// TestTransparentAuth_InternalHeaderStrippedOnErrors verifies that the
// X-Tigris-Proxy-Signing-Keys header is stripped on error responses (403, 404, 500).
func TestTransparentAuth_InternalHeaderStrippedOnErrors(t *testing.T) {
	var statusCode int32
	atomic.StoreInt32(&statusCode, http.StatusForbidden)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Always set the internal header
		w.Header().Set("X-Tigris-Proxy-Signing-Keys", "leaked-header-value")
		code := int(atomic.LoadInt32(&statusCode))
		w.WriteHeader(code)
		w.Write([]byte("error response"))
	})

	env := NewTestEnvironmentWithTransparentAuth(t, handler)
	defer env.Close()

	tests := []struct {
		name string
		code int
	}{
		{"403 Forbidden", http.StatusForbidden},
		{"404 Not Found", http.StatusNotFound},
		{"500 Internal Server Error", http.StatusInternalServerError},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			atomic.StoreInt32(&statusCode, int32(tc.code))

			resp, err := env.DoSignedRequest("GET", "/error-bucket/test.txt", nil)
			require.NoError(t, err)
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			assert.Equal(t, tc.code, resp.StatusCode)
			assert.Empty(t, resp.Header.Get("X-Tigris-Proxy-Signing-Keys"),
				"Internal header must be stripped on %d response", tc.code)
		})
	}
}
