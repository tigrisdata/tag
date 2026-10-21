package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tag/auth"
)

func newSDKPresignedRequestForLocalAuthTest(t *testing.T, endpoint, method, bucket, key, accessKey, secretKey string, signingTime time.Time, expiry time.Duration) *http.Request {
	t.Helper()

	req, err := http.NewRequest(method, endpoint+"/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	query := req.URL.Query()
	query.Set("X-Amz-Expires", strconv.FormatInt(int64(expiry/time.Second), 10))
	req.URL.RawQuery = query.Encode()

	signedURI, signedHeaders, err := v4.NewSigner().PresignHTTP(
		t.Context(),
		aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey},
		req,
		"UNSIGNED-PAYLOAD",
		"s3",
		TestRegion,
		signingTime,
	)
	require.NoError(t, err)
	req.URL, err = url.Parse(signedURI)
	require.NoError(t, err)
	req.Host = req.URL.Host
	for name, values := range signedHeaders {
		if strings.EqualFold(name, "host") {
			continue
		}
		req.Header[name] = append([]string(nil), values...)
	}
	return req
}

func cloneSDKPresignedRequestForLocalAuthTest(req *http.Request) *http.Request {
	clone := req.Clone(context.Background())
	clone.Header = req.Header.Clone()
	urlCopy := *req.URL
	clone.URL = &urlCopy
	return clone
}

func performLocalAuthRequestForTest(t *testing.T, req *http.Request) (int, string, []byte) {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, resp.Header.Get("X-Cache"), body
}

func requirePresignedFallbackForLocalAuthTest(t *testing.T, env *TestEnvironment, req *http.Request, expectedBody []byte) {
	t.Helper()
	before := env.GetUpstreamRequestCount()
	status, xCache, body := performLocalAuthRequestForTest(t, req)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "MISS", xCache)
	require.Equal(t, expectedBody, body)
	require.Greater(t, env.GetUpstreamRequestCount(), before, "request should be forwarded rather than served from cache")
}

func waitForReadableCachedObjectForLocalAuthTest(t *testing.T, env *TestEnvironment, bucket, key string, timeout time.Duration) ([]byte, bool) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if body, found := env.GetCachedObject(bucket, key); found {
			return body, true
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-deadline:
			return nil, false
		}
	}
}

// TestTransparentAuth_PresignedGETWithoutContentHash exercises the actual
// handlers.Server.Router and transparent local-auth cache gate. The object is
// private by default in s3mem; only a successful controlled-origin read warms it.
func TestTransparentAuth_PresignedGETWithoutContentHash(t *testing.T) {
	backend := s3mem.New()
	env := NewTestEnvironmentWithTransparentAuth(t, newSigningKeysUpstreamHandler(t, backend))
	env.S3Backend = backend
	defer env.Close()

	bucket := fmt.Sprintf("presigned-local-auth-%x", time.Now().UnixNano())
	key := "private-object"
	content := []byte("private object read through transparent local auth")
	require.NoError(t, env.PutTestObject(bucket, key, content))

	warmResp, err := env.DoSignedRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	require.NoError(t, err)
	warmBody, err := io.ReadAll(warmResp.Body)
	require.NoError(t, err)
	require.NoError(t, warmResp.Body.Close())
	require.Equal(t, http.StatusOK, warmResp.StatusCode)
	require.Equal(t, "MISS", warmResp.Header.Get("X-Cache"))
	require.Equal(t, content, warmBody)
	require.True(t, env.DerivedKeyStore.HasKey(TestAccessKey), "origin response should teach TAG the derived signing key")
	require.True(t, env.AuthzCache.IsAuthorized(TestAccessKey, bucket), "origin response should grant authorization for this bucket")
	cachedBody, found := waitForReadableCachedObjectForLocalAuthTest(t, env, bucket, key, 2*time.Second)
	require.True(t, found, "cache metadata and body should be readable")
	require.Equal(t, content, cachedBody)
	upstreamAfterWarm := env.GetUpstreamRequestCount()

	urlOnly := newSDKPresignedRequestForLocalAuthTest(
		t,
		env.TAGServer.URL,
		http.MethodGet,
		bucket,
		key,
		TestAccessKey,
		TestSecretKey,
		time.Now().UTC().Truncate(time.Second),
		5*time.Minute,
	)
	require.Equal(t, "AWS4-HMAC-SHA256", urlOnly.URL.Query().Get("X-Amz-Algorithm"))
	require.NotEmpty(t, urlOnly.URL.Query().Get("X-Amz-Credential"))
	require.Equal(t, "host", urlOnly.URL.Query().Get("X-Amz-SignedHeaders"))
	require.Empty(t, urlOnly.Header.Get("X-Amz-Content-Sha256"))
	require.Empty(t, urlOnly.Header.Get("Authorization"))

	extraHeader := cloneSDKPresignedRequestForLocalAuthTest(urlOnly)
	extraHeader.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	require.Equal(t, urlOnly.URL.String(), extraHeader.URL.String(), "the additional payload-hash header is not part of the signed URL")

	status, xCache, body := performLocalAuthRequestForTest(t, extraHeader)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "HIT", xCache)
	require.Equal(t, content, body)
	status, xCache, body = performLocalAuthRequestForTest(t, urlOnly)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "HIT", xCache)
	require.Equal(t, content, body)
	require.Equal(t, upstreamAfterWarm, env.GetUpstreamRequestCount(), "both presigned GETs should be served locally")

	tampered := cloneSDKPresignedRequestForLocalAuthTest(urlOnly)
	query := tampered.URL.Query()
	signature := query.Get("X-Amz-Signature")
	require.NotEmpty(t, signature)
	if signature[0] == '0' {
		signature = "1" + signature[1:]
	} else {
		signature = "0" + signature[1:]
	}
	query.Set("X-Amz-Signature", signature)
	tampered.URL.RawQuery = query.Encode()
	requirePresignedFallbackForLocalAuthTest(t, env, tampered, content)

	expiredTime := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Second)
	expired := newSDKPresignedRequestForLocalAuthTest(
		t,
		env.TAGServer.URL,
		http.MethodGet,
		bucket,
		key,
		TestAccessKey,
		TestSecretKey,
		expiredTime,
		time.Second,
	)
	expiredAuth, err := auth.ParseAuthInfo(expired)
	require.NoError(t, err)
	env.DerivedKeyStore.Store(
		expiredAuth.AccessKey,
		expiredAuth.Date,
		expiredAuth.Region,
		deriveSigningKeyForTest(TestSecretKey, expiredAuth.Date, expiredAuth.Region),
	)
	requirePresignedFallbackForLocalAuthTest(t, env, expired, content)

	unknownKey := newSDKPresignedRequestForLocalAuthTest(
		t,
		env.TAGServer.URL,
		http.MethodGet,
		bucket,
		key,
		"AKIDUNKNOWNLOCALAUTH1234",
		"unknown-secret-for-test",
		time.Now().UTC().Truncate(time.Second),
		5*time.Minute,
	)
	require.False(t, env.DerivedKeyStore.HasKey("AKIDUNKNOWNLOCALAUTH1234"))
	requirePresignedFallbackForLocalAuthTest(t, env, unknownKey, content)

	env.AuthzCache.Revoke(TestAccessKey, bucket)
	require.False(t, env.AuthzCache.IsAuthorized(TestAccessKey, bucket))
	missingGrant := cloneSDKPresignedRequestForLocalAuthTest(urlOnly)
	requirePresignedFallbackForLocalAuthTest(t, env, missingGrant, content)

	headerSigned, err := env.Signer.SignRequest(
		t.Context(),
		http.MethodGet,
		"/"+bucket+"/"+key,
		nil,
		"",
		TestAccessKey,
		TestSecretKey,
		http.Header{},
	)
	require.NoError(t, err)
	headerSigned.Header.Del("X-Amz-Content-Sha256")
	requirePresignedFallbackForLocalAuthTest(t, env, headerSigned, content)
}
