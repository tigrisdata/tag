package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/proxy"
)

const (
	completionReplayKey          = "object"
	completionReplayUploadID     = "upload-123"
	completionReplayMissUploadID = "missing-upload"
	completionReplaySlowUploadID = "near-expiry-upload"
	completionReplayUnboundID    = "unbound-upload"
	completionReplayAliasID      = "id|suffix"
	completionReplayAliasKey     = "object|id"
	completionReplayAliasUpload  = "suffix"
	completionReplayAccessA      = TestAccessKey
	completionReplaySecretA      = TestSecretKey
	completionReplayAccessB      = "CLIENT-B"
	completionReplaySecretB      = "secret-b"
)

var (
	completionReplayRequestBody = []byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"part"</ETag></Part></CompleteMultipartUpload>`)
	completionReplaySuccessBody = []byte(`<CompleteMultipartUploadResult><ETag>"completed"</ETag></CompleteMultipartUploadResult>`)
)

type completionReplayResponse struct {
	status      int
	contentType string
	etag        string
	body        string
}

func TestCompleteMultipartUploadReplayAuthorization(t *testing.T) {
	t.Run("transparent", func(t *testing.T) {
		testCompleteMultipartUploadReplayAuthorization(t, true)
	})
	t.Run("signing", func(t *testing.T) {
		testCompleteMultipartUploadReplayAuthorization(t, false)
	})
	t.Run("transparent-cold-auth", func(t *testing.T) {
		testColdCompletionReplayAfterSignatureWindow(t)
	})
}

func testCompleteMultipartUploadReplayAuthorization(t *testing.T, transparent bool) {
	t.Helper()
	bucket := completionReplayBucket(t)
	otherBucket := bucket + "-other"
	upstream, completionCalls, slowCalls := newCompletionReplayUpstream(t, bucket, transparent)
	env := newCompletionReplayEnvironment(t, transparent, upstream)
	defer env.Close()

	if transparent {
		seedTransparentPrincipal(t, env, completionReplayAccessB, completionReplaySecretB, bucket)
		// Exercise the cache's bucket dimension even when the same principal is
		// authorized for both buckets.
		env.AuthzCache.Grant(completionReplayAccessA, otherBucket)
	} else {
		env.CredStore.AddCredential(completionReplayAccessB, completionReplaySecretB)
	}

	client := env.TAGServer.Client()
	unboundEntry := completionReplayResponse{status: http.StatusOK, contentType: "application/xml", etag: `"legacy-etag"`, body: `<LegacyCompletion/>`}
	seedUnboundCompletionEntry(t, env, bucket, completionReplayKey, completionReplayUnboundID, unboundEntry)
	before := completionCalls.Load()
	unbound := sendCompletionReplayRequest(t, client, env.TAGServer.URL, bucket, completionReplayKey, completionReplayUnboundID, completionReplayAccessA, completionReplaySecretA, false, false)
	assertCompletionReplayDenied(t, "completion without a saved caller identity", unbound, unboundEntry)
	assertCompletionReplayForwarded(t, "completion without a saved caller identity", unbound, completionCalls.Load(), before, http.StatusForbidden)

	before = completionCalls.Load()
	first := sendCompletionReplayRequest(t, client, env.TAGServer.URL, bucket, completionReplayKey, completionReplayUploadID, completionReplayAccessA, completionReplaySecretA, false, false)
	if first.status != http.StatusOK || first.contentType != "application/xml" || first.etag != `"completed-etag"` || first.body != string(completionReplaySuccessBody) {
		t.Fatalf("initial completion = %#v; want successful upstream response", first)
	}
	if got := completionCalls.Load(); got != before+1 {
		t.Fatalf("initial completion made %d upstream completion calls; want 1", got-before)
	}

	before = completionCalls.Load()
	samePrincipal := sendCompletionReplayRequest(t, client, env.TAGServer.URL, bucket, completionReplayKey, completionReplayUploadID, completionReplayAccessA, completionReplaySecretA, false, false)
	if samePrincipal != first {
		t.Errorf("validated same-principal replay = %#v; want saved response %#v", samePrincipal, first)
	}
	assertCompletionReplayCallCount(t, "same-principal replay", completionCalls.Load(), before)

	if transparent {
		data, err := env.EmbeddedCache.Get(context.Background(), cache.MakeCompletionKey(bucket, completionReplayKey, completionReplayUploadID))
		if err != nil || data == nil {
			t.Fatalf("completion cache entry was not live before authorization revocation: data=%d bytes err=%v", len(data), err)
		}
		var entry struct {
			StatusCode int    `json:"status_code"`
			Body       []byte `json:"body"`
		}
		if err := json.Unmarshal(data, &entry); err != nil || entry.StatusCode != first.status || string(entry.Body) != first.body {
			t.Fatalf("completion cache entry = status %d body %q err=%v; want live saved response", entry.StatusCode, entry.Body, err)
		}
		env.AuthzCache.Revoke(completionReplayAccessA, bucket)
		before = completionCalls.Load()
		unauthorizedBucket := sendCompletionReplayRequest(t, client, env.TAGServer.URL, bucket, completionReplayKey, completionReplayUploadID, completionReplayAccessA, completionReplaySecretA, false, false)
		assertCompletionReplayDenied(t, "same principal without bucket authorization", unauthorizedBucket, first)
		assertCompletionReplayForwarded(t, "same principal without bucket authorization", unauthorizedBucket, completionCalls.Load(), before, http.StatusForbidden)
		env.AuthzCache.Grant(completionReplayAccessA, bucket)
	}

	validator := auth.NewRequestValidator(env.CredStore)
	if transparent {
		validator = auth.NewRequestValidator(env.DerivedKeyStore)
	}
	before = completionCalls.Load()
	presigned := sendPresignedCompletionReplayRequest(t, client, env.TAGServer.URL, bucket, completionReplayKey, completionReplayUploadID, completionReplayAccessA, completionReplaySecretA, time.Now().UTC().Truncate(time.Second), validator)
	if presigned != first {
		t.Errorf("valid same-principal presigned retry = %#v; want saved response %#v", presigned, first)
	}
	assertCompletionReplayCallCount(t, "presigned replay", completionCalls.Load(), before)

	aliasSeed := sendCompletionReplayRequest(t, client, env.TAGServer.URL, bucket, completionReplayKey, completionReplayAliasID, completionReplayAccessA, completionReplaySecretA, false, false)
	if aliasSeed.status != http.StatusOK || aliasSeed.etag != `"completed-etag"` || aliasSeed.body != string(completionReplaySuccessBody) {
		t.Fatalf("delimiter-alias seed completion = %#v; want successful upstream response", aliasSeed)
	}
	aliasCallsBefore := completionCalls.Load()
	alias := sendCompletionReplayRequest(t, client, env.TAGServer.URL, bucket, completionReplayAliasKey, completionReplayAliasUpload, completionReplayAccessA, completionReplaySecretA, false, false)
	assertCompletionReplayDenied(t, "same-principal delimiter-alias tuple", alias, aliasSeed)
	assertCompletionReplayForwarded(t, "same-principal delimiter-alias tuple", alias, completionCalls.Load(), aliasCallsBefore, http.StatusNotFound)

	before = completionCalls.Load()
	otherBucketResponse := sendCompletionReplayRequest(t, client, env.TAGServer.URL, otherBucket, completionReplayKey, completionReplayUploadID, completionReplayAccessA, completionReplaySecretA, false, false)
	assertCompletionReplayDenied(t, "same principal in another bucket", otherBucketResponse, first)
	assertCompletionReplayForwarded(t, "same principal in another bucket", otherBucketResponse, completionCalls.Load(), before, http.StatusForbidden)

	before = completionCalls.Load()
	miss := sendCompletionReplayRequest(t, client, env.TAGServer.URL, bucket, completionReplayKey, completionReplayMissUploadID, completionReplayAccessA, completionReplaySecretA, false, false)
	assertCompletionReplayDenied(t, "cache miss", miss, first)
	if miss.status != http.StatusNotFound {
		t.Errorf("cache miss status = %d; want upstream 404", miss.status)
	}
	assertCompletionReplayForwarded(t, "cache miss", miss, completionCalls.Load(), before, http.StatusNotFound)

	before = completionCalls.Load()
	differentPrincipal := sendCompletionReplayRequest(t, client, env.TAGServer.URL, bucket, completionReplayKey, completionReplayUploadID, completionReplayAccessB, completionReplaySecretB, false, false)
	assertCompletionReplayDenied(t, "different validated principal", differentPrincipal, first)
	assertCompletionReplayForwarded(t, "different validated principal", differentPrincipal, completionCalls.Load(), before, http.StatusForbidden)

	before = completionCalls.Load()
	anonymous := sendCompletionReplayRequest(t, client, env.TAGServer.URL, bucket, completionReplayKey, completionReplayUploadID, "", "", true, false)
	assertCompletionReplayDenied(t, "anonymous caller", anonymous, first)
	if transparent {
		assertCompletionReplayForwarded(t, "anonymous caller", anonymous, completionCalls.Load(), before, http.StatusForbidden)
	} else {
		assertCompletionReplayNotForwarded(t, "signing-mode anonymous caller", completionCalls.Load(), before)
	}

	before = completionCalls.Load()
	badSignature := sendCompletionReplayRequest(t, client, env.TAGServer.URL, bucket, completionReplayKey, completionReplayUploadID, completionReplayAccessA, completionReplaySecretA, false, true)
	assertCompletionReplayDenied(t, "bad-signature caller", badSignature, first)
	if transparent {
		assertCompletionReplayForwarded(t, "bad-signature caller", badSignature, completionCalls.Load(), before, http.StatusForbidden)
	} else {
		assertCompletionReplayNotForwarded(t, "signing-mode bad-signature caller", completionCalls.Load(), before)
	}

	if transparent {
		env.AuthzCache.Grant(completionReplayAccessA, bucket)
	}
	testCompletionReplayAfterSignatureWindow(t, env, bucket, completionCalls, slowCalls, validator)

}

func seedUnboundCompletionEntry(t *testing.T, env *TestEnvironment, bucket, key, uploadID string, response completionReplayResponse) {
	t.Helper()
	entry := cache.CompletionEntry{
		StatusCode: response.status,
		Headers: map[string]string{
			"Content-Type": response.contentType,
			"ETag":         response.etag,
		},
		Body: []byte(response.body),
	}
	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal unbound completion entry: %v", err)
	}
	if err := env.EmbeddedCache.Put(context.Background(), cache.MakeCompletionKey(bucket, key, uploadID), data, int64(cache.CompletionCacheTTL)); err != nil {
		t.Fatalf("seed unbound completion entry: %v", err)
	}
}

func completionReplayBucket(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("replay-%d", time.Now().UnixNano())
}

func newCompletionReplayEnvironment(t *testing.T, transparent bool, upstream http.HandlerFunc) *TestEnvironment {
	t.Helper()
	if transparent {
		return NewTestEnvironmentWithTransparentAuth(t, upstream)
	}
	return NewTestEnvironmentWithCacheHandler(upstream)
}

func newCompletionReplayUpstream(t *testing.T, bucket string, transparent bool) (http.HandlerFunc, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	var responseKeys string
	if transparent {
		responseKeys = completionReplaySigningKeysHeader(t, completionReplayAccessA, completionReplaySecretA)
	}
	var completionCalls atomic.Int32
	var mainCalls atomic.Int32
	var aliasCalls atomic.Int32
	var slowCalls atomic.Int32
	writeSuccess := func(w http.ResponseWriter) {
		if responseKeys != "" {
			w.Header().Set("X-Tigris-Proxy-Signing-Keys", responseKeys)
		}
		w.Header().Set("ETag", `"completed-etag"`)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(completionReplaySuccessBody)
	}
	writeError := func(w http.ResponseWriter, status int) {
		w.WriteHeader(status)
		if status == http.StatusNotFound {
			_, _ = io.WriteString(w, `<Error><Code>NoSuchUpload</Code></Error>`)
		} else {
			_, _ = io.WriteString(w, `<Error><Code>AccessDenied</Code></Error>`)
		}
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Query().Get("uploadId") == "" {
			http.NotFound(w, r)
			return
		}
		completionCalls.Add(1)
		requestBucket, key := proxy.ParseBucketKey(r)
		uploadID := r.URL.Query().Get("uploadId")
		info, authErr := auth.ParseAuthInfo(r)
		if requestBucket == bucket && key == completionReplayKey && uploadID == completionReplayUploadID && authErr == nil && info.AccessKey == completionReplayAccessA {
			if mainCalls.Add(1) == 1 {
				writeSuccess(w)
				return
			}
			writeError(w, http.StatusForbidden)
			return
		}
		if requestBucket == bucket && key == completionReplayKey && uploadID == completionReplayAliasID && authErr == nil && info.AccessKey == completionReplayAccessA {
			if aliasCalls.Add(1) == 1 {
				writeSuccess(w)
				return
			}
			writeError(w, http.StatusForbidden)
			return
		}
		if requestBucket == bucket && key == completionReplayAliasKey && uploadID == completionReplayAliasUpload {
			writeError(w, http.StatusNotFound)
			return
		}
		if requestBucket == bucket && key == completionReplayKey && uploadID == completionReplaySlowUploadID && authErr == nil && info.AccessKey == completionReplayAccessA {
			if slowCalls.Add(1) == 1 {
				time.Sleep(8 * time.Second)
				writeSuccess(w)
				return
			}
			writeError(w, http.StatusNotFound)
			return
		}
		if requestBucket == bucket && key == completionReplayKey && uploadID == completionReplayMissUploadID {
			writeError(w, http.StatusNotFound)
			return
		}
		writeError(w, http.StatusForbidden)
	})
	return handler, &completionCalls, &slowCalls
}

func completionReplaySigningKeysHeader(t *testing.T, accessKey, secretKey string) string {
	t.Helper()
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(accessKey, secretKey)
	now := time.Now().UTC()
	entries := make([]auth.SigningKeyEntry, 0, 3)
	for offset := -1; offset <= 1; offset++ {
		date := now.AddDate(0, 0, offset).Format("20060102")
		key, err := credentials.GetSigningKey(accessKey, date, TestRegion)
		if err != nil {
			t.Fatalf("derive response signing key for %s: %v", accessKey, err)
		}
		entries = append(entries, auth.SigningKeyEntry{Date: date, Region: TestRegion, SigningKey: hex.EncodeToString(key)})
	}
	return encryptSigningKeysHeader(t, TestProxySecretKey, accessKey, entries)
}

func seedTransparentPrincipal(t *testing.T, env *TestEnvironment, accessKey, secretKey, bucket string) {
	t.Helper()
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(accessKey, secretKey)
	now := time.Now().UTC()
	for offset := -1; offset <= 1; offset++ {
		date := now.AddDate(0, 0, offset).Format("20060102")
		key, err := credentials.GetSigningKey(accessKey, date, TestRegion)
		if err != nil {
			t.Fatalf("derive local signing key for %s: %v", accessKey, err)
		}
		env.DerivedKeyStore.Store(accessKey, date, TestRegion, key)
	}
	env.AuthzCache.Grant(accessKey, bucket)
}

func assertCompletionReplayDenied(t *testing.T, name string, got, cached completionReplayResponse) {
	t.Helper()
	if got.status >= http.StatusOK && got.status < http.StatusMultipleChoices || got.etag == cached.etag || got.body == cached.body {
		t.Errorf("%s received status=%d ETag=%q body=%q from a cached completion", name, got.status, got.etag, got.body)
	}
}

func assertCompletionReplayForwarded(t *testing.T, name string, response completionReplayResponse, got, before int32, wantStatus int) {
	t.Helper()
	if got != before+1 {
		t.Errorf("%s made %d upstream calls; want 1 authorization request", name, got-before)
	}
	if response.status != wantStatus {
		t.Errorf("forwarded %s response status = %d; want upstream status %d", name, response.status, wantStatus)
	}
}

func assertCompletionReplayNotForwarded(t *testing.T, name string, got, before int32) {
	t.Helper()
	if got != before {
		t.Errorf("%s made %d upstream calls; want local rejection", name, got-before)
	}
}

func assertCompletionReplayCallCount(t *testing.T, name string, got, before int32) {
	t.Helper()
	if got != before {
		t.Errorf("%s made %d upstream calls; want cached response", name, got-before)
	}
}

func sendCompletionReplayRequest(t *testing.T, client *http.Client, endpoint, bucket, key, uploadID, accessKey, secretKey string, anonymous, badSignature bool) completionReplayResponse {
	t.Helper()
	path := "/" + bucket + "/" + key + "?" + url.Values{"uploadId": {uploadID}}.Encode()
	var req *http.Request
	var err error
	if anonymous {
		req, err = http.NewRequest(http.MethodPost, endpoint+path, bytes.NewReader(completionReplayRequestBody))
		if err == nil {
			req.Header.Set("Content-Type", "application/xml")
		}
	} else {
		sum := sha256.Sum256(completionReplayRequestBody)
		req, err = auth.NewRequestSigner(endpoint, TestRegion).SignRequest(context.Background(), http.MethodPost, path, bytes.NewReader(completionReplayRequestBody), hex.EncodeToString(sum[:]), accessKey, secretKey, http.Header{"Content-Type": {"application/xml"}})
	}
	if err != nil {
		t.Fatalf("create completion request: %v", err)
	}
	if badSignature {
		authorization := req.Header.Get("Authorization")
		marker := "Signature="
		i := strings.LastIndex(authorization, marker)
		if i < 0 {
			t.Fatalf("signed request has no %q field: %q", marker, authorization)
		}
		i += len(marker)
		if i >= len(authorization) {
			t.Fatalf("signed request has empty signature: %q", authorization)
		}
		if authorization[i] == '0' {
			authorization = authorization[:i] + "1" + authorization[i+1:]
		} else {
			authorization = authorization[:i] + "0" + authorization[i+1:]
		}
		req.Header.Set("Authorization", authorization)
	}
	return executeCompletionReplayRequest(t, client, req)
}

func sendPresignedCompletionReplayRequest(t *testing.T, client *http.Client, endpoint, bucket, key, uploadID, accessKey, secretKey string, signedAt time.Time, validator *auth.RequestValidator) completionReplayResponse {
	t.Helper()
	request := presignCompletionReplayRequest(t, endpoint, bucket, key, uploadID, accessKey, secretKey, signedAt, validator)
	if request.Header.Get("X-Amz-Content-Sha256") != "" {
		t.Fatal("presigned request unexpectedly includes X-Amz-Content-Sha256")
	}
	return executeCompletionReplayRequest(t, client, request)
}

func executeCompletionReplayRequest(t *testing.T, client *http.Client, req *http.Request) completionReplayResponse {
	t.Helper()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("send completion request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read completion response: %v", err)
	}
	return completionReplayResponse{status: resp.StatusCode, contentType: resp.Header.Get("Content-Type"), etag: resp.Header.Get("ETag"), body: string(body)}
}

func presignCompletionReplayRequest(t *testing.T, endpoint, bucket, key, uploadID, accessKey, secretKey string, signedAt time.Time, validator *auth.RequestValidator) *http.Request {
	t.Helper()
	path := "/" + bucket + "/" + key
	query := url.Values{"uploadId": {uploadID}}
	dateTime := signedAt.UTC().Format(auth.TimeFormat)
	shortDate := signedAt.UTC().Format("20060102")
	scope := shortDate + "/" + TestRegion + "/s3/aws4_request"
	query.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	query.Set("X-Amz-Credential", accessKey+"/"+scope)
	query.Set("X-Amz-Date", dateTime)
	query.Set("X-Amz-Expires", "300")
	query.Set("X-Amz-SignedHeaders", "host")
	request, err := http.NewRequest(http.MethodPost, endpoint+path+"?"+query.Encode(), bytes.NewReader(completionReplayRequestBody))
	if err != nil {
		t.Fatalf("create presigned completion request: %v", err)
	}
	request.Host = request.URL.Host
	request.Header.Set("Content-Type", "application/xml")
	canonicalRequest := strings.Join([]string{
		request.Method,
		request.URL.EscapedPath(),
		query.Encode(),
		"host:" + request.Host + "\n",
		"host",
		"UNSIGNED-PAYLOAD",
	}, "\n")
	canonicalHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := "AWS4-HMAC-SHA256\n" + dateTime + "\n" + scope + "\n" + hex.EncodeToString(canonicalHash[:])
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(accessKey, secretKey)
	signingKey, err := credentials.GetSigningKey(accessKey, shortDate, TestRegion)
	if err != nil {
		t.Fatalf("derive presigned signing key: %v", err)
	}
	query.Set("X-Amz-Signature", hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign))))
	request.URL.RawQuery = query.Encode()
	validationRequest := request.Clone(request.Context())
	validationRequest.Header = request.Header.Clone()
	validationRequest.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	if got, err := validator.ValidateRequest(validationRequest); err != nil || got != accessKey {
		t.Fatalf("presigned signature invalid with its standard unsigned-payload hash: accessKey=%q err=%v", got, err)
	}
	return request
}

func signCompletionReplayRequestAt(t *testing.T, endpoint, bucket, key, uploadID, accessKey, secretKey string, signedAt time.Time) *http.Request {
	t.Helper()
	path := "/" + bucket + "/" + key + "?" + url.Values{"uploadId": {uploadID}}.Encode()
	request, err := http.NewRequest(http.MethodPost, endpoint+path, bytes.NewReader(completionReplayRequestBody))
	if err != nil {
		t.Fatalf("create completion request: %v", err)
	}
	request.Host = request.URL.Host
	request.Header.Set("Content-Type", "application/xml")
	sum := sha256.Sum256(completionReplayRequestBody)
	bodyHash := hex.EncodeToString(sum[:])
	request.Header.Set("X-Amz-Content-Sha256", bodyHash)
	dateTime := signedAt.UTC().Format(auth.TimeFormat)
	shortDate := signedAt.UTC().Format("20060102")
	signedHeaders := "content-type;host;x-amz-content-sha256;x-amz-date"
	request.Header.Set("X-Amz-Date", dateTime)
	canonicalHeaders := strings.Join([]string{
		"content-type:application/xml",
		"host:" + request.Host,
		"x-amz-content-sha256:" + bodyHash,
		"x-amz-date:" + dateTime,
	}, "\n") + "\n"
	canonicalRequest := strings.Join([]string{
		request.Method,
		request.URL.EscapedPath(),
		"uploadId=" + uploadID,
		canonicalHeaders,
		signedHeaders,
		bodyHash,
	}, "\n")
	canonicalHash := sha256.Sum256([]byte(canonicalRequest))
	credentialScope := shortDate + "/" + TestRegion + "/s3/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + dateTime + "\n" + credentialScope + "\n" + hex.EncodeToString(canonicalHash[:])
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(accessKey, secretKey)
	signingKey, err := credentials.GetSigningKey(accessKey, shortDate, TestRegion)
	if err != nil {
		t.Fatalf("derive signing key: %v", err)
	}
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))
	request.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s", accessKey, credentialScope, signedHeaders, signature))
	return request
}

func testCompletionReplayAfterSignatureWindow(t *testing.T, env *TestEnvironment, bucket string, completionCalls, slowCalls *atomic.Int32, validator *auth.RequestValidator) {
	t.Helper()
	signedAt := time.Now().UTC().Add(-14*time.Minute - 55*time.Second).Truncate(time.Second)
	request := signCompletionReplayRequestAt(t, env.TAGServer.URL, bucket, completionReplayKey, completionReplaySlowUploadID, completionReplayAccessA, completionReplaySecretA, signedAt)
	if accessKey, err := validator.ValidateRequest(request); err != nil || accessKey != completionReplayAccessA {
		t.Fatalf("near-expiry signature invalid at dispatch: accessKey=%q err=%v", accessKey, err)
	}

	callsBefore := completionCalls.Load()
	firstResponse := executeCompletionReplayRequest(t, env.TAGServer.Client(), request)
	if firstResponse.status != http.StatusOK || firstResponse.etag != `"completed-etag"` || firstResponse.body != string(completionReplaySuccessBody) {
		t.Fatalf("near-expiry completion = %#v; want upstream success", firstResponse)
	}
	if got := completionCalls.Load(); got != callsBefore+1 {
		t.Fatalf("near-expiry completion made %d upstream calls; want one", got-callsBefore)
	}
	if got := slowCalls.Load(); got != 1 {
		t.Fatalf("near-expiry upload reached upstream %d times; want one", got)
	}

	retry := sendCompletionReplayRequest(t, env.TAGServer.Client(), env.TAGServer.URL, bucket, completionReplayKey, completionReplaySlowUploadID, completionReplayAccessA, completionReplaySecretA, false, false)
	if retry.status != firstResponse.status || retry.contentType != firstResponse.contentType || retry.etag != firstResponse.etag || retry.body != firstResponse.body {
		t.Errorf("same-principal retry after the original signature expired = %#v; want the captured response %#v", retry, firstResponse)
	}
	if got := completionCalls.Load(); got != callsBefore+1 {
		t.Errorf("same-principal retry after signature expiry made %d upstream calls; want saved response", got-callsBefore-1)
	}
	if got := slowCalls.Load(); got != 1 {
		t.Errorf("near-expiry upload was forwarded %d times; want one", got)
	}
}

func testColdCompletionReplayAfterSignatureWindow(t *testing.T) {
	t.Helper()
	bucket := completionReplayBucket(t)
	upstream, completionCalls, slowCalls := newCompletionReplayUpstream(t, bucket, true)
	env := NewTestEnvironmentWithTransparentAuth(t, upstream)
	defer env.Close()
	if env.DerivedKeyStore.HasKey(completionReplayAccessA) || env.AuthzCache.IsAuthorized(completionReplayAccessA, bucket) {
		t.Fatal("test did not begin with cold transparent authentication state")
	}

	signedAt := time.Now().UTC().Add(-14*time.Minute - 55*time.Second).Truncate(time.Second)
	firstRequest := signCompletionReplayRequestAt(t, env.TAGServer.URL, bucket, completionReplayKey, completionReplaySlowUploadID, completionReplayAccessA, completionReplaySecretA, signedAt)
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(completionReplayAccessA, completionReplaySecretA)
	if accessKey, err := auth.NewRequestValidator(credentials).ValidateRequest(firstRequest); err != nil || accessKey != completionReplayAccessA {
		t.Fatalf("near-expiry request is not valid at dispatch: accessKey=%q err=%v", accessKey, err)
	}

	firstResponse := executeCompletionReplayRequest(t, env.TAGServer.Client(), firstRequest)
	if firstResponse.status != http.StatusOK || firstResponse.body != string(completionReplaySuccessBody) {
		t.Fatalf("cold-auth completion = %#v; want upstream success", firstResponse)
	}
	if got := completionCalls.Load(); got != 1 || slowCalls.Load() != 1 {
		t.Fatalf("cold-auth completion made %d completion calls and %d delayed calls; want one each", got, slowCalls.Load())
	}
	if !env.DerivedKeyStore.HasKey(completionReplayAccessA) || !env.AuthzCache.IsAuthorized(completionReplayAccessA, bucket) {
		t.Fatal("successful upstream response did not teach local signing and bucket authorization state")
	}
	if _, err := auth.NewRequestValidator(env.DerivedKeyStore).ValidateRequest(firstRequest); err == nil {
		t.Fatal("original completion signature did not expire during upstream work")
	}

	before := completionCalls.Load()
	retry := sendCompletionReplayRequest(t, env.TAGServer.Client(), env.TAGServer.URL, bucket, completionReplayKey, completionReplaySlowUploadID, completionReplayAccessA, completionReplaySecretA, false, false)
	if retry.status != firstResponse.status || retry.contentType != firstResponse.contentType || retry.etag != firstResponse.etag || retry.body != firstResponse.body {
		t.Errorf("cold-auth same-principal retry = %#v; want captured response %#v", retry, firstResponse)
	}
	if got := completionCalls.Load(); got != before {
		t.Errorf("cold-auth retry made %d upstream calls; want saved response", got-before)
	}
}
