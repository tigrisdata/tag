//go:build originless

// Package originless is the end-to-end suite for origin-less mode, driven by the
// s3-test-originless Makefile target. Fully hermetic — no credentials, no
// network, no mock — because it exercises exactly the production topology: an
// origin-less TAG written to and read from directly by its caller (in
// production, the tigris gateway; here, a stock aws-sdk-go-v2 client).
//
// Phases, selected by ORIGINLESS_PHASE:
//
//	warm  — SDK PUTs against origin-less TAG populate the tier, and reads verify
//	        the round trip.
//	serve — TAG restarted on the SAME cache directory (persistence across
//	        restart), then the full read/write contract is asserted over the
//	        wire.
package originless

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	objKey       = "warmed.txt"
	objContent   = "origin-less lifecycle test object"
	emptyKey     = "empty.txt"
	missingKey   = "never-written.txt"
	waitPopulate = 700 * time.Millisecond
)

func endpoint() string {
	if e := os.Getenv("ORIGINLESS_ENDPOINT"); e != "" {
		return e
	}
	return "http://localhost:8080"
}

func bucket(t *testing.T) string {
	b := os.Getenv("ORIGINLESS_BUCKET")
	if b == "" {
		t.Fatal("ORIGINLESS_BUCKET not set (run via make s3-test-originless)")
	}
	return b
}

func phase() string { return os.Getenv("ORIGINLESS_PHASE") }

// sdkClient returns an aws-sdk-go-v2 S3 client against the given endpoint.
// anonymous selects unsigned requests — the only kind phase-1 origin-less serves.
func sdkClient(t *testing.T, endpointURL string, anonymous bool) *s3.Client {
	t.Helper()
	var provider aws.CredentialsProvider = aws.AnonymousCredentials{}
	if !anonymous {
		provider = credentials.NewStaticCredentialsProvider(
			os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), "")
	}
	return s3.NewFromConfig(aws.Config{}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpointURL)
		o.Region = "auto"
		o.UsePathStyle = true
		o.Credentials = provider
	})
}

func rawDo(t *testing.T, method, path string, hdr map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, endpoint()+path, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

func TestWarmPhase(t *testing.T) {
	if phase() != "warm" {
		t.Skip("not the warm phase")
	}
	b := bucket(t)
	ctx := context.Background()
	client := sdkClient(t, endpoint(), true) // unsigned: the network is the boundary

	// Populate the tier the way production does: direct PUTs.
	for key, body := range map[string][]byte{objKey: []byte(objContent), emptyKey: {}} {
		out, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(b), Key: aws.String(key), Body: bytes.NewReader(body),
		})
		if err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		if aws.ToString(out.ETag) == "" {
			t.Fatalf("put %s returned no ETag", key)
		}
	}

	// Immediate read-back through the same surface.
	for _, key := range []string{objKey, emptyKey} {
		resp := rawDo(t, http.MethodGet, "/"+b+"/"+key, nil)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("read-back %s: %d", key, resp.StatusCode)
		}
	}
}

func TestServePhase(t *testing.T) {
	if phase() != "serve" {
		t.Skip("not the serve phase")
	}
	b := bucket(t)

	t.Run("warmed object serves with matching bytes", func(t *testing.T) {
		resp := rawDo(t, http.MethodGet, "/"+b+"/"+objKey, nil)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK || string(body) != objContent {
			t.Fatalf("status=%d body=%q", resp.StatusCode, body)
		}
		if resp.Header.Get("X-Cache") != "HIT" {
			t.Fatalf("X-Cache=%q, want HIT", resp.Header.Get("X-Cache"))
		}
	})

	t.Run("empty object serves", func(t *testing.T) {
		resp := rawDo(t, http.MethodGet, "/"+b+"/"+emptyKey, nil)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK || len(body) != 0 {
			t.Fatalf("status=%d len=%d, want 200/0", resp.StatusCode, len(body))
		}
	})

	t.Run("signed requests are served too (auth ignored, not evaluated)", func(t *testing.T) {
		resp := rawDo(t, http.MethodGet, "/"+b+"/"+objKey, map[string]string{
			"Authorization": "AWS4-HMAC-SHA256 Credential=AKIA/20260824/auto/s3/aws4_request, SignedHeaders=host, Signature=deadbeef",
		})
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("signed GET: %d — the gateway signs every request; rejecting it breaks the primary caller", resp.StatusCode)
		}
	})

	t.Run("write-delete loop works after restart", func(t *testing.T) {
		client := sdkClient(t, endpoint(), true)
		ctx := context.Background()
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(b), Key: aws.String("post-restart.txt"), Body: bytes.NewReader([]byte("fresh")),
		}); err != nil {
			t.Fatalf("put after restart: %v", err)
		}
		out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("post-restart.txt")})
		if err != nil {
			t.Fatalf("get after put: %v", err)
		}
		body, _ := io.ReadAll(out.Body)
		out.Body.Close()
		if string(body) != "fresh" {
			t.Fatalf("body=%q", body)
		}
		if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(b), Key: aws.String("post-restart.txt")}); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if _, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String("post-restart.txt")}); err == nil {
			t.Fatal("get after delete must miss")
		}
	})

	t.Run("stock SDK client works (x-id)", func(t *testing.T) {
		out, err := sdkClient(t, endpoint(), true).GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(b), Key: aws.String(objKey),
		})
		if err != nil {
			t.Fatalf("SDK GetObject: %v", err)
		}
		defer out.Body.Close()
		body, _ := io.ReadAll(out.Body)
		if string(body) != objContent {
			t.Fatalf("SDK body=%q", body)
		}
	})

	t.Run("SDK miss is NoSuchKey, not an error class", func(t *testing.T) {
		_, err := sdkClient(t, endpoint(), true).GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(b), Key: aws.String(missingKey),
		})
		var nsk *types.NoSuchKey
		if !errors.As(err, &nsk) {
			t.Fatalf("want NoSuchKey, got %v", err)
		}
	})

	t.Run("HEAD and conditionals agree with GET", func(t *testing.T) {
		resp := rawDo(t, http.MethodHead, "/"+b+"/"+objKey, nil)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("HEAD warmed: %d", resp.StatusCode)
		}
		etag := resp.Header.Get("ETag")
		if etag == "" {
			t.Fatal("HEAD returned no ETag")
		}
		resp = rawDo(t, http.MethodGet, "/"+b+"/"+objKey, map[string]string{"If-None-Match": etag})
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotModified {
			t.Fatalf("If-None-Match: %d, want 304", resp.StatusCode)
		}
	})

	t.Run("client Cache-Control cannot 404 a cached object", func(t *testing.T) {
		for _, cc := range []string{"no-cache", "no-store", "max-age=0"} {
			resp := rawDo(t, http.MethodGet, "/"+b+"/"+objKey, map[string]string{"Cache-Control": cc})
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("Cache-Control %q: %d, want 200", cc, resp.StatusCode)
			}
		}
	})

	t.Run("mutations and listings answer 501 and leave the cache intact", func(t *testing.T) {
		for name, r := range map[string]struct{ method, path string }{
			"bulk delete":        {http.MethodPost, "/" + b + "?delete"},
			"list":               {http.MethodGet, "/" + b + "?list-type=2"},
			"initiate multipart": {http.MethodPost, "/" + b + "/" + objKey + "?uploads"},
			"upload part":        {http.MethodPut, "/" + b + "/" + objKey + "?uploadId=u1&partNumber=1"},
			"versioned read":     {http.MethodGet, "/" + b + "/" + objKey + "?versionId=abc"},
		} {
			resp := rawDo(t, r.method, r.path, nil)
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusNotImplemented {
				t.Fatalf("%s: %d, want 501", name, resp.StatusCode)
			}
		}
		// The rejected mutations above named the warmed object; it must be intact.
		resp := rawDo(t, http.MethodGet, "/"+b+"/"+objKey, nil)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK || string(body) != objContent {
			t.Fatalf("after rejected mutations: status=%d body=%q", resp.StatusCode, body)
		}
	})

	t.Run("miss answers fast", func(t *testing.T) {
		start := time.Now()
		resp := rawDo(t, http.MethodGet, "/"+b+"/"+missingKey, nil)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("miss: %d", resp.StatusCode)
		}
		if d := time.Since(start); d > 2*time.Second {
			t.Fatalf("miss took %v — a hang means a fetch path escaped the router", d)
		}
	})
}
