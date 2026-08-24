//go:build originless

// Package originless is the end-to-end suite for origin-less mode, driven by the
// s3-test-originless Makefile target. It runs against a REAL tag binary over HTTP
// in two phases, selected by ORIGINLESS_PHASE, and is fully hermetic — no
// credentials, no network upstream, no shared account:
//
//	warm  — TAG in proxy mode against a local in-memory mock upstream
//	        (tests/originless/mockupstream). Objects are written through TAG and
//	        read anonymously so entries are cached with the inferred public-read
//	        ACL — the only entries phase-1 origin-less serves. A HIT is asserted
//	        so the disk cache is proven warm.
//	serve — the SAME cache directory, TAG restarted with no upstream and no
//	        credentials in its environment. Asserts the whole origin-less
//	        contract over the wire with a stock aws-sdk-go-v2 client.
//
// In production this tier is its own deployment: the tigris gateway writes into
// it and reads from it directly, and content never arrives via a proxy-mode
// flip. The mock-upstream warm is a stand-in for those gateway writes until
// local writes land (phase 3), at which point the warm phase becomes SDK PUTs
// against origin-less TAG itself and the mock is deleted. What the flip DOES
// faithfully exercise, and what this suite is for, is the serve contract of a
// real binary over a real RocksDB cache with a genuinely absent origin.
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
	client := sdkClient(t, endpoint(), false)

	// No CreateBucket: the mock upstream is keyspace-only. The mock's 200 on an
	// anonymous GET is what stands in for "public" — TAG caches the entry with the
	// inferred public-read ACL, exactly as it does for a genuinely public bucket.
	for key, body := range map[string][]byte{objKey: []byte(objContent), emptyKey: {}} {
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(b), Key: aws.String(key), Body: bytes.NewReader(body),
		}); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	// Anonymous GETs through TAG: the first fetches from Tigris and caches with
	// the inferred public-read ACL; the second must HIT, proving the disk cache
	// holds a phase-1-servable entry before the mode flip.
	for _, key := range []string{objKey, emptyKey} {
		resp := rawDo(t, http.MethodGet, "/"+b+"/"+key, nil)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("anonymous GET %s: %d", key, resp.StatusCode)
		}
		time.Sleep(waitPopulate)

		resp = rawDo(t, http.MethodGet, "/"+b+"/"+key, nil)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || resp.Header.Get("X-Cache") != "HIT" {
			t.Fatalf("second anonymous GET %s: status=%d X-Cache=%q, want 200/HIT",
				key, resp.StatusCode, resp.Header.Get("X-Cache"))
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
			"put":                {http.MethodPut, "/" + b + "/" + objKey},
			"delete":             {http.MethodDelete, "/" + b + "/" + objKey},
			"bulk delete":        {http.MethodPost, "/" + b + "?delete"},
			"list":               {http.MethodGet, "/" + b + "?list-type=2"},
			"initiate multipart": {http.MethodPost, "/" + b + "/" + objKey + "?uploads"},
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
