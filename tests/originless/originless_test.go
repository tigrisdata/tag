//go:build originless

// Package originless is the end-to-end suite for origin-less mode, driven by the
// s3-test-originless Makefile target. It runs against a REAL tag binary over HTTP
// in three phases, selected by ORIGINLESS_PHASE:
//
//	warm    — TAG in proxy mode against live Tigris. Creates a public bucket,
//	          writes objects, and reads them anonymously through TAG so entries
//	          are cached with the inferred public-read ACL — the only entries
//	          phase-1 origin-less will serve. Asserts a HIT so the disk cache is
//	          proven warm before the mode flip.
//	serve   — the SAME cache directory, TAG restarted with no upstream, no
//	          credentials, and no upstream reachable at all. Asserts the whole
//	          origin-less contract over the wire.
//	cleanup — deletes the test bucket directly against the upstream (TAG is not
//	          involved; origin-less TAG cannot delete anything).
//
// The suite exists because the mode's unit tests seed the cache in-process; only
// this harness proves the real lifecycle — warmed in proxy mode, served
// origin-less from the same disk — with a stock aws-sdk-go-v2 client (which
// appends ?x-id to every request) and a genuinely unreachable origin.
package originless

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
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

	// Public bucket: objects inherit public-read, which is what lets an anonymous
	// GET succeed and re-cache the entry with the inferred ACL.
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(b),
		ACL:    types.BucketCannedACLPublicRead,
	}); err != nil {
		t.Fatalf("create public bucket: %v", err)
	}

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

func TestCleanupPhase(t *testing.T) {
	if phase() != "cleanup" {
		t.Skip("not the cleanup phase")
	}
	b := bucket(t)
	if !strings.HasPrefix(b, "tag-diag-") {
		t.Fatalf("refusing to clean bucket %q without the tag-diag- test prefix", b)
	}
	ctx := context.Background()
	// Directly against the upstream: TAG is origin-less (or down) and cannot delete.
	client := sdkClient(t, os.Getenv("ORIGINLESS_UPSTREAM"), false)

	// Sweep everything rather than the keys we think we wrote, then retry the
	// bucket delete: object deletion is eventually consistent, and a DeleteBucket
	// issued immediately after the deletes can still see them (observed as a 409
	// BucketNotEmpty in CI for a bucket that was in fact empty moments later).
	for attempt := 1; attempt <= 6; attempt++ {
		list, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(b)})
		if err != nil {
			t.Fatalf("list for cleanup: %v", err)
		}
		for _, obj := range list.Contents {
			if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(b), Key: obj.Key}); err != nil {
				t.Logf("delete %s: %v", aws.ToString(obj.Key), err)
			}
		}
		if _, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(b)}); err == nil {
			fmt.Println("cleaned", b)
			return
		} else if attempt == 6 {
			t.Errorf("delete bucket after %d attempts: %v", attempt, err)
		}
		time.Sleep(2 * time.Second)
	}
}
