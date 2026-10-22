package cache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
)

// retryingPutClient emulates ocache's local put retry (server/operations
// PutLocal): a first attempt that fails mid-body is retried, after rewinding
// with Seek(0, io.SeekStart), only when the body it was handed implements
// io.Seeker. A non-seekable body gets a single attempt.
type retryingPutClient struct {
	cacheclient.CacheClient
	failFirst bool
	attempts  int
	sawSeeker bool
}

var errTransientPut = errors.New("transient put failure")

func (f *retryingPutClient) PutStream(ctx context.Context, key string, r io.Reader, ttl int64) error {
	seeker, canRetry := r.(io.Seeker)
	f.sawSeeker = canRetry
	for {
		f.attempts++
		if f.attempts == 1 && f.failFirst {
			// Consume part of the body, then fail like a storage error mid-write.
			if _, err := io.CopyN(io.Discard, r, 3); err != nil {
				return err
			}
			if !canRetry {
				return errTransientPut
			}
			if _, err := seeker.Seek(0, io.SeekStart); err != nil {
				return err
			}
			continue
		}
		return f.CacheClient.PutStream(ctx, key, r, ttl)
	}
}

func newRetryingPutCache(t *testing.T, failFirst bool) (*Cache, *retryingPutClient) {
	t.Helper()
	client := &retryingPutClient{CacheClient: cacheclient.NewMemoryCache(), failFirst: failFirst}
	cfg := config.NewDefault()
	cfg.Cache.SetLegacyCoordination(false)
	return NewCacheWithClient(client, &cfg.Cache), client
}

// A seekable body (the write-through fill's bytes.Reader) must stay seekable
// through the counting wrapper so the store's retry-with-rewind still
// applies, and the retried attempt must count its bytes once so the length
// check commits.
func TestPutWithMetaStreamIfVersion_SeekableBodyRetriesAndCommits(t *testing.T) {
	c, client := newRetryingPutCache(t, true)
	ctx := context.Background()
	bucket, key := "b", "k"
	body := []byte("1234567890")

	meta := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"v1"`, ContentLength: int64(len(body)), StatusCode: 200}
	wrote, err := c.PutWithMetaStreamIfVersion(ctx, bucket, key, meta, bytes.NewReader(body), 60, VersionAny)
	if err != nil {
		t.Fatalf("PutWithMetaStreamIfVersion: %v", err)
	}
	if !wrote {
		t.Fatal("wrote = false, want true")
	}
	if !client.sawSeeker {
		t.Fatal("store did not see an io.Seeker for a seekable body: retries lost")
	}
	if client.attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (failed first, retried after Seek)", client.attempts)
	}
	if _, found, _ := c.GetMeta(ctx, bucket, key); !found {
		t.Fatal("meta not committed after retried put")
	}
	var buf bytes.Buffer
	if err := c.GetBodyStream(ctx, bucket, key, meta.ETag, &buf); err != nil {
		t.Fatalf("GetBodyStream: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), body) {
		t.Fatalf("stored body = %q, want %q", buf.Bytes(), body)
	}
}

// A non-seekable body (the streaming io.Pipe populate) is unchanged: one
// attempt, the failure surfaces, nothing is committed.
func TestPutWithMetaStreamIfVersion_NonSeekableBodyNotRetried(t *testing.T) {
	c, client := newRetryingPutCache(t, true)
	ctx := context.Background()
	bucket, key := "b", "k"
	body := struct{ io.Reader }{bytes.NewReader([]byte("1234567890"))} // hides Seek

	meta := &CachedObjectMeta{Bucket: bucket, Key: key, ETag: `"v1"`, ContentLength: 10, StatusCode: 200}
	wrote, err := c.PutWithMetaStreamIfVersion(ctx, bucket, key, meta, body, 60, VersionAny)
	if !errors.Is(err, errTransientPut) {
		t.Fatalf("error = %v, want %v", err, errTransientPut)
	}
	if wrote {
		t.Fatal("wrote = true, want false")
	}
	if client.sawSeeker {
		t.Fatal("non-seekable body was presented to the store as an io.Seeker")
	}
	if client.attempts != 1 {
		t.Fatalf("attempts = %d, want 1", client.attempts)
	}
	if _, found, _ := c.GetMeta(ctx, bucket, key); found {
		t.Fatal("meta committed after a failed put")
	}
}

// Seek resets the count to the new absolute offset.
func TestCountingReadSeeker_SeekResetsCount(t *testing.T) {
	wrapped, counted := newCountingReader(bytes.NewReader([]byte("1234567890")))
	seeker, ok := wrapped.(io.Seeker)
	if !ok {
		t.Fatal("wrapper for a seekable body does not implement io.Seeker")
	}
	if _, err := io.CopyN(io.Discard, wrapped, 4); err != nil {
		t.Fatalf("CopyN: %v", err)
	}
	if counted.n != 4 {
		t.Fatalf("n after 4 bytes = %d, want 4", counted.n)
	}
	if _, err := seeker.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	if counted.n != 0 {
		t.Fatalf("n after Seek(0) = %d, want 0", counted.n)
	}
	if _, err := seeker.Seek(2, io.SeekStart); err != nil {
		t.Fatalf("Seek(2): %v", err)
	}
	if _, err := io.Copy(io.Discard, wrapped); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if counted.n != 10 {
		t.Fatalf("n after Seek(2)+drain = %d, want 10 (absolute offset)", counted.n)
	}

	plain, _ := newCountingReader(struct{ io.Reader }{bytes.NewReader(nil)})
	if _, ok := plain.(io.Seeker); ok {
		t.Fatal("wrapper for a non-seekable body must not claim io.Seeker")
	}
}
