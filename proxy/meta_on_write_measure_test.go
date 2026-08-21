package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

// Measures what a parquet reader's FIRST open costs in upstream round trips, which
// is the question meta-on-write and footer warming both claim to answer.
//
// The reader's opening sequence is fixed by the format: read the 8-byte trailer to
// learn the metadata length, then read the metadata region. Everything below counts
// upstream requests across exactly those two reads, varying only what the cache was
// given beforehand.
func TestMeasure_FirstOpenUpstreamRoundTrips(t *testing.T) {
	const (
		blockSize = 4
		bucket    = "b"
		key       = "m.parquet"
		etag      = `"v1"`
	)
	// 10 blocks; metadata spans the last 3 (blocks 7,8,9).
	object := make([]byte, blockSize*10)
	footerLen := int64(blockSize*3 - parquetTrailerSize - 1)
	binary.LittleEndian.PutUint32(object[len(object)-parquetTrailerSize:], uint32(footerLen))
	copy(object[len(object)-4:], parquetMagic)
	total := int64(len(object))
	metaStart := total - parquetTrailerSize - footerLen

	// The two reads a parquet reader actually issues, in order.
	trailerRange := fmt.Sprintf("bytes=%d-%d", total-parquetTrailerSize, total-1)
	metadataRange := fmt.Sprintf("bytes=%d-%d", metaStart, total-1)

	run := func(t *testing.T, label string, prime func(*Service, *cache.Cache, *cache.CachedObjectMeta)) int32 {
		t.Helper()
		mock := newBlockMock(object, etag)
		svc, c := newBlockService(t, mock)
		svc.config.Cache.ParquetOptimization = true

		if prime != nil {
			// Build the entry exactly as the range path does, from a realistic 206's
			// headers -- a hand-made struct is not what the serve path expects to find.
			h := http.Header{}
			h.Set("ETag", etag)
			h.Set("Content-Type", "application/octet-stream")
			h.Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			h.Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", total-1, total))
			prime(svc, c, svc.buildBlockMeta(bucket, key, h, total))
		}

		mock.blockGets.Store(0)
		before := mock.clientForwards()

		for _, rng := range []string{trailerRange, metadataRange} {
			rec := httptest.NewRecorder()
			if err := svc.HandleGetObject(rec, blockGet(bucket, key, rng)); err != nil {
				t.Fatalf("read %s: %v", rng, err)
			}
			if rec.Code != 206 {
				t.Fatalf("read %s: status %d, want 206", rng, rec.Code)
			}
		}
		// Background populates are detached; give them a bounded chance to land so
		// they are counted rather than silently excluded.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if metaCached(c, bucket, key, 10*time.Millisecond) {
				break
			}
		}
		blocks, fwd := mock.blockGets.Load(), mock.clientForwards()-before
		t.Logf("  %-22s client forwards=%d  block fetches=%d  total=%d", label, fwd, blocks, blocks+fwd)
		return blocks + fwd
	}

	// (a) Today: nothing cached. The first read must establish metadata upstream.
	cold := run(t, "cold", nil)

	// (b) Meta-on-write: metadata present, every block absent.
	metaOnly := run(t, "meta-on-write", func(svc *Service, c *cache.Cache, meta *cache.CachedObjectMeta) {
		mustPrimeMeta(t, c, bucket, key, meta)
	})

	// (c) Meta-on-write + footer warm: metadata present and the footer blocks cached.
	metaAndFooter := run(t, "meta+footer", func(svc *Service, c *cache.Cache, meta *cache.CachedObjectMeta) {
		for _, idx := range parquetFooterBlocks(meta, footerLen, false) {
			start, end := blockBounds(idx, blockSize, total)
			if err := c.PutBlock(context.Background(), bucket, key, etag, blockSize, idx, object[start:end+1], 3600); err != nil {
				t.Fatalf("prime block %d: %v", idx, err)
			}
		}
		mustPrimeMeta(t, c, bucket, key, meta)
	})

	t.Logf("first-open upstream round trips: cold=%d  meta-on-write=%d  meta+footer-warm=%d", cold, metaOnly, metaAndFooter)

	if metaAndFooter != 0 {
		t.Errorf("meta+footer-warm should serve the first open entirely from cache, got %d upstream round trips", metaAndFooter)
	}
	if metaOnly >= cold {
		t.Errorf("meta-on-write did not reduce first-open round trips: cold=%d meta-on-write=%d", cold, metaOnly)
	}
	if metaAndFooter >= metaOnly {
		t.Errorf("footer warming added nothing over meta-on-write: meta-on-write=%d meta+footer=%d", metaOnly, metaAndFooter)
	}
}

// mustPrimeMeta writes the entry and asserts it landed. PutMetaTombstoneAware
// reports refusal through its bool, not an error, so discarding that return makes a
// silently empty cache look like a primed one -- which is exactly how the first run
// of this measurement produced "no improvement".
func mustPrimeMeta(t *testing.T, c *cache.Cache, bucket, key string, meta *cache.CachedObjectMeta) {
	t.Helper()
	wrote, err := c.PutMetaTombstoneAware(context.Background(), bucket, key, meta, 3600, time.Now().UnixNano())
	if err != nil || !wrote {
		t.Fatalf("prime meta: wrote=%v err=%v", wrote, err)
	}
	if _, found, _ := c.GetMeta(context.Background(), bucket, key); !found {
		t.Fatal("prime meta: entry not readable after write")
	}
}

// S3 returns the completed object's ETag in the CompleteMultipartUpload XML body,
// not reliably as a header. Reading only the header would leave the ETag empty and
// silently disable meta-on-write on multipart -- the one path it exists for.
func TestCompletedMultipartETag(t *testing.T) {
	const body = `<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Location>https://example/b/k</Location>
  <Bucket>b</Bucket>
  <Key>k</Key>
  <ETag>"abc-3"</ETag>
</CompleteMultipartUploadResult>`

	t.Run("reads the ETag from the XML body", func(t *testing.T) {
		got := completedMultipartETag(&ResponseCapture{Headers: http.Header{}, Body: []byte(body)})
		if got != `"abc-3"` {
			t.Fatalf("etag = %q, want \"abc-3\"", got)
		}
	})

	t.Run("prefers the header when present", func(t *testing.T) {
		h := http.Header{}
		h.Set("ETag", `"from-header"`)
		if got := completedMultipartETag(&ResponseCapture{Headers: h, Body: []byte(body)}); got != `"from-header"` {
			t.Fatalf("etag = %q, want the header value", got)
		}
	})

	t.Run("returns empty rather than guessing", func(t *testing.T) {
		for _, b := range [][]byte{nil, []byte("not xml"), []byte(`<Error><Code>NoSuchUpload</Code></Error>`)} {
			if got := completedMultipartETag(&ResponseCapture{Headers: http.Header{}, Body: b}); got != "" {
				t.Fatalf("etag = %q for body %q, want empty", got, b)
			}
		}
	})
}

// headForwarder answers the authoritative HEAD that meta-on-write uses, and counts
// how often it was asked -- a trigger that stays silent must not reach upstream.
type headForwarder struct {
	mockForwarder
	etag   string
	size   int64
	extra  map[string]string
	status int

	mu    sync.Mutex
	heads int
}

func (f *headForwarder) DoConditionalHeadRequest(_ context.Context, _, _, _, _, _ string, _ int64) (*http.Response, error) {
	f.mu.Lock()
	f.heads++
	f.mu.Unlock()
	h := http.Header{}
	h.Set("ETag", f.etag)
	h.Set("Content-Length", fmt.Sprint(f.size))
	for k, v := range f.extra {
		h.Set(k, v)
	}
	status := f.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{StatusCode: status, Header: h, ContentLength: f.size, Body: io.NopCloser(bytes.NewReader(nil))}, nil
}

func (f *headForwarder) headCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.heads
}

func TestCacheBlockMetaOnWrite(t *testing.T) {
	const (
		bucket = "b"
		key    = "o.bin"
		etag   = `"v1"`
		size   = int64(64)
	)
	newSvc := func(t *testing.T, fwd *headForwarder) (*Service, *cache.Cache) {
		t.Helper()
		cfg := config.NewDefault()
		cfg.Cache.SetBlockCachingEnabled(true)
		cfg.Cache.BlockSize = 4
		cfg.Cache.SizeThreshold = 1 << 20
		cfg.Cache.MetaOnWrite = true
		c := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
		return NewService(fwd, c, cfg), c
	}
	req := func() *http.Request { return fullGet(bucket, key) }

	// Established synchronously so the assertion does not race the goroutine.
	t.Run("establishes a block-mode entry for the written version", func(t *testing.T) {
		fwd := &headForwarder{etag: etag, size: size}
		svc, c := newSvc(t, fwd)
		svc.establishBlockMetaFromHead(bucket, key, "ak", "sk", etag)

		meta, found, _ := c.GetMeta(context.Background(), bucket, key)
		if !found {
			t.Fatal("no entry established")
		}
		if meta.BlockSize != 4 || meta.ContentLength != size || meta.ETag != etag {
			t.Fatalf("meta = %+v, want blockSize=4 len=%d etag=%s", meta, size, etag)
		}
		if meta.BlocksComplete {
			t.Error("BlocksComplete set, but no block was written - a full serve would stream optimistically over absent blocks")
		}
	})

	t.Run("refuses a version other than the one written", func(t *testing.T) {
		fwd := &headForwarder{etag: `"someone-elses"`, size: size}
		svc, c := newSvc(t, fwd)
		svc.establishBlockMetaFromHead(bucket, key, "ak", "sk", etag)
		if _, found, _ := c.GetMeta(context.Background(), bucket, key); found {
			t.Fatal("cached meta for an object a concurrent write had replaced")
		}
	})

	t.Run("refuses an object the client marked uncacheable", func(t *testing.T) {
		fwd := &headForwarder{etag: etag, size: size, extra: map[string]string{"Cache-Control": "no-store"}}
		svc, c := newSvc(t, fwd)
		svc.establishBlockMetaFromHead(bucket, key, "ak", "sk", etag)
		if _, found, _ := c.GetMeta(context.Background(), bucket, key); found {
			t.Fatal("cached an object marked no-store")
		}
	})

	t.Run("refuses a sub-block object", func(t *testing.T) {
		fwd := &headForwarder{etag: etag, size: 2} // smaller than block_size
		svc, c := newSvc(t, fwd)
		svc.establishBlockMetaFromHead(bucket, key, "ak", "sk", etag)
		if _, found, _ := c.GetMeta(context.Background(), bucket, key); found {
			t.Fatal("established a block-mode entry for an object that is whole-cached")
		}
	})

	// The trigger must not reach upstream at all when it is off or has no version.
	t.Run("stays off the wire when disabled or unversioned", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			enabled bool
			etag    string
		}{
			{"disabled", false, etag},
			{"no etag from the write", true, ""},
		} {
			t.Run(tc.name, func(t *testing.T) {
				fwd := &headForwarder{etag: etag, size: size}
				svc, _ := newSvc(t, fwd)
				svc.config.Cache.MetaOnWrite = tc.enabled
				svc.cacheBlockMetaOnWrite(req(), bucket, key, tc.etag)
				if got := fwd.headCount(); got != 0 {
					t.Fatalf("issued %d HEADs, want none", got)
				}
			})
		}
	})
}

// A metadata-only entry must survive a full-object GET. The full-GET path bails when
// most blocks are absent and invalidates the entry, which is right when it holds
// cached blocks that could serve stale bytes — but an entry with NO blocks has no
// such hazard, and wiping it would make the full GET destroy exactly what
// meta-on-write (and a footer warm before its first read) just established.
func TestFullGet_PreservesMetadataOnlyEntry(t *testing.T) {
	const (
		blockSize = 4
		bucket    = "b"
		key       = "o.bin"
		etag      = `"v1"`
	)
	object := make([]byte, blockSize*10)
	for i := range object {
		object[i] = byte('a' + i%26)
	}

	mock := newBlockMock(object, etag)
	svc, c := newBlockService(t, mock)

	h := http.Header{}
	h.Set("ETag", etag)
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
	mustPrimeMeta(t, c, bucket, key, svc.buildBlockMeta(bucket, key, h, int64(len(object))))

	rec := httptest.NewRecorder()
	if err := svc.HandleGetObject(rec, fullGet(bucket, key)); err != nil {
		t.Fatalf("full GET: %v", err)
	}
	if rec.Code != 200 || rec.Body.Len() != len(object) {
		t.Fatalf("full GET served status=%d len=%d, want 200 and %d bytes", rec.Code, rec.Body.Len(), len(object))
	}

	if _, found, _ := c.GetMeta(context.Background(), bucket, key); !found {
		t.Fatal("full GET destroyed the metadata-only entry that later range reads were meant to reuse")
	}
}
