package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tigrisdata/tag/cache"
)

// metaCached polls for up to d, returning true as soon as the key's metadata is
// visible. Populates finish on a background goroutine, so tests poll rather than
// sleep; a negative assertion must exhaust d to be meaningful.
func metaCached(c *cache.Cache, bucket, key string, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for {
		if _, found, _ := c.GetMeta(context.Background(), bucket, key); found {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestStreamFromUpstream_TombstoneDuringFetchBlocksPopulate covers issue #97: the
// cache-write timestamp must be stamped BEFORE the upstream request, not after its
// headers arrive.
//
// Upstream takes its read snapshot some time after we send. If an invalidation
// (a racing PUT/DELETE) lands during that round-trip, a timestamp stamped after the
// response is NEWER than the tombstone, the guard `tombTs >= writeStartTime` is
// false, and the pre-invalidation body we just read gets cached — stale. Stamping
// before the request makes our timestamp strictly earlier than the read snapshot,
// so any racing invalidation is provably newer and blocks the write.
// A fenced invalidation landing while the foreground fetch streams must block
// the populate: its decision-time token predates the fence, and the commit
// loses atomically — the guarantee the timestamp tombstones used to provide,
// now enforced by the store (ocache #267).
func TestStreamFromUpstream_FencedDeleteDuringFetchBlocksPopulate(t *testing.T) {
	body := "pre-invalidation bytes"
	var c *cache.Cache
	mock := &mockForwarder{}
	mock.doRequestFunc = func(ctx context.Context, r *http.Request, accessKey, secretKey string) (*http.Response, error) {
		if err := c.DeleteWithMeta(context.Background(), "race-bucket", "race-key"); err != nil {
			t.Errorf("fenced invalidation: %v", err)
		}
		return cacheableGetResponse(body, `"race-etag"`), nil
	}
	var svc *Service
	svc, c = newTestService(mock, true)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/race-bucket/race-key", nil)
	if err := svc.HandleGetObject(w, r); err != nil {
		t.Fatalf("HandleGetObject: %v", err)
	}
	if w.Body.String() != body {
		t.Fatalf("client body = %q, want upstream body", w.Body.String())
	}
	if metaCached(c, "race-bucket", "race-key", 2*time.Second) {
		t.Fatal("populate holding a pre-fence token committed over the fenced invalidation")
	}
}
