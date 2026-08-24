package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// decodedMetaTestClient records metadata reads while delegating storage behavior
// to MemoryCache. Its mode can model a routed cluster without starting servers.
type decodedMetaTestClient struct {
	cacheclient.CacheClient

	mu           sync.Mutex
	metaGets     map[string]int
	failMetaGets error
	failMetaPuts bool
	mode         cacheclient.ConnectionMode
}

func newDecodedMetaTestClient(mode cacheclient.ConnectionMode) *decodedMetaTestClient {
	return newDecodedMetaTestClientWithStore(cacheclient.NewMemoryCache(), mode)
}

func newDecodedMetaTestClientWithStore(store cacheclient.CacheClient, mode cacheclient.ConnectionMode) *decodedMetaTestClient {
	return &decodedMetaTestClient{
		CacheClient: store,
		metaGets:    make(map[string]int),
		mode:        mode,
	}
}

func (c *decodedMetaTestClient) Get(ctx context.Context, key string) ([]byte, error) {
	var err error
	if len(key) >= len(metaKeyPrefix) && key[:len(metaKeyPrefix)] == metaKeyPrefix {
		c.mu.Lock()
		c.metaGets[key]++
		err = c.failMetaGets
		c.mu.Unlock()
	}
	if err != nil {
		return nil, err
	}
	return c.CacheClient.Get(ctx, key)
}

func (c *decodedMetaTestClient) Put(ctx context.Context, key string, data []byte, ttlSeconds int64) error {
	c.mu.Lock()
	fail := c.failMetaPuts && len(key) >= len(metaKeyPrefix) && key[:len(metaKeyPrefix)] == metaKeyPrefix
	c.mu.Unlock()
	if fail {
		return errors.New("metadata write failed")
	}
	return c.CacheClient.Put(ctx, key, data, ttlSeconds)
}

func (c *decodedMetaTestClient) GetMode() cacheclient.ConnectionMode {
	if c.mode != "" {
		return c.mode
	}
	return c.CacheClient.GetMode()
}

func (c *decodedMetaTestClient) getCount(bucket, key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.metaGets[MakeMetaKey(bucket, key)]
}

func (c *decodedMetaTestClient) setFailMetaGets(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failMetaGets = err
}

func (c *decodedMetaTestClient) setFailMetaPuts(fail bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failMetaPuts = fail
}

func newDecodedMetaTestCache(t *testing.T, client cacheclient.CacheClient) *Cache {
	t.Helper()
	cfg := config.NewDefault()
	// The deployed configuration has a node ID. Byte validation makes the tier
	// safe in that routed configuration too.
	cfg.Cache.NodeID = "configured-node"
	c := NewCacheWithClient(client, &cfg.Cache)
	if c.decodedMeta == nil {
		t.Fatal("cache did not create decoded metadata tier")
	}
	return c
}

func testDecodedMeta(bucket, key, etag string) *CachedObjectMeta {
	return &CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          etag,
		ContentType:   "application/octet-stream",
		ContentLength: 4,
		StatusCode:    200,
		UserMetadata: map[string]string{
			"x-amz-meta-color": "blue",
		},
	}
}

func mustGetDecodedMeta(t *testing.T, c *Cache, bucket, key string) *CachedObjectMeta {
	t.Helper()
	meta, found, err := c.GetMeta(context.Background(), bucket, key)
	if err != nil {
		t.Fatalf("GetMeta: %v", err)
	}
	if !found {
		t.Fatal("GetMeta did not find cached metadata")
	}
	return meta
}

func admitDecodedMeta(t *testing.T, c *Cache, bucket, key string) {
	t.Helper()
	for range decodedMetaAdmissionThreshold {
		mustGetDecodedMeta(t, c, bucket, key)
	}
	if _, ok := c.decodedMeta.Peek(MakeMetaKey(bucket, key)); !ok {
		t.Fatal("repeated reads did not retain a decoded snapshot")
	}
}

// replaceDecodedSnapshot changes only the test-visible decoded half of a tier
// entry. Matching backend bytes remain untouched, so a later read can prove it
// used the resident decode instead of calling DecodeMeta again.
func replaceDecodedSnapshot(t *testing.T, c *Cache, bucket, key, etag string) {
	t.Helper()
	metaKey := MakeMetaKey(bucket, key)
	entry, ok := c.decodedMeta.Get(metaKey)
	if !ok {
		t.Fatal("decoded metadata entry was not present")
	}
	entry.meta = cloneCachedObjectMeta(entry.meta)
	entry.meta.ETag = etag
	c.decodedMeta.Add(metaKey, entry)
}

func TestDecodedMetaKeyHashUsesFullDirectMappedSlots(t *testing.T) {
	admissionSlots := make(map[uint64]struct{}, decodedMetaAdmissionSlots)
	residentSlots := make(map[uint64]struct{}, decodedMetaResidentSlots)
	for index := 0; index < decodedMetaResidentSlots*8; index++ {
		hash := decodedMetaKeyHash(fmt.Sprintf("meta|bucket|slot-%x", index))
		if hash == 0 {
			t.Fatal("metadata hash used the empty resident-slot sentinel")
		}
		admissionSlots[hash&(decodedMetaAdmissionSlots-1)] = struct{}{}
		residentSlots[hash&(decodedMetaResidentSlots-1)] = struct{}{}
	}
	if got := len(admissionSlots); got != decodedMetaAdmissionSlots {
		t.Fatalf("admission slots reached = %d, want %d", got, decodedMetaAdmissionSlots)
	}
	if got := len(residentSlots); got != decodedMetaResidentSlots {
		t.Fatalf("resident slots reached = %d, want %d", got, decodedMetaResidentSlots)
	}
}

func TestDecodedMetaTierValidatesBytesAndReturnsSnapshots(t *testing.T) {
	client := newDecodedMetaTestClient(cacheclient.ModeCluster)
	c := newDecodedMetaTestCache(t, client)
	const bucket, key = "bucket", "key"

	meta := testDecodedMeta(bucket, key, `"v1"`)
	if err := c.PutWithMeta(context.Background(), bucket, key, meta, []byte("body"), 60); err != nil {
		t.Fatalf("PutWithMeta: %v", err)
	}

	// Writes retain only encoded backend data, so caller mutation cannot change
	// cached headers and a one-off read does not allocate a resident snapshot.
	meta.UserMetadata["x-amz-meta-color"] = "caller-mutated"
	first := mustGetDecodedMeta(t, c, bucket, key)
	if got := first.UserMetadata["x-amz-meta-color"]; got != "blue" {
		t.Fatalf("metadata after caller mutation = %q, want blue", got)
	}
	if _, ok := c.decodedMeta.Get(MakeMetaKey(bucket, key)); ok {
		t.Fatal("metadata write or first read retained a decoded snapshot")
	}
	if got := client.getCount(bucket, key); got != 1 {
		t.Fatalf("metadata backend reads = %d, want 1 byte validation", got)
	}

	// A second clustered read still avoids a snapshot allocation. The third
	// confirms this key is hot enough to retain; returned public values remain clones.
	_ = mustGetDecodedMeta(t, c, bucket, key)
	if _, ok := c.decodedMeta.Peek(MakeMetaKey(bucket, key)); ok {
		t.Fatal("two-hit burst retained a decoded snapshot")
	}
	_ = mustGetDecodedMeta(t, c, bucket, key)
	first.UserMetadata["x-amz-meta-color"] = "returned-mutated"
	replaceDecodedSnapshot(t, c, bucket, key, `"resident"`)
	fourth := mustGetDecodedMeta(t, c, bucket, key)
	if fourth == first {
		t.Fatal("GetMeta returned the resident metadata pointer directly")
	}
	if got := fourth.ETag; got != `"resident"` {
		t.Fatalf("matching bytes decoded again: ETag = %q, want resident snapshot", got)
	}
	if got := fourth.UserMetadata["x-amz-meta-color"]; got != "blue" {
		t.Fatalf("later metadata read = %q, want immutable snapshot", got)
	}
	if got := client.getCount(bucket, key); got != 4 {
		t.Fatalf("metadata backend reads = %d, want 4 byte validations", got)
	}

	headersMeta, found, err := c.GetMetaForHeaders(context.Background(), bucket, key)
	if err != nil || !found {
		t.Fatalf("GetMetaForHeaders = (found=%t, err=%v), want (true, nil)", found, err)
	}
	entry, ok := c.decodedMeta.Peek(MakeMetaKey(bucket, key))
	if !ok {
		t.Fatal("decoded metadata entry disappeared")
	}
	if headersMeta != entry.meta {
		t.Fatal("GetMetaForHeaders copied an immutable resident snapshot")
	}

	const headersKey = "headers-owned"
	if err := c.PutWithMeta(context.Background(), bucket, headersKey, testDecodedMeta(bucket, headersKey, `"headers"`), []byte("body"), 60); err != nil {
		t.Fatalf("PutWithMeta(%q): %v", headersKey, err)
	}
	for read := 1; read <= decodedMetaAdmissionThreshold; read++ {
		headersMeta, found, err = c.GetMetaForHeaders(context.Background(), bucket, headersKey)
		if err != nil || !found {
			t.Fatalf("GetMetaForHeaders read %d = (found=%t, err=%v), want (true, nil)", read, found, err)
		}
	}
	headersEntry, ok := c.decodedMeta.Peek(MakeMetaKey(bucket, headersKey))
	if !ok {
		t.Fatal("header-only admission did not retain a decoded snapshot")
	}
	if headersMeta != headersEntry.meta {
		t.Fatal("header-only admission copied the freshly decoded snapshot")
	}
}

func TestDecodedMetaTierSkipsOversizedEntries(t *testing.T) {
	client := newDecodedMetaTestClient(cacheclient.ModeCluster)
	c := newDecodedMetaTestCache(t, client)
	const bucket, key = "bucket", "oversized"
	meta := testDecodedMeta(bucket, key, `"large"`)
	meta.UserMetadata["x-amz-meta-large"] = strings.Repeat("x", maxDecodedMetaEntryBytes)
	if err := c.PutWithMeta(context.Background(), bucket, key, meta, []byte("body"), 60); err != nil {
		t.Fatalf("PutWithMeta: %v", err)
	}
	if _, ok := c.decodedMeta.Get(MakeMetaKey(bucket, key)); ok {
		t.Fatalf("metadata larger than %d bytes was retained", maxDecodedMetaEntryBytes)
	}
	if got := mustGetDecodedMeta(t, c, bucket, key).ETag; got != `"large"` {
		t.Fatalf("oversized metadata ETag = %s, want large", got)
	}
	if _, ok := c.decodedMeta.Get(MakeMetaKey(bucket, key)); ok {
		t.Fatal("backend read retained oversized metadata")
	}
}

func TestDecodedMetaTierAdmitsAtCapEntries(t *testing.T) {
	client := newDecodedMetaTestClient(cacheclient.ModeCluster)
	c := newDecodedMetaTestCache(t, client)
	const bucket, key = "bucket", "at-cap"
	const encodedBytes = maxDecodedMetaEntryBytes

	meta := testDecodedMeta(bucket, key, `"at-cap"`)
	const metadataKey = "x-amz-meta-at-cap"
	meta.UserMetadata = map[string]string{metadataKey: ""}
	encoded, err := meta.Encode()
	if err != nil {
		t.Fatalf("encode at-cap metadata: %v", err)
	}
	valueLen := encodedBytes - len(encoded)
	if valueLen < 0 {
		t.Fatalf("at-cap metadata base length = %d, exceeds target %d", len(encoded), encodedBytes)
	}
	meta.UserMetadata[metadataKey] = strings.Repeat("x", valueLen)
	encoded, err = meta.Encode()
	if err != nil {
		t.Fatalf("encode filled at-cap metadata: %v", err)
	}
	if len(encoded) != encodedBytes {
		t.Fatalf("at-cap metadata length = %d, want %d", len(encoded), encodedBytes)
	}
	if err := c.PutWithMeta(context.Background(), bucket, key, meta, []byte("body"), 60); err != nil {
		t.Fatalf("PutWithMeta: %v", err)
	}
	admitDecodedMeta(t, c, bucket, key)
	entry, ok := c.decodedMeta.Peek(MakeMetaKey(bucket, key))
	if !ok {
		t.Fatal("at-cap metadata was not retained")
	}
	if len(entry.encoded) != encodedBytes {
		t.Fatalf("retained encoded metadata length = %d, want %d", len(entry.encoded), encodedBytes)
	}
}

func TestDecodedMetaTierRefillsAfterRestartAndEviction(t *testing.T) {
	shared := cacheclient.NewMemoryCache()
	writerClient := newDecodedMetaTestClientWithStore(shared, cacheclient.ModeCluster)
	writer := newDecodedMetaTestCache(t, writerClient)
	const bucket, key = "bucket", "restart"
	if err := writer.PutWithMeta(context.Background(), bucket, key, testDecodedMeta(bucket, key, `"v1"`), []byte("body"), 60); err != nil {
		t.Fatalf("PutWithMeta: %v", err)
	}

	readerClient := newDecodedMetaTestClientWithStore(shared, cacheclient.ModeCluster)
	reader := newDecodedMetaTestCache(t, readerClient)
	if got := mustGetDecodedMeta(t, reader, bucket, key).ETag; got != `"v1"` {
		t.Fatalf("metadata after restart = %s, want v1", got)
	}
	if _, ok := reader.decodedMeta.Get(MakeMetaKey(bucket, key)); ok {
		t.Fatal("first read after restart retained a one-off decoded snapshot")
	}
	if got := mustGetDecodedMeta(t, reader, bucket, key).ETag; got != `"v1"` {
		t.Fatalf("second metadata after restart = %s, want v1", got)
	}
	if _, ok := reader.decodedMeta.Peek(MakeMetaKey(bucket, key)); ok {
		t.Fatal("two-hit restart read retained a decoded snapshot")
	}
	if got := mustGetDecodedMeta(t, reader, bucket, key).ETag; got != `"v1"` {
		t.Fatalf("third metadata after restart = %s, want v1", got)
	}
	replaceDecodedSnapshot(t, reader, bucket, key, `"resident-after-restart"`)
	if got := mustGetDecodedMeta(t, reader, bucket, key).ETag; got != `"resident-after-restart"` {
		t.Fatalf("fourth read after restart decoded again: ETag = %s", got)
	}

	for i := 0; i < maxDecodedMetaEntries; i++ {
		entryKey := fmt.Sprintf("eviction-%d", i)
		reader.putDecodedMeta(
			MakeMetaKey(bucket, entryKey),
			testDecodedMeta(bucket, entryKey, fmt.Sprintf("\"%d\"", i)),
			[]byte(entryKey),
		)
	}
	if _, ok := reader.decodedMeta.Get(MakeMetaKey(bucket, key)); ok {
		t.Fatal("original metadata entry survived a full LRU eviction")
	}
	for read := 1; read <= decodedMetaAdmissionThreshold; read++ {
		if got := mustGetDecodedMeta(t, reader, bucket, key).ETag; got != `"v1"` {
			t.Fatalf("metadata read %d after eviction = %s, want v1", read, got)
		}
	}
	// A stale resident-filter slot may discover the eviction on the first read;
	// the admission threshold refills the tier either way.
	replaceDecodedSnapshot(t, reader, bucket, key, `"resident-after-eviction"`)
	if got := mustGetDecodedMeta(t, reader, bucket, key).ETag; got != `"resident-after-eviction"` {
		t.Fatalf("fourth read after eviction decoded again: ETag = %s", got)
	}
	if got := readerClient.getCount(bucket, key); got != 8 {
		t.Fatalf("metadata backend reads = %d, want 8 byte validations", got)
	}
	if got := reader.decodedMeta.Len(); got > maxDecodedMetaEntries {
		t.Fatalf("decoded metadata entries = %d, want at most %d", got, maxDecodedMetaEntries)
	}
}

func TestDecodedMetaTierAdmissionSkipsTwoHitBursts(t *testing.T) {
	ctx := context.Background()
	shared := cacheclient.NewMemoryCache()
	writer := newDecodedMetaTestCache(t, newDecodedMetaTestClientWithStore(shared, cacheclient.ModeCluster))
	reader := newDecodedMetaTestCache(t, newDecodedMetaTestClientWithStore(shared, cacheclient.ModeCluster))
	const (
		bucket           = "bucket"
		shortBurstKeySet = decodedMetaAdmissionSlots + 1
	)
	keys := make([]string, shortBurstKeySet)

	for i := range keys {
		key := fmt.Sprintf("one-off-%d", i)
		keys[i] = key
		if err := writer.PutWithMeta(ctx, bucket, key, testDecodedMeta(bucket, key, `"v1"`), []byte("body"), 60); err != nil {
			t.Fatalf("PutWithMeta(%q): %v", key, err)
		}
		if got := mustGetDecodedMeta(t, reader, bucket, key).ETag; got != `"v1"` {
			t.Fatalf("first GetMeta(%q) ETag = %s, want v1", key, got)
		}
		if _, ok := reader.decodedMeta.Get(MakeMetaKey(bucket, key)); ok {
			t.Fatalf("one-off read for %q retained a decoded snapshot", key)
		}
		if got := mustGetDecodedMeta(t, reader, bucket, key).ETag; got != `"v1"` {
			t.Fatalf("second GetMeta(%q) ETag = %s, want v1", key, got)
		}
		if _, ok := reader.decodedMeta.Peek(MakeMetaKey(bucket, key)); ok {
			t.Fatalf("two-hit burst for %q retained a decoded snapshot", key)
		}
	}

	const repeatedKey = "repeated"
	if err := writer.PutWithMeta(ctx, bucket, repeatedKey, testDecodedMeta(bucket, repeatedKey, `"v1"`), []byte("body"), 60); err != nil {
		t.Fatalf("PutWithMeta(%q): %v", repeatedKey, err)
	}
	for read := 1; read <= decodedMetaAdmissionThreshold; read++ {
		if got := mustGetDecodedMeta(t, reader, bucket, repeatedKey).ETag; got != `"v1"` {
			t.Fatalf("read %d GetMeta(%q) ETag = %s, want v1", read, repeatedKey, got)
		}
		_, retained := reader.decodedMeta.Peek(MakeMetaKey(bucket, repeatedKey))
		if retained != (read == decodedMetaAdmissionThreshold) {
			t.Fatalf("read %d retained snapshot = %t, want %t", read, retained, read == decodedMetaAdmissionThreshold)
		}
	}
}

func TestDecodedMetaTierWritePathsAndInvalidation(t *testing.T) {
	ctx := context.Background()

	t.Run("metadata writes evict until repeated reads admit", func(t *testing.T) {
		client := newDecodedMetaTestClient(cacheclient.ModeCluster)
		c := newDecodedMetaTestCache(t, client)
		assertNoSnapshot := func(key string) {
			t.Helper()
			if _, ok := c.decodedMeta.Get(MakeMetaKey("bucket", key)); ok {
				t.Fatalf("metadata write for %q retained a decoded snapshot", key)
			}
		}

		if err := c.PutWithMeta(ctx, "bucket", "put", testDecodedMeta("bucket", "put", `"v1"`), []byte("body"), 60); err != nil {
			t.Fatalf("PutWithMeta: %v", err)
		}
		assertNoSnapshot("put")
		if got := mustGetDecodedMeta(t, c, "bucket", "put").ETag; got != `"v1"` {
			t.Fatalf("PutWithMeta ETag = %s, want v1", got)
		}

		streamMeta := testDecodedMeta("bucket", "stream", `"stream"`)
		wrote, err := c.PutWithMetaStreamTombstoneAware(ctx, "bucket", "stream", streamMeta, bytes.NewBufferString("body"), 60, time.Now().UnixNano())
		if err != nil || !wrote {
			t.Fatalf("PutWithMetaStreamTombstoneAware = (%t, %v), want (true, nil)", wrote, err)
		}
		assertNoSnapshot("stream")
		if got := mustGetDecodedMeta(t, c, "bucket", "stream").ETag; got != `"stream"` {
			t.Fatalf("streamed metadata ETag = %s, want stream", got)
		}

		blockMeta := testDecodedMeta("bucket", "block", `"block"`)
		wrote, err = c.PutMetaTombstoneAware(ctx, "bucket", "block", blockMeta, 60, time.Now().UnixNano())
		if err != nil || !wrote {
			t.Fatalf("PutMetaTombstoneAware = (%t, %v), want (true, nil)", wrote, err)
		}
		assertNoSnapshot("block")
		if got := mustGetDecodedMeta(t, c, "bucket", "block").ETag; got != `"block"` {
			t.Fatalf("block metadata ETag = %s, want block", got)
		}
	})

	t.Run("tombstone, delete, and uncertain writes evict", func(t *testing.T) {
		client := newDecodedMetaTestClient(cacheclient.ModeCluster)
		c := newDecodedMetaTestCache(t, client)
		const bucket, key = "bucket", "invalidate"
		if err := c.PutWithMeta(ctx, bucket, key, testDecodedMeta(bucket, key, `"v1"`), []byte("body"), 60); err != nil {
			t.Fatalf("PutWithMeta: %v", err)
		}
		admitDecodedMeta(t, c, bucket, key)
		if err := c.WriteTombstone(ctx, bucket, key); err != nil {
			t.Fatalf("WriteTombstone: %v", err)
		}
		if _, ok := c.decodedMeta.Get(MakeMetaKey(bucket, key)); ok {
			t.Fatal("WriteTombstone left a decoded snapshot resident")
		}
		if got := mustGetDecodedMeta(t, c, bucket, key).ETag; got != `"v1"` {
			t.Fatalf("metadata after tombstone = %s, want v1", got)
		}
		if err := c.DeleteWithMeta(ctx, bucket, key); err != nil {
			t.Fatalf("DeleteWithMeta: %v", err)
		}
		if _, found, err := c.GetMeta(ctx, bucket, key); err != nil || found {
			t.Fatalf("GetMeta after delete = (found=%t, err=%v), want (false, nil)", found, err)
		}

		if err := c.PutWithMeta(ctx, bucket, key, testDecodedMeta(bucket, key, `"v1"`), []byte("body"), 60); err != nil {
			t.Fatalf("PutWithMeta v1: %v", err)
		}
		admitDecodedMeta(t, c, bucket, key)
		client.setFailMetaPuts(true)
		if err := c.PutWithMeta(ctx, bucket, key, testDecodedMeta(bucket, key, `"v2"`), []byte("body"), 60); err == nil {
			t.Fatal("PutWithMeta v2 succeeded despite injected metadata failure")
		}
		client.setFailMetaPuts(false)
		if _, ok := c.decodedMeta.Get(MakeMetaKey(bucket, key)); ok {
			t.Fatal("failed metadata write left a decoded snapshot resident")
		}
	})

	t.Run("tombstone-gated writes evict stale snapshots", func(t *testing.T) {
		client := newDecodedMetaTestClient(cacheclient.ModeCluster)
		c := newDecodedMetaTestCache(t, client)
		const bucket, key = "bucket", "tombstone-gate"
		meta := testDecodedMeta(bucket, key, `"old"`)
		encoded, err := meta.Encode()
		if err != nil {
			t.Fatalf("encode metadata: %v", err)
		}
		if err := c.WriteTombstone(ctx, bucket, key); err != nil {
			t.Fatalf("WriteTombstone: %v", err)
		}

		c.putDecodedMeta(MakeMetaKey(bucket, key), meta, encoded)
		wrote, err := c.PutMetaTombstoneAware(ctx, bucket, key, meta, 60, 0)
		if err != nil || wrote {
			t.Fatalf("PutMetaTombstoneAware = (%t, %v), want (false, nil)", wrote, err)
		}
		if _, ok := c.decodedMeta.Get(MakeMetaKey(bucket, key)); ok {
			t.Fatal("skipped block metadata write left a decoded snapshot resident")
		}

		c.putDecodedMeta(MakeMetaKey(bucket, key), meta, encoded)
		wrote, err = c.PutWithMetaStreamTombstoneAware(ctx, bucket, key, meta, bytes.NewBufferString("body"), 60, 0)
		if err != nil || wrote {
			t.Fatalf("PutWithMetaStreamTombstoneAware = (%t, %v), want (false, nil)", wrote, err)
		}
		if _, ok := c.decodedMeta.Get(MakeMetaKey(bucket, key)); ok {
			t.Fatal("skipped streamed metadata write left a decoded snapshot resident")
		}
	})
}

func TestDecodedMetaTierValidatesRemoteUpdateAndExpiry(t *testing.T) {
	ctx := context.Background()
	shared := cacheclient.NewMemoryCache()
	firstClient := newDecodedMetaTestClientWithStore(shared, cacheclient.ModeCluster)
	secondClient := newDecodedMetaTestClientWithStore(shared, cacheclient.ModeCluster)
	first := newDecodedMetaTestCache(t, firstClient)
	second := newDecodedMetaTestCache(t, secondClient)
	const bucket, key = "bucket", "cluster-key"

	if err := first.PutWithMeta(ctx, bucket, key, testDecodedMeta(bucket, key, `"v1"`), []byte("v1"), 60); err != nil {
		t.Fatalf("first PutWithMeta v1: %v", err)
	}
	admitDecodedMeta(t, first, bucket, key)
	if err := second.PutWithMeta(ctx, bucket, key, testDecodedMeta(bucket, key, `"v2"`), []byte("v2"), 60); err != nil {
		t.Fatalf("second PutWithMeta v2: %v", err)
	}
	for read := 1; read <= decodedMetaAdmissionThreshold; read++ {
		if got := mustGetDecodedMeta(t, first, bucket, key).ETag; got != `"v2"` {
			t.Fatalf("metadata read %d after remote update = %s, want v2", read, got)
		}
	}
	if _, ok := first.decodedMeta.Peek(MakeMetaKey(bucket, key)); !ok {
		t.Fatal("repeated remote-update reads did not retain a decoded snapshot")
	}

	// Delete through the backend to model expiry or an invalidation observed from
	// another process. GetMeta must drop, rather than serve, its old snapshot.
	if err := shared.Delete(ctx, MakeMetaKey(bucket, key)); err != nil {
		t.Fatalf("backend metadata delete: %v", err)
	}
	if _, found, err := first.GetMeta(ctx, bucket, key); err != nil || found {
		t.Fatalf("GetMeta after backend expiry = (found=%t, err=%v), want (false, nil)", found, err)
	}
	if _, ok := first.decodedMeta.Get(MakeMetaKey(bucket, key)); ok {
		t.Fatal("expired backend metadata left a decoded snapshot resident")
	}
}

func TestDecodedMetaTierRetainsSnapshotAcrossTransientGetError(t *testing.T) {
	ctx := context.Background()
	client := newDecodedMetaTestClient(cacheclient.ModeCluster)
	c := newDecodedMetaTestCache(t, client)
	const bucket, key = "bucket", "transient-get"

	if err := c.PutWithMeta(ctx, bucket, key, testDecodedMeta(bucket, key, `"v1"`), []byte("body"), 60); err != nil {
		t.Fatalf("PutWithMeta: %v", err)
	}
	admitDecodedMeta(t, c, bucket, key)
	replaceDecodedSnapshot(t, c, bucket, key, `"resident"`)

	client.setFailMetaGets(context.DeadlineExceeded)
	meta, found, err := c.GetMeta(ctx, bucket, key)
	if !errors.Is(err, context.DeadlineExceeded) || found || meta != nil {
		t.Fatalf("GetMeta during transient failure = (meta=%v, found=%t, err=%v), want (nil, false, DeadlineExceeded)", meta, found, err)
	}
	if _, ok := c.decodedMeta.Peek(MakeMetaKey(bucket, key)); !ok {
		t.Fatal("transient metadata read error evicted the decoded snapshot")
	}

	client.setFailMetaGets(nil)
	if got := mustGetDecodedMeta(t, c, bucket, key).ETag; got != `"resident"` {
		t.Fatalf("metadata after transient read recovery = %s, want retained resident snapshot", got)
	}
}

func TestIsMetaNotFoundError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"grpc not found", status.Error(codes.NotFound, "key not found"), true},
		{"unavailable routing", status.Error(codes.Unavailable, "routing error: node not found in ring"), false},
		{"unavailable forwarding", status.Error(codes.Unavailable, "forwarding error: key not found"), false},
		{"plain missing-key text", errors.New("key not found"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isMetaNotFoundError(tc.err); got != tc.want {
				t.Fatalf("isMetaNotFoundError(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

func TestDecodedMetaTierRetainsSnapshotAcrossRoutingGetError(t *testing.T) {
	for _, tc := range []struct {
		name    string
		message string
	}{
		{"ring lookup", "routing error: node not found in ring"},
		{"forwarded miss", "forwarding error: key not found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			client := newDecodedMetaTestClient(cacheclient.ModeCluster)
			c := newDecodedMetaTestCache(t, client)
			const bucket, key = "bucket", "routing-get"

			if err := c.PutWithMeta(ctx, bucket, key, testDecodedMeta(bucket, key, `"v1"`), []byte("body"), 60); err != nil {
				t.Fatalf("PutWithMeta: %v", err)
			}
			admitDecodedMeta(t, c, bucket, key)
			replaceDecodedSnapshot(t, c, bucket, key, `"resident"`)

			unavailableErr := status.Error(codes.Unavailable, tc.message)
			client.setFailMetaGets(unavailableErr)
			meta, found, err := c.GetMeta(ctx, bucket, key)
			if got := status.Code(err); got != codes.Unavailable || found || meta != nil {
				t.Fatalf("GetMeta during unavailable failure = (meta=%v, found=%t, code=%v), want (nil, false, Unavailable)", meta, found, got)
			}
			if _, ok := c.decodedMeta.Peek(MakeMetaKey(bucket, key)); !ok {
				t.Fatal("unavailable metadata read error evicted the decoded snapshot")
			}

			client.setFailMetaGets(nil)
			if got := mustGetDecodedMeta(t, c, bucket, key).ETag; got != `"resident"` {
				t.Fatalf("metadata after unavailable read recovery = %s, want retained resident snapshot", got)
			}
		})
	}
}

func TestDecodedMetaTierConcurrentAccess(t *testing.T) {
	client := newDecodedMetaTestClient(cacheclient.ModeCluster)
	c := newDecodedMetaTestCache(t, client)
	const bucket, key = "bucket", "concurrent"
	ctx := context.Background()
	if err := c.PutWithMeta(ctx, bucket, key, testDecodedMeta(bucket, key, `"initial"`), []byte("body"), 60); err != nil {
		t.Fatalf("PutWithMeta: %v", err)
	}

	start := make(chan struct{})
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 100; i++ {
				meta, found, err := c.GetMeta(ctx, bucket, key)
				if err != nil {
					errs <- err
					return
				}
				if found {
					meta.UserMetadata["x-amz-meta-color"] = "reader-mutated"
				}
			}
		}()
	}
	for writer := 0; writer < 2; writer++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			<-start
			for i := 0; i < 25; i++ {
				etag := fmt.Sprintf("\"writer-%d-%d\"", writer, i)
				if err := c.PutWithMeta(ctx, bucket, key, testDecodedMeta(bucket, key, etag), []byte("body"), 60); err != nil {
					errs <- err
					return
				}
				if i%5 == 0 {
					if err := c.DeleteWithMeta(ctx, bucket, key); err != nil {
						errs <- err
						return
					}
				}
			}
		}(writer)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent cache operation: %v", err)
	}
}
