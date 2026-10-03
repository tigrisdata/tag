package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

const (
	metaRaceBucket  = "meta-race-bucket"
	metaRaceKey     = "meta-race-key"
	metaRaceOldETag = `"v1"`
	metaRaceNewETag = `"v2"`
)

var errTombstoneReadInjected = errors.New("injected tombstone read failure")

type tombstoneReadFaultClient struct {
	cacheclient.CacheClient
	markerKey string
	armed     atomic.Bool
	failed    chan struct{}
}

func (c *tombstoneReadFaultClient) Get(ctx context.Context, key string) ([]byte, error) {
	if key == c.markerKey && c.armed.CompareAndSwap(true, false) {
		close(c.failed)
		return nil, errTombstoneReadInjected
	}
	return c.CacheClient.Get(ctx, key)
}

type multipartTombstoneRaceForwarder struct {
	mockForwarder

	mu               sync.Mutex
	currentETag      string
	headCalls        int
	firstHeadReady   chan string
	releaseFirstHead <-chan struct{}
}

func (f *multipartTombstoneRaceForwarder) ForwardWithCapture(_ context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
	etag := metaRaceOldETag
	if r.URL.Query().Get("uploadId") == "second" {
		etag = metaRaceNewETag
	}
	body := []byte(fmt.Sprintf(`<CompleteMultipartUploadResult><ETag>%s</ETag></CompleteMultipartUploadResult>`, etag))

	f.mu.Lock()
	f.currentETag = etag
	f.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(body); err != nil {
		return nil, err
	}
	return &ResponseCapture{
		StatusCode: http.StatusOK,
		Headers:    http.Header{},
		Body:       body,
		Complete:   true,
	}, nil
}

func (f *multipartTombstoneRaceForwarder) DoConditionalHeadRequest(_ context.Context, _, _, _, _, _ string, _ int64) (*http.Response, error) {
	f.mu.Lock()
	f.headCalls++
	call := f.headCalls
	etag := f.currentETag
	f.mu.Unlock()

	resp := multipartHeadResponse(etag)
	if call == 1 {
		f.firstHeadReady <- etag
		<-f.releaseFirstHead
	}
	return resp, nil
}

func (f *multipartTombstoneRaceForwarder) Forward(_ context.Context, w http.ResponseWriter, _ *http.Request) error {
	f.mu.Lock()
	etag := f.currentETag
	f.mu.Unlock()

	w.Header().Set("ETag", etag)
	w.Header().Set("Content-Length", "8")
	w.WriteHeader(http.StatusOK)
	return nil
}

func (f *multipartTombstoneRaceForwarder) DoRequestWithCreds(_ context.Context, r *http.Request, _, _ string) (*http.Response, error) {
	f.mu.Lock()
	etag := f.currentETag
	f.mu.Unlock()

	if r.Header.Get("If-None-Match") == etag {
		header := make(http.Header)
		header.Set("ETag", etag)
		return &http.Response{
			StatusCode: http.StatusNotModified,
			Header:     header,
			Body:       io.NopCloser(bytes.NewReader(nil)),
		}, nil
	}

	body := []byte("new-body")
	header := make(http.Header)
	header.Set("ETag", etag)
	header.Set("Content-Length", fmt.Sprint(len(body)))
	header.Set("Content-Type", "application/octet-stream")
	return &http.Response{
		StatusCode:    http.StatusOK,
		Header:        header,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}, nil
}

func multipartHeadResponse(etag string) *http.Response {
	header := make(http.Header)
	header.Set("ETag", etag)
	header.Set("Content-Length", "8")
	header.Set("Content-Type", "application/octet-stream")
	header.Set("Last-Modified", time.Unix(1_700_000_000, 0).UTC().Format(http.TimeFormat))
	return &http.Response{
		StatusCode:    http.StatusOK,
		Header:        header,
		Body:          io.NopCloser(bytes.NewReader(nil)),
		ContentLength: 8,
	}
}

func completeMultipartRaceRequest(uploadID string) *http.Request {
	return httptest.NewRequest(http.MethodPost, "/"+metaRaceBucket+"/"+metaRaceKey+"?uploadId="+uploadID, nil)
}

// TestCompleteMultipart_TombstoneReadFailureDoesNotResurrectOldMetadata holds the
// first write's metadata HEAD after it has observed the old ETag, completes a newer
// upload on the same key, then fails only the first worker's tombstone read at commit.
func TestCompleteMultipart_TombstoneReadFailureDoesNotResurrectOldMetadata(t *testing.T) {
	cfg := config.NewDefault()
	cfg.Cache.SetLegacyCoordination(true)
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 4
	cfg.Cache.SizeThreshold = 1 << 20
	cfg.Cache.MetaOnWrite = true
	cfg.Cache.WarmOnWrite = false
	cfg.Cache.ParquetOptimization = false

	faultyClient := &tombstoneReadFaultClient{
		CacheClient: cacheclient.NewMemoryCache(),
		markerKey:   cache.MakeTombstoneKey(metaRaceBucket, metaRaceKey),
		failed:      make(chan struct{}),
	}
	store := cache.NewCacheWithClient(faultyClient, &cfg.Cache)
	releaseFirstHead := make(chan struct{})
	var releaseOnce sync.Once
	releaseHead := func() { releaseOnce.Do(func() { close(releaseFirstHead) }) }
	defer releaseHead()

	forwarder := &multipartTombstoneRaceForwarder{
		firstHeadReady:   make(chan string, 1),
		releaseFirstHead: releaseFirstHead,
	}
	svc := NewService(forwarder, store, cfg)

	firstResponse := httptest.NewRecorder()
	if err := svc.HandleCompleteMultipartUpload(firstResponse, completeMultipartRaceRequest("first")); err != nil {
		t.Fatalf("first completion: %v", err)
	}
	if firstResponse.Code != http.StatusOK || firstResponse.Body.String() != `<CompleteMultipartUploadResult><ETag>`+metaRaceOldETag+`</ETag></CompleteMultipartUploadResult>` {
		t.Fatalf("first completion response = %d %q, want successful v1 XML", firstResponse.Code, firstResponse.Body.String())
	}

	select {
	case etag := <-forwarder.firstHeadReady:
		if etag != metaRaceOldETag {
			t.Fatalf("first HEAD observed ETag %q, want %q", etag, metaRaceOldETag)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first meta-on-write HEAD did not start")
	}

	secondResponse := httptest.NewRecorder()
	if err := svc.HandleCompleteMultipartUpload(secondResponse, completeMultipartRaceRequest("second")); err != nil {
		t.Fatalf("second completion: %v", err)
	}
	if secondResponse.Code != http.StatusOK || secondResponse.Body.String() != `<CompleteMultipartUploadResult><ETag>`+metaRaceNewETag+`</ETag></CompleteMultipartUploadResult>` {
		t.Fatalf("second completion response = %d %q, want successful v2 XML", secondResponse.Code, secondResponse.Body.String())
	}

	const dedupKey = "meta:" + metaRaceBucket + "/" + metaRaceKey
	if _, active := svc.activeBackgroundFetches.Load(dedupKey); !active {
		t.Fatal("first meta warm left its per-key marker while its HEAD was held")
	}
	forwarder.mu.Lock()
	headCalls := forwarder.headCalls
	currentETag := forwarder.currentETag
	forwarder.mu.Unlock()
	if headCalls != 1 || currentETag != metaRaceNewETag {
		t.Fatalf("after second completion, HEAD calls=%d current ETag=%q; want one held old HEAD and current v2", headCalls, currentETag)
	}

	faultyClient.armed.Store(true)
	releaseHead()
	select {
	case <-faultyClient.failed:
	case <-time.After(3 * time.Second):
		t.Fatal("the first worker did not read the tombstone after its HEAD was released")
	}
	waitForBackgroundMetaWarm(t, svc, dedupKey)

	meta, found, err := store.GetMeta(context.Background(), metaRaceBucket, metaRaceKey)
	if err != nil {
		t.Fatalf("read metadata after both completions: %v", err)
	}
	oldMetaVisible := found && meta != nil && meta.ETag == metaRaceOldETag

	headRecorder := httptest.NewRecorder()
	headRequest := httptest.NewRequest(http.MethodHead, "/"+metaRaceBucket+"/"+metaRaceKey, nil)
	if err := svc.HandleHeadObject(headRecorder, headRequest); err != nil {
		t.Fatalf("HEAD after both completions: %v", err)
	}
	oldMetaFromHead := headRecorder.Header().Get("ETag") == metaRaceOldETag

	getRecorder := httptest.NewRecorder()
	getRequest := httptest.NewRequest(http.MethodGet, "/"+metaRaceBucket+"/"+metaRaceKey, nil)
	getRequest.Header.Set("If-None-Match", metaRaceOldETag)
	if err := svc.HandleGetObject(getRecorder, getRequest); err != nil {
		t.Fatalf("conditional GET after both completions: %v", err)
	}
	oldMetaFromGet := getRecorder.Code == http.StatusNotModified || getRecorder.Header().Get("ETag") == metaRaceOldETag

	if oldMetaVisible || oldMetaFromHead || oldMetaFromGet {
		t.Errorf("older multipart metadata remained visible after a newer completion: stored=%v HEAD=(%d,%q) conditional GET=(%d,%q)",
			oldMetaVisible, headRecorder.Code, headRecorder.Header().Get("ETag"), getRecorder.Code, getRecorder.Header().Get("ETag"))
	}
}

func waitForBackgroundMetaWarm(t *testing.T, svc *Service, key string) {
	t.Helper()
	timeout := time.After(3 * time.Second)
	for {
		if _, active := svc.activeBackgroundFetches.Load(key); !active {
			return
		}
		select {
		case <-timeout:
			t.Fatal("first meta-on-write worker did not finish after its tombstone read")
		case <-time.After(time.Millisecond):
		}
	}
}
