package handlers

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tigrisdata/tag/proxy"
)

type handlerParquetHeadGate struct {
	next    http.Handler
	started chan struct{}
	release chan struct{}
	once    sync.Once
	endOnce sync.Once
}

func newHandlerParquetHeadGate(next http.Handler) *handlerParquetHeadGate {
	return &handlerParquetHeadGate{
		next:    next,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (g *handlerParquetHeadGate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodHead {
		g.once.Do(func() { close(g.started) })
		select {
		case <-g.release:
		case <-r.Context().Done():
			return
		}
	}
	g.next.ServeHTTP(w, r)
}

func (g *handlerParquetHeadGate) releaseHead() {
	g.endOnce.Do(func() { close(g.release) })
}

type handlerParquetCompletion struct {
	status int
	body   []byte
	err    error
}

func startHandlerParquetCompletion(replay *handlerParquetReplay, uploadID string) <-chan handlerParquetCompletion {
	completed := make(chan handlerParquetCompletion, 1)
	go func() {
		path := "/" + handlerParquetBucket + "/" + handlerParquetKey + "?uploadId=" + uploadID
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, replay.gateway.URL+path, strings.NewReader(handlerParquetUploadXML))
		if err != nil {
			completed <- handlerParquetCompletion{err: err}
			return
		}
		req.Header.Set("Authorization", handlerParquetAuthHeader)
		req.Header.Set(handlerParquetPhaseHeader, "completion")
		req.Header.Set("Content-Type", "application/xml")
		resp, err := replay.client.Do(req)
		if err != nil {
			completed <- handlerParquetCompletion{err: err}
			return
		}
		body, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if readErr == nil {
			readErr = closeErr
		}
		completed <- handlerParquetCompletion{status: resp.StatusCode, body: body, err: readErr}
	}()
	return completed
}

func TestServer_CompleteMultipartUploadFooterFirstStillWarms(t *testing.T) {
	for _, tc := range []struct {
		name   string
		legacy bool
	}{
		{name: "legacy", legacy: true},
		{name: "cas", legacy: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			origin := newHandlerParquetOrigin(64, 16, 24, 0)
			originHandler := newHandlerParquetHeadGate(origin)
			originHTTP := httptest.NewServer(originHandler)
			replay := newHandlerParquetReplayWithOrigin(t, origin, originHTTP, tc.legacy)
			defer replay.close()
			defer originHandler.releaseHead()

			completed := startHandlerParquetCompletion(replay, "route-footer-first")
			waitHandlerParquetSignal(t, replay.order.firstValidation, "footer credential validation")
			replay.gate.armed.Store(true)
			replay.order.releaseFirst()
			waitHandlerParquetSignal(t, replay.gate.entered, "footer metadata decision")
			waitHandlerParquetSignal(t, replay.order.secondValidation, "metadata HEAD credential validation")
			replay.order.releaseSecond()
			waitHandlerParquetSignal(t, originHandler.started, "metadata HEAD request")

			if _, found, err := replay.cache.GetMeta(handlerParquetPollContext(), handlerParquetBucket, handlerParquetKey); err != nil || found {
				t.Fatalf("metadata appeared before the footer worker's decision: found=%t err=%v", found, err)
			}
			select {
			case result := <-completed:
				want := []byte(`<CompleteMultipartUploadResult><ETag>` + handlerParquetETag + `</ETag></CompleteMultipartUploadResult>`)
				if result.err != nil || result.status != http.StatusOK || !bytes.Equal(result.body, want) {
					t.Fatalf("CompleteMultipartUpload response = %d %q, err=%v; want 200 %q", result.status, result.body, result.err, want)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("completion waited for the gated HEAD")
			}
			if got := replay.forwarder.completionForwards.Load(); got != 1 {
				t.Fatalf("completion forward count = %d, want one nonreplayed completion", got)
			}

			replay.gate.releaseRead()
			pollCtx := handlerParquetPollContext()
			waitHandlerParquetCondition(t, func() bool {
				meta, found, err := replay.cache.GetMeta(pollCtx, handlerParquetBucket, handlerParquetKey)
				if err != nil || !found || meta == nil || meta.ETag != handlerParquetETag || meta.BlocksComplete {
					return false
				}
				for _, idx := range origin.footerBlockIndices(false) {
					if !replay.cache.BlockExists(pollCtx, handlerParquetBucket, handlerParquetKey, handlerParquetETag, origin.blockSize, idx) {
						return false
					}
				}
				return replay.forwarder.inflightCount("write-warm") == 0
			}, "footer-first block publication")

			originHandler.releaseHead()
			warm := replay.finishFooterWarm(t)
			gotRanges := make([]string, 0, len(warm.ranges))
			for _, request := range warm.ranges {
				if request.role == "write-warm" {
					gotRanges = append(gotRanges, request.rangeHeader)
				}
			}
			slices.Sort(gotRanges)
			wantRanges := []string{"bytes=-8", "bytes=32-47", "bytes=48-63"}
			if warm.heads != 1 || warm.byRole["write-warm"] != 3 || warm.rangeGets != 3 || !slices.Equal(gotRanges, wantRanges) {
				t.Fatalf("footer-first warm: HEAD=%d ranges=%d roles=%v ranges=%v; want one HEAD and the suffix plus exact footer blocks", warm.heads, warm.rangeGets, warm.byRole, gotRanges)
			}

			meta, found, err := replay.cache.GetMeta(pollCtx, handlerParquetBucket, handlerParquetKey)
			if err != nil || !found || meta.ETag != handlerParquetETag || meta.BlocksComplete {
				t.Fatalf("footer-first metadata = %+v found=%t err=%v", meta, found, err)
			}
			replay.resetMeasurements()
			replay.observeReadPrefetch()
			responses := replay.firstOpen(t)
			replay.verifyFirstOpen(t, responses)
			for i, response := range responses {
				if got := response.header.Get(proxy.XCacheHeader); got != proxy.XCacheHit {
					t.Fatalf("first-open response %d cache status = %q, want HIT", i, got)
				}
			}
			if got := replay.foregroundGets(); got != 0 || replay.forwarder.foregroundBytes() != 0 {
				t.Fatalf("first open after footer-first warm made %d foreground origin GETs and consumed %d bytes", got, replay.forwarder.foregroundBytes())
			}
			replay.waitReadPrefetch(t)
		})
	}
}
