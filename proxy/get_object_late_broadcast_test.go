package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tigrisdata/tag/metrics"
)

type getBroadcastGate struct {
	ch   chan struct{}
	once sync.Once
}

func newGetBroadcastGate() *getBroadcastGate {
	return &getBroadcastGate{ch: make(chan struct{})}
}

func (g *getBroadcastGate) release() {
	g.once.Do(func() { close(g.ch) })
}

// getBroadcastScheduleGauge pauses secondary GETs after GetOrCreate and before
// their first Subscribe call.
type getBroadcastScheduleGauge struct {
	prometheus.Gauge
	calls   atomic.Int32
	blocked chan struct{}
	release chan struct{}
}

func (g *getBroadcastScheduleGauge) Set(value float64) {
	call := g.calls.Add(1)
	if call == 2 || call == 3 {
		g.blocked <- struct{}{}
		<-g.release
	}
	g.Gauge.Set(value)
}

type getBroadcastResponseBody struct {
	data        []byte
	firstChunk  <-chan struct{}
	finish      <-chan struct{}
	firstRead   chan struct{}
	afterChunk  chan struct{}
	readStarted bool
	readEOF     bool
}

func newGetBroadcastResponseBody(data string, firstChunk, finish <-chan struct{}) *getBroadcastResponseBody {
	return &getBroadcastResponseBody{
		data:       []byte(data),
		firstChunk: firstChunk,
		finish:     finish,
		firstRead:  make(chan struct{}),
		afterChunk: make(chan struct{}),
	}
}

func (b *getBroadcastResponseBody) Read(p []byte) (int, error) {
	if !b.readStarted {
		b.readStarted = true
		close(b.firstRead)
		<-b.firstChunk
		return copy(p, b.data), nil
	}
	if !b.readEOF {
		b.readEOF = true
		close(b.afterChunk)
		<-b.finish
	}
	return 0, io.EOF
}

func (*getBroadcastResponseBody) Close() error { return nil }

type getBroadcastResponseSpec struct {
	id          string
	etag        string
	contentType string
	body        string
	firstChunk  *getBroadcastGate
	finish      *getBroadcastGate
	reader      *getBroadcastResponseBody
}

func newGetBroadcastResponseSpec(id, etag, contentType, body string) *getBroadcastResponseSpec {
	firstChunk := newGetBroadcastGate()
	finish := newGetBroadcastGate()
	return &getBroadcastResponseSpec{
		id:          id,
		etag:        etag,
		contentType: contentType,
		body:        body,
		firstChunk:  firstChunk,
		finish:      finish,
		reader:      newGetBroadcastResponseBody(body, firstChunk.ch, finish.ch),
	}
}

func (s *getBroadcastResponseSpec) response() *http.Response {
	headers := make(http.Header)
	headers.Set("Content-Length", fmt.Sprint(len(s.body)))
	headers.Set("Content-Type", s.contentType)
	headers.Set("ETag", s.etag)
	headers.Set("X-Upstream-ID", s.id)
	return &http.Response{
		StatusCode:    http.StatusOK,
		Header:        headers,
		Body:          s.reader,
		ContentLength: int64(len(s.body)),
	}
}

type getBroadcastHandlerResult struct {
	name string
	err  error
}

func awaitGetBroadcastSignal(t *testing.T, name string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

// TestHandleGetObject_LateGETsKeepUpstreamResponsesSeparate forces two callers
// to retain the original unstreamed broadcaster, then subscribes them only after
// its first chunk has started streaming. Distinct gated upstream responses keep
// both late producers active while their chunks reach the client listeners.
func TestHandleGetObject_LateGETsKeepUpstreamResponsesSeparate(t *testing.T) {
	for _, cacheEnabled := range []bool{false, true} {
		runLateGetResponseIdentitySchedule(t, cacheEnabled)
	}
}

func runLateGetResponseIdentitySchedule(t *testing.T, cacheEnabled bool) {
	t.Helper()
	origin := newGetBroadcastResponseSpec("origin", `"origin-etag"`, "application/x-origin", "origin-body")
	lateA := newGetBroadcastResponseSpec("late-a", `"late-a-etag"`, "application/x-late-a", "late-a-body")
	lateB := newGetBroadcastResponseSpec("late-b", `"late-b-etag"`, "application/x-late-b", "late-b-body")
	var upstreamCalls atomic.Int32
	forwarder := &mockForwarder{
		doRequestFunc: func(_ context.Context, _ *http.Request, _, _ string) (*http.Response, error) {
			switch upstreamCalls.Add(1) {
			case 1:
				return origin.response(), nil
			case 2:
				return lateA.response(), nil
			case 3:
				return lateB.response(), nil
			default:
				return nil, fmt.Errorf("unexpected upstream request")
			}
		},
	}
	svc, _ := newTestService(forwarder, cacheEnabled)

	previousGauge := metrics.ActiveBroadcasts
	scheduleGauge := &getBroadcastScheduleGauge{
		Gauge:   previousGauge,
		blocked: make(chan struct{}, 2),
		release: make(chan struct{}, 2),
	}
	metrics.ActiveBroadcasts = scheduleGauge

	var handlers sync.WaitGroup
	handlerResults := make(chan getBroadcastHandlerResult, 3)
	startHandler := func(name string) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/late-bucket/late-key", nil)
		handlers.Add(1)
		go func() {
			defer handlers.Done()
			handlerResults <- getBroadcastHandlerResult{
				name: name,
				err:  svc.HandleGetObject(response, request),
			}
		}()
		return response
	}

	// Unblock the schedule and upstream readers before joining handlers, including
	// when a setup assertion fails. Restore the process-wide metric after all users
	// of the temporary gauge have returned.
	defer func() {
		origin.firstChunk.release()
		origin.finish.release()
		lateA.firstChunk.release()
		lateB.firstChunk.release()
		lateA.finish.release()
		lateB.finish.release()
		scheduleGauge.release <- struct{}{}
		scheduleGauge.release <- struct{}{}
		handlers.Wait()
		metrics.ActiveBroadcasts = previousGauge
	}()

	originResponse := startHandler("origin")
	awaitGetBroadcastSignal(t, "origin upstream body", origin.reader.firstRead)

	lateResponseA := startHandler("late A")
	lateResponseB := startHandler("late B")
	for range 2 {
		awaitGetBroadcastSignal(t, "late caller's GetOrCreate/Subscribe window", scheduleGauge.blocked)
	}

	// Both late callers now hold the same unstreamed original broadcaster.
	// Advance it through Broadcast before either caller can invoke Subscribe.
	origin.firstChunk.release()
	awaitGetBroadcastSignal(t, "original broadcast's first chunk", origin.reader.afterChunk)

	// Release one late caller at a time so the upstream responses are distinct.
	scheduleGauge.release <- struct{}{}
	awaitGetBroadcastSignal(t, "first late upstream body", lateA.reader.firstRead)
	scheduleGauge.release <- struct{}{}
	awaitGetBroadcastSignal(t, "second late upstream body", lateB.reader.firstRead)

	// Hold both late streams after their first Read, and wait until both chunks
	// have been broadcast before allowing either producer to complete.
	lateA.firstChunk.release()
	lateB.firstChunk.release()
	awaitGetBroadcastSignal(t, "late A first broadcast", lateA.reader.afterChunk)
	awaitGetBroadcastSignal(t, "late B first broadcast", lateB.reader.afterChunk)
	lateA.finish.release()
	lateB.finish.release()

	for range 2 {
		select {
		case result := <-handlerResults:
			if result.err != nil {
				t.Fatalf("%s HandleGetObject: %v", result.name, result.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("late GET handlers did not complete")
		}
	}

	origin.finish.release()
	select {
	case result := <-handlerResults:
		if result.err != nil {
			t.Fatalf("%s HandleGetObject: %v", result.name, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("original GET handler did not complete")
	}

	assertGetResponseIdentity(t, "origin", cacheEnabled, originResponse, origin)
	assertGetResponseIdentity(t, "late A", cacheEnabled, lateResponseA, lateA, lateB)
	assertGetResponseIdentity(t, "late B", cacheEnabled, lateResponseB, lateA, lateB)
}

func assertGetResponseIdentity(
	t *testing.T,
	name string,
	cacheEnabled bool,
	response *httptest.ResponseRecorder,
	upstreams ...*getBroadcastResponseSpec,
) {
	t.Helper()
	id := response.Header().Get("X-Upstream-ID")
	for _, upstream := range upstreams {
		if id != upstream.id {
			continue
		}
		if response.Code != http.StatusOK ||
			response.Header().Get("ETag") != upstream.etag ||
			response.Header().Get("Content-Type") != upstream.contentType ||
			response.Header().Get("Content-Length") != fmt.Sprint(len(upstream.body)) ||
			response.Body.String() != upstream.body {
			t.Errorf("response identity mismatch: cacheEnabled=%t %s got status=%d ID=%q ETag=%q Content-Type=%q Content-Length=%q body=%q; upstream %q has status=%d ETag=%q Content-Type=%q Content-Length=%d body=%q",
				cacheEnabled, name, response.Code, id, response.Header().Get("ETag"),
				response.Header().Get("Content-Type"), response.Header().Get("Content-Length"),
				response.Body.String(), upstream.id, http.StatusOK, upstream.etag,
				upstream.contentType, len(upstream.body), upstream.body)
		}
		return
	}
	t.Errorf("response identity mismatch: cacheEnabled=%t %s has unknown upstream ID %q with status=%d ETag=%q Content-Type=%q Content-Length=%q body=%q",
		cacheEnabled, name, id, response.Code, response.Header().Get("ETag"),
		response.Header().Get("Content-Type"), response.Header().Get("Content-Length"), response.Body.String())
}
