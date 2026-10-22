package handlers

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
)

const (
	handlerParquetBucket      = "my-bucket-name"
	handlerParquetKey         = "a.parquet"
	handlerParquetETag        = `"v1"`
	handlerParquetTrailerSize = 8
	handlerParquetMagic       = "PAR1"
	handlerParquetPhaseHeader = "X-Test-Parquet-Phase"
	handlerParquetRoleHeader  = "X-Test-Parquet-Origin-Role"
	handlerParquetAuthHeader  = "AWS4-HMAC-SHA256 Credential=fixture/20261001/us-east-1/s3/aws4_request, Signature=deadbeef"
	handlerParquetUploadXML   = `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"part"</ETag></Part></CompleteMultipartUpload>`
)

type handlerParquetPhaseKey struct{}

func handlerParquetPhaseRouter(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		phase := r.Header.Get(handlerParquetPhaseHeader)
		r.Header.Del(handlerParquetPhaseHeader)
		ctx := context.WithValue(r.Context(), handlerParquetPhaseKey{}, phase)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type handlerParquetOrder struct {
	validations atomic.Int32

	firstValidation  chan struct{}
	allowFirst       chan struct{}
	secondValidation chan struct{}
	allowSecond      chan struct{}

	firstOnce  sync.Once
	secondOnce sync.Once
}

func newHandlerParquetOrder() *handlerParquetOrder {
	return &handlerParquetOrder{
		firstValidation:  make(chan struct{}),
		allowFirst:       make(chan struct{}),
		secondValidation: make(chan struct{}),
		allowSecond:      make(chan struct{}),
	}
}

func (o *handlerParquetOrder) validateCompletion() (string, error) {
	switch o.validations.Add(1) {
	case 1:
		close(o.firstValidation)
		<-o.allowFirst
		return "footer", nil
	case 2:
		close(o.secondValidation)
		<-o.allowSecond
		return "head", nil
	default:
		return "", errors.New("unexpected completion credential validation")
	}
}

func (o *handlerParquetOrder) releaseFirst() {
	o.firstOnce.Do(func() { close(o.allowFirst) })
}

func (o *handlerParquetOrder) releaseSecond() {
	o.secondOnce.Do(func() { close(o.allowSecond) })
}

func (o *handlerParquetOrder) releaseAll() {
	o.releaseFirst()
	o.releaseSecond()
}

// handlerParquetCacheGate orders HEAD publication before the footer worker's
// first metadata decision and observes the read-triggered prefetch's block scan.
type handlerParquetCacheGate struct {
	cacheclient.CacheClient
	metaKey string

	armed    atomic.Bool
	once     sync.Once
	entered  chan struct{}
	release  chan struct{}
	contexts chan context.Context
	endOnce  sync.Once

	prefetchMu      sync.Mutex
	prefetchKeys    map[string]struct{}
	prefetchSeen    map[string]struct{}
	prefetchDone    chan struct{}
	prefetchContext context.Context
	observePrefetch atomic.Bool
	failToken       atomic.Bool
}

func newHandlerParquetCacheGate(client cacheclient.CacheClient, bucket, key string) *handlerParquetCacheGate {
	return &handlerParquetCacheGate{
		CacheClient: client,
		metaKey:     cache.MakeMetaKey(bucket, key),
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
		contexts:    make(chan context.Context, 4),
	}
}

func (g *handlerParquetCacheGate) wait(ctx context.Context, key string) error {
	if key != g.metaKey || !g.armed.Load() {
		return nil
	}
	if _, requestContext := ctx.Value(handlerParquetPhaseKey{}).(string); requestContext {
		return nil
	}
	first := false
	g.once.Do(func() {
		first = true
		g.contexts <- ctx
		close(g.entered)
	})
	if first && g.failToken.CompareAndSwap(true, false) {
		return errors.New("injected metadata version read failure")
	}
	if !first {
		g.contexts <- ctx
		return nil
	}
	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *handlerParquetCacheGate) Get(ctx context.Context, key string) ([]byte, error) {
	if err := g.wait(ctx, key); err != nil {
		return nil, err
	}
	return g.CacheClient.Get(ctx, key)
}

func (g *handlerParquetCacheGate) GetWithVersion(ctx context.Context, key string) ([]byte, uint64, bool, error) {
	if err := g.wait(ctx, key); err != nil {
		return nil, 0, false, err
	}
	return g.CacheClient.GetWithVersion(ctx, key)
}

func (g *handlerParquetCacheGate) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if g.observePrefetch.Load() && ctx.Value(handlerParquetPhaseKey{}) == nil && start == 0 && end <= 1 {
		g.prefetchMu.Lock()
		if _, expected := g.prefetchKeys[key]; expected {
			if _, seen := g.prefetchSeen[key]; !seen {
				g.prefetchSeen[key] = struct{}{}
				g.prefetchContext = ctx
				if len(g.prefetchSeen) == len(g.prefetchKeys) {
					close(g.prefetchDone)
				}
			}
		}
		g.prefetchMu.Unlock()
	}
	return g.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

func (g *handlerParquetCacheGate) releaseRead() {
	g.endOnce.Do(func() { close(g.release) })
}

func (g *handlerParquetCacheGate) observeReadPrefetch(keys []string) {
	g.prefetchMu.Lock()
	g.prefetchKeys = make(map[string]struct{}, len(keys))
	g.prefetchSeen = make(map[string]struct{}, len(keys))
	g.prefetchDone = make(chan struct{})
	g.prefetchContext = nil
	for _, key := range keys {
		g.prefetchKeys[key] = struct{}{}
	}
	if len(keys) == 0 {
		close(g.prefetchDone)
	}
	g.observePrefetch.Store(true)
	g.prefetchMu.Unlock()
}

func (g *handlerParquetCacheGate) prefetchScan() (context.Context, <-chan struct{}) {
	g.prefetchMu.Lock()
	defer g.prefetchMu.Unlock()
	return g.prefetchContext, g.prefetchDone
}

func (g *handlerParquetCacheGate) stopObservingPrefetch() {
	g.observePrefetch.Store(false)
}

type handlerParquetRangeObservation struct {
	role        string
	rangeHeader string
}

type handlerParquetOriginObservation struct {
	heads         int
	rangeGets     int
	responseBytes int64
	byRole        map[string]int
	ranges        []handlerParquetRangeObservation
}

type handlerParquetOrigin struct {
	objectSize  int64
	blockSize   int64
	footerLen   int64
	footerStart int64
	footer      []byte
	trailer     [handlerParquetTrailerSize]byte
	etag        string

	suffixETag    string
	suffixStatus  int
	responseDelay time.Duration
	blockStarted  chan struct{}
	blockRelease  chan struct{}
	blockOnce     sync.Once
	releaseOnce   sync.Once

	mu            sync.Mutex
	heads         int
	rangeGets     int
	responseBytes int64
	byRole        map[string]int
	ranges        []handlerParquetRangeObservation
}

func newHandlerParquetOrigin(objectSize, blockSize, footerLen int64, delay time.Duration) *handlerParquetOrigin {
	footer := make([]byte, footerLen)
	for i := range footer {
		footer[i] = byte(i % 251)
	}
	o := &handlerParquetOrigin{
		objectSize:    objectSize,
		blockSize:     blockSize,
		footerLen:     footerLen,
		footerStart:   objectSize - handlerParquetTrailerSize - footerLen,
		footer:        footer,
		etag:          handlerParquetETag,
		responseDelay: delay,
		byRole:        make(map[string]int),
	}
	binary.LittleEndian.PutUint32(o.trailer[:4], uint32(footerLen))
	copy(o.trailer[4:], handlerParquetMagic)
	return o
}

func (o *handlerParquetOrigin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	role := r.Header.Get(handlerParquetRoleHeader)
	if r.Method == http.MethodHead {
		o.mu.Lock()
		o.heads++
		o.byRole[role]++
		o.mu.Unlock()
		w.Header().Set("ETag", o.etag)
		w.Header().Set("Content-Length", strconv.FormatInt(o.objectSize, 10))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Last-Modified", time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC).Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "unsupported method", http.StatusMethodNotAllowed)
		return
	}

	rangeHeader := r.Header.Get("Range")
	start, end, suffix, err := parseHandlerParquetRange(rangeHeader, o.objectSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
		return
	}
	o.mu.Lock()
	o.rangeGets++
	o.byRole[role]++
	o.ranges = append(o.ranges, handlerParquetRangeObservation{role: role, rangeHeader: rangeHeader})
	o.mu.Unlock()

	if !suffix && o.blockStarted != nil {
		o.blockOnce.Do(func() { close(o.blockStarted) })
		select {
		case <-o.blockRelease:
		case <-r.Context().Done():
			return
		}
	}
	if o.responseDelay > 0 {
		timer := time.NewTimer(o.responseDelay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
	}

	body := o.rangeBytes(start, end)
	etag := o.etag
	status := http.StatusPartialContent
	if suffix && o.suffixETag != "" {
		etag = o.suffixETag
	}
	if suffix && o.suffixStatus != 0 {
		status = o.suffixStatus
	}
	w.Header().Set("ETag", etag)
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, o.objectSize))
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Last-Modified", time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC).Format(http.TimeFormat))
	w.WriteHeader(status)
	n, _ := w.Write(body)
	o.mu.Lock()
	o.responseBytes += int64(n)
	o.mu.Unlock()
}

func (o *handlerParquetOrigin) observations() handlerParquetOriginObservation {
	o.mu.Lock()
	defer o.mu.Unlock()
	byRole := make(map[string]int, len(o.byRole))
	for role, count := range o.byRole {
		byRole[role] = count
	}
	return handlerParquetOriginObservation{
		heads: o.heads, rangeGets: o.rangeGets, responseBytes: o.responseBytes,
		byRole: byRole, ranges: append([]handlerParquetRangeObservation(nil), o.ranges...),
	}
}

func (o *handlerParquetOrigin) resetObservations() {
	o.mu.Lock()
	o.heads = 0
	o.rangeGets = 0
	o.responseBytes = 0
	o.byRole = make(map[string]int)
	o.ranges = nil
	o.mu.Unlock()
}

func (o *handlerParquetOrigin) rangeBytes(start, end int64) []byte {
	body := bytes.Repeat([]byte{0x5a}, int(end-start+1))
	copyHandlerParquetSegment(body, start, o.footerStart, o.footer)
	copyHandlerParquetSegment(body, start, o.objectSize-handlerParquetTrailerSize, o.trailer[:])
	return body
}

func copyHandlerParquetSegment(dst []byte, dstStart, segmentStart int64, src []byte) {
	dstEnd := dstStart + int64(len(dst))
	segmentEnd := segmentStart + int64(len(src))
	start := max(dstStart, segmentStart)
	end := min(dstEnd, segmentEnd)
	if start >= end {
		return
	}
	copy(dst[start-dstStart:end-dstStart], src[start-segmentStart:end-segmentStart])
}

func parseHandlerParquetRange(value string, total int64) (start, end int64, suffix bool, err error) {
	if strings.HasPrefix(value, "bytes=-") {
		n, parseErr := strconv.ParseInt(strings.TrimPrefix(value, "bytes=-"), 10, 64)
		if parseErr != nil || n <= 0 {
			return 0, 0, false, fmt.Errorf("invalid suffix range %q", value)
		}
		if n > total {
			n = total
		}
		return total - n, total - 1, true, nil
	}
	if !strings.HasPrefix(value, "bytes=") {
		return 0, 0, false, fmt.Errorf("invalid range %q", value)
	}
	parts := strings.Split(strings.TrimPrefix(value, "bytes="), "-")
	if len(parts) != 2 {
		return 0, 0, false, fmt.Errorf("invalid range %q", value)
	}
	start, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false, err
	}
	end, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, false, err
	}
	if end >= total {
		end = total - 1
	}
	if start < 0 || start > end {
		return 0, 0, false, fmt.Errorf("invalid range bounds %q", value)
	}
	return start, end, false, nil
}

func (o *handlerParquetOrigin) releaseBlocks() {
	if o.blockRelease != nil {
		o.releaseOnce.Do(func() { close(o.blockRelease) })
	}
}

func (o *handlerParquetOrigin) footerBlockIndices(tailServedByCaller bool) []int64 {
	first := (o.objectSize - handlerParquetTrailerSize - o.footerLen) / o.blockSize
	last := (o.objectSize - 1) / o.blockSize
	if tailServedByCaller {
		last--
	}
	indices := make([]int64, 0, last-first+1)
	for idx := first; idx <= last; idx++ {
		indices = append(indices, idx)
	}
	return indices
}

func (o *handlerParquetOrigin) blockRange(idx int64) string {
	start := idx * o.blockSize
	end := min(start+o.blockSize-1, o.objectSize-1)
	return fmt.Sprintf("bytes=%d-%d", start, end)
}

func (o *handlerParquetOrigin) trailerRange() string {
	return fmt.Sprintf("bytes=%d-%d", o.objectSize-handlerParquetTrailerSize, o.objectSize-1)
}

func (o *handlerParquetOrigin) metadataRange() string {
	return fmt.Sprintf("bytes=%d-%d", o.footerStart, o.objectSize-handlerParquetTrailerSize-1)
}

func (o *handlerParquetOrigin) firstFooterBlock() int64 {
	return (o.objectSize - handlerParquetTrailerSize - o.footerLen) / o.blockSize
}

func (o *handlerParquetOrigin) lastBlock() int64 {
	return (o.objectSize - 1) / o.blockSize
}

func (o *handlerParquetOrigin) isAlignedFooterBlock(rangeHeader string, includeTail bool) bool {
	start, end, suffix, err := parseHandlerParquetRange(rangeHeader, o.objectSize)
	if err != nil || suffix || start%o.blockSize != 0 {
		return false
	}
	idx := start / o.blockSize
	last := o.lastBlock()
	if !includeTail {
		last--
	}
	wantEnd := min(start+o.blockSize-1, o.objectSize-1)
	return idx >= o.firstFooterBlock() && idx <= last && end == wantEnd
}

type handlerParquetForwarder struct {
	order              *handlerParquetOrder
	origin             *handlerParquetOrigin
	originHTTP         *httptest.Server
	client             *http.Client
	mu                 sync.Mutex
	consumed           map[string]int64
	inflight           map[string]int
	completionForwards atomic.Int32
}

func newHandlerParquetForwarder(order *handlerParquetOrder, origin *handlerParquetOrigin, server *httptest.Server) *handlerParquetForwarder {
	return &handlerParquetForwarder{
		order: order, origin: origin, originHTTP: server, client: server.Client(),
		consumed: make(map[string]int64), inflight: make(map[string]int),
	}
}

func (f *handlerParquetForwarder) Forward(_ context.Context, w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodDelete {
		return fmt.Errorf("unexpected forwarded method %s", r.Method)
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (f *handlerParquetForwarder) ForwardWithCapture(_ context.Context, w http.ResponseWriter, r *http.Request) (*proxy.ResponseCapture, error) {
	if r.Method != http.MethodPost || r.URL.Query().Get("uploadId") == "" {
		return nil, errors.New("expected a multipart completion request")
	}
	requestBody, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	if string(requestBody) != handlerParquetUploadXML {
		return nil, fmt.Errorf("unexpected completion body %q", requestBody)
	}
	f.completionForwards.Add(1)
	body := []byte(`<CompleteMultipartUploadResult><ETag>` + handlerParquetETag + `</ETag></CompleteMultipartUploadResult>`)
	headers := make(http.Header)
	headers.Set("Content-Type", "application/xml")
	headers.Set("ETag", handlerParquetETag)
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("ETag", handlerParquetETag)
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(body); err != nil {
		return nil, err
	}
	return &proxy.ResponseCapture{StatusCode: http.StatusOK, Headers: headers, Body: body, Complete: true}, nil
}

func (f *handlerParquetForwarder) ValidateAndGetCredentials(r *http.Request) (proxy.AuthResult, string, string, error) {
	if r.Header.Get("Authorization") == "" {
		return proxy.AuthNotValidated, "", "", errors.New("missing authentication")
	}
	if r.Method == http.MethodPost {
		phase, err := f.order.validateCompletion()
		return proxy.AuthValidated, "fixture-access", phase, err
	}
	phase, _ := r.Context().Value(handlerParquetPhaseKey{}).(string)
	switch phase {
	case "trailer", "metadata", "delete":
		return proxy.AuthValidated, "fixture-access", phase, nil
	default:
		return proxy.AuthNotValidated, "", "", fmt.Errorf("unknown request phase %q", phase)
	}
}

func (f *handlerParquetForwarder) DoRequestWithCreds(ctx context.Context, r *http.Request, _, secretKey string) (*http.Response, error) {
	return f.request(ctx, r.Method, r.URL.EscapedPath(), r.Header.Get("Range"), secretKey)
}

func (f *handlerParquetForwarder) DoFullObjectRequest(ctx context.Context, bucket, key, _, secretKey string) (*http.Response, error) {
	return f.request(ctx, http.MethodGet, "/"+bucket+"/"+key, "", secretKey)
}

func (f *handlerParquetForwarder) DoAnonymousFullObjectRequest(context.Context, string, string) (*http.Response, error) {
	return nil, errors.New("unexpected anonymous full-object request")
}

func (f *handlerParquetForwarder) DoObjectDeleteRequest(context.Context, string, string, string, string, string) (*http.Response, error) {
	return nil, errors.New("unexpected synthetic object delete")
}

func (f *handlerParquetForwarder) DoConditionalGetRequest(ctx context.Context, bucket, key, _, secretKey, _ string, _ int64, rangeHeader string) (*http.Response, error) {
	return f.request(ctx, http.MethodGet, "/"+bucket+"/"+key, rangeHeader, secretKey)
}

func (f *handlerParquetForwarder) DoConditionalHeadRequest(ctx context.Context, bucket, key, _, secretKey, _ string, _ int64) (*http.Response, error) {
	return f.request(ctx, http.MethodHead, "/"+bucket+"/"+key, "", secretKey)
}

func (f *handlerParquetForwarder) request(ctx context.Context, method, path, rangeHeader, secretKey string) (*http.Response, error) {
	role := f.originRole(method, rangeHeader, secretKey)
	req, err := http.NewRequestWithContext(ctx, method, f.originHTTP.URL+path, nil)
	if err != nil {
		return nil, err
	}
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	req.Header.Set(handlerParquetRoleHeader, role)
	if method == http.MethodGet && rangeHeader != "" {
		f.mu.Lock()
		f.inflight[role]++
		f.mu.Unlock()
	}
	resp, err := f.client.Do(req)
	if err != nil {
		if method == http.MethodGet && rangeHeader != "" {
			f.finishRequest(role)
		}
		return nil, err
	}
	if method == http.MethodGet && rangeHeader != "" {
		resp.Body = &handlerParquetObservedBody{
			ReadCloser: resp.Body,
			onRead: func(n int) {
				f.mu.Lock()
				f.consumed[role] += int64(n)
				f.mu.Unlock()
			},
			onClose: func() { f.finishRequest(role) },
		}
	}
	return resp, nil
}

func (f *handlerParquetForwarder) originRole(method, rangeHeader, secretKey string) string {
	switch secretKey {
	case "footer":
		return "write-warm"
	case "head":
		if method == http.MethodHead {
			return "metadata-head"
		}
		return "unexpected"
	case "trailer":
		if method != http.MethodGet {
			return "unexpected"
		}
		if rangeHeader == "bytes=-8" || rangeHeader == f.origin.blockRange(f.origin.lastBlock()) || rangeHeader == f.origin.trailerRange() {
			return "foreground-trailer"
		}
		if f.origin.isAlignedFooterBlock(rangeHeader, false) {
			return "read-prefetch"
		}
		return "unexpected"
	case "metadata":
		if method == http.MethodGet && (rangeHeader == f.origin.metadataRange() || f.origin.isAlignedFooterBlock(rangeHeader, true)) {
			return "foreground-metadata"
		}
		return "unexpected"
	case "delete":
		return "delete"
	default:
		return "unexpected"
	}
}

func (f *handlerParquetForwarder) finishRequest(role string) {
	f.mu.Lock()
	f.inflight[role]--
	f.mu.Unlock()
}

func (f *handlerParquetForwarder) resetMeasurements() {
	f.mu.Lock()
	f.consumed = make(map[string]int64)
	f.mu.Unlock()
	f.origin.resetObservations()
}

func (f *handlerParquetForwarder) consumedBytes(role string) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.consumed[role]
}

func (f *handlerParquetForwarder) inflightCount(role string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.inflight[role]
}

func (f *handlerParquetForwarder) foregroundBytes() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.consumed["foreground-trailer"] + f.consumed["foreground-metadata"]
}

type handlerParquetObservedBody struct {
	io.ReadCloser
	onRead  func(int)
	onClose func()
	once    sync.Once
}

func (b *handlerParquetObservedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		b.onRead(n)
	}
	return n, err
}

func (b *handlerParquetObservedBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.onClose)
	return err
}

type handlerParquetReplay struct {
	cache      *cache.Cache
	gate       *handlerParquetCacheGate
	order      *handlerParquetOrder
	origin     *handlerParquetOrigin
	forwarder  *handlerParquetForwarder
	originHTTP *httptest.Server
	gateway    *httptest.Server
	client     *http.Client
	blockSize  int64
}

func (r *handlerParquetReplay) close() {
	r.order.releaseAll()
	r.gate.releaseRead()
	r.origin.releaseBlocks()
	r.client.CloseIdleConnections()
	r.gateway.Close()
	r.originHTTP.Close()
}

func (r *handlerParquetReplay) request(tb testing.TB, method, query, phase, rangeHeader string) *http.Response {
	tb.Helper()
	resp, err := r.requestWithError(method, query, phase, rangeHeader)
	if err != nil {
		tb.Fatal(err)
	}
	return resp
}

func (r *handlerParquetReplay) requestWithError(method, query, phase, rangeHeader string) (*http.Response, error) {
	path := "/" + handlerParquetBucket + "/" + handlerParquetKey + query
	req, err := http.NewRequest(method, r.gateway.URL+path, strings.NewReader(handlerParquetUploadXML))
	if method == http.MethodGet || method == http.MethodDelete {
		req, err = http.NewRequest(method, r.gateway.URL+path, nil)
	}
	if err != nil {
		return nil, fmt.Errorf("new %s request: %w", method, err)
	}
	req.Header.Set("Authorization", handlerParquetAuthHeader)
	if phase != "" {
		req.Header.Set(handlerParquetPhaseHeader, phase)
	}
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/xml")
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, path, err)
	}
	return resp, nil
}

func (r *handlerParquetReplay) completeAndPauseFooter(tb testing.TB, uploadID string) {
	tb.Helper()
	type completionResult struct {
		status      int
		body        []byte
		requestErr  error
		responseErr error
	}
	completed := make(chan completionResult, 1)
	go func() {
		resp, err := r.requestWithError(http.MethodPost, "?uploadId="+uploadID, "completion", "")
		if err != nil {
			completed <- completionResult{requestErr: err}
			return
		}
		body, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if readErr == nil {
			readErr = closeErr
		}
		completed <- completionResult{status: resp.StatusCode, body: body, responseErr: readErr}
	}()

	var result *completionResult
	checkResult := func(completed completionResult) {
		result = &completed
		if completed.requestErr != nil {
			tb.Fatalf("complete multipart request: %v", completed.requestErr)
		}
		if completed.responseErr != nil {
			tb.Fatalf("read completion response: %v", completed.responseErr)
		}
	}
	waitForSignal := func(signal <-chan struct{}, name string) {
		timer := time.NewTimer(3 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-signal:
				return
			case completed := <-completed:
				checkResult(completed)
			case <-timer.C:
				tb.Fatalf("timed out waiting for %s", name)
			}
		}
	}

	waitForSignal(r.order.firstValidation, "footer credential validation")
	r.gate.armed.Store(true)
	r.order.releaseFirst()
	waitForSignal(r.gate.entered, "footer metadata decision")
	waitForSignal(r.order.secondValidation, "metadata HEAD credential validation")
	r.order.releaseSecond()

	if result == nil {
		select {
		case completed := <-completed:
			checkResult(completed)
		case <-time.After(3 * time.Second):
			tb.Fatal("completion waited for detached footer or HEAD work")
		}
	}
	want := []byte(`<CompleteMultipartUploadResult><ETag>` + handlerParquetETag + `</ETag></CompleteMultipartUploadResult>`)
	if result.status != http.StatusOK || !bytes.Equal(result.body, want) {
		tb.Fatalf("CompleteMultipartUpload response = %d %q; want 200 %q", result.status, result.body, want)
	}
	if got := r.forwarder.completionForwards.Load(); got != 1 {
		tb.Fatalf("completion forward count = %d, want one nonreplayed completion", got)
	}

	meta := waitHandlerParquetMeta(tb, r.cache)
	if meta.ETag != handlerParquetETag || meta.ContentLength != r.origin.objectSize || meta.BlockSize != r.blockSize || meta.BlocksComplete {
		tb.Fatalf("metadata HEAD published %+v; want matching metadata-only block entry", meta)
	}
	for _, idx := range r.origin.footerBlockIndices(false) {
		if r.cache.BlockExists(context.Background(), handlerParquetBucket, handlerParquetKey, meta.ETag, meta.BlockSize, idx) {
			tb.Fatalf("footer block %d exists while its write warm is gated", idx)
		}
	}
}

func (r *handlerParquetReplay) finishFooterWarm(tb testing.TB) handlerParquetOriginObservation {
	tb.Helper()
	footerCtx := waitHandlerParquetContext(tb, r.gate.contexts, "footer worker context")
	headCtx := waitHandlerParquetContext(tb, r.gate.contexts, "metadata HEAD worker context")
	waitHandlerParquetMeta(tb, r.cache)
	r.gate.releaseRead()
	waitHandlerParquetCancellation(tb, footerCtx, "footer worker")
	waitHandlerParquetCancellation(tb, headCtx, "metadata HEAD worker")
	return r.origin.observations()
}

func (r *handlerParquetReplay) firstOpen(tb testing.TB) []handlerParquetHTTPResponse {
	tb.Helper()
	ranges := []string{"bytes=-8", r.origin.metadataRange()}
	phases := []string{"trailer", "metadata"}
	responses := make([]handlerParquetHTTPResponse, 0, len(ranges))
	for i, rangeHeader := range ranges {
		resp := r.request(tb, http.MethodGet, "", phases[i], rangeHeader)
		body, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if readErr == nil {
			readErr = closeErr
		}
		if readErr != nil {
			tb.Fatalf("read %s response: %v", phases[i], readErr)
		}
		responses = append(responses, handlerParquetHTTPResponse{status: resp.StatusCode, header: resp.Header.Clone(), body: body})
	}
	return responses
}

type handlerParquetHTTPResponse struct {
	status int
	header http.Header
	body   []byte
}

func (r *handlerParquetReplay) verifyFirstOpen(tb testing.TB, responses []handlerParquetHTTPResponse) {
	tb.Helper()
	starts := []int64{r.origin.objectSize - handlerParquetTrailerSize, r.origin.footerStart}
	ends := []int64{r.origin.objectSize - 1, r.origin.objectSize - handlerParquetTrailerSize - 1}
	if len(responses) != 2 {
		tb.Fatalf("first open returned %d responses, want 2", len(responses))
	}
	wantLastModified := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC).Format(http.TimeFormat)
	for i, response := range responses {
		wantRange := fmt.Sprintf("bytes %d-%d/%d", starts[i], ends[i], r.origin.objectSize)
		wantBody := r.origin.rangeBytes(starts[i], ends[i])
		cacheStatus := response.header.Get(proxy.XCacheHeader)
		if response.status != http.StatusPartialContent || response.header.Get("Content-Range") != wantRange || response.header.Get("ETag") != handlerParquetETag || response.header.Get("Content-Type") != "application/octet-stream" || response.header.Get("Last-Modified") != wantLastModified || (cacheStatus != proxy.XCacheHit && cacheStatus != proxy.XCacheMiss) || !bytes.Equal(response.body, wantBody) {
			tb.Fatalf("first-open response %d: status=%d content-range=%q etag=%q cache=%q body=%d bytes; want 206 %q %q and a valid cache status for %d bytes",
				i, response.status, response.header.Get("Content-Range"), response.header.Get("ETag"), response.header.Get(proxy.XCacheHeader), len(response.body), wantRange, handlerParquetETag, len(wantBody))
		}
		if response.header.Get("Content-Length") != strconv.Itoa(len(wantBody)) {
			tb.Fatalf("first-open response %d Content-Length=%q, want %d", i, response.header.Get("Content-Length"), len(wantBody))
		}
	}
}

func (r *handlerParquetReplay) readPrefetchKeys() []string {
	indices := r.origin.footerBlockIndices(true)
	keys := make([]string, 0, len(indices))
	for _, idx := range indices {
		keys = append(keys, cache.MakeBlockKey(handlerParquetBucket, handlerParquetKey, handlerParquetETag, r.blockSize, idx))
	}
	return keys
}

func (r *handlerParquetReplay) observeReadPrefetch() {
	r.gate.observeReadPrefetch(r.readPrefetchKeys())
}

func (r *handlerParquetReplay) waitReadPrefetch(tb testing.TB) {
	tb.Helper()
	ctx, scanned := r.gate.prefetchScan()
	if ctx == nil {
		waitHandlerParquetSignal(tb, scanned, "read-triggered footer block scan")
		ctx, _ = r.gate.prefetchScan()
	}
	if ctx == nil {
		tb.Fatal("read-prefetch scan did not retain its bounded context")
	}
	waitHandlerParquetCondition(tb, func() bool {
		meta, found, err := r.cache.GetMeta(handlerParquetPollContext(), handlerParquetBucket, handlerParquetKey)
		if err != nil || !found || meta == nil {
			return false
		}
		for _, idx := range r.origin.footerBlockIndices(false) {
			if !r.cache.BlockExists(context.WithValue(context.Background(), handlerParquetPhaseKey{}, "fixture-poll"), handlerParquetBucket, handlerParquetKey, meta.ETag, meta.BlockSize, idx) {
				return false
			}
		}
		return r.forwarder.inflightCount("read-prefetch") == 0
	}, "read-prefetch block completion")
	r.gate.stopObservingPrefetch()
	waitHandlerParquetCancellation(tb, ctx, "read-triggered prefetch")
}

func (r *handlerParquetReplay) resetMeasurements() {
	r.forwarder.resetMeasurements()
}

func (r *handlerParquetReplay) foregroundGets() int {
	observed := r.origin.observations()
	return observed.byRole["foreground-trailer"] + observed.byRole["foreground-metadata"]
}

func (r *handlerParquetReplay) unexpectedRanges() []handlerParquetRangeObservation {
	observed := r.origin.observations()
	var unexpected []handlerParquetRangeObservation
	for _, item := range observed.ranges {
		if item.role == "unexpected" {
			unexpected = append(unexpected, item)
		}
	}
	return unexpected
}

func (r *handlerParquetReplay) startFooterWarm(tb testing.TB) {
	tb.Helper()
	r.gate.releaseRead()
}

func (r *handlerParquetReplay) deleteObject(tb testing.TB) {
	tb.Helper()
	resp := r.request(tb, http.MethodDelete, "", "delete", "")
	body, err := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil || resp.StatusCode != http.StatusNoContent {
		tb.Fatalf("DELETE response = %d %q, err=%v; want 204", resp.StatusCode, body, err)
	}
}

func waitHandlerParquetSignal(tb testing.TB, signal <-chan struct{}, name string) {
	tb.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		tb.Fatalf("timed out waiting for %s", name)
	}
}

func waitHandlerParquetContext(tb testing.TB, contexts <-chan context.Context, name string) context.Context {
	tb.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case ctx := <-contexts:
		return ctx
	case <-timer.C:
		tb.Fatalf("timed out waiting for %s", name)
		return nil
	}
}

func waitHandlerParquetCancellation(tb testing.TB, ctx context.Context, name string) {
	tb.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.Canceled) {
			tb.Fatalf("%s context ended with %v, want cancellation after successful return", name, ctx.Err())
		}
	case <-timer.C:
		tb.Fatalf("timed out waiting for %s completion", name)
	}
}

func handlerParquetPollContext() context.Context {
	return context.WithValue(context.Background(), handlerParquetPhaseKey{}, "fixture-poll")
}

func waitHandlerParquetMeta(tb testing.TB, store *cache.Cache) *cache.CachedObjectMeta {
	tb.Helper()
	ctx := handlerParquetPollContext()
	timer := time.NewTimer(3 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer timer.Stop()
	defer ticker.Stop()
	for {
		meta, found, err := store.GetMeta(ctx, handlerParquetBucket, handlerParquetKey)
		if err != nil {
			tb.Fatalf("GetMeta: %v", err)
		}
		if found {
			return meta
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			tb.Fatal("metadata-on-write HEAD did not publish")
		}
	}
}

func waitHandlerParquetCondition(tb testing.TB, ready func() bool, name string) {
	tb.Helper()
	timer := time.NewTimer(5 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer timer.Stop()
	defer ticker.Stop()
	for !ready() {
		select {
		case <-ticker.C:
		case <-timer.C:
			tb.Fatalf("timed out waiting for %s", name)
		}
	}
}

func TestServer_CompleteMultipartUploadWarmsFooterForFirstOpen(t *testing.T) {
	for _, tc := range []struct {
		name   string
		legacy bool
	}{
		{name: "legacy", legacy: true},
		{name: "cas", legacy: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			origin := newHandlerParquetOrigin(64, 16, 24, 0)
			originHTTP := httptest.NewServer(origin)
			replay := newHandlerParquetReplayWithOrigin(t, origin, originHTTP, tc.legacy)
			defer replay.close()

			replay.completeAndPauseFooter(t, "route-success")
			warm := replay.finishFooterWarm(t)
			gotRanges := make([]string, 0, len(warm.ranges))
			for _, request := range warm.ranges {
				if request.role == "write-warm" {
					gotRanges = append(gotRanges, request.rangeHeader)
				}
			}
			slices.Sort(gotRanges)
			wantRanges := []string{"bytes=-8", "bytes=32-47", "bytes=48-63"}
			if warm.heads != 1 || warm.byRole["write-warm"] != 3 || warm.rangeGets != 3 || warm.responseBytes != 40 || !slices.Equal(gotRanges, wantRanges) {
				t.Fatalf("completed footer warm: HEAD=%d ranges=%d bytes=%d roles=%v ranges=%v; want one HEAD, suffix plus exact footer blocks", warm.heads, warm.rangeGets, warm.responseBytes, warm.byRole, gotRanges)
			}
			for _, idx := range origin.footerBlockIndices(false) {
				if !replay.cache.BlockExists(context.Background(), handlerParquetBucket, handlerParquetKey, handlerParquetETag, origin.blockSize, idx) {
					t.Fatalf("footer block %d is absent after the completed write warm", idx)
				}
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
				t.Fatalf("first open after completed footer warm made %d foreground origin GETs and consumed %d bytes", got, replay.forwarder.foregroundBytes())
			}
			if unexpected := replay.unexpectedRanges(); len(unexpected) != 0 {
				t.Fatalf("unexpected origin ranges during first open: %v", unexpected)
			}
			replay.waitReadPrefetch(t)
		})
	}
}

func TestServer_CompleteMultipartUploadSkipsFooterWhenVersionLookupFails(t *testing.T) {
	for _, tc := range []struct {
		name   string
		legacy bool
	}{
		{name: "legacy", legacy: true},
		{name: "cas", legacy: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			origin := newHandlerParquetOrigin(64, 16, 24, 0)
			originHTTP := httptest.NewServer(origin)
			replay := newHandlerParquetReplayWithOrigin(t, origin, originHTTP, tc.legacy)
			defer replay.close()
			replay.gate.failToken.Store(true)

			replay.completeAndPauseFooter(t, "route-token-read-failure")
			warm := replay.finishFooterWarm(t)
			if warm.heads != 1 || warm.rangeGets != 0 {
				t.Fatalf("token-read failure: HEAD=%d ranges=%d roles=%v; want HEAD fallback and no suffix/block GET", warm.heads, warm.rangeGets, warm.byRole)
			}
			for _, idx := range origin.footerBlockIndices(false) {
				if replay.cache.BlockExists(context.Background(), handlerParquetBucket, handlerParquetKey, handlerParquetETag, origin.blockSize, idx) {
					t.Fatalf("footer block %d was cached after a version-read failure", idx)
				}
			}
		})
	}
}

func TestServer_CompleteMultipartUploadKeepsHeadOnSuffixETagMismatch(t *testing.T) {
	origin := newHandlerParquetOrigin(64, 16, 24, 0)
	origin.suffixETag = `"newer"`
	originHTTP := httptest.NewServer(origin)
	replay := newHandlerParquetReplayWithOrigin(t, origin, originHTTP, true)
	defer replay.close()

	replay.completeAndPauseFooter(t, "route-etag-mismatch")
	warm := replay.finishFooterWarm(t)
	if warm.heads != 1 || warm.rangeGets != 1 || warm.byRole["write-warm"] != 1 {
		t.Fatalf("mismatched suffix: HEAD=%d ranges=%d roles=%v; want one HEAD and one guarded suffix", warm.heads, warm.rangeGets, warm.byRole)
	}
	meta, found, err := replay.cache.GetMeta(handlerParquetPollContext(), handlerParquetBucket, handlerParquetKey)
	if err != nil || !found || meta.ETag != handlerParquetETag {
		t.Fatalf("completion metadata was not preserved after an ETag mismatch: meta=%+v found=%t err=%v", meta, found, err)
	}
	for _, idx := range origin.footerBlockIndices(false) {
		if replay.cache.BlockExists(context.Background(), handlerParquetBucket, handlerParquetKey, handlerParquetETag, origin.blockSize, idx) {
			t.Fatalf("block %d from a mismatched suffix was cached", idx)
		}
	}
}

func TestServer_CompleteMultipartUploadKeepsHeadWhenSuffixIsNotPartial(t *testing.T) {
	origin := newHandlerParquetOrigin(64, 16, 24, 0)
	origin.suffixStatus = http.StatusOK
	originHTTP := httptest.NewServer(origin)
	replay := newHandlerParquetReplayWithOrigin(t, origin, originHTTP, true)
	defer replay.close()

	replay.completeAndPauseFooter(t, "route-unsafe-suffix")
	warm := replay.finishFooterWarm(t)
	if warm.heads != 1 || warm.rangeGets != 1 || warm.byRole["write-warm"] != 1 || len(warm.ranges) != 1 || warm.ranges[0].rangeHeader != "bytes=-8" {
		t.Fatalf("unsafe suffix: HEAD=%d ranges=%d roles=%v ranges=%v; want one HEAD and one guarded suffix", warm.heads, warm.rangeGets, warm.byRole, warm.ranges)
	}
	meta, found, err := replay.cache.GetMeta(handlerParquetPollContext(), handlerParquetBucket, handlerParquetKey)
	if err != nil || !found || meta.ETag != handlerParquetETag {
		t.Fatalf("matching HEAD metadata was lost after an unsafe suffix: meta=%+v found=%t err=%v", meta, found, err)
	}
	for _, idx := range origin.footerBlockIndices(false) {
		if replay.cache.BlockExists(context.Background(), handlerParquetBucket, handlerParquetKey, handlerParquetETag, origin.blockSize, idx) {
			t.Fatalf("footer block %d was cached from an unsafe suffix", idx)
		}
	}
}

func TestServer_DeleteDuringFooterFetchDoesNotRepublishMetadata(t *testing.T) {
	for _, tc := range []struct {
		name   string
		legacy bool
	}{
		{name: "legacy", legacy: true},
		{name: "cas", legacy: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			origin := newHandlerParquetOrigin(64, 16, 24, 0)
			origin.blockStarted = make(chan struct{})
			origin.blockRelease = make(chan struct{})
			originHTTP := httptest.NewServer(origin)
			replay := newHandlerParquetReplayWithOrigin(t, origin, originHTTP, tc.legacy)
			defer replay.close()

			replay.completeAndPauseFooter(t, "route-delete-race")
			footerCtx := waitHandlerParquetContext(t, replay.gate.contexts, "footer worker context")
			_ = waitHandlerParquetContext(t, replay.gate.contexts, "metadata HEAD worker context")
			replay.startFooterWarm(t)
			waitHandlerParquetSignal(t, origin.blockStarted, "footer block request")
			replay.deleteObject(t)
			if _, found, err := replay.cache.GetMeta(handlerParquetPollContext(), handlerParquetBucket, handlerParquetKey); err != nil || found {
				t.Fatalf("metadata after routed DELETE: found=%t err=%v", found, err)
			}
			origin.releaseBlocks()
			waitHandlerParquetCancellation(t, footerCtx, "footer worker")
			if _, found, err := replay.cache.GetMeta(handlerParquetPollContext(), handlerParquetBucket, handlerParquetKey); err != nil || found {
				t.Fatalf("footer worker republished invalidated metadata: found=%t err=%v", found, err)
			}
		})
	}
}

func newHandlerParquetReplayWithOrigin(tb testing.TB, origin *handlerParquetOrigin, originHTTP *httptest.Server, legacy bool) *handlerParquetReplay {
	tb.Helper()
	order := newHandlerParquetOrder()
	baseClient := cacheclient.NewMemoryCache()
	gate := newHandlerParquetCacheGate(baseClient, handlerParquetBucket, handlerParquetKey)

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = origin.blockSize
	cfg.Cache.SizeThreshold = 1 << 30
	cfg.Cache.ParquetOptimization = true
	cfg.Cache.MetaOnWrite = true
	cfg.Cache.WarmOnWrite = false
	cfg.Cache.SetLegacyCoordination(legacy)
	store := cache.NewCacheWithClient(gate, &cfg.Cache)
	forwarder := newHandlerParquetForwarder(order, origin, originHTTP)
	service := proxy.NewService(forwarder, store, cfg)
	router := NewServer(service, "127.0.0.1", 0, false, 0).Router()
	gateway := httptest.NewServer(handlerParquetPhaseRouter(router))
	client := gateway.Client()
	replay := &handlerParquetReplay{
		cache: store, gate: gate, order: order, origin: origin, forwarder: forwarder,
		originHTTP: originHTTP, gateway: gateway, client: client, blockSize: origin.blockSize,
	}
	return replay
}
