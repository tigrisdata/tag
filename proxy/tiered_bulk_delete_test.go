package proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/tigrisdata/tag/cache"
)

const (
	tieredBulkDeleteBucket  = "bulk-bucket"
	tieredBulkDeleteKey     = "local-key"
	tieredBulkDeleteBody    = "local object body"
	tieredBulkDeleteRequest = `<Delete><Object><Key>local-key</Key></Object></Delete>`

	tieredBulkDeleteViolation = "tiered bulk delete removed or changed a local object after an incomplete or unparseable upstream result"
)

type tieredBulkDeleteResponseBody struct {
	reader          *bytes.Reader
	readErr         error
	readErrReturned bool
	closed          bool
}

func (b *tieredBulkDeleteResponseBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	if err == io.EOF && b.readErr != nil {
		b.readErrReturned = true
		return n, b.readErr
	}
	return n, err
}

func (b *tieredBulkDeleteResponseBody) Close() error {
	b.closed = true
	return nil
}

type tieredBulkDeleteFixture struct {
	service *Service
	cache   *cache.Cache
	body    *tieredBulkDeleteResponseBody
}

func newTieredBulkDeleteFixture(t *testing.T, responseBody string, readErr error, beforeResponse func()) *tieredBulkDeleteFixture {
	t.Helper()

	service, objectCache := newTieredTestService(&mockForwarder{}, 1024)
	put := httptest.NewRequest(http.MethodPut, "/"+tieredBulkDeleteBucket+"/"+tieredBulkDeleteKey, strings.NewReader(tieredBulkDeleteBody))
	putWriter := httptest.NewRecorder()
	if err := service.HandlePutObject(putWriter, put); err != nil {
		t.Fatalf("local-tier PUT: %v", err)
	}
	if putWriter.Code != http.StatusOK {
		t.Fatalf("local-tier PUT status = %d, want %d", putWriter.Code, http.StatusOK)
	}

	meta, _, found, err := objectCache.GetMetaWithVersion(context.Background(), tieredBulkDeleteBucket, tieredBulkDeleteKey)
	if err != nil {
		t.Fatalf("read local-tier metadata after PUT: %v", err)
	}
	if !found || meta == nil || meta.BodyUpstream {
		t.Fatalf("PUT did not create a local-tier object: found=%t meta=%+v", found, meta)
	}
	prior, _, known := service.captureMarkerPrior(context.Background(), tieredBulkDeleteBucket, tieredBulkDeleteKey)
	if !known || !reflect.DeepEqual(prior, meta) {
		t.Fatalf("pre-forward metadata lookup is not available: known=%t prior=%+v want=%+v", known, prior, meta)
	}

	body := &tieredBulkDeleteResponseBody{reader: bytes.NewReader([]byte(responseBody)), readErr: readErr}
	transport := forwarderTestTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || !r.URL.Query().Has("delete") {
			return nil, fmt.Errorf("unexpected forwarded request: %s %s", r.Method, r.URL)
		}
		requestBody, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, fmt.Errorf("read forwarded DeleteObjects request: %w", err)
		}
		if string(requestBody) != tieredBulkDeleteRequest {
			return nil, fmt.Errorf("forwarded DeleteObjects body = %q", requestBody)
		}
		if beforeResponse != nil {
			beforeResponse()
		}
		headers := make(http.Header)
		headers.Set("Content-Type", "application/xml")
		return &http.Response{
			StatusCode:    http.StatusOK,
			Header:        headers,
			Body:          body,
			ContentLength: -1,
			Request:       r,
		}, nil
	})
	service.forwarder = newTransparentForwarderForTest(transport)

	return &tieredBulkDeleteFixture{service: service, cache: objectCache, body: body}
}

func (f *tieredBulkDeleteFixture) delete(t *testing.T) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/"+tieredBulkDeleteBucket+"?delete", strings.NewReader(tieredBulkDeleteRequest))
	writer := httptest.NewRecorder()
	if err := f.service.HandleDeleteObjects(writer, request); err != nil {
		t.Fatalf("HandleDeleteObjects: %v", err)
	}
	return writer
}

type tieredBulkDeleteRead struct {
	status int
	header http.Header
	body   string
}

func readTieredBulkDeleteObject(t *testing.T, service *Service, method string) tieredBulkDeleteRead {
	t.Helper()
	request := httptest.NewRequest(method, "/"+tieredBulkDeleteBucket+"/"+tieredBulkDeleteKey, nil)
	writer := httptest.NewRecorder()
	var err error
	if method == http.MethodGet {
		err = service.HandleGetObject(writer, request)
	} else {
		err = service.HandleHeadObject(writer, request)
	}
	if err != nil {
		t.Fatalf("validated %s local read: %v", method, err)
	}
	return tieredBulkDeleteRead{status: writer.Code, header: writer.Header().Clone(), body: writer.Body.String()}
}

func assertTieredBulkDeleteObjectUnchanged(t *testing.T, f *tieredBulkDeleteFixture, beforeMeta *cache.CachedObjectMeta, beforeVersion uint64, beforeGet, beforeHead tieredBulkDeleteRead) {
	t.Helper()
	meta, version, found, err := f.cache.GetMetaWithVersion(context.Background(), tieredBulkDeleteBucket, tieredBulkDeleteKey)
	if err != nil {
		t.Fatalf("read local-tier metadata after DeleteObjects: %v", err)
	}
	if !found || meta == nil || version != beforeVersion || !reflect.DeepEqual(meta, beforeMeta) {
		t.Errorf("%s: metadata after DeleteObjects = found %t, version %d, meta %+v; want version %d, meta %+v", tieredBulkDeleteViolation, found, version, meta, beforeVersion, beforeMeta)
	}
	if got := readTieredBulkDeleteObject(t, f.service, http.MethodGet); !reflect.DeepEqual(got, beforeGet) {
		t.Errorf("%s: GET after DeleteObjects = %+v, want %+v", tieredBulkDeleteViolation, got, beforeGet)
	}
	if got := readTieredBulkDeleteObject(t, f.service, http.MethodHead); !reflect.DeepEqual(got, beforeHead) {
		t.Errorf("%s: HEAD after DeleteObjects = %+v, want %+v", tieredBulkDeleteViolation, got, beforeHead)
	}
}

func TestTieredBulkDeleteTruncatedFailurePreservesLocalObject(t *testing.T) {
	const rejectedPrefix = `<DeleteResult><Error><Key>local-key</Key><Code>AccessDenied</Code></Error>`
	tests := []struct {
		name         string
		responseBody string
		readErr      error
	}{
		{name: "truncated partial failure", responseBody: rejectedPrefix, readErr: io.ErrUnexpectedEOF},
		{name: "parseable result with response read error", responseBody: `<DeleteResult/>`, readErr: io.ErrUnexpectedEOF},
		{name: "fully received malformed result", responseBody: rejectedPrefix},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newTieredBulkDeleteFixture(t, tt.responseBody, tt.readErr, nil)
			beforeMeta, beforeVersion, found, err := f.cache.GetMetaWithVersion(context.Background(), tieredBulkDeleteBucket, tieredBulkDeleteKey)
			if err != nil || !found || beforeMeta == nil {
				t.Fatalf("capture pre-forward local metadata: found=%t meta=%+v err=%v", found, beforeMeta, err)
			}
			beforeGet := readTieredBulkDeleteObject(t, f.service, http.MethodGet)
			beforeHead := readTieredBulkDeleteObject(t, f.service, http.MethodHead)
			if beforeGet.status != http.StatusOK || beforeGet.body != tieredBulkDeleteBody || beforeHead.status != http.StatusOK {
				t.Fatalf("local object is not readable before bulk delete: GET=%+v HEAD=%+v", beforeGet, beforeHead)
			}

			writer := f.delete(t)
			if writer.Code != http.StatusOK || writer.Body.String() != tt.responseBody {
				t.Fatalf("upstream response = %d %q, want captured 200 body %q", writer.Code, writer.Body.String(), tt.responseBody)
			}
			if !f.body.closed {
				t.Fatal("transparent forwarder did not close the upstream response body")
			}
			if tt.readErr != nil && !f.body.readErrReturned {
				t.Fatal("upstream response body did not return io.ErrUnexpectedEOF")
			}

			assertTieredBulkDeleteObjectUnchanged(t, f, beforeMeta, beforeVersion, beforeGet, beforeHead)
		})
	}
}

func TestTieredBulkDeleteResultControls(t *testing.T) {
	t.Run("complete all-error result keeps local object", func(t *testing.T) {
		body := `<DeleteResult><Error><Key>local-key</Key><Code>AccessDenied</Code></Error></DeleteResult>`
		f := newTieredBulkDeleteFixture(t, body, nil, nil)
		beforeMeta, beforeVersion, found, err := f.cache.GetMetaWithVersion(context.Background(), tieredBulkDeleteBucket, tieredBulkDeleteKey)
		if err != nil || !found || beforeMeta == nil {
			t.Fatalf("read local-tier metadata: found=%t meta=%+v err=%v", found, beforeMeta, err)
		}
		beforeGet := readTieredBulkDeleteObject(t, f.service, http.MethodGet)
		beforeHead := readTieredBulkDeleteObject(t, f.service, http.MethodHead)

		writer := f.delete(t)
		if writer.Code != http.StatusOK || writer.Body.String() != body {
			t.Fatalf("upstream response = %d %q, want 200 %q", writer.Code, writer.Body.String(), body)
		}
		assertTieredBulkDeleteObjectUnchanged(t, f, beforeMeta, beforeVersion, beforeGet, beforeHead)
	})

	t.Run("quiet success removes local object", func(t *testing.T) {
		f := newTieredBulkDeleteFixture(t, `<DeleteResult/>`, nil, nil)
		writer := f.delete(t)
		if writer.Code != http.StatusOK || writer.Body.String() != `<DeleteResult/>` {
			t.Fatalf("upstream response = %d %q, want quiet 200 result", writer.Code, writer.Body.String())
		}
		if _, found, err := f.cache.GetMeta(context.Background(), tieredBulkDeleteBucket, tieredBulkDeleteKey); err != nil || found {
			t.Fatalf("quiet success retained local metadata: found=%t err=%v", found, err)
		}
		if got := readTieredBulkDeleteObject(t, f.service, http.MethodGet); got.status != http.StatusNotFound {
			t.Fatalf("GET after quiet success = %d, want %d", got.status, http.StatusNotFound)
		}
		if got := readTieredBulkDeleteObject(t, f.service, http.MethodHead); got.status != http.StatusNotFound {
			t.Fatalf("HEAD after quiet success = %d, want %d", got.status, http.StatusNotFound)
		}
	})

	t.Run("newer local PUT survives old delete token", func(t *testing.T) {
		forwardStarted := make(chan struct{})
		releaseResponse := make(chan struct{})
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(releaseResponse) }) }
		defer release()

		f := newTieredBulkDeleteFixture(t, `<DeleteResult/>`, nil, func() {
			close(forwardStarted)
			<-releaseResponse
		})
		deleteWriter := httptest.NewRecorder()
		deleteDone := make(chan error, 1)
		go func() {
			request := httptest.NewRequest(http.MethodPost, "/"+tieredBulkDeleteBucket+"?delete", strings.NewReader(tieredBulkDeleteRequest))
			deleteDone <- f.service.HandleDeleteObjects(deleteWriter, request)
		}()

		select {
		case <-forwardStarted:
		case err := <-deleteDone:
			t.Fatalf("DeleteObjects ended before forwarding: %v", err)
		}

		const newerBody = "newer local object body"
		put := httptest.NewRequest(http.MethodPut, "/"+tieredBulkDeleteBucket+"/"+tieredBulkDeleteKey, strings.NewReader(newerBody))
		putWriter := httptest.NewRecorder()
		if err := f.service.HandlePutObject(putWriter, put); err != nil || putWriter.Code != http.StatusOK {
			t.Fatalf("racing local PUT = status %d, error %v", putWriter.Code, err)
		}
		newerMeta, newerVersion, found, err := f.cache.GetMetaWithVersion(context.Background(), tieredBulkDeleteBucket, tieredBulkDeleteKey)
		if err != nil || !found || newerMeta == nil || newerMeta.BodyUpstream {
			t.Fatalf("racing PUT did not commit a local object: found=%t meta=%+v err=%v", found, newerMeta, err)
		}

		release()
		if err := <-deleteDone; err != nil {
			t.Fatalf("HandleDeleteObjects: %v", err)
		}
		if deleteWriter.Code != http.StatusOK || deleteWriter.Body.String() != `<DeleteResult/>` {
			t.Fatalf("DeleteObjects response = %d %q, want quiet 200 result", deleteWriter.Code, deleteWriter.Body.String())
		}
		current, version, found, err := f.cache.GetMetaWithVersion(context.Background(), tieredBulkDeleteBucket, tieredBulkDeleteKey)
		if err != nil || !found || current == nil || version != newerVersion || !reflect.DeepEqual(current, newerMeta) {
			t.Fatalf("old delete token removed or changed newer metadata: found=%t version=%d meta=%+v, want version=%d meta=%+v err=%v", found, version, current, newerVersion, newerMeta, err)
		}
		if got := readTieredBulkDeleteObject(t, f.service, http.MethodGet); got.status != http.StatusOK || got.body != newerBody {
			t.Fatalf("GET after racing PUT = %+v, want newer body %q", got, newerBody)
		}
		if got := readTieredBulkDeleteObject(t, f.service, http.MethodHead); got.status != http.StatusOK || got.header.Get("Content-Length") != fmt.Sprint(len(newerBody)) {
			t.Fatalf("HEAD after racing PUT = %+v, want newer local object", got)
		}
	})
}
