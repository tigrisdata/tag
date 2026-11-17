package proxy

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/tigrisdata/tag/metrics"
)

func TestHandleDeleteObjectsCacheDisabledResponse(t *testing.T) {
	transportErr := errors.New("upstream unavailable")
	for _, tt := range []struct {
		name         string
		status       int
		body         string
		readErr      error
		transportErr error
	}{
		{name: "success", status: http.StatusOK, body: `<DeleteResult><Deleted><Key>key</Key></Deleted></DeleteResult>`, readErr: io.EOF},
		{name: "rejected", status: http.StatusForbidden, body: `<Error><Code>AccessDenied</Code></Error>`, readErr: io.EOF},
		{name: "empty", status: http.StatusNoContent, readErr: io.EOF},
		{name: "incomplete", status: http.StatusOK, body: `<DeleteResult><Deleted>`, readErr: io.ErrUnexpectedEOF},
		{name: "transport error", transportErr: transportErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			requestBody := `<Delete><Object><Key>key</Key></Object></Delete>`
			body := &failedResponseBody{reader: strings.NewReader(tt.body), err: tt.readErr}
			headers := http.Header{"Content-Type": {"application/xml"}, "X-Amz-Request-Id": {"origin-request"}, "X-Result": {"one", "two"}}
			forwarder := newTransparentForwarderForTest(forwarderTestTransport(func(r *http.Request) (*http.Response, error) {
				got, err := io.ReadAll(r.Body)
				if err != nil || string(got) != requestBody {
					t.Fatalf("forwarded request = %q, %v; want %q", got, err, requestBody)
				}
				if tt.transportErr != nil {
					return nil, tt.transportErr
				}
				upstreamHeaders := headers.Clone()
				upstreamHeaders.Set(signingKeysHeader, "internal-only")
				return &http.Response{StatusCode: tt.status, Header: upstreamHeaders, Body: body, ContentLength: -1}, nil
			}))
			service, _ := newTestService(forwarder, false)
			request := httptest.NewRequest(http.MethodPost, "/test-bucket?delete", strings.NewReader(requestBody))
			request.Header.Set("Authorization", "client-auth")
			writer := httptest.NewRecorder()
			outBytes := metrics.BytesTransferred.WithLabelValues("out")
			before := testutil.ToFloat64(outBytes)
			err := service.HandleDeleteObjects(writer, request)
			if tt.transportErr != nil {
				if !errors.Is(err, tt.transportErr) {
					t.Fatalf("forwarding error = %v, want %v", err, tt.transportErr)
				}
				if len(writer.Header()) != 0 || writer.Body.Len() != 0 {
					t.Fatal("transport failure wrote a response")
				}
				return
			}
			// Capture forwarding reports incomplete delivery in Complete, not its
			// returned error. Cache-disabled deletes preserve that behavior.
			if err != nil {
				t.Fatalf("HandleDeleteObjects: %v", err)
			}
			if writer.Code != tt.status || writer.Body.String() != tt.body || !reflect.DeepEqual(writer.Header(), headers) {
				t.Fatalf("response = %d, %v, %q; want %d, %v, %q", writer.Code, writer.Header(), writer.Body.String(), tt.status, headers, tt.body)
			}
			if !body.closed {
				t.Error("upstream response body was not closed")
			}
			if got := testutil.ToFloat64(outBytes) - before; got != float64(len(tt.body)) {
				t.Errorf("outbound bytes = %v, want %d", got, len(tt.body))
			}
		})
	}
}

func TestHandleDeleteObjectsCacheDisabledWriteError(t *testing.T) {
	body := &failedResponseBody{reader: strings.NewReader(strings.Repeat("x", 64*1024)), err: io.EOF}
	forwarder := newTransparentForwarderForTest(forwarderTestTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body, ContentLength: -1}, nil
	}))
	service, _ := newTestService(forwarder, false)
	request := httptest.NewRequest(http.MethodPost, "/test-bucket?delete", strings.NewReader(`<Delete/>`))
	writer := &partialResponseWriter{header: make(http.Header)}
	outBytes := metrics.BytesTransferred.WithLabelValues("out")
	before := testutil.ToFloat64(outBytes)
	if err := service.HandleDeleteObjects(writer, request); err != nil {
		t.Fatalf("HandleDeleteObjects: %v", err)
	}
	if writer.statusCode != http.StatusOK || writer.bytesAccepted != 3 || !body.closed {
		t.Fatalf("delivery = status %d, %d bytes, closed=%t; want 200, 3 bytes, closed", writer.statusCode, writer.bytesAccepted, body.closed)
	}
	if got := testutil.ToFloat64(outBytes) - before; got != 3 {
		t.Errorf("outbound bytes = %v, want 3", got)
	}
}
