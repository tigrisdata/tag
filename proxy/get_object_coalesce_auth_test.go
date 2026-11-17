package proxy

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// coalesceAuthHeader marks a test request as one TAG could not validate locally.
const coalesceAuthHeader = "X-Test-Unvalidated"

// runCoalesceAuthSchedule starts a first GET whose upstream request is held
// before any response headers, so its broadcast is still open to subscribers,
// then starts a second GET for the same object. The second caller must reach
// upstream itself; if it had joined the first caller's broadcast it would wait
// on the held request and never arrive. Returns both responses.
func runCoalesceAuthSchedule(t *testing.T, cacheEnabled, firstValidated, secondValidated bool) (first, second *httptest.ResponseRecorder, upstreamCalls int) {
	t.Helper()
	const body = "private-object-body"

	firstEntered := make(chan struct{})
	secondEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFirst) }) }

	var mu sync.Mutex
	calls := 0
	forwarder := &mockForwarder{
		validateFunc: func(r *http.Request) (AuthResult, string, string, error) {
			if r.Header.Get(coalesceAuthHeader) != "" {
				return AuthNotValidated, "proxy-access", "proxy-secret", nil
			}
			return AuthValidated, "proxy-access", "proxy-secret", nil
		},
		doRequestFunc: func(_ context.Context, r *http.Request, _, _ string) (*http.Response, error) {
			mu.Lock()
			calls++
			call := calls
			mu.Unlock()
			switch call {
			case 1:
				close(firstEntered)
				<-releaseFirst
			case 2:
				close(secondEntered)
			default:
				return nil, fmt.Errorf("unexpected upstream request %d", call)
			}
			// The origin authorizes each request on its own: an unvalidated
			// caller here stands for one Tigris would refuse.
			if r.Header.Get(coalesceAuthHeader) != "" {
				return &http.Response{
					StatusCode: http.StatusForbidden,
					Header:     http.Header{"Content-Length": {"0"}},
					Body:       http.NoBody,
				}, nil
			}
			return &http.Response{
				StatusCode:    http.StatusOK,
				Header:        http.Header{"Content-Length": {fmt.Sprint(len(body))}, "Etag": {`"etag"`}},
				Body:          readCloser{strings.NewReader(body)},
				ContentLength: int64(len(body)),
			}, nil
		},
	}
	svc, _ := newTestService(forwarder, cacheEnabled)

	newRequest := func(validated bool) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/coalesce-bucket/private-key", nil)
		if !validated {
			r.Header.Set(coalesceAuthHeader, "1")
		}
		return r
	}

	first = httptest.NewRecorder()
	second = httptest.NewRecorder()
	var handlers sync.WaitGroup
	errs := make(chan error, 2)
	run := func(w *httptest.ResponseRecorder, r *http.Request) {
		handlers.Add(1)
		go func() {
			defer handlers.Done()
			errs <- svc.HandleGetObject(w, r)
		}()
	}
	defer func() {
		release()
		handlers.Wait()
	}()

	run(first, newRequest(firstValidated))
	awaitGetBroadcastSignal(t, "first caller's upstream request", firstEntered)
	run(second, newRequest(secondValidated))
	awaitGetBroadcastSignal(t, "second caller's own upstream request", secondEntered)
	release()
	handlers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("HandleGetObject: %v", err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	return first, second, calls
}

type readCloser struct{ *strings.Reader }

func (readCloser) Close() error { return nil }

// An unvalidated caller must not receive a validated caller's in-flight object.
func TestHandleGetObject_UnvalidatedCallerDoesNotJoinValidatedFetch(t *testing.T) {
	for _, cacheEnabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("cache=%t", cacheEnabled), func(t *testing.T) {
			first, second, calls := runCoalesceAuthSchedule(t, cacheEnabled, true, false)
			if calls != 2 {
				t.Fatalf("upstream calls = %d, want 2", calls)
			}
			if first.Code != http.StatusOK || first.Body.String() != "private-object-body" {
				t.Errorf("validated caller got %d %q, want 200 with the object", first.Code, first.Body.String())
			}
			if second.Code != http.StatusForbidden || second.Body.Len() != 0 {
				t.Errorf("unvalidated caller got %d %q, want the origin's own 403", second.Code, second.Body.String())
			}
		})
	}
}

// A validated caller must not inherit an unvalidated caller's upstream answer.
func TestHandleGetObject_ValidatedCallerDoesNotJoinUnvalidatedFetch(t *testing.T) {
	for _, cacheEnabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("cache=%t", cacheEnabled), func(t *testing.T) {
			first, second, calls := runCoalesceAuthSchedule(t, cacheEnabled, false, true)
			if calls != 2 {
				t.Fatalf("upstream calls = %d, want 2", calls)
			}
			if first.Code != http.StatusForbidden {
				t.Errorf("unvalidated caller got %d, want the origin's own 403", first.Code)
			}
			if second.Code != http.StatusOK || second.Body.String() != "private-object-body" {
				t.Errorf("validated caller got %d %q, want 200 with the object", second.Code, second.Body.String())
			}
		})
	}
}
