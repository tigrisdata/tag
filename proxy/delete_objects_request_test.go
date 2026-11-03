package proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

func TestDeleteObjectsDisabledStreamsRequest(t *testing.T) {
	for _, knownLength := range []bool{false, true} {
		t.Run(strconv.FormatBool(knownLength), func(t *testing.T) {
			const body = "<Delete><Object><Key>streamed-object-key</Key></Object></Delete>"
			const result = "<DeleteResult><Error><Key>streamed-object-key</Key><Code>AccessDenied</Code></Error></DeleteResult>"
			first := make(chan struct{})
			upstreamResult := make(chan error, 1)
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				prefix := make([]byte, 8)
				_, err := io.ReadFull(r.Body, prefix)
				close(first)
				rest, readErr := io.ReadAll(r.Body)
				if err != nil || readErr != nil || string(append(prefix, rest...)) != body {
					upstreamResult <- fmt.Errorf("upstream bytes differ: prefix=%q rest=%q errors=%v, %v", prefix, rest, err, readErr)
				} else {
					upstreamResult <- nil
				}
				w.Header().Set("Content-Type", "application/xml")
				w.Header().Set("X-Amz-Request-Id", "delete-request")
				w.Header().Set("Content-Length", strconv.Itoa(len(result)))
				_, _ = io.WriteString(w, result)
			}))
			defer origin.Close()
			cfg := config.NewDefault()
			f := NewForwarder(nil, origin.URL, cfg.Upstream.Region, cfg.Upstream.MaxIdleConnsPerHost, auth.NewProxySigner("delete-test", "delete-secret"), nil)
			defer f.(*transparentForwarder).httpClient.CloseIdleConnections()
			svc := NewService(f, cache.NewDisabledCache(), cfg)
			reader, writer := io.Pipe()
			defer reader.Close()
			defer writer.Close()
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			go func() {
				_, err := io.WriteString(writer, body[:8])
				<-release
				if err == nil {
					_, err = io.WriteString(writer, body[8:])
				}
				_ = writer.CloseWithError(err)
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			r := httptest.NewRequest(http.MethodPost, "/delete-bucket?delete", reader).WithContext(ctx)
			if knownLength {
				r.ContentLength = int64(len(body))
			} else {
				r.ContentLength = -1
			}
			w := httptest.NewRecorder()
			done := make(chan error, 1)
			go func() { done <- svc.HandleDeleteObjects(w, r) }()
			select {
			case <-first:
			case <-ctx.Done():
				unblock()
				<-done
				t.Fatal("upstream did not receive the prefix while the request tail was held")
			}
			unblock()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if err := <-upstreamResult; err != nil {
				t.Fatal(err)
			}
			if w.Code != http.StatusOK || w.Header().Get("Content-Type") != "application/xml" || w.Header().Get("X-Amz-Request-Id") != "delete-request" || !bytes.Equal(w.Body.Bytes(), []byte(result)) {
				t.Fatalf("changed response: %d %v %q", w.Code, w.Header(), w.Body.String())
			}
		})
	}
}

func TestDeleteObjectsDisabledPreservesResponse(t *testing.T) {
	for _, body := range []string{"", "<Delete/>", "malformed XML", "<Delete><Object><Key>a&amp;b</Key></Object></Delete>"} {
		for _, status := range []int{http.StatusOK, http.StatusBadRequest, http.StatusForbidden} {
			t.Run(fmt.Sprintf("%s/%d", body, status), func(t *testing.T) {
				origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					got, err := io.ReadAll(r.Body)
					if err != nil || string(got) != body {
						t.Errorf("request = %q, %v; want %q", got, err, body)
					}
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(status)
					_, _ = io.WriteString(w, "<DeleteResult/>")
				}))
				defer origin.Close()
				cfg := config.NewDefault()
				f := NewForwarder(nil, origin.URL, cfg.Upstream.Region, cfg.Upstream.MaxIdleConnsPerHost, auth.NewProxySigner("delete-test", "delete-secret"), nil)
				defer f.(*transparentForwarder).httpClient.CloseIdleConnections()
				svc := NewService(f, cache.NewDisabledCache(), cfg)
				r := httptest.NewRequest(http.MethodPost, "/delete-bucket?delete", strings.NewReader(body))
				w := httptest.NewRecorder()
				if err := svc.HandleDeleteObjects(w, r); err != nil {
					t.Fatal(err)
				}
				if w.Code != status || w.Header().Get("Content-Type") != "application/xml" || w.Body.String() != "<DeleteResult/>" {
					t.Fatalf("changed response: %d %v %q", w.Code, w.Header(), w.Body.String())
				}
			})
		}
	}
}
