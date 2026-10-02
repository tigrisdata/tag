package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tigrisdata/tag/auth"
)

func TestSigningForwarderStagingCleanup(t *testing.T) {
	var dispatched atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dispatched.Add(1)
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	credentials := auth.NewCredentialStore()
	credentials.AddCredential(signedStreamTestAccessKey, signedStreamTestSecretKey)
	forwarder := NewForwarder(credentials, upstream.URL, "us-east-1", 1, nil, nil)
	payload := []byte("staged signed payload")
	seed := newSignedStreamSeed(t, upstream.URL, StreamingPayloadHash, len(payload), true)
	stream := makeSignedStreamWire(t, seed, payload)

	t.Run("success removes stage file", func(t *testing.T) {
		tempDir := t.TempDir()
		t.Setenv("TMPDIR", tempDir)
		before := dispatched.Load()
		if err := forwarder.Forward(t.Context(), httptest.NewRecorder(), seed.request(t, stream.wire)); err != nil {
			t.Fatalf("forward valid stream: %v", err)
		}
		if dispatched.Load() != before+1 {
			t.Fatalf("valid stream dispatch count = %d, want 1", dispatched.Load()-before)
		}
		assertStageDirectoryEmpty(t, tempDir)
	})

	t.Run("zero-byte stream removes its unused stage file", func(t *testing.T) {
		tempDir := t.TempDir()
		t.Setenv("TMPDIR", tempDir)
		emptySeed := newSignedStreamSeed(t, upstream.URL, StreamingPayloadHash, 0, true)
		emptyStream := makeSignedStreamWire(t, emptySeed)
		before := dispatched.Load()
		if err := forwarder.Forward(t.Context(), httptest.NewRecorder(), emptySeed.request(t, emptyStream.wire)); err != nil {
			t.Fatalf("forward valid empty stream: %v", err)
		}
		if dispatched.Load() != before+1 {
			t.Fatalf("empty stream dispatch count = %d, want 1", dispatched.Load()-before)
		}
		assertStageDirectoryEmpty(t, tempDir)
	})

	t.Run("invalid signature removes stage file", func(t *testing.T) {
		tempDir := t.TempDir()
		t.Setenv("TMPDIR", tempDir)
		badWire := changeStreamSignature(t, stream.wire, stream.terminalSignatureStart)
		before := dispatched.Load()
		err := forwarder.Forward(t.Context(), httptest.NewRecorder(), seed.request(t, badWire))
		if authErr, ok := IsAuthError(err); !ok || authErr.Code != ErrCodeSignatureMismatch {
			t.Fatalf("invalid terminal signature error = %v, want signature-mismatch AuthError", err)
		}
		if dispatched.Load() != before {
			t.Fatalf("invalid terminal signature dispatched upstream %d times", dispatched.Load()-before)
		}
		assertStageDirectoryEmpty(t, tempDir)
	})

	t.Run("cancellation interrupts a blocked ingress read and removes stage file", func(t *testing.T) {
		tempDir := t.TempDir()
		t.Setenv("TMPDIR", tempDir)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		body := &blockingSignedStreamBody{readStarted: make(chan struct{}), closed: make(chan struct{})}
		defer body.Close()
		req := seed.request(t, stream.wire).WithContext(ctx)
		req.Body = body
		before := dispatched.Load()
		done := make(chan error, 1)
		go func() {
			done <- forwarder.Forward(ctx, httptest.NewRecorder(), req)
		}()
		select {
		case <-body.readStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("forwarder did not begin reading the signed stream")
		}
		cancel()
		var err error
		select {
		case err = <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("cancellation did not interrupt the signed-stream read")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled staging error = %v, want context.Canceled", err)
		}
		if dispatched.Load() != before {
			t.Fatalf("canceled stream dispatched upstream %d times", dispatched.Load()-before)
		}
		assertStageDirectoryEmpty(t, tempDir)
	})

	t.Run("temporary storage creation failure does not dispatch", func(t *testing.T) {
		parent := t.TempDir()
		blockedPath := filepath.Join(parent, "not-a-directory")
		if err := os.WriteFile(blockedPath, []byte("sentinel"), 0o600); err != nil {
			t.Fatalf("create blocked temp path: %v", err)
		}
		t.Setenv("TMPDIR", blockedPath)
		before := dispatched.Load()
		err := forwarder.Forward(t.Context(), httptest.NewRecorder(), seed.request(t, stream.wire))
		if err == nil {
			t.Fatal("forward succeeded without a usable staging directory")
		}
		if dispatched.Load() != before {
			t.Fatalf("staging creation failure dispatched upstream %d times", dispatched.Load()-before)
		}
		if data, readErr := os.ReadFile(blockedPath); readErr != nil || string(data) != "sentinel" {
			t.Fatalf("blocked temp path changed: data=%q err=%v", data, readErr)
		}
	})
}

type blockingSignedStreamBody struct {
	readStarted chan struct{}
	closed      chan struct{}
	readOnce    sync.Once
	closeOnce   sync.Once
}

func (b *blockingSignedStreamBody) Read([]byte) (int, error) {
	b.readOnce.Do(func() { close(b.readStarted) })
	<-b.closed
	return 0, io.ErrClosedPipe
}

func (b *blockingSignedStreamBody) Close() error {
	b.closeOnce.Do(func() { close(b.closed) })
	return nil
}

func TestSigningForwarderTransfersStagedBodyOwnership(t *testing.T) {
	payload := []byte("transport-owned staged payload")
	for _, route := range []string{"Forward", "ForwardWithCapture", "ForwardTeeingBody"} {
		t.Run(route, func(t *testing.T) {
			tempDir := t.TempDir()
			t.Setenv("TMPDIR", tempDir)
			upstream := httptest.NewServer(http.NotFoundHandler())
			defer upstream.Close()

			credentials := auth.NewCredentialStore()
			credentials.AddCredential(signedStreamTestAccessKey, signedStreamTestSecretKey)
			forwarder := NewForwarder(credentials, upstream.URL, "us-east-1", 1, nil, nil).(*signingForwarder)
			transport := &delayedSignedBodyTransport{
				readStarted: make(chan struct{}),
				release:     make(chan struct{}),
				completed:   make(chan stagedBodyReadResult, 1),
			}
			defer func() {
				select {
				case <-transport.release:
				default:
					close(transport.release)
				}
			}()
			forwarder.httpClient.Transport = transport

			seed := newSignedStreamSeed(t, upstream.URL, StreamingPayloadHash, len(payload), true)
			stream := makeSignedStreamWire(t, seed, payload)
			request := seed.request(t, stream.wire)
			var err error
			switch route {
			case "Forward":
				err = forwarder.Forward(t.Context(), httptest.NewRecorder(), request)
			case "ForwardWithCapture":
				_, err = forwarder.ForwardWithCapture(t.Context(), httptest.NewRecorder(), request)
			case "ForwardTeeingBody":
				var tee bytes.Buffer
				_, _, _, _, err = forwarder.ForwardTeeingBody(t.Context(), httptest.NewRecorder(), request, &tee)
				defer func() {
					if err == nil && !bytes.Equal(tee.Bytes(), payload) {
						t.Errorf("tee body = %q, want %q", tee.Bytes(), payload)
					}
				}()
			}
			if err != nil {
				t.Fatalf("forward signed stream: %v", err)
			}
			select {
			case <-transport.readStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("round trip did not begin")
			}
			select {
			case result := <-transport.completed:
				t.Fatalf("transport consumed the body before release: %q, %v", result.body, result.readErr)
			default:
			}

			close(transport.release)
			select {
			case result := <-transport.completed:
				if result.readErr != nil {
					t.Fatalf("transport read staged body after forwarding returned: %v", result.readErr)
				}
				if result.closeErr != nil {
					t.Fatalf("transport closed staged body: %v", result.closeErr)
				}
				if !bytes.Equal(result.body, payload) {
					t.Fatalf("transport body = %q, want %q", result.body, payload)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("transport did not complete staged-body read")
			}
			assertStageDirectoryEmpty(t, tempDir)
		})
	}
}

type stagedBodyReadResult struct {
	body     []byte
	readErr  error
	closeErr error
}

type delayedSignedBodyTransport struct {
	readStarted chan struct{}
	release     chan struct{}
	completed   chan stagedBodyReadResult
}

func (t *delayedSignedBodyTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	body := request.Body
	close(t.readStarted)
	go func() {
		<-t.release
		data, readErr := io.ReadAll(body)
		closeErr := body.Close()
		t.completed <- stagedBodyReadResult{body: data, readErr: readErr, closeErr: closeErr}
	}()
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       http.NoBody,
		Request:    request,
	}, nil
}

func assertStageDirectoryEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read staging directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("staging directory retains files after request: %v", entries)
	}
}
