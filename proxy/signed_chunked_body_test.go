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

func TestStatusRecorderUnwrapsReadDeadline(t *testing.T) {
	writer := &readDeadlineResponseWriter{ResponseWriter: httptest.NewRecorder()}
	wrapped := &statusRecorder{ResponseWriter: writer}
	deadline := time.Now()
	if err := http.NewResponseController(wrapped).SetReadDeadline(deadline); err != nil {
		t.Fatalf("set read deadline through statusRecorder: %v", err)
	}
	if !writer.deadline.Equal(deadline) {
		t.Fatalf("underlying read deadline = %v, want %v", writer.deadline, deadline)
	}
}

type readDeadlineResponseWriter struct {
	http.ResponseWriter
	deadline time.Time
}

func (w *readDeadlineResponseWriter) SetReadDeadline(deadline time.Time) error {
	w.deadline = deadline
	return nil
}

type blockingReadDeadlineWriter struct {
	http.ResponseWriter
	body    io.Closer
	started chan struct{}
	release chan struct{}
	start   sync.Once
	unblock sync.Once
}

func (w *blockingReadDeadlineWriter) SetReadDeadline(time.Time) error {
	w.start.Do(func() { close(w.started) })
	_ = w.body.Close()
	<-w.release
	return nil
}

func (w *blockingReadDeadlineWriter) releaseDeadline() {
	w.unblock.Do(func() { close(w.release) })
}

func TestSigningForwarderWaitsForCancellationCallbackBeforeReturning(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	budget := newSignedStreamStageBudget(func(string) (signedStreamStageSpace, error) {
		return signedStreamStageSpace{availableBytes: 1 << 20, blockSize: 1}, nil
	})
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(signedStreamTestAccessKey, signedStreamTestSecretKey)
	forwarder := NewForwarder(credentials, "http://stage.test", "us-east-1", 1, nil, nil).(*signingForwarder)
	forwarder.stageBudgetOverride = budget
	body := &blockingSignedStreamBody{readStarted: make(chan struct{}), closed: make(chan struct{})}
	writer := &blockingReadDeadlineWriter{
		ResponseWriter: httptest.NewRecorder(),
		body:           body,
		started:        make(chan struct{}),
		release:        make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer func() {
		cancel()
		_ = body.Close()
		writer.releaseDeadline()
	}()
	payload := []byte("staging before canceled read")
	seed := newSignedStreamSeed(t, "http://stage.test", StreamingPayloadHash, len(payload), true)
	stream := makeSignedStreamWire(t, seed, payload)
	request := seed.request(t, stream.wire).WithContext(ctx)
	request.Body = body
	done := make(chan error, 1)
	go func() { done <- forwarder.Forward(ctx, writer, request) }()
	select {
	case <-body.readStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("forwarder did not begin the ingress body read")
	}
	budget.mu.Lock()
	reservedBeforeCancel := budget.reserved
	budget.mu.Unlock()
	if reservedBeforeCancel == 0 {
		t.Fatal("staging reservation was not held before the body read")
	}
	cancel()
	select {
	case <-writer.started:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not invoke the read-deadline callback")
	}
	select {
	case err := <-done:
		t.Fatalf("forwarding returned before the cancellation callback completed: %v", err)
	default:
	}
	writer.releaseDeadline()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled forwarding error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("forwarding did not return after the cancellation callback completed")
	}
	budget.mu.Lock()
	reservedAfterCancel := budget.reserved
	budget.mu.Unlock()
	if reservedAfterCancel != 0 {
		t.Fatalf("canceled forwarding retained %d reserved bytes", reservedAfterCancel)
	}
	assertStageDirectoryEmpty(t, tempDir)
}

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

func TestSigningForwarderRejectsSignedStreamLengthMismatch(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	var dispatched atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dispatched.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	credentials := auth.NewCredentialStore()
	credentials.AddCredential(signedStreamTestAccessKey, signedStreamTestSecretKey)
	forwarder := NewForwarder(credentials, upstream.URL, "us-east-1", 1, nil, nil).(*signingForwarder)
	forwarder.stageBudgetOverride = newSignedStreamStageBudget(func(string) (signedStreamStageSpace, error) {
		return signedStreamStageSpace{availableBytes: 1 << 30, blockSize: 1}, nil
	})

	methods := []struct {
		name string
		call func(*http.Request) error
	}{
		{
			name: "Forward",
			call: func(r *http.Request) error {
				return forwarder.Forward(r.Context(), httptest.NewRecorder(), r)
			},
		},
		{
			name: "ForwardWithCapture",
			call: func(r *http.Request) error {
				_, err := forwarder.ForwardWithCapture(r.Context(), httptest.NewRecorder(), r)
				return err
			},
		},
		{
			name: "ForwardTeeingBody",
			call: func(r *http.Request) error {
				_, _, _, _, err := forwarder.ForwardTeeingBody(r.Context(), httptest.NewRecorder(), r, io.Discard)
				return err
			},
		},
	}
	mismatches := []struct {
		name           string
		declaredLength int
		payload        []byte
	}{
		{name: "declared zero with nonempty payload", declaredLength: 0, payload: []byte("not empty")},
		{name: "declared length shorter than payload", declaredLength: 3, payload: []byte("longer")},
		{name: "declared length longer than payload", declaredLength: 8, payload: []byte("short")},
	}
	for _, method := range methods {
		t.Run(method.name, func(t *testing.T) {
			for _, mismatch := range mismatches {
				t.Run(mismatch.name, func(t *testing.T) {
					seed := newSignedStreamSeed(t, upstream.URL, StreamingPayloadHash, mismatch.declaredLength, true)
					stream := makeSignedStreamWire(t, seed, mismatch.payload)
					before := dispatched.Load()
					err := method.call(seed.request(t, stream.wire))
					if !errors.Is(err, errSignedStreamLengthMismatch) || dispatched.Load() != before {
						t.Errorf("length mismatch err=%v, dispatches=%d; want length rejection before dispatch", err, dispatched.Load()-before)
					}
				})
			}
		})
	}
	assertStageDirectoryEmpty(t, tempDir)
}

func TestSigningForwarderForwardsUnknownLengthSignedStream(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	var (
		dispatched atomic.Int32
		mu         sync.Mutex
		seen       []signedStreamObservation
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dispatched.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		seen = append(seen, signedStreamObservation{
			body:          body,
			contentHash:   r.Header.Get("X-Amz-Content-Sha256"),
			contentLength: r.ContentLength,
			decodedLength: r.Header.Get("X-Amz-Decoded-Content-Length"),
		})
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	credentials := auth.NewCredentialStore()
	credentials.AddCredential(signedStreamTestAccessKey, signedStreamTestSecretKey)
	forwarder := NewForwarder(credentials, upstream.URL, "us-east-1", 1, nil, nil).(*signingForwarder)
	forwarder.stageBudgetOverride = newSignedStreamStageBudget(func(string) (signedStreamStageSpace, error) {
		return signedStreamStageSpace{availableBytes: 1 << 20, blockSize: 1}, nil
	})
	payload := []byte("unknown decoded length")
	seed := newSignedStreamSeed(t, upstream.URL, StreamingPayloadHash, -1, true)
	stream := makeSignedStreamWire(t, seed, payload)
	response := httptest.NewRecorder()
	if err := forwarder.Forward(t.Context(), response, seed.request(t, stream.wire)); err != nil {
		t.Fatalf("forward signed stream without decoded-length header: %v", err)
	}
	if response.Code != http.StatusOK || dispatched.Load() != 1 {
		t.Fatalf("unknown-length response=%d dispatches=%d, want one successful dispatch", response.Code, dispatched.Load())
	}
	got, ok := observationAt(&mu, &seen, 0)
	if !ok || !bytes.Equal(got.body, payload) || got.contentHash != "UNSIGNED-PAYLOAD" || got.contentLength != -1 || got.decodedLength != "" {
		t.Fatalf("unknown-length forwarded observation=%+v want exact bytes=%q and unknown length", got, payload)
	}
	assertStageDirectoryEmpty(t, tempDir)
}

func TestSigningForwarderPreservesLargeChunkSegmentation(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	var (
		dispatched atomic.Int32
		mu         sync.Mutex
		seen       []signedStreamObservation
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dispatched.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		seen = append(seen, signedStreamObservation{
			body:          body,
			contentHash:   r.Header.Get("X-Amz-Content-Sha256"),
			contentLength: r.ContentLength,
			contentCoding: r.Header.Get("Content-Encoding"),
			decodedLength: r.Header.Get("X-Amz-Decoded-Content-Length"),
		})
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	credentials := auth.NewCredentialStore()
	credentials.AddCredential(signedStreamTestAccessKey, signedStreamTestSecretKey)
	forwarder := NewForwarder(credentials, upstream.URL, "us-east-1", 1, nil, nil)
	forwarder.(*signingForwarder).stageBudgetOverride = newSignedStreamStageBudget(func(string) (signedStreamStageSpace, error) {
		return signedStreamStageSpace{availableBytes: 1 << 20, blockSize: 1}, nil
	})
	payload := bytes.Repeat([]byte("x"), 256<<10)
	seed := newSignedStreamSeed(t, upstream.URL, StreamingPayloadHash, len(payload), true)
	t.Run("one large chunk", func(t *testing.T) {
		stream := makeSignedStreamWire(t, seed, payload)
		assertSignedStreamForwarded(t, func(r *http.Request) (int, error) {
			w := httptest.NewRecorder()
			err := forwarder.Forward(r.Context(), w, r)
			return w.Code, err
		}, seed, stream.wire, payload, &dispatched, &mu, &seen)
		assertStageDirectoryEmpty(t, tempDir)
	})
	t.Run("same payload split into smaller chunks", func(t *testing.T) {
		chunks := [][]byte{payload[:64<<10], payload[64<<10 : 128<<10], payload[128<<10 : 192<<10], payload[192<<10:]}
		stream := makeSignedStreamWire(t, seed, chunks...)
		assertSignedStreamForwarded(t, func(r *http.Request) (int, error) {
			w := httptest.NewRecorder()
			err := forwarder.Forward(r.Context(), w, r)
			return w.Code, err
		}, seed, stream.wire, payload, &dispatched, &mu, &seen)
		assertStageDirectoryEmpty(t, tempDir)
	})
}

func TestSignedStreamStageBudgetSerializesConcurrentReservations(t *testing.T) {
	budget := newSignedStreamStageBudget(func(string) (signedStreamStageSpace, error) {
		return signedStreamStageSpace{availableBytes: 100, blockSize: 1}, nil
	})
	dir := t.TempDir()
	reservations := []*signedStreamStageReservation{
		{budget: budget, dir: dir},
		{budget: budget, dir: dir},
	}
	start := make(chan struct{})
	ready := sync.WaitGroup{}
	ready.Add(len(reservations))
	type result struct {
		reservation *signedStreamStageReservation
		err         error
	}
	results := make(chan result, len(reservations))
	for _, reservation := range reservations {
		go func(reservation *signedStreamStageReservation) {
			ready.Done()
			<-start
			results <- result{reservation: reservation, err: reservation.reservePayload(40)}
		}(reservation)
	}
	ready.Wait()
	close(start)

	var accepted *signedStreamStageReservation
	for range reservations {
		result := <-results
		if result.err == nil {
			if accepted != nil {
				t.Fatal("both concurrent requests reserved 40 bytes from a 50-byte pool")
			}
			accepted = result.reservation
		} else if !errors.Is(result.err, errSignedStreamStagingCapacity) {
			t.Fatalf("concurrent reservation error = %v, want capacity error", result.err)
		}
	}
	if accepted == nil {
		t.Fatal("neither request acquired the available staging reservation")
	}
	accepted.release(true)
}

func TestSigningForwarderReservesKnownLengthBeforeReading(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	var dispatched atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		dispatched.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	budget := newSignedStreamStageBudget(func(string) (signedStreamStageSpace, error) {
		return signedStreamStageSpace{availableBytes: 8, blockSize: 1}, nil
	})
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(signedStreamTestAccessKey, signedStreamTestSecretKey)
	forwarder := NewForwarder(credentials, upstream.URL, "us-east-1", 1, nil, nil).(*signingForwarder)
	forwarder.stageBudgetOverride = budget

	payload := []byte("12345")
	seed := newSignedStreamSeed(t, upstream.URL, StreamingPayloadHash, len(payload), true)
	stream := makeSignedStreamWire(t, seed, payload)
	body := &readCountingSignedStreamBody{reader: bytes.NewReader(stream.wire)}
	request := seed.request(t, stream.wire)
	request.Body = body
	err := forwarder.Forward(t.Context(), httptest.NewRecorder(), request)
	if !errors.Is(err, errSignedStreamStagingCapacity) {
		t.Fatalf("forward without enough staging reservation = %v, want capacity error", err)
	}
	if body.reads.Load() != 0 {
		t.Fatalf("request body read %d times before its full decoded length was reserved", body.reads.Load())
	}
	if dispatched.Load() != 0 {
		t.Fatalf("capacity rejection dispatched upstream %d times", dispatched.Load())
	}
	assertStageDirectoryEmpty(t, tempDir)
}

func TestSigningForwarderReservesUnknownChunkBeforeCopy(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	var dispatched atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		dispatched.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	budget := newSignedStreamStageBudget(func(string) (signedStreamStageSpace, error) {
		return signedStreamStageSpace{availableBytes: 8, blockSize: 1}, nil
	})
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(signedStreamTestAccessKey, signedStreamTestSecretKey)
	forwarder := NewForwarder(credentials, upstream.URL, "us-east-1", 1, nil, nil).(*signingForwarder)
	forwarder.stageBudgetOverride = budget

	payload := []byte("12345")
	seed := newSignedStreamSeed(t, upstream.URL, StreamingPayloadHash, -1, true)
	stream := makeSignedStreamWire(t, seed, payload)
	headerEnd := bytes.Index(stream.wire, []byte("\r\n")) + len("\r\n")
	body := &splitSignedStreamBody{first: stream.wire[:headerEnd], rest: stream.wire[headerEnd:]}
	request := seed.request(t, stream.wire)
	request.Body = body
	err := forwarder.Forward(t.Context(), httptest.NewRecorder(), request)
	if !errors.Is(err, errSignedStreamStagingCapacity) {
		t.Fatalf("forward unknown-length chunk without a reservation = %v, want capacity error", err)
	}
	if body.reads.Load() != 1 {
		t.Fatalf("unknown-length staging read request body %d times, want only the chunk header before reservation", body.reads.Load())
	}
	if dispatched.Load() != 0 {
		t.Fatalf("capacity rejection dispatched upstream %d times", dispatched.Load())
	}
	assertStageDirectoryEmpty(t, tempDir)
}

type splitSignedStreamBody struct {
	first []byte
	rest  []byte
	reads atomic.Int32
}

func (b *splitSignedStreamBody) Read(p []byte) (int, error) {
	switch b.reads.Add(1) {
	case 1:
		return copy(p, b.first), nil
	case 2:
		return copy(p, b.rest), nil
	default:
		return 0, io.EOF
	}
}

func (*splitSignedStreamBody) Close() error { return nil }

func TestSigningForwarderAccountsForPartialChunkWritesDuringConcurrentAdmission(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	var (
		dispatched atomic.Int32
		mu         sync.Mutex
		bodies     [][]byte
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dispatched.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		bodies = append(bodies, body)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	budget := newSignedStreamStageBudget(func(dir string) (signedStreamStageSpace, error) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return signedStreamStageSpace{}, err
		}
		var used int64
		for _, entry := range entries {
			info, err := entry.Info()
			if err != nil {
				return signedStreamStageSpace{}, err
			}
			used += info.Size()
		}
		return signedStreamStageSpace{availableBytes: 100 - used, blockSize: 1}, nil
	})
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(signedStreamTestAccessKey, signedStreamTestSecretKey)
	forwarder := NewForwarder(credentials, upstream.URL, "us-east-1", 1, nil, nil).(*signingForwarder)
	forwarder.stageBudgetOverride = budget

	firstPayload := bytes.Repeat([]byte("a"), 40)
	firstSeed := newSignedStreamSeed(t, upstream.URL, StreamingPayloadHash, len(firstPayload), true)
	firstWire := makeSignedStreamWire(t, firstSeed, firstPayload)
	headerEnd := bytes.Index(firstWire.wire, []byte("\r\n")) + len("\r\n")
	dataEnd := headerEnd + 30
	firstBody := &blockedSignedStreamBody{
		header:  firstWire.wire[:headerEnd],
		partial: firstWire.wire[headerEnd:dataEnd],
		rest:    firstWire.wire[dataEnd:],
		blocked: make(chan struct{}),
		release: make(chan struct{}),
	}
	defer firstBody.unblock()
	firstRequest := firstSeed.request(t, firstWire.wire)
	firstRequest.Body = firstBody
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- forwarder.Forward(t.Context(), httptest.NewRecorder(), firstRequest)
	}()
	select {
	case <-firstBody.blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("first upload did not block after writing its partial chunk")
	}
	if dispatched.Load() != 0 {
		t.Fatalf("incomplete first stream dispatched upstream %d times", dispatched.Load())
	}

	secondPayload := bytes.Repeat([]byte("b"), 10)
	secondSeed := newSignedStreamSeed(t, upstream.URL, StreamingPayloadHash, len(secondPayload), true)
	secondWire := makeSignedStreamWire(t, secondSeed, secondPayload)
	if err := forwarder.Forward(t.Context(), httptest.NewRecorder(), secondSeed.request(t, secondWire.wire)); err != nil {
		t.Fatalf("admit second upload while first is ingress-blocked: %v", err)
	}
	if dispatched.Load() != 1 {
		t.Fatalf("second upload dispatch count = %d, want 1", dispatched.Load())
	}

	firstBody.unblock()
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("finish first staged upload: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first upload did not finish after ingress release")
	}
	if dispatched.Load() != 2 {
		t.Fatalf("both admitted upload dispatches = %d, want 2", dispatched.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 || !bytes.Equal(bodies[0], secondPayload) || !bytes.Equal(bodies[1], firstPayload) {
		t.Fatalf("upstream bodies = %q, want second %q then first %q", bodies, secondPayload, firstPayload)
	}
	assertStageDirectoryEmpty(t, tempDir)
}

type blockedSignedStreamBody struct {
	header  []byte
	partial []byte
	rest    []byte
	reads   int
	blocked chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockedSignedStreamBody) Read(p []byte) (int, error) {
	switch b.reads {
	case 0:
		b.reads++
		return copy(p, b.header), nil
	case 1:
		b.reads++
		return copy(p, b.partial), nil
	case 2:
		b.reads++
		close(b.blocked)
		<-b.release
		return copy(p, b.rest), nil
	default:
		return 0, io.EOF
	}
}

func (b *blockedSignedStreamBody) unblock() {
	b.once.Do(func() { close(b.release) })
}

func (b *blockedSignedStreamBody) Close() error {
	b.unblock()
	return nil
}

func TestSigningForwarderStagingReservationLivesUntilTransportClose(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	budget := newSignedStreamStageBudget(func(string) (signedStreamStageSpace, error) {
		return signedStreamStageSpace{availableBytes: 100, blockSize: 1}, nil
	})
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(signedStreamTestAccessKey, signedStreamTestSecretKey)
	forwarder := NewForwarder(credentials, "http://stage.test", "us-east-1", 1, nil, nil).(*signingForwarder)
	forwarder.stageBudgetOverride = budget
	transport := &holdingSignedBodyTransport{}
	forwarder.httpClient.Transport = transport

	firstPayload := bytes.Repeat([]byte("a"), 40)
	firstSeed := newSignedStreamSeed(t, "http://stage.test", StreamingPayloadHash, len(firstPayload), true)
	firstWire := makeSignedStreamWire(t, firstSeed, firstPayload)
	if err := forwarder.Forward(t.Context(), httptest.NewRecorder(), firstSeed.request(t, firstWire.wire)); err != nil {
		t.Fatalf("forward first staged request: %v", err)
	}
	if transport.requestCount() != 1 {
		t.Fatalf("first request dispatch count = %d, want 1", transport.requestCount())
	}

	secondPayload := bytes.Repeat([]byte("b"), 11)
	secondSeed := newSignedStreamSeed(t, "http://stage.test", StreamingPayloadHash, len(secondPayload), true)
	secondWire := makeSignedStreamWire(t, secondSeed, secondPayload)
	secondRequest := secondSeed.request(t, secondWire.wire)
	err := forwarder.Forward(t.Context(), httptest.NewRecorder(), secondRequest)
	if !errors.Is(err, errSignedStreamStagingCapacity) {
		t.Fatalf("concurrent request without staging reservation = %v, want capacity error", err)
	}
	if transport.requestCount() != 1 {
		t.Fatalf("capacity-rejected request reached transport; dispatch count = %d", transport.requestCount())
	}

	firstRequest := transport.requestAt(0)
	got, err := io.ReadAll(firstRequest.Body)
	if err != nil || !bytes.Equal(got, firstPayload) {
		t.Fatalf("transport-owned first body = %q, err=%v, want %q", got, err, firstPayload)
	}
	if err := firstRequest.Body.Close(); err != nil {
		t.Fatalf("close first transport-owned body: %v", err)
	}
	assertStageDirectoryEmpty(t, tempDir)

	if err := forwarder.Forward(t.Context(), httptest.NewRecorder(), secondSeed.request(t, secondWire.wire)); err != nil {
		t.Fatalf("forward second request after reservation release: %v", err)
	}
	if transport.requestCount() != 2 {
		t.Fatalf("retry dispatch count = %d, want 2", transport.requestCount())
	}
	secondForwarded := transport.requestAt(1)
	got, err = io.ReadAll(secondForwarded.Body)
	if err != nil || !bytes.Equal(got, secondPayload) {
		t.Fatalf("transport-owned second body = %q, err=%v, want %q", got, err, secondPayload)
	}
	if err := secondForwarded.Body.Close(); err != nil {
		t.Fatalf("close second transport-owned body: %v", err)
	}
	assertStageDirectoryEmpty(t, tempDir)
}

type readCountingSignedStreamBody struct {
	reader io.Reader
	reads  atomic.Int32
}

func (b *readCountingSignedStreamBody) Read(p []byte) (int, error) {
	b.reads.Add(1)
	return b.reader.Read(p)
}

func (*readCountingSignedStreamBody) Close() error { return nil }

type holdingSignedBodyTransport struct {
	mu       sync.Mutex
	requests []*http.Request
}

func (t *holdingSignedBodyTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	t.mu.Lock()
	t.requests = append(t.requests, request)
	t.mu.Unlock()
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       http.NoBody,
		Request:    request,
	}, nil
}

func (t *holdingSignedBodyTransport) requestCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.requests)
}

func (t *holdingSignedBodyTransport) requestAt(index int) *http.Request {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.requests[index]
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
