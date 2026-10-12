package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tigrisdata/tag/auth"
)

type deadlineGateConnContextKey struct{}

type signedStreamCancelResult struct {
	err              error
	connectionHeader string
	stageFiles       int
	reservedBytes    int64
	readDirErr       error
}

type deadlineGateConn struct {
	net.Conn
	armed       atomic.Bool
	bodyReads   atomic.Int32
	blockedRead chan struct{}
	deadline    chan struct{}
	deadlineOne sync.Once
}

func (c *deadlineGateConn) Read(p []byte) (int, error) {
	if c.armed.Load() && c.bodyReads.Add(1) == 2 {
		close(c.blockedRead)
		<-c.deadline
		return 0, os.ErrDeadlineExceeded
	}
	return c.Conn.Read(p)
}

func (c *deadlineGateConn) unblockRead() {
	c.deadlineOne.Do(func() { close(c.deadline) })
}

func (c *deadlineGateConn) SetReadDeadline(deadline time.Time) error {
	if c.armed.Load() && !deadline.IsZero() && !deadline.After(time.Now()) {
		c.unblockRead()
	}
	return c.Conn.SetReadDeadline(deadline)
}

type deadlineGateListener struct {
	net.Listener
	accepted chan *deadlineGateConn
}

func (l *deadlineGateListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	gated := &deadlineGateConn{
		Conn:        conn,
		blockedRead: make(chan struct{}),
		deadline:    make(chan struct{}),
	}
	l.accepted <- gated
	return gated, nil
}

func TestSigningForwarderHMACFailureInterruptsRealHTTP1BodyClose(t *testing.T) {
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	budget := newSignedStreamStageBudget(func(string) (signedStreamStageSpace, error) {
		return signedStreamStageSpace{availableBytes: 1 << 20, blockSize: 1}, nil
	})
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(signedStreamTestAccessKey, signedStreamTestSecretKey)
	forwarder := NewForwarder(credentials, upstream.URL, "us-east-1", 1, nil, nil).(*signingForwarder)
	useSignedStreamStageBudget(t, budget)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for ingress: %v", err)
	}
	gatedListener := &deadlineGateListener{Listener: listener, accepted: make(chan *deadlineGateConn, 1)}
	handlerStarted := make(chan struct{})
	handlerDone := make(chan struct{})
	releaseHandler := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseHandler) }) }
	type result struct {
		err              error
		connectionHeader string
		stageFiles       int
		reservedBytes    int64
		readDirErr       error
	}
	resultCh := make(chan result, 1)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gatedConn := r.Context().Value(deadlineGateConnContextKey{}).(*deadlineGateConn)
		gatedConn.armed.Store(true)
		close(handlerStarted)
		err := forwarder.Forward(r.Context(), w, r)
		entries, readDirErr := os.ReadDir(tempDir)
		budget.mu.Lock()
		reserved := budget.reserved
		budget.mu.Unlock()
		resultCh <- result{err: err, connectionHeader: w.Header().Get("Connection"), stageFiles: len(entries), reservedBytes: reserved, readDirErr: readDirErr}
		<-releaseHandler
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
		}
		close(handlerDone)
	}), ConnContext: func(ctx context.Context, conn net.Conn) context.Context {
		return context.WithValue(ctx, deadlineGateConnContextKey{}, conn.(*deadlineGateConn))
	}}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(gatedListener) }()
	var conn net.Conn
	var gatedConn *deadlineGateConn
	defer func() {
		release()
		if gatedConn != nil {
			gatedConn.unblockRead()
		}
		if conn != nil {
			_ = conn.Close()
		}
		_ = server.Close()
		_ = gatedListener.Close()
		<-serveDone
	}()
	conn, err = net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial ingress: %v", err)
	}
	gatedConn = <-gatedListener.accepted

	payload := []byte("0123456789")
	seed := newSignedStreamSeed(t, "http://"+listener.Addr().String(), StreamingPayloadHash, len(payload), true)
	stream := makeSignedStreamWire(t, seed, payload)
	badWire := changeStreamSignature(t, stream.wire, stream.dataSignatureStart[0])
	headerEnd := bytes.Index(badWire, []byte("\r\n")) + len("\r\n")
	bodyEnd := headerEnd + len(payload) + len("\r\n")
	var request bytes.Buffer
	if _, err := fmt.Fprintf(&request, "PUT %s HTTP/1.1\r\nHost: %s\r\n", seed.path, seed.host); err != nil {
		t.Fatalf("write request line: %v", err)
	}
	for name, values := range seed.headers {
		if name == "Host" {
			continue
		}
		for _, value := range values {
			if _, err := fmt.Fprintf(&request, "%s: %s\r\n", name, value); err != nil {
				t.Fatalf("write request header: %v", err)
			}
		}
	}
	if _, err := fmt.Fprintf(&request, "Content-Length: %d\r\nConnection: keep-alive\r\n\r\n", len(badWire)); err != nil {
		t.Fatalf("write request framing: %v", err)
	}
	if _, err := conn.Write(request.Bytes()); err != nil {
		t.Fatalf("write request headers: %v", err)
	}
	select {
	case <-handlerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP server did not start request handling")
	}
	if _, err := conn.Write(badWire[:bodyEnd]); err != nil {
		t.Fatalf("write signed chunk with bad HMAC: %v", err)
	}
	select {
	case <-gatedConn.blockedRead:
	case <-time.After(5 * time.Second):
		t.Fatal("body Close did not reach the unread remainder after HMAC rejection")
	}
	select {
	case got := <-resultCh:
		if authErr, ok := IsAuthError(got.err); !ok || authErr.Code != ErrCodeSignatureMismatch {
			t.Fatalf("bad chunk signature error = %v, want signature mismatch", got.err)
		}
		if got.connectionHeader != "close" {
			t.Fatalf("rejected HTTP/1 response Connection = %q, want close", got.connectionHeader)
		}
		if got.readDirErr != nil || got.stageFiles != 0 || got.reservedBytes != 0 {
			t.Fatalf("rejected stream retained staging resources: files=%d reserved=%d err=%v", got.stageFiles, got.reservedBytes, got.readDirErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bad HMAC rejection blocked while closing the incomplete request body")
	}
	if upstreamCalls.Load() != 0 {
		t.Fatalf("bad HMAC stream dispatched upstream %d times", upstreamCalls.Load())
	}
	release()
	_ = conn.Close()
	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP/1 handler did not finish after rejecting the stream")
	}
}

func TestSigningForwarderCancellationInterruptsRealHTTP1Body(t *testing.T) {
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	methods := []struct {
		name string
		call func(*signingForwarder, context.Context, http.ResponseWriter, *http.Request) error
	}{
		{
			name: "Forward",
			call: func(f *signingForwarder, ctx context.Context, w http.ResponseWriter, r *http.Request) error {
				return f.Forward(ctx, w, r)
			},
		},
		{
			name: "ForwardWithCapture",
			call: func(f *signingForwarder, ctx context.Context, w http.ResponseWriter, r *http.Request) error {
				_, err := f.ForwardWithCapture(ctx, w, r)
				return err
			},
		},
		{
			name: "ForwardTeeingBody",
			call: func(f *signingForwarder, ctx context.Context, w http.ResponseWriter, r *http.Request) error {
				_, _, _, _, err := f.ForwardTeeingBody(ctx, w, r, io.Discard)
				return err
			},
		},
	}
	for _, method := range methods {
		method := method
		t.Run(method.name, func(t *testing.T) {
			tempDir := t.TempDir()
			t.Setenv("TMPDIR", tempDir)
			budget := newSignedStreamStageBudget(func(string) (signedStreamStageSpace, error) {
				return signedStreamStageSpace{availableBytes: 1 << 20, blockSize: 1}, nil
			})
			credentials := auth.NewCredentialStore()
			credentials.AddCredential(signedStreamTestAccessKey, signedStreamTestSecretKey)
			forwarder := NewForwarder(credentials, upstream.URL, "us-east-1", 1, nil, nil).(*signingForwarder)
			useSignedStreamStageBudget(t, budget)

			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen for ingress: %v", err)
			}
			gatedListener := &deadlineGateListener{Listener: listener, accepted: make(chan *deadlineGateConn, 1)}
			cancelRequest := make(chan context.CancelFunc, 1)
			handlerStarted := make(chan struct{})
			handlerDone := make(chan struct{})
			releaseHandler := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseHandler) }) }
			resultCh := make(chan signedStreamCancelResult, 1)
			server := &http.Server{
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					gatedConn := r.Context().Value(deadlineGateConnContextKey{}).(*deadlineGateConn)
					gatedConn.armed.Store(true)
					close(handlerStarted)
					ctx, cancel := context.WithCancel(r.Context())
					cancelRequest <- cancel
					err := method.call(forwarder, ctx, w, r)
					entries, readDirErr := os.ReadDir(tempDir)
					budget.mu.Lock()
					reserved := budget.reserved
					budget.mu.Unlock()
					resultCh <- signedStreamCancelResult{
						err:              err,
						connectionHeader: w.Header().Get("Connection"),
						stageFiles:       len(entries),
						reservedBytes:    reserved,
						readDirErr:       readDirErr,
					}
					<-releaseHandler
					if err != nil {
						w.WriteHeader(http.StatusBadRequest)
					}
					close(handlerDone)
				}),
				ConnContext: func(ctx context.Context, conn net.Conn) context.Context {
					return context.WithValue(ctx, deadlineGateConnContextKey{}, conn.(*deadlineGateConn))
				},
			}
			serveDone := make(chan error, 1)
			go func() { serveDone <- server.Serve(gatedListener) }()
			var (
				conn      net.Conn
				gatedConn *deadlineGateConn
			)
			defer func() {
				release()
				if gatedConn != nil {
					gatedConn.unblockRead()
				}
				if conn != nil {
					_ = conn.Close()
				}
				_ = server.Close()
				_ = gatedListener.Close()
				<-serveDone
			}()
			conn, err = net.Dial("tcp", listener.Addr().String())
			if err != nil {
				t.Fatalf("dial ingress: %v", err)
			}
			gatedConn = <-gatedListener.accepted

			payload := []byte("0123456789")
			seed := newSignedStreamSeed(t, "http://"+listener.Addr().String(), StreamingPayloadHash, len(payload), true)
			stream := makeSignedStreamWire(t, seed, payload)
			headerEnd := bytes.Index(stream.wire, []byte("\r\n")) + len("\r\n")
			if headerEnd < len("\r\n") || headerEnd+2 >= len(stream.wire) {
				t.Fatalf("invalid signed-stream test wire length %d", len(stream.wire))
			}

			var request bytes.Buffer
			if _, err := fmt.Fprintf(&request, "PUT %s HTTP/1.1\r\nHost: %s\r\n", seed.path, seed.host); err != nil {
				t.Fatalf("write request line: %v", err)
			}
			for name, values := range seed.headers {
				if name == "Host" {
					continue
				}
				for _, value := range values {
					if _, err := fmt.Fprintf(&request, "%s: %s\r\n", name, value); err != nil {
						t.Fatalf("write request header: %v", err)
					}
				}
			}
			if _, err := fmt.Fprintf(&request, "Content-Length: %d\r\nConnection: keep-alive\r\n\r\n", len(stream.wire)); err != nil {
				t.Fatalf("write request framing: %v", err)
			}
			if _, err := conn.Write(request.Bytes()); err != nil {
				t.Fatalf("write request headers: %v", err)
			}
			select {
			case <-handlerStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("HTTP server did not start request handling")
			}
			if _, err := conn.Write(stream.wire[:headerEnd+2]); err != nil {
				t.Fatalf("write partial signed stream: %v", err)
			}
			cancel := <-cancelRequest
			select {
			case <-gatedConn.blockedRead:
			case <-time.After(5 * time.Second):
				t.Fatal("HTTP/1 request body did not block inside its second read")
			}
			entries, err := os.ReadDir(tempDir)
			if err != nil {
				t.Fatalf("read active staging directory: %v", err)
			}
			if len(entries) == 0 {
				t.Fatal("staging file was not owned while the ingress body was blocked")
			}
			budget.mu.Lock()
			reservedBeforeCancel := budget.reserved
			budget.mu.Unlock()
			if reservedBeforeCancel == 0 {
				t.Fatal("staging reservation was not held while the ingress body was blocked")
			}

			cancel()
			var result signedStreamCancelResult
			select {
			case result = <-resultCh:
			case <-time.After(5 * time.Second):
				t.Fatal("canceled forwarding remained blocked until the client closed ingress")
			}
			if !errors.Is(result.err, context.Canceled) {
				t.Fatalf("canceled forwarding error = %v, want context.Canceled", result.err)
			}
			if result.connectionHeader != "close" {
				t.Fatalf("canceled HTTP/1 response Connection = %q, want close", result.connectionHeader)
			}
			if result.readDirErr != nil || result.stageFiles != 0 || result.reservedBytes != 0 {
				t.Fatalf("cancellation retained staging resources: files=%d reserved=%d err=%v", result.stageFiles, result.reservedBytes, result.readDirErr)
			}
			if upstreamCalls.Load() != 0 {
				t.Fatalf("canceled signed stream dispatched upstream %d times", upstreamCalls.Load())
			}
			release()
			_ = conn.Close()
			select {
			case <-handlerDone:
			case <-time.After(5 * time.Second):
				t.Fatal("HTTP/1 handler did not finish after ingress cleanup")
			}
		})
	}
}
