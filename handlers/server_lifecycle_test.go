package handlers

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/proxy"
)

func TestServerStartStopLifecycle(t *testing.T) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	t.Cleanup(func() { log.Logger = oldLogger })

	t.Run("stop-before-start", func(t *testing.T) {
		port := availableServerPort(t)
		addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
		server := NewServer(nil, "127.0.0.1", port, false, 0)
		initial := server.httpServer
		if initial == nil {
			t.Errorf("NewServer did not initialize the HTTP server")
		} else {
			if initial.Addr != server.bindAddr {
				t.Errorf("server address = %q, want %q", initial.Addr, server.bindAddr)
			}
			// The server wraps the router in the handler lifecycle barrier, so
			// check dispatch rather than handler identity.
			if initial.Handler == nil {
				t.Errorf("server handler is nil")
			} else {
				rec := httptest.NewRecorder()
				initial.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
				if rec.Code != http.StatusOK || rec.Body.String() != "OK" {
					t.Errorf("server handler /health = (%d, %q), want (200, \"OK\") from the configured router", rec.Code, rec.Body.String())
				}
			}
			if initial.ReadTimeout != 5*time.Minute || initial.WriteTimeout != 5*time.Minute || initial.IdleTimeout != 120*time.Second {
				t.Errorf("server timeouts = (%s, %s, %s), want (5m, 5m, 2m)", initial.ReadTimeout, initial.WriteTimeout, initial.IdleTimeout)
			}
		}

		if err := stopServer(server); err != nil {
			t.Errorf("Stop before Start returned %v", err)
		}

		startDone := make(chan error, 1)
		go func() { startDone <- server.Start() }()
		select {
		case err := <-startDone:
			if !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("Start after Stop returned %v, want http.ErrServerClosed", err)
			}
			status, body, probeErr := requestHealth(addr)
			if probeErr != nil {
				if !errors.Is(probeErr, syscall.ECONNREFUSED) {
					t.Errorf("setup: health probe after Stop failed unexpectedly: %v", probeErr)
				}
			} else if status == http.StatusOK && body == "OK" {
				t.Errorf("Start served /health after Stop completed")
			} else {
				t.Errorf("setup: unexpected health probe response after Stop: (%d, %q)", status, body)
			}
		case <-time.After(time.Second):
			status, body, probeErr := requestHealth(addr)
			if probeErr == nil && status == http.StatusOK && body == "OK" {
				t.Errorf("Start served /health after Stop completed")
			} else {
				t.Errorf("setup: Start did not return and health probe did not confirm serving: (%d, %q, %v)", status, body, probeErr)
			}
			if err := stopServer(server); err != nil {
				t.Errorf("setup: cleanup Stop returned %v", err)
			}
			select {
			case <-startDone:
			case <-time.After(time.Second):
				t.Fatalf("setup: server did not exit after cleanup Stop")
			}
		}
		if server.httpServer != initial {
			t.Errorf("Start replaced the HTTP server initialized by NewServer")
		}
	})

	t.Run("concurrent-start-stop", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			server := NewServer(nil, "127.0.0.1", 0, false, 0)
			startGate := make(chan struct{})
			startDone := make(chan error, 1)
			stopDone := make(chan error, 1)

			go func() {
				<-startGate
				startDone <- server.Start()
			}()
			go func() {
				<-startGate
				stopDone <- stopServer(server)
			}()
			close(startGate)

			select {
			case err := <-stopDone:
				if err != nil {
					t.Errorf("Stop returned %v", err)
				}
			case <-time.After(time.Second):
				_ = stopServer(server)
				t.Errorf("setup: Stop did not return within one second")
				return
			}

			select {
			case err := <-startDone:
				if !errors.Is(err, http.ErrServerClosed) {
					t.Errorf("Start returned %v after Stop", err)
				}
			case <-time.After(100 * time.Millisecond):
				if err := stopServer(server); err != nil {
					t.Errorf("setup: cleanup Stop returned %v", err)
				}
				select {
				case <-startDone:
				case <-time.After(time.Second):
					t.Fatalf("setup: server did not exit after cleanup Stop")
				}
				t.Errorf("setup: Start did not return within 100 ms")
				return
			}
		}
	})
}

func TestServerShutdownDrainsHTTPAndTLS(t *testing.T) {
	certFile, keyFile, tlsClient := lifecycleTLSFiles(t)
	for _, test := range []struct {
		name   string
		tls    bool
		client *http.Client
	}{
		{name: "http", client: &http.Client{Timeout: 10 * time.Second}},
		{name: "https", tls: true, client: tlsClient},
	} {
		t.Run(test.name, func(t *testing.T) {
			forwarder := &drainingForwarder{
				entered: make(chan struct{}, 1),
				release: make(chan struct{}),
			}
			service := proxy.NewService(forwarder, cache.NewDisabledCache(), config.NewDefault())
			port := availableServerPort(t)
			server := NewServer(service, "127.0.0.1", port, false, 0)
			if test.tls {
				server.SetTLS(certFile, keyFile)
			}

			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(forwarder.release) }) }
			t.Cleanup(func() {
				release()
				_ = stopServer(server)
				test.client.CloseIdleConnections()
			})

			startDone := make(chan error, 1)
			go func() { startDone <- server.Start() }()
			addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
			if err := waitForServerListener(addr, true, test.tls); err != nil {
				t.Fatalf("setup: server did not listen: %v", err)
			}

			requestDone := make(chan struct {
				status int
				body   string
				err    error
			}, 1)
			scheme := "http"
			if test.tls {
				scheme = "https"
			}
			go func() {
				response, err := test.client.Get(scheme + "://" + addr + "/bucket?uploads=")
				if err != nil {
					requestDone <- struct {
						status int
						body   string
						err    error
					}{err: err}
					return
				}
				defer response.Body.Close()
				body, err := io.ReadAll(response.Body)
				requestDone <- struct {
					status int
					body   string
					err    error
				}{status: response.StatusCode, body: string(body), err: err}
			}()

			select {
			case <-forwarder.entered:
			case <-time.After(5 * time.Second):
				t.Fatalf("setup: request did not reach the upstream handler")
			}

			stopDone := make(chan error, 1)
			stopCtx, cancelStop := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancelStop()
			go func() { stopDone <- server.Stop(stopCtx) }()
			if err := waitForServerListener(addr, false, test.tls); err != nil {
				t.Errorf("listener remained open during shutdown: %v", err)
			}
			release()

			select {
			case response := <-requestDone:
				if response.err != nil {
					t.Errorf("request failed during shutdown: %v", response.err)
				} else if response.status != http.StatusOK || response.body != "drained" {
					t.Errorf("response = (%d, %q), want (200, %q)", response.status, response.body, "drained")
				}
			case <-time.After(5 * time.Second):
				t.Errorf("request did not finish within the drain budget")
			}

			select {
			case err := <-stopDone:
				if err != nil {
					t.Errorf("Stop returned %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Errorf("Stop did not finish after the request drained")
			}
			select {
			case err := <-startDone:
				if !errors.Is(err, http.ErrServerClosed) {
					t.Errorf("Start returned %v after shutdown", err)
				}
			case <-time.After(time.Second):
				t.Errorf("Start did not return after shutdown")
			}
		})
	}
}

type drainingForwarder struct {
	proxy.RequestForwarder
	entered chan struct{}
	release chan struct{}
}

func (f *drainingForwarder) Forward(_ context.Context, w http.ResponseWriter, _ *http.Request) error {
	f.entered <- struct{}{}
	<-f.release
	w.WriteHeader(http.StatusOK)
	_, err := io.WriteString(w, "drained")
	return err
}

func requestHealth(addr string) (int, string, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("http://" + addr + "/health")
	if err != nil {
		return 0, "", err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	return response.StatusCode, string(body), err
}

func stopServer(server *Server) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return server.Stop(ctx)
}

func availableServerPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("setup: reserve server port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("setup: release server port: %v", err)
	}
	return port
}

func waitForServerListener(addr string, wantOpen, useTLS bool) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		dialer := &net.Dialer{Timeout: 50 * time.Millisecond}
		var conn net.Conn
		var err error
		if useTLS {
			// This local probe checks listener readiness, not certificate trust.
			conn, err = tls.DialWithDialer(dialer, "tcp", addr, &tls.Config{InsecureSkipVerify: true})
		} else {
			conn, err = dialer.Dial("tcp", addr)
		}
		open := err == nil
		if open {
			_ = conn.Close()
		}
		if open == wantOpen {
			return nil
		}
		<-time.After(10 * time.Millisecond)
	}
	if wantOpen {
		return fmt.Errorf("listener did not open")
	}
	return fmt.Errorf("listener did not close")
}

func lifecycleTLSFiles(t *testing.T) (string, string, *http.Client) {
	t.Helper()
	tlsServer := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(tlsServer.Close)
	certificate := tlsServer.TLS.Certificates[0]
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]})
	keyDER, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		t.Fatalf("setup: encode test TLS key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	certFile := filepath.Join(t.TempDir(), "server.crt")
	keyFile := filepath.Join(t.TempDir(), "server.key")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("setup: write test TLS certificate: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("setup: write test TLS key: %v", err)
	}
	client := tlsServer.Client()
	client.Timeout = 10 * time.Second
	return certFile, keyFile, client
}
