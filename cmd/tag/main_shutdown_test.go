package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
)

const (
	shutdownProbeBucket = "shutdown-probe"
	shutdownProbeKey    = "range-object"
	shutdownProbeETag   = `"shutdown-etag"`

	shutdownTestChildEnv  = "TAG_SHUTDOWN_TEST_CHILD"
	shutdownTestConfigEnv = "TAG_SHUTDOWN_TEST_CONFIG"
	shutdownTestHoldEnv   = "TAG_SHUTDOWN_TEST_HOLD"
)

var errShutdownOrderingViolation = errors.New("embedded storage closed before the detached cache writer exited")

type storageCloseObserver interface {
	IsClosed() bool
}

type shutdownTestChildState struct {
	storageMu sync.RWMutex
	storage   storageCloseObserver

	writerActive atomic.Bool

	releaseWriter     chan struct{}
	releaseWriterOnce sync.Once
	finish            chan struct{}
	finishOnce        sync.Once
	signalHandled     chan struct{}
	signalHandledOnce sync.Once
}

var shutdownTestChild atomic.Pointer[shutdownTestChildState]

func (s *shutdownTestChildState) setStorage(storage storageCloseObserver) {
	s.storageMu.Lock()
	s.storage = storage
	s.storageMu.Unlock()
}

func (s *shutdownTestChildState) storageClosed() bool {
	s.storageMu.RLock()
	storage := s.storage
	s.storageMu.RUnlock()
	return storage != nil && storage.IsClosed()
}

func (s *shutdownTestChildState) event(name string) {
	fmt.Fprintf(os.Stdout, "EVENT %s closed=%t writer=%t\n", name, s.storageClosed(), s.writerActive.Load())
}

type shutdownSignalLogWriter struct {
	state *shutdownTestChildState
	once  sync.Once
}

func (w *shutdownSignalLogWriter) Write(p []byte) (int, error) {
	_, _ = os.Stderr.Write(p)
	if bytes.Contains(p, []byte("Received shutdown signal")) {
		w.once.Do(func() {
			w.state.signalHandledOnce.Do(func() { close(w.state.signalHandled) })
			w.state.event("shutdown-signal-received")
		})
	}
	return len(p), nil
}

// PutStream pauses the real embedded-client operation at its cache boundary.
// After release it delegates to embedded storage unless Close has already run;
// in that case it reports the forbidden ordering without touching a closed DB.
func (c *embeddedBlockCacheClient) PutStream(ctx context.Context, key string, r io.Reader, ttlSeconds int64) error {
	state := shutdownTestChild.Load()
	if os.Getenv(shutdownTestChildEnv) == "1" && state != nil {
		state.setStorage(c.Client.Storage())
	}
	if os.Getenv(shutdownTestChildEnv) != "1" || os.Getenv(shutdownTestHoldEnv) != "true" || key != cache.MakeBodyKey(shutdownProbeBucket, shutdownProbeKey, shutdownProbeETag) {
		return c.Client.PutStream(ctx, key, r, ttlSeconds)
	}

	if state == nil {
		return errors.New("shutdown probe state is unavailable")
	}
	// This operation is reached only after the detached writer's service-task admission.
	state.event("worker-admitted")
	state.writerActive.Store(true)
	state.event("writer-entered")
	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.Canceled) {
			state.event("writer-canceled")
		} else {
			state.event("writer-deadline")
		}
		<-state.releaseWriter
	case <-state.releaseWriter:
	}

	if c.Client.Storage().IsClosed() {
		state.event("cache-after-close")
		state.writerActive.Store(false)
		state.event("writer-exited")
		return errors.New("embedded storage closed while a cache write was active")
	}
	err := c.Client.PutStream(ctx, key, r, ttlSeconds)
	state.writerActive.Store(false)
	state.event("writer-exited")
	return err
}

// TestShutdownChild is the subprocess entry for the real-main shutdown probe.
func TestShutdownChild(t *testing.T) {
	if os.Getenv(shutdownTestChildEnv) != "1" {
		t.Skip("subprocess helper")
	}

	state := &shutdownTestChildState{
		releaseWriter: make(chan struct{}),
		finish:        make(chan struct{}),
		signalHandled: make(chan struct{}),
	}
	shutdownTestChild.Store(state)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			switch strings.TrimSpace(scanner.Text()) {
			case "release":
				state.releaseWriterOnce.Do(func() { close(state.releaseWriter) })
			case "finish":
				state.finishOnce.Do(func() { close(state.finish) })
			}
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	relayStop := make(chan struct{})
	go func() {
		select {
		case <-signals:
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-state.signalHandled:
					return
				case <-relayStop:
					return
				case <-ticker.C:
					// The test process catches a signal sent before main has installed
					// its listener, then relays it until main reaches its shutdown wait.
					_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
				}
			}
		case <-relayStop:
		}
	}()
	previousLogger := log.Logger
	log.Logger = log.Logger.Output(&shutdownSignalLogWriter{state: state})

	flag.CommandLine = flag.NewFlagSet("tag", flag.ContinueOnError)
	os.Args = []string{"tag", "--config", os.Getenv(shutdownTestConfigEnv)}
	main()
	log.Logger = previousLogger
	signal.Stop(signals)
	close(relayStop)
	state.event("backend-closed")
	<-state.finish
}

func TestShutdownWaitsForBackgroundWriterBeforeEmbeddedClose(t *testing.T) {
	if err := runShutdownOrderingProbe(t); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownDrainsNormalRequestWithinGrace(t *testing.T) {
	if err := runShutdownGraceProbe(); err != nil {
		t.Fatal(err)
	}
}

func runShutdownGraceProbe() error {
	started := make(chan struct{}, 1)
	releaseBody := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseBody) }) }
	defer release()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"grace-etag"`)
		w.Header().Set("Content-Length", "8")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-releaseBody:
			_, _ = io.WriteString(w, "ABCDEFGH")
		case <-r.Context().Done():
		}
	}))
	defer origin.Close()

	child, err := startShutdownChild(origin.URL, false)
	if err != nil {
		return fmt.Errorf("start TAG subprocess: %w", err)
	}
	defer child.cleanup()
	if err := child.waitReady(); err != nil {
		return fmt.Errorf("wait for TAG readiness: %w", err)
	}

	tagURL := fmt.Sprintf("http://127.0.0.1:%d", child.port)
	signer := auth.NewRequestSigner(tagURL, "us-east-1")
	req, err := signer.SignRequest(context.Background(), http.MethodGet, "/grace-bucket/grace-key", nil, "", shutdownTestAccessKey, shutdownTestSecretKey, make(http.Header))
	if err != nil {
		return fmt.Errorf("sign in-flight GET: %w", err)
	}
	type response struct {
		status int
		body   string
		err    error
	}
	responseDone := make(chan response, 1)
	go func() {
		resp, err := (&http.Client{Timeout: shutdownProbeTimeout}).Do(req)
		if err != nil {
			responseDone <- response{err: err}
			return
		}
		body, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		responseDone <- response{status: resp.StatusCode, body: string(body), err: errors.Join(readErr, closeErr)}
	}()
	select {
	case <-started:
	case <-time.After(shutdownProbeTimeout):
		release()
		return errors.New("normal GET did not reach the controlled origin")
	}
	if err := child.signal(syscall.SIGTERM); err != nil {
		release()
		return fmt.Errorf("send SIGTERM during normal request: %w", err)
	}
	if _, err := child.waitLine("EVENT shutdown-signal-received ", shutdownProbeTimeout); err != nil {
		release()
		return fmt.Errorf("main did not receive SIGTERM during normal request: %w", err)
	}
	release()

	select {
	case result := <-responseDone:
		if result.err != nil {
			return fmt.Errorf("normal request during HTTP drain: %w", result.err)
		}
		if result.status != http.StatusOK || result.body != "ABCDEFGH" {
			return fmt.Errorf("normal request during HTTP drain = (%d, %q), want (200, ABCDEFGH)", result.status, result.body)
		}
	case <-time.After(shutdownProbeTimeout):
		return errors.New("normal request did not finish within the HTTP grace")
	}
	closed, err := child.waitLine("EVENT backend-closed ", shutdownProbeTimeout)
	if err != nil {
		return fmt.Errorf("wait for embedded storage close: %w", err)
	}
	if !strings.Contains(closed, "closed=true") {
		return fmt.Errorf("embedded storage did not report closed after graceful drain: %s", closed)
	}
	child.finish()
	if err := child.waitExit(shutdownProbeTimeout); err != nil {
		return fmt.Errorf("TAG subprocess exit: %w", err)
	}
	if err := checkShutdownDrainResult(child); err != nil {
		return err
	}
	return nil
}

func runShutdownOrderingProbe(t *testing.T) error {
	t.Helper()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", shutdownProbeETag)
		w.Header().Set("Content-Type", "application/octet-stream")
		if r.Header.Get("Range") != "" {
			w.Header().Set("Content-Range", "bytes 0-3/8")
			w.Header().Set("Content-Length", "4")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = io.WriteString(w, "ABCD")
			return
		}
		w.Header().Set("Content-Length", "8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ABCDEFGH")
	}))
	defer origin.Close()

	child, err := startShutdownChild(origin.URL, true)
	if err != nil {
		return fmt.Errorf("start TAG subprocess: %w", err)
	}
	defer func() {
		child.cleanup()
		child.recordEvents(t)
	}()
	if err := child.waitReady(); err != nil {
		return fmt.Errorf("wait for TAG readiness: %w", err)
	}

	tagURL := fmt.Sprintf("http://127.0.0.1:%d", child.port)
	headers := make(http.Header)
	headers.Set("Range", "bytes=0-3")
	signer := auth.NewRequestSigner(tagURL, "us-east-1")
	req, err := signer.SignRequest(context.Background(), http.MethodGet, "/"+shutdownProbeBucket+"/"+shutdownProbeKey, nil, "", shutdownTestAccessKey, shutdownTestSecretKey, headers)
	if err != nil {
		return fmt.Errorf("sign cold range request: %w", err)
	}
	resp, err := (&http.Client{Timeout: shutdownProbeTimeout}).Do(req)
	if err != nil {
		return fmt.Errorf("cold range request: %w", err)
	}
	body, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil || closeErr != nil {
		return fmt.Errorf("read cold range response: read=%v close=%v", readErr, closeErr)
	}
	if resp.StatusCode != http.StatusPartialContent || string(body) != "ABCD" {
		return fmt.Errorf("cold range response = (%d, %q), want (206, ABCD)", resp.StatusCode, body)
	}
	if _, err := child.waitLine("EVENT writer-entered ", shutdownProbeTimeout); err != nil {
		return fmt.Errorf("background populate did not reach embedded cache PutStream: %w", err)
	}
	if !child.seen("EVENT worker-admitted ") {
		return errors.New("detached cache writer reached PutStream without a recorded admission event")
	}
	if err := child.signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("send SIGTERM: %w", err)
	}
	if _, err := child.waitLine("EVENT shutdown-signal-received ", shutdownProbeTimeout); err != nil {
		return fmt.Errorf("main did not receive SIGTERM: %w", err)
	}

	first, err := child.waitAny([]string{"EVENT writer-canceled ", "EVENT backend-closed "}, shutdownProbeTimeout)
	if err != nil {
		return fmt.Errorf("wait for shutdown event: %w", err)
	}
	if strings.HasPrefix(first, "EVENT backend-closed ") {
		if err := finishShutdownChild(child); err != nil {
			return err
		}
		if strings.Contains(first, "closed=true") && strings.Contains(first, "writer=true") {
			return fmt.Errorf("%w: %s", errShutdownOrderingViolation, first)
		}
		return fmt.Errorf("shutdown closed storage before a service-cancellation event, but the held writer state was not observable: %s", first)
	}

	if err := child.send("release"); err != nil {
		return fmt.Errorf("release embedded cache writer: %w", err)
	}
	order, err := child.waitAny([]string{"EVENT writer-exited ", "EVENT backend-closed "}, shutdownProbeTimeout)
	if err != nil {
		return fmt.Errorf("wait for writer exit or embedded-storage close: %w", err)
	}
	if strings.HasPrefix(order, "EVENT backend-closed ") && strings.Contains(order, "writer=true") {
		if err := finishShutdownChild(child); err != nil {
			return err
		}
		return fmt.Errorf("%w: %s", errShutdownOrderingViolation, order)
	}
	if !strings.HasPrefix(order, "EVENT writer-exited ") {
		if err := finishShutdownChild(child); err != nil {
			return err
		}
		return fmt.Errorf("unexpected shutdown event before writer exit: %s", order)
	}
	if child.seen("EVENT cache-after-close ") {
		if err := finishShutdownChild(child); err != nil {
			return err
		}
		return fmt.Errorf("%w: cache access was attempted after embedded storage closed", errShutdownOrderingViolation)
	}
	closed, err := child.waitLine("EVENT backend-closed ", shutdownProbeTimeout)
	if err != nil {
		return fmt.Errorf("wait for embedded-storage close: %w", err)
	}
	if !strings.Contains(closed, "closed=true") || !strings.Contains(closed, "writer=false") {
		if err := finishShutdownChild(child); err != nil {
			return err
		}
		return fmt.Errorf("backend close did not follow writer exit: %s", closed)
	}
	return finishShutdownChild(child)
}

func finishShutdownChild(child *shutdownChildProcess) error {
	child.release()
	child.finish()
	if err := child.waitExit(shutdownProbeTimeout); err != nil {
		return fmt.Errorf("TAG subprocess exit: %w", err)
	}
	return checkShutdownDrainResult(child)
}

func checkShutdownDrainResult(child *shutdownChildProcess) error {
	if output := child.stderr.String(); strings.Contains(output, "Error during server shutdown") {
		return fmt.Errorf("HTTP drain did not complete successfully: %s", output)
	}
	return nil
}

const (
	shutdownTestAccessKey = "AKIAIOSFODNN7EXAMPLE"
	shutdownTestSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	shutdownProbeTimeout  = 20 * time.Second
)

type shutdownLockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *shutdownLockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *shutdownLockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type shutdownChildProcess struct {
	command *exec.Cmd
	input   io.WriteCloser
	lines   chan string
	done    chan struct{}

	mu      sync.Mutex
	waitErr error
	history []string
	stderr  shutdownLockedBuffer
	port    int
	dir     string
}

func startShutdownChild(upstream string, holdWriter bool) (*shutdownChildProcess, error) {
	port, err := availableTCPPort()
	if err != nil {
		return nil, err
	}
	clusterPort, err := availableTCPPort()
	if err != nil {
		return nil, err
	}
	grpcPort, err := availableTCPPort()
	if err != nil {
		return nil, err
	}

	dir, err := os.MkdirTemp("", "tag-shutdown-")
	if err != nil {
		return nil, err
	}
	cacheDir := filepath.Join(dir, "cache")
	if err := os.Mkdir(cacheDir, 0o700); err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	configPath := filepath.Join(dir, "config.yaml")
	configText := fmt.Sprintf("mode: signing\nserver:\n  bind_ip: 127.0.0.1\n  http_port: %d\nupstream:\n  endpoint: %s\n  region: us-east-1\ncache:\n  enabled: true\n  disk_path: %s\n  node_id: shutdown-probe\n  cluster_addr: 127.0.0.1:%d\n  grpc_addr: 127.0.0.1:%d\n  grpc_auth: false\n  legacy_coordination: false\n  block_caching_enabled: false\n  size_threshold: 1048576\nlog:\n  level: info\n", port, strconv.Quote(upstream), strconv.Quote(cacheDir), clusterPort, grpcPort)
	if err := os.WriteFile(configPath, []byte(configText), 0o600); err != nil {
		os.RemoveAll(dir)
		return nil, err
	}

	executable, err := os.Executable()
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	command := exec.Command(executable, "-test.run=^TestShutdownChild$")
	command.Env = shutdownChildEnvironment(configPath, holdWriter)
	input, err := command.StdinPipe()
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	stdoutReader, stdoutWriter := io.Pipe()
	command.Stdout = stdoutWriter
	child := &shutdownChildProcess{
		command: command,
		input:   input,
		lines:   make(chan string, 256),
		done:    make(chan struct{}),
		port:    port,
		dir:     dir,
	}
	command.Stderr = &child.stderr
	if err := command.Start(); err != nil {
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
		_ = input.Close()
		os.RemoveAll(dir)
		return nil, err
	}
	go func() {
		scanner := bufio.NewScanner(stdoutReader)
		for scanner.Scan() {
			child.lines <- scanner.Text()
		}
		close(child.lines)
		_ = stdoutReader.Close()
	}()
	go func() {
		err := command.Wait()
		_ = stdoutWriter.Close()
		child.mu.Lock()
		child.waitErr = err
		child.mu.Unlock()
		close(child.done)
	}()
	return child, nil
}

func availableTCPPort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		return 0, err
	}
	return port, nil
}

func shutdownChildEnvironment(configPath string, holdWriter bool) []string {
	env := make([]string, 0, len(os.Environ())+8)
	for _, item := range os.Environ() {
		name, _, _ := strings.Cut(item, "=")
		if strings.HasPrefix(name, "TAG_") || strings.HasPrefix(name, "AWS_") {
			continue
		}
		env = append(env, item)
	}
	env = append(env,
		"AWS_ACCESS_KEY_ID="+shutdownTestAccessKey,
		"AWS_SECRET_ACCESS_KEY="+shutdownTestSecretKey,
		"AWS_EC2_METADATA_DISABLED=true",
		"TAG_LOG_LEVEL=info",
		shutdownTestChildEnv+"=1",
		shutdownTestConfigEnv+"="+configPath,
		shutdownTestHoldEnv+"="+strconv.FormatBool(holdWriter),
	)
	return env
}

func (c *shutdownChildProcess) waitReady() error {
	client := &http.Client{Timeout: time.Second}
	deadline := time.NewTimer(shutdownProbeTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	url := fmt.Sprintf("http://127.0.0.1:%d/health", c.port)
	for {
		resp, err := client.Get(url)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-deadline.C:
			return fmt.Errorf("TAG health endpoint did not become ready: %v", err)
		case <-ticker.C:
		case <-c.done:
			return fmt.Errorf("TAG exited before becoming ready: %w; stderr=%s", c.waitError(), c.stderr.String())
		}
	}
}

func (c *shutdownChildProcess) waitLine(prefix string, timeout time.Duration) (string, error) {
	return c.waitAny([]string{prefix}, timeout)
}

func (c *shutdownChildProcess) waitAny(prefixes []string, timeout time.Duration) (string, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case line, ok := <-c.lines:
			if !ok {
				return "", fmt.Errorf("TAG subprocess output closed: %w", c.waitError())
			}
			c.history = append(c.history, line)
			for _, prefix := range prefixes {
				if strings.HasPrefix(line, prefix) {
					return line, nil
				}
			}
		case <-c.done:
			return "", fmt.Errorf("TAG subprocess exited: %w", c.waitError())
		case <-timer.C:
			return "", fmt.Errorf("timed out waiting for %q; stderr=%s", prefixes, c.stderr.String())
		}
	}
}

func (c *shutdownChildProcess) recordEvents(t *testing.T) {
	t.Helper()
	for line := range c.lines {
		c.history = append(c.history, line)
	}
	for _, line := range c.history {
		if strings.HasPrefix(line, "EVENT ") || strings.HasPrefix(line, "STATUS ") {
			t.Log(line)
		}
	}
}

func (c *shutdownChildProcess) seen(prefix string) bool {
	for _, line := range c.history {
		if strings.HasPrefix(line, prefix) {
			return true
		}
	}
	return false
}

func (c *shutdownChildProcess) send(command string) error {
	_, err := fmt.Fprintln(c.input, command)
	return err
}

func (c *shutdownChildProcess) release() {
	_ = c.send("release")
}

func (c *shutdownChildProcess) finish() {
	_ = c.send("finish")
}

func (c *shutdownChildProcess) signal(sig os.Signal) error {
	if c.command.Process == nil {
		return errors.New("TAG subprocess has not started")
	}
	return c.command.Process.Signal(sig)
}

func (c *shutdownChildProcess) waitExit(timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-c.done:
		return c.waitError()
	case <-timer.C:
		return fmt.Errorf("timed out waiting for TAG subprocess exit: %s", c.stderr.String())
	}
}

func (c *shutdownChildProcess) waitError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.waitErr
}

func (c *shutdownChildProcess) cleanup() {
	c.release()
	c.finish()
	if c.command.Process != nil {
		select {
		case <-c.done:
		default:
			_ = c.command.Process.Kill()
		}
	}
	_ = c.waitExit(shutdownProbeTimeout)
	_ = c.input.Close()
	_ = os.RemoveAll(c.dir)
}
