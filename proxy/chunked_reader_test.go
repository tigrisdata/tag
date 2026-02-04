package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAWSChunkedReader_SingleChunk(t *testing.T) {
	// 4 bytes of data + terminal chunk
	input := "4;chunk-signature=abc123\r\ntest\r\n0;chunk-signature=def456\r\n\r\n"
	reader := newAWSChunkedReader(strings.NewReader(input))

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "test" {
		t.Errorf("got %q, want %q", string(data), "test")
	}
}

func TestAWSChunkedReader_MultipleChunks(t *testing.T) {
	input := "4;chunk-signature=sig1\r\ntest\r\n5;chunk-signature=sig2\r\nhello\r\n0;chunk-signature=sig3\r\n\r\n"
	reader := newAWSChunkedReader(strings.NewReader(input))

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "testhello" {
		t.Errorf("got %q, want %q", string(data), "testhello")
	}
}

func TestAWSChunkedReader_EmptyBody(t *testing.T) {
	input := "0;chunk-signature=sig1\r\n\r\n"
	reader := newAWSChunkedReader(strings.NewReader(input))

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("got %d bytes, want 0", len(data))
	}
}

func TestAWSChunkedReader_LargeChunk(t *testing.T) {
	// 64KB chunk
	payload := strings.Repeat("A", 65536)
	input := "10000;chunk-signature=sig1\r\n" + payload + "\r\n0;chunk-signature=sig2\r\n\r\n"
	reader := newAWSChunkedReader(strings.NewReader(input))

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 65536 {
		t.Errorf("got %d bytes, want 65536", len(data))
	}
	if string(data) != payload {
		t.Error("payload mismatch")
	}
}

func TestAWSChunkedReader_SmallReads(t *testing.T) {
	input := "a;chunk-signature=sig1\r\n0123456789\r\n0;chunk-signature=sig2\r\n\r\n"
	reader := newAWSChunkedReader(strings.NewReader(input))

	// Read 3 bytes at a time
	var result []byte
	buf := make([]byte, 3)
	for {
		n, err := reader.Read(buf)
		result = append(result, buf[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if string(result) != "0123456789" {
		t.Errorf("got %q, want %q", string(result), "0123456789")
	}
}

func TestAWSChunkedReader_LongSignature(t *testing.T) {
	// Real-world signatures are 64 hex chars
	sig := strings.Repeat("ab", 32)
	input := "3;chunk-signature=" + sig + "\r\nfoo\r\n0;chunk-signature=" + sig + "\r\n\r\n"
	reader := newAWSChunkedReader(strings.NewReader(input))

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "foo" {
		t.Errorf("got %q, want %q", string(data), "foo")
	}
}

func TestDecodeChunkedIfNeeded_NotChunked(t *testing.T) {
	body := io.NopCloser(strings.NewReader("hello"))
	req := httptest.NewRequest(http.MethodPut, "http://localhost/bucket/key", body)
	req.Header.Set("X-Amz-Content-Sha256", "abc123hash")
	req.ContentLength = 5

	gotBody, gotHash, gotLen := decodeChunkedIfNeeded(req)

	if gotHash != "abc123hash" {
		t.Errorf("bodyHash = %q, want %q", gotHash, "abc123hash")
	}
	if gotLen != 5 {
		t.Errorf("contentLength = %d, want 5", gotLen)
	}

	data, _ := io.ReadAll(gotBody)
	if string(data) != "hello" {
		t.Errorf("body = %q, want %q", string(data), "hello")
	}
}

func TestDecodeChunkedIfNeeded_Chunked(t *testing.T) {
	chunkedBody := "4;chunk-signature=sig1\r\ntest\r\n0;chunk-signature=sig2\r\n\r\n"
	body := io.NopCloser(strings.NewReader(chunkedBody))
	req := httptest.NewRequest(http.MethodPut, "http://localhost/bucket/key", body)
	req.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	req.Header.Set("X-Amz-Decoded-Content-Length", "4")
	req.ContentLength = int64(len(chunkedBody))

	gotBody, gotHash, gotLen := decodeChunkedIfNeeded(req)

	if gotHash != "UNSIGNED-PAYLOAD" {
		t.Errorf("bodyHash = %q, want %q", gotHash, "UNSIGNED-PAYLOAD")
	}
	if gotLen != 4 {
		t.Errorf("contentLength = %d, want 4", gotLen)
	}

	data, err := io.ReadAll(gotBody)
	if err != nil {
		t.Fatalf("reading decoded body: %v", err)
	}
	if string(data) != "test" {
		t.Errorf("body = %q, want %q", string(data), "test")
	}
}

func TestDecodeChunkedIfNeeded_MissingDecodedLength(t *testing.T) {
	chunkedBody := "3;chunk-signature=sig1\r\nfoo\r\n0;chunk-signature=sig2\r\n\r\n"
	body := io.NopCloser(strings.NewReader(chunkedBody))
	req := httptest.NewRequest(http.MethodPut, "http://localhost/bucket/key", body)
	req.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	// No X-Amz-Decoded-Content-Length header

	_, gotHash, gotLen := decodeChunkedIfNeeded(req)

	if gotHash != "UNSIGNED-PAYLOAD" {
		t.Errorf("bodyHash = %q, want %q", gotHash, "UNSIGNED-PAYLOAD")
	}
	if gotLen != -1 {
		t.Errorf("contentLength = %d, want -1", gotLen)
	}
}
