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

	gotBody, gotHash, gotLen, gotChunked := decodeChunkedIfNeeded(req)

	if gotChunked {
		t.Error("chunked = true, want false")
	}
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

	gotBody, gotHash, gotLen, gotChunked := decodeChunkedIfNeeded(req)

	if !gotChunked {
		t.Error("chunked = false, want true")
	}
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

	_, gotHash, gotLen, gotChunked := decodeChunkedIfNeeded(req)

	if !gotChunked {
		t.Error("chunked = false, want true")
	}
	if gotHash != "UNSIGNED-PAYLOAD" {
		t.Errorf("bodyHash = %q, want %q", gotHash, "UNSIGNED-PAYLOAD")
	}
	if gotLen != -1 {
		t.Errorf("contentLength = %d, want -1", gotLen)
	}
}

func TestAWSChunkedReader_InvalidTrailer(t *testing.T) {
	// Chunk data followed by invalid trailer (not \r\n)
	input := "3;chunk-signature=sig1\r\nfooXX"
	reader := newAWSChunkedReader(strings.NewReader(input))

	_, err := io.ReadAll(reader)
	if err == nil {
		t.Fatal("expected error for invalid chunk trailer")
	}
	if !strings.Contains(err.Error(), "expected CRLF") {
		t.Errorf("error = %q, want it to mention expected CRLF", err.Error())
	}
}

func TestAWSChunkedReader_InvalidChunkSize(t *testing.T) {
	input := "ZZZZ;chunk-signature=sig1\r\n"
	reader := newAWSChunkedReader(strings.NewReader(input))

	_, err := io.ReadAll(reader)
	if err == nil {
		t.Fatal("expected error for invalid hex chunk size")
	}
	if !strings.Contains(err.Error(), "parsing chunk size") {
		t.Errorf("error = %q, want it to mention parsing chunk size", err.Error())
	}
}

func TestDecodeChunkedIfNeeded_ZeroByteChunked(t *testing.T) {
	// Zero-byte upload via chunked encoding: only a terminal chunk
	chunkedBody := "0;chunk-signature=sig1\r\n\r\n"
	body := io.NopCloser(strings.NewReader(chunkedBody))
	req := httptest.NewRequest(http.MethodPut, "http://localhost/bucket/key", body)
	req.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	req.Header.Set("X-Amz-Decoded-Content-Length", "0")
	req.ContentLength = int64(len(chunkedBody))

	gotBody, gotHash, gotLen, gotChunked := decodeChunkedIfNeeded(req)

	if !gotChunked {
		t.Error("chunked = false, want true")
	}
	if gotHash != "UNSIGNED-PAYLOAD" {
		t.Errorf("bodyHash = %q, want %q", gotHash, "UNSIGNED-PAYLOAD")
	}
	if gotLen != 0 {
		t.Errorf("contentLength = %d, want 0", gotLen)
	}

	data, err := io.ReadAll(gotBody)
	if err != nil {
		t.Fatalf("reading decoded body: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("body length = %d, want 0", len(data))
	}
}

func TestPrepareForwardedRequest_Chunked(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "http://localhost/bucket/key", nil)
	req.Header.Set("X-Amz-Decoded-Content-Length", "1024")
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.ContentLength = 2000 // wire size

	prepareForwardedRequest(req, 1024, true)

	if req.ContentLength != 1024 {
		t.Errorf("ContentLength = %d, want 1024", req.ContentLength)
	}
	if req.Header.Get("X-Amz-Decoded-Content-Length") != "" {
		t.Error("X-Amz-Decoded-Content-Length should be removed")
	}
	if req.Header.Get("Content-Encoding") != "" {
		t.Error("Content-Encoding: aws-chunked should be removed")
	}
}

func TestPrepareForwardedRequest_ChunkedZeroByte(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "http://localhost/bucket/key", nil)
	req.Header.Set("X-Amz-Decoded-Content-Length", "0")
	req.ContentLength = 100 // wire size with chunk framing

	prepareForwardedRequest(req, 0, true)

	if req.ContentLength != 0 {
		t.Errorf("ContentLength = %d, want 0", req.ContentLength)
	}
	if req.Header.Get("X-Amz-Decoded-Content-Length") != "" {
		t.Error("X-Amz-Decoded-Content-Length should be removed")
	}
}

func TestPrepareForwardedRequest_NonChunked(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "http://localhost/bucket/key", nil)
	req.ContentLength = 0

	prepareForwardedRequest(req, 512, false)

	if req.ContentLength != 512 {
		t.Errorf("ContentLength = %d, want 512", req.ContentLength)
	}
}

func TestPrepareForwardedRequest_NonChunkedNoBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://localhost/bucket/key", nil)
	req.ContentLength = 0

	prepareForwardedRequest(req, 0, false)

	// Should not change ContentLength for non-chunked with 0/negative length
	if req.ContentLength != 0 {
		t.Errorf("ContentLength = %d, want 0", req.ContentLength)
	}
}

func TestAWSChunkedReader_NegativeChunkSize(t *testing.T) {
	// Negative hex size should be treated as terminal (size <= 0), not panic.
	input := "-1;chunk-signature=sig1\r\n"
	reader := newAWSChunkedReader(strings.NewReader(input))

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("got %d bytes, want 0", len(data))
	}
}

func TestAWSChunkedReader_UnboundedHeaderLine(t *testing.T) {
	// A body with no newline should hit the max header length limit, not OOM.
	input := strings.Repeat("A", maxChunkHeaderLen+10)
	reader := newAWSChunkedReader(strings.NewReader(input))

	_, err := io.ReadAll(reader)
	if err == nil {
		t.Fatal("expected error for oversized chunk header")
	}
	if !strings.Contains(err.Error(), "exceeds maximum length") {
		t.Errorf("error = %q, want it to mention exceeds maximum length", err.Error())
	}
}

func TestPrepareForwardedRequest_PreservesNonAWSContentEncoding(t *testing.T) {
	req := httptest.NewRequest(http.MethodPut, "http://localhost/bucket/key", nil)
	req.Header.Set("Content-Encoding", "gzip")

	prepareForwardedRequest(req, 1024, true)

	if req.Header.Get("Content-Encoding") != "gzip" {
		t.Errorf("Content-Encoding = %q, want %q", req.Header.Get("Content-Encoding"), "gzip")
	}
}

func TestPrepareForwardedRequest_CombinedContentEncoding(t *testing.T) {
	// AWS S3 allows "aws-chunked,gzip" — strip aws-chunked, keep gzip.
	req := httptest.NewRequest(http.MethodPut, "http://localhost/bucket/key", nil)
	req.Header.Set("X-Amz-Decoded-Content-Length", "1024")
	req.Header.Set("Content-Encoding", "aws-chunked,gzip")
	req.ContentLength = 2000

	prepareForwardedRequest(req, 1024, true)

	if got := req.Header.Get("Content-Encoding"); got != "gzip" {
		t.Errorf("Content-Encoding = %q, want %q", got, "gzip")
	}
}

func TestPrepareForwardedRequest_CombinedContentEncodingWithSpaces(t *testing.T) {
	// Handle whitespace around tokens.
	req := httptest.NewRequest(http.MethodPut, "http://localhost/bucket/key", nil)
	req.Header.Set("Content-Encoding", "aws-chunked , gzip")

	prepareForwardedRequest(req, 1024, true)

	if got := req.Header.Get("Content-Encoding"); got != "gzip" {
		t.Errorf("Content-Encoding = %q, want %q", got, "gzip")
	}
}
