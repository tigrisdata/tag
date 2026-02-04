package proxy

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// StreamingPayloadHash is the X-Amz-Content-Sha256 value used by AWS SDKs
// to indicate AWS chunked transfer encoding (streaming SigV4).
const StreamingPayloadHash = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"

// awsChunkedReader decodes AWS S3 chunked transfer encoding.
//
// Wire format per chunk:
//
//	<hex-chunk-size>;chunk-signature=<signature>\r\n
//	<chunk-data>\r\n
//
// Terminal chunk:
//
//	0;chunk-signature=<signature>\r\n
//	\r\n
//
// The reader strips the framing and returns only the raw chunk data.
// Chunk signatures are not validated (the request-level signature was
// already verified by the auth validator).
type awsChunkedReader struct {
	reader    *bufio.Reader
	remaining int
	done      bool
}

func newAWSChunkedReader(r io.Reader) *awsChunkedReader {
	return &awsChunkedReader{
		reader: bufio.NewReaderSize(r, 64*1024),
	}
}

func (r *awsChunkedReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}

	// If no data remaining in current chunk, read next chunk header.
	if r.remaining == 0 {
		if err := r.readChunkHeader(); err != nil {
			return 0, err
		}
		if r.done {
			return 0, io.EOF
		}
	}

	// Read up to remaining bytes from current chunk.
	toRead := len(p)
	if toRead > r.remaining {
		toRead = r.remaining
	}

	n, err := r.reader.Read(p[:toRead])
	r.remaining -= n

	// When we've consumed the entire chunk, read the trailing \r\n.
	if r.remaining == 0 && err == nil {
		if err := r.readTrailingCRLF(); err != nil {
			return n, err
		}
	}

	return n, err
}

// maxChunkHeaderLen is the maximum allowed length of a chunk header line.
// AWS chunk headers contain a hex size and a signature (~88 chars for the
// signature), so 256 bytes is generous. This prevents memory exhaustion
// from a malicious request body containing no newlines.
const maxChunkHeaderLen = 256

// readChunkHeader reads a line like "<hex-size>;chunk-signature=<sig>\r\n"
// and sets r.remaining to the chunk data size. Sets r.done if size is 0.
func (r *awsChunkedReader) readChunkHeader() error {
	line, err := r.readLine()
	if err != nil {
		return fmt.Errorf("reading chunk header: %w", err)
	}

	// Extract hex size before the semicolon
	sizeStr := line
	if idx := strings.IndexByte(line, ';'); idx != -1 {
		sizeStr = line[:idx]
	}

	size, err := strconv.ParseInt(strings.TrimSpace(sizeStr), 16, 64)
	if err != nil {
		return fmt.Errorf("parsing chunk size %q: %w", sizeStr, err)
	}

	if size <= 0 {
		r.done = true
		return nil
	}

	r.remaining = int(size)
	return nil
}

// readLine reads a single \n-terminated line from the reader, enforcing
// maxChunkHeaderLen to prevent unbounded memory allocation.
func (r *awsChunkedReader) readLine() (string, error) {
	var line []byte
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return "", err
		}
		if b == '\n' {
			return strings.TrimRight(string(line), "\r"), nil
		}
		line = append(line, b)
		if len(line) > maxChunkHeaderLen {
			return "", fmt.Errorf("chunk header exceeds maximum length (%d bytes)", maxChunkHeaderLen)
		}
	}
}

// readTrailingCRLF consumes and validates the \r\n after chunk data.
func (r *awsChunkedReader) readTrailingCRLF() error {
	var buf [2]byte
	_, err := io.ReadFull(r.reader, buf[:])
	if err != nil {
		return fmt.Errorf("reading chunk trailer: %w", err)
	}
	if buf[0] != '\r' || buf[1] != '\n' {
		return fmt.Errorf("invalid chunk trailer: expected CRLF, got %q", buf)
	}
	return nil
}

// decodeChunkedIfNeeded checks if the request uses AWS chunked transfer encoding.
// If so, returns a decoded body reader, UNSIGNED-PAYLOAD as the body hash, the
// decoded content length, and chunked=true. Otherwise returns the original values
// unchanged with chunked=false.
func decodeChunkedIfNeeded(r *http.Request) (body io.ReadCloser, bodyHash string, contentLength int64, chunked bool) {
	bodyHash = r.Header.Get("X-Amz-Content-Sha256")

	if bodyHash != StreamingPayloadHash {
		return r.Body, bodyHash, r.ContentLength, false
	}

	// Parse decoded content length from header
	contentLength = -1
	if v := r.Header.Get("X-Amz-Decoded-Content-Length"); v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			contentLength = parsed
		}
	}

	decoded := newAWSChunkedReader(r.Body)
	return io.NopCloser(decoded), "UNSIGNED-PAYLOAD", contentLength, true
}
