package proxy

import (
	"bufio"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/tigrisdata/tag/auth"
)

// StreamingPayloadHash is the X-Amz-Content-Sha256 value used by AWS SDKs
// to indicate AWS chunked transfer encoding (streaming SigV4).
const StreamingPayloadHash = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"

// StreamingUnsignedTrailerHash is the X-Amz-Content-Sha256 value used by
// AWS SDK v2 for unsigned chunked encoding with trailing checksums.
const StreamingUnsignedTrailerHash = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"

// streamingPayloadHashes is the full set of X-Amz-Content-Sha256 markers AWS
// SDKs use for aws-chunked bodies: SigV4 and SigV4A (ECDSA) signed chunks, each
// with and without trailing checksums, plus unsigned chunks with trailers. All
// share the same chunk framing; missing one here would store the framing as
// object bytes.
var streamingPayloadHashes = map[string]bool{
	StreamingPayloadHash:                               true,
	StreamingPayloadHash + "-TRAILER":                  true,
	StreamingUnsignedTrailerHash:                       true,
	"STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD":         true,
	"STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER": true,
}

// IsStreamingPayload returns true if the given body hash indicates
// AWS chunked transfer encoding (signed, signed-with-trailer, ECDSA,
// or unsigned-with-trailer variants).
func IsStreamingPayload(bodyHash string) bool {
	return streamingPayloadHashes[bodyHash]
}

// awsChunkedReader decodes AWS S3 chunked transfer encoding.
//
// Signed wire format per chunk (STREAMING-AWS4-HMAC-SHA256-PAYLOAD):
//
//	<hex-chunk-size>;chunk-signature=<signature>\r\n
//	<chunk-data>\r\n
//
// Unsigned wire format per chunk (STREAMING-UNSIGNED-PAYLOAD-TRAILER):
//
//	<hex-chunk-size>\r\n
//	<chunk-data>\r\n
//
// Terminal chunk (both formats):
//
//	0[;chunk-signature=<signature>]\r\n
//
// The reader strips the framing and returns only the raw chunk data.
//
// With a verifier, every chunk's signature is checked against the request's
// SigV4 chunk chain as the chunk is consumed. The data is hashed as it streams
// through, and the chunk's LAST byte is withheld until the trailing CRLF has
// been read, the signature verified, and the NEXT frame header read — a
// terminal frame being verified too, before it is accepted as end of body. A
// mismatch (or a missing chunk-signature) is a sticky, wrapped
// auth.ErrChunkSignatureMismatch from Read. So a chunk that fails
// verification is never completely delivered, and a clean io.EOF exists only
// after the whole chain — terminal frame included — verified.
//
// The frames are also bound to the DECLARED decoded length (the request's
// X-Amz-Decoded-Content-Length, a signed header, which the forwarder sends
// upstream as the exact Content-Length and the engine stages the body
// against): a frame claiming more bytes than the declaration has left is
// refused before a single byte of it is delivered, and a terminal frame that
// arrives with declared bytes still unsent is refused too. Both are sticky
// auth.ErrChunkSignatureMismatch failures: the chain cannot authenticate the
// declared body when the framing is not that body. Withholding the last byte
// of a chunk alone would not do: a consumer taking exactly the declared N
// bytes would be satisfied by the first N bytes of a frame CLAIMING more,
// before that frame's signature was ever checked. With the bound, the
// declared body's final byte is always the last byte of a frame whose
// successor can only be the terminal frame, so it is delivered only after
// the terminal signature verified — a consumer counting bytes against the
// declared length can never hold a complete body that was not authenticated
// end to end. A verifier with no declared length (-1) keeps the per-chunk
// rules only; its consumer has no exact length to be satisfied by and must
// read to the verified EOF.
//
// Withholding one byte rather than buffering the chunk keeps memory at the
// fixed bufio buffer whatever chunk size the client chose. Without a verifier
// the framing is decoded and signatures are ignored; trailing checksums are
// never validated.
type awsChunkedReader struct {
	reader    *bufio.Reader
	remaining int
	done      bool

	verifier  *auth.ChunkVerifier // nil: decode framing only
	chunkSig  string              // signature claimed by the current chunk
	chunkHash hash.Hash           // SHA-256 of the current chunk's data
	err       error               // sticky verification failure
	// undeclared is how many bytes of the declared decoded length no frame
	// has claimed yet; -1 when the request declared no decoded length.
	// Verifying readers only.
	undeclared int64
}

// awsChunkedReaderBufSize is the decoder's internal bufio buffer. Named so
// budget accounting (origin-less PUT admission) can reserve exactly what the
// decoder allocates.
const awsChunkedReaderBufSize = 64 * 1024

func newAWSChunkedReader(r io.Reader) *awsChunkedReader {
	return &awsChunkedReader{
		reader:     bufio.NewReaderSize(r, awsChunkedReaderBufSize),
		undeclared: -1,
	}
}

// newVerifyingAWSChunkedReader decodes an aws-chunked body and verifies each
// chunk's signature against verifier, binding the frames to decodedLength —
// the request's declared decoded length, or -1 when it declared none. A nil
// verifier decodes framing only, unbounded.
func newVerifyingAWSChunkedReader(r io.Reader, verifier *auth.ChunkVerifier, decodedLength int64) *awsChunkedReader {
	cr := newAWSChunkedReader(r)
	if verifier != nil {
		cr.verifier = verifier
		cr.chunkHash = sha256.New()
		if decodedLength >= 0 {
			cr.undeclared = decodedLength
		}
	}
	return cr
}

func (r *awsChunkedReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
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

	// With a verifier, the chunk's last byte is delivered only once the
	// chunk has verified.
	if r.verifier != nil && r.remaining == 1 {
		return r.readVerifiedLastByte(p)
	}

	// Read up to remaining bytes from current chunk — all but the withheld
	// last byte when verifying (remaining >= 2 here, so toRead stays >= 1
	// for a non-empty p).
	toRead := len(p)
	if toRead > r.remaining {
		toRead = r.remaining
	}
	if r.verifier != nil && toRead == r.remaining {
		toRead--
	}

	n, err := r.reader.Read(p[:toRead])
	r.remaining -= n
	if r.chunkHash != nil {
		r.chunkHash.Write(p[:n])
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			// The body ended INSIDE a chunk's data. A bare io.EOF here would
			// read as clean termination to callers — and one whose byte
			// count happens to match the declared length would commit a
			// never-terminated body. Clean EOF exists ONLY after the
			// terminal 0-chunk (r.done above); every truncation shape
			// surfaces as a wrapped, non-sentinel error.
			return n, fmt.Errorf("aws-chunked body truncated mid-chunk: %w", io.ErrUnexpectedEOF)
		}
		return n, err
	}

	// When we've consumed the entire chunk, read the trailing \r\n.
	if r.remaining == 0 {
		if err := r.readTrailingCRLF(); err != nil {
			return n, err
		}
	}

	return n, nil
}

// readVerifiedLastByte consumes the current chunk's withheld last byte and
// trailing CRLF, verifies the chunk's signature, reads the next frame header
// (verifying it when terminal), and only then delivers the byte. A mismatch
// anywhere is sticky: the chunk — and so the body — stays incomplete.
func (r *awsChunkedReader) readVerifiedLastByte(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	var last [1]byte
	if _, err := io.ReadFull(r.reader, last[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, fmt.Errorf("aws-chunked body truncated mid-chunk: %w", io.ErrUnexpectedEOF)
		}
		return 0, err
	}
	r.chunkHash.Write(last[:])
	if err := r.readTrailingCRLF(); err != nil {
		return 0, err
	}
	if err := r.verifier.VerifyChunk(r.chunkSig, r.chunkHash.Sum(nil)); err != nil {
		r.err = fmt.Errorf("verifying chunk signature: %w", err)
		return 0, r.err
	}
	r.remaining = 0
	// Look ahead to the next frame: it sets up the next chunk, or verifies
	// the terminal frame and marks the body done. Either way the withheld
	// byte goes out only with a verified continuation.
	if err := r.readChunkHeader(); err != nil {
		return 0, err
	}
	p[0] = last[0]
	return 1, nil
}

// chunkSignatureExtension extracts the chunk-signature value from a chunk
// header's extensions ("chunk-signature=<sig>[;...]").
func chunkSignatureExtension(ext string) (string, bool) {
	for _, part := range strings.Split(ext, ";") {
		if sig, ok := strings.CutPrefix(strings.TrimSpace(part), "chunk-signature="); ok {
			return sig, true
		}
	}
	return "", false
}

// maxChunkHeaderLen is the maximum allowed length of a chunk header line.
// AWS chunk headers contain a hex size and a signature (~88 chars for the
// signature), so 256 bytes is generous. This prevents memory exhaustion
// from a malicious request body containing no newlines.
const maxChunkHeaderLen = 256

// readChunkHeader reads a line like "<hex-size>;chunk-signature=<sig>\r\n"
// and sets r.remaining to the chunk data size. Sets r.done if size is 0.
// With a verifier it also binds the frame to the declared decoded length and
// verifies a terminal frame's signature, so no byte of an over-claiming frame
// and no EOF of a short or unverified stream ever reaches the consumer.
func (r *awsChunkedReader) readChunkHeader() error {
	line, err := r.readLine()
	if err != nil {
		return fmt.Errorf("reading chunk header: %w", err)
	}

	// Extract hex size before the semicolon
	sizeStr, ext, _ := strings.Cut(line, ";")

	size, err := strconv.ParseInt(strings.TrimSpace(sizeStr), 16, 64)
	if err != nil {
		return fmt.Errorf("parsing chunk size %q: %w", sizeStr, err)
	}

	if r.verifier != nil {
		sig, ok := chunkSignatureExtension(ext)
		if !ok {
			r.err = fmt.Errorf("chunk header carries no chunk-signature: %w", auth.ErrChunkSignatureMismatch)
			return r.err
		}
		switch {
		case size <= 0:
			// The terminal chunk is signed over empty data; verify it before
			// accepting it as end of body.
			emptySum := sha256.Sum256(nil)
			if err := r.verifier.VerifyChunk(sig, emptySum[:]); err != nil {
				r.err = fmt.Errorf("verifying final chunk signature: %w", err)
				return r.err
			}
			if r.undeclared > 0 {
				// The frames stopped short of the declared body: a
				// verified chain over some OTHER body is no authentication
				// of the declared one, and a clean EOF here would let a
				// consumer treat the shortfall as the client's transfer
				// fault rather than a body that never verified.
				r.err = fmt.Errorf("terminal chunk arrived with %d bytes of the declared decoded length unsent: %w", r.undeclared, auth.ErrChunkSignatureMismatch)
				return r.err
			}
		case r.undeclared >= 0 && size > r.undeclared:
			// Refused BEFORE any of the frame's bytes are read: a consumer
			// taking the declared length would otherwise be satisfied by
			// the frame's first bytes, ahead of its signature check.
			r.err = fmt.Errorf("chunk claims %d bytes, more than the %d left of the declared decoded length: %w", size, r.undeclared, auth.ErrChunkSignatureMismatch)
			return r.err
		case r.undeclared >= 0:
			r.undeclared -= size
		}
		r.chunkSig = sig
		r.chunkHash.Reset()
	}

	if size <= 0 {
		r.done = true
		return nil
	}

	r.remaining = int(size)
	return nil
}

// readLine reads a single \n-terminated line from the reader, enforcing
// maxChunkHeaderLen to prevent unbounded memory allocation. Its fixed buffer
// avoids growing and copying a slice for each header.
func (r *awsChunkedReader) readLine() (string, error) {
	var line [maxChunkHeaderLen]byte
	n := 0
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return "", err
		}
		if b == '\n' {
			return strings.TrimRight(string(line[:n]), "\r"), nil
		}
		if n == len(line) {
			return "", fmt.Errorf("chunk header exceeds maximum length (%d bytes)", maxChunkHeaderLen)
		}
		line[n] = b
		n++
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
//
// Chunk-signature verification covers exactly the STREAMING-AWS4-HMAC-SHA256-PAYLOAD
// marker, using the chain seeded by the validated request (vr): without it the
// request-level signature only authenticates the headers, and a captured
// header-signed PUT could be replayed with an arbitrary body (TAG re-signs
// upstream as UNSIGNED-PAYLOAD, so nothing downstream would catch it). The
// other markers keep framing-only decoding, as before: the -TRAILER variant
// (signed chunks plus a signed trailer) and the ECDSA (SigV4A) variants would
// need their own chain and key handling, and STREAMING-UNSIGNED-PAYLOAD-TRAILER
// carries no chunk signatures at all. A nil vr for the signed marker decodes
// without verification; callers in signing mode always validate first.
func decodeChunkedIfNeeded(r *http.Request, vr *auth.ValidatedRequest) (body io.ReadCloser, bodyHash string, contentLength int64, chunked bool) {
	bodyHash = r.Header.Get("X-Amz-Content-Sha256")

	if !IsStreamingPayload(bodyHash) {
		return r.Body, bodyHash, r.ContentLength, false
	}

	// Parse decoded content length from header
	contentLength = -1
	if v := r.Header.Get("X-Amz-Decoded-Content-Length"); v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			contentLength = parsed
		}
	}

	decoded := newVerifyingAWSChunkedReader(r.Body, streamChunkVerifier(r, vr), contentLength)
	return io.NopCloser(decoded), "UNSIGNED-PAYLOAD", contentLength, true
}

// streamChunkVerifier returns the chunk-chain verifier a body decoded from r
// is checked against: one seeded by vr for the signed-chunk marker, nil for
// the framing-only markers or when the request was not validated. The single
// place that decides whether a stream is verified, so a caller that must act
// on the decoder's existence (the empty-stream drain) cannot disagree with
// decodeChunkedIfNeeded.
func streamChunkVerifier(r *http.Request, vr *auth.ValidatedRequest) *auth.ChunkVerifier {
	if vr == nil || r.Header.Get("X-Amz-Content-Sha256") != StreamingPayloadHash {
		return nil
	}
	return vr.ChunkVerifier()
}

// stripAWSChunkedToken removes the aws-chunked token from a Content-Encoding
// value, preserving any remaining encodings ("aws-chunked,gzip" → "gzip").
// Content-coding tokens are case-insensitive (RFC 9110), and chunked decoding
// keys off the streaming SHA-256 marker rather than this header's spelling, so
// any casing of the token must be stripped once the framing is decoded.
func stripAWSChunkedToken(ce string) string {
	var remaining []string
	for _, part := range strings.Split(ce, ",") {
		if p := strings.TrimSpace(part); p != "" && !strings.EqualFold(p, "aws-chunked") {
			remaining = append(remaining, p)
		}
	}
	return strings.Join(remaining, ",")
}
