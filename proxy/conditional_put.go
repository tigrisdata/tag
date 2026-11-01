package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/tigrisdata/tag/auth"
)

// preparePutWithoutConditions consumes conditions checked against the local
// prior. Authenticate the original body before changing its request signature.
func preparePutWithoutConditions(ctx context.Context, r *http.Request, vr *auth.ValidatedRequest) (*http.Request, error) {
	modified := r.Clone(ctx)
	marker := r.Header.Get("X-Amz-Content-Sha256")
	if marker == StreamingPayloadHash || marker == StreamingPayloadHash+"-TRAILER" {
		if values := r.Header.Values("X-Amz-Decoded-Content-Length"); len(values) > 0 {
			length, err := strconv.ParseInt(values[0], 10, 64)
			if err != nil || length < 0 {
				return nil, mapAuthError(auth.ErrChunkSignatureMismatch)
			}
		}
	}
	switch marker {
	case StreamingUnsignedTrailerHash:
		ensureAWSChunkedEncoding(modified)
		// There is no signed chunk chain to reseed. Preserve the wire body and
		// trailers so upstream still enforces the client's checksum.
	case StreamingPayloadHash + "-TRAILER":
		length, ok := originlessPutSize(r)
		if !ok {
			length = -1
		}
		modified.Body = struct {
			io.Reader
			io.Closer
		}{newAuthenticatedTrailerReader(r.Body, vr.ChunkVerifier(), length, r.Header.Get("X-Amz-Trailer")), r.Body}
		modified.ContentLength = -1 // re-framing changes the wire length
		modified.Header.Set("X-Amz-Content-Sha256", StreamingUnsignedTrailerHash)
		ensureAWSChunkedEncoding(modified)
	default:
		if IsStreamingPayload(marker) && marker != StreamingPayloadHash {
			// Local SigV4 validation supplies no SigV4A body verifier. Never
			// turn an unsupported signed stream into authenticated unsigned data.
			return nil, mapAuthError(auth.ErrUnsupportedAuthScheme)
		}
		body, bodyHash, length, chunked, err := decodeBody(r, vr)
		if err != nil {
			return nil, err
		}
		modified.Body = body
		if chunked {
			modified.Body = struct {
				io.Reader
				io.Closer
			}{body, r.Body}
		}
		modified.Header = headersForForwardedSigning(modified.Header, chunked)
		modified.Header.Set("X-Amz-Content-Sha256", bodyHash)
		prepareForwardedRequest(modified, length, chunked)
	}
	modified.Header.Del("If-Match")
	modified.Header.Del("If-None-Match")
	return modified, nil
}

// authenticatedTrailerReader re-frames verified HMAC chunks as unsigned chunks,
// retaining the authenticated checksum trailers for upstream validation. No
// successful terminator is emitted unless the entire original chain and trailer
// signature verify. The fixed buffer is independent of the client's chunk size.
type authenticatedTrailerReader struct {
	decoded  *awsChunkedReader
	buffer   [32 * 1024]byte
	pending  io.Reader
	trailers string
	done     bool
}

func newAuthenticatedTrailerReader(body io.Reader, verifier *auth.ChunkVerifier, length int64, declared string) *authenticatedTrailerReader {
	r := &authenticatedTrailerReader{decoded: newVerifyingAWSChunkedReader(body, verifier, length)}
	r.decoded.verifyTerminal = func(size int64) error {
		if size != 0 {
			return fmt.Errorf("invalid terminal chunk size: %w", auth.ErrChunkSignatureMismatch)
		}
		var err error
		r.trailers, err = r.readTrailers(verifier, declared)
		return err
	}
	return r
}

func (r *authenticatedTrailerReader) readTrailers(verifier *auth.ChunkVerifier, declared string) (string, error) {
	fail := func() (string, error) {
		return "", fmt.Errorf("invalid signed trailers: %w", auth.ErrChunkSignatureMismatch)
	}
	names := strings.Split(declared, ",")
	values := make(map[string]string, len(names))
	for i, name := range names {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" || name == "x-amz-trailer-signature" {
			return fail()
		}
		if _, duplicate := values[name]; duplicate {
			return fail()
		}
		names[i] = name
		values[name] = ""
	}
	var signature string
	seen := make(map[string]bool, len(names))
	// Exactly the declared trailers, a signature, and the final empty line.
	for i := 0; i <= len(names)+1; i++ {
		line, err := r.decoded.readLine()
		if err != nil {
			return fail()
		}
		if line == "" {
			if len(seen) != len(names) || signature == "" {
				return fail()
			}
			sort.Strings(names)
			var canonical strings.Builder
			for _, name := range names {
				canonical.WriteString(name + ":" + values[name] + "\n")
			}
			sum := sha256.Sum256([]byte(canonical.String()))
			if err := verifier.VerifyTrailer(signature, sum[:]); err != nil {
				return "", err
			}
			return strings.ReplaceAll(canonical.String(), "\n", "\r\n"), nil
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			return fail()
		}
		name, value = strings.ToLower(name), strings.TrimSpace(value)
		if name == "x-amz-trailer-signature" {
			if signature != "" {
				return fail()
			}
			signature = value
		} else if _, declared := values[name]; !declared || seen[name] {
			return fail()
		} else {
			values[name], seen[name] = value, true
		}
	}
	return fail()
}

func (r *authenticatedTrailerReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if r.pending != nil {
			n, err := r.pending.Read(p)
			if err == io.EOF {
				r.pending = nil
				if n != 0 {
					return n, nil
				}
				continue
			}
			return n, err
		}
		if r.done {
			return 0, io.EOF
		}
		n, err := r.decoded.Read(r.buffer[:])
		if err != nil && err != io.EOF {
			return 0, err
		}
		if n != 0 {
			r.pending = io.MultiReader(strings.NewReader(fmt.Sprintf("%x\r\n", n)), bytes.NewReader(r.buffer[:n]), strings.NewReader("\r\n"))
		} else if err == io.EOF {
			r.done = true
			r.pending = strings.NewReader("0\r\n" + r.trailers + "\r\n")
		}
	}
}
