package proxy

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/auth"
)

const signedChunkStringToSignPrefix = "AWS4-HMAC-SHA256-PAYLOAD"

// stagedSignedChunkedBody owns the verified payload file until the outbound
// request has finished using it. Close is idempotent because the transport and
// the local cleanup path may both close the request body.
type stagedSignedChunkedBody struct {
	*os.File
	path      string
	closeOnce sync.Once
	closeErr  error
}

func (b *stagedSignedChunkedBody) Close() error {
	b.closeOnce.Do(func() {
		closeErr := b.File.Close()
		removeErr := os.Remove(b.path)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
		b.closeErr = errors.Join(closeErr, removeErr)
	})
	return b.closeErr
}

// closeStagedSignedChunkedBody runs as the forwarding method's deferred owner.
// A cleanup error cannot undo a request already sent upstream, so report it
// rather than silently leaving sensitive request bytes on disk.
func closeStagedSignedChunkedBody(body *stagedSignedChunkedBody) {
	if body == nil {
		return
	}
	if err := body.Close(); err != nil {
		log.Error().Err(err).Msg("Failed to clean up verified AWS streaming payload")
	}
}

// stagedTeeReadCloser preserves the staging file's Close ownership while the
// outbound transport reads through the write-through tee.
type stagedTeeReadCloser struct {
	io.Reader
	io.Closer
}

// handoffStagedSignedChunkedBody transfers a non-empty staged body to the
// outbound request. A zero-length request is rewritten to http.NoBody, so its
// staging file has no transport owner and is closed here instead.
func handoffStagedSignedChunkedBody(body **stagedSignedChunkedBody, request *http.Request) {
	if *body == nil {
		return
	}
	if request.Body == http.NoBody {
		closeStagedSignedChunkedBody(*body)
	}
	*body = nil
}

// closeReadCloserWhenCanceled interrupts an ingress body read when its request
// context is canceled. Calling it from both the cancellation callback and normal
// cleanup is safe; each body is closed once.
func closeReadCloserWhenCanceled(ctx context.Context, body io.ReadCloser) func() {
	if body == nil {
		return func() {}
	}
	var once sync.Once
	closeBody := func() {
		once.Do(func() { _ = body.Close() })
	}
	stop := context.AfterFunc(ctx, closeBody)
	return func() {
		stop()
		closeBody()
	}
}

// stageSignedAWSChunkedBody authenticates every SigV4 chunk and the terminal
// chunk before returning a seeked, cleanup-owned file containing only payload
// bytes. Chunk data is hashed and copied with bounded buffers; the body is not
// exposed to the upstream request until the complete chain has been verified.
func stageSignedAWSChunkedBody(ctx context.Context, source io.Reader, signingKey []byte, signingTime, credentialScope, seedSignature string) (*stagedSignedChunkedBody, error) {
	file, err := os.CreateTemp("", "tag-signed-stream-*")
	if err != nil {
		return nil, fmt.Errorf("create temporary file for signed AWS stream: %w", err)
	}
	path := file.Name()
	reader := bufio.NewReaderSize(source, awsChunkedReaderBufSize)
	previousSignature := seedSignature
	decodedLength := int64(0)

	for {
		if err := ctx.Err(); err != nil {
			return nil, cleanupFailedSignedStage(file, path, fmt.Errorf("read signed AWS stream: %w", err))
		}
		line, err := readSignedChunkLine(reader)
		if err != nil {
			return nil, cleanupFailedSignedStage(file, path, fmt.Errorf("read signed AWS chunk header: %w", signedStreamReadError(ctx, err)))
		}
		chunkLength, signature, err := parseSignedChunkHeader(line)
		if err != nil {
			return nil, cleanupFailedSignedStage(file, path, err)
		}

		if chunkLength == 0 {
			expected := signedChunkSignature(signingKey, signingTime, credentialScope, previousSignature, sha256HexBytes(nil))
			if !hmac.Equal([]byte(signature), []byte(expected)) {
				return nil, cleanupFailedSignedStage(file, path, auth.ErrSignatureMismatch)
			}
			if err := readSignedChunkTerminator(reader); err != nil {
				return nil, cleanupFailedSignedStage(file, path, fmt.Errorf("read signed AWS terminal chunk terminator: %w", signedStreamReadError(ctx, err)))
			}
			if err := ctx.Err(); err != nil {
				return nil, cleanupFailedSignedStage(file, path, fmt.Errorf("finish signed AWS stream: %w", err))
			}
			break
		}

		if chunkLength > math.MaxInt64-decodedLength {
			return nil, cleanupFailedSignedStage(file, path, fmt.Errorf("signed AWS stream payload length overflows int64"))
		}
		chunkHash := sha256.New()
		written, err := io.CopyN(io.MultiWriter(file, chunkHash), reader, chunkLength)
		if err != nil {
			return nil, cleanupFailedSignedStage(file, path, fmt.Errorf("stage signed AWS chunk data: %w", signedStreamReadError(ctx, err)))
		}
		if written != chunkLength {
			return nil, cleanupFailedSignedStage(file, path, io.ErrUnexpectedEOF)
		}
		if err := ctx.Err(); err != nil {
			return nil, cleanupFailedSignedStage(file, path, fmt.Errorf("stage signed AWS stream: %w", err))
		}
		decodedLength += written

		expected := signedChunkSignature(signingKey, signingTime, credentialScope, previousSignature, hex.EncodeToString(chunkHash.Sum(nil)))
		if !hmac.Equal([]byte(signature), []byte(expected)) {
			return nil, cleanupFailedSignedStage(file, path, auth.ErrSignatureMismatch)
		}
		previousSignature = signature
		if err := readSignedChunkTerminator(reader); err != nil {
			return nil, cleanupFailedSignedStage(file, path, fmt.Errorf("read signed AWS chunk terminator: %w", signedStreamReadError(ctx, err)))
		}
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, cleanupFailedSignedStage(file, path, fmt.Errorf("rewind verified AWS stream: %w", err))
	}
	return &stagedSignedChunkedBody{File: file, path: path}, nil
}

func cleanupFailedSignedStage(file *os.File, path string, cause error) error {
	closeErr := file.Close()
	removeErr := os.Remove(path)
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("close signed AWS stream staging file: %w", closeErr)
	}
	if removeErr != nil {
		removeErr = fmt.Errorf("remove signed AWS stream staging file: %w", removeErr)
	}
	return errors.Join(cause, closeErr, removeErr)
}

func signedStreamReadError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return err
}

func readSignedChunkLine(reader *bufio.Reader) (string, error) {
	line := make([]byte, 0, 64)
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return "", err
		}
		if b == '\n' {
			if len(line) == 0 || line[len(line)-1] != '\r' {
				return "", errors.New("signed AWS chunk header is not CRLF-terminated")
			}
			return string(line[:len(line)-1]), nil
		}
		line = append(line, b)
		if len(line) > maxChunkHeaderLen {
			return "", fmt.Errorf("signed AWS chunk header exceeds maximum length (%d bytes)", maxChunkHeaderLen)
		}
	}
}

func parseSignedChunkHeader(line string) (int64, string, error) {
	sizeText, extensions, hasExtensions := strings.Cut(line, ";")
	size, err := strconv.ParseUint(strings.TrimSpace(sizeText), 16, 64)
	if err != nil {
		return 0, "", fmt.Errorf("parsing signed AWS chunk size %q: %w", sizeText, err)
	}
	if size > math.MaxInt64 {
		return 0, "", fmt.Errorf("signed AWS chunk size %d exceeds int64", size)
	}

	var signature string
	haveSignature := false
	if hasExtensions {
		for _, extension := range strings.Split(extensions, ";") {
			name, value, hasValue := strings.Cut(extension, "=")
			if name != "chunk-signature" {
				continue
			}
			if haveSignature || !hasValue || len(value) != sha256.Size*2 {
				return 0, "", auth.ErrSignatureMismatch
			}
			if _, err := hex.DecodeString(value); err != nil || strings.ToLower(value) != value {
				return 0, "", auth.ErrSignatureMismatch
			}
			signature = value
			haveSignature = true
		}
	}
	if !haveSignature {
		return 0, "", auth.ErrSignatureMismatch
	}
	return int64(size), signature, nil
}

func readSignedChunkTerminator(reader *bufio.Reader) error {
	var terminator [2]byte
	if _, err := io.ReadFull(reader, terminator[:]); err != nil {
		return err
	}
	if terminator != [2]byte{'\r', '\n'} {
		return fmt.Errorf("invalid signed AWS chunk terminator %q", terminator)
	}
	return nil
}

func signedChunkSignature(signingKey []byte, signingTime, credentialScope, previousSignature, chunkHash string) string {
	stringToSign := strings.Join([]string{
		signedChunkStringToSignPrefix,
		signingTime,
		credentialScope,
		previousSignature,
		sha256HexBytes(nil),
		chunkHash,
	}, "\n")
	mac := hmac.New(sha256.New, signingKey)
	_, _ = mac.Write([]byte(stringToSign))
	return hex.EncodeToString(mac.Sum(nil))
}

func sha256HexBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// decodeIncomingBody authenticates the only signed HMAC marker repaired by this
// path. Other supported streaming markers keep the existing decoder behavior.
func (f *signingForwarder) decodeIncomingBody(ctx context.Context, r *http.Request, accessKey string) (io.ReadCloser, string, int64, bool, *stagedSignedChunkedBody, error) {
	if r.Header.Get("X-Amz-Content-Sha256") != StreamingPayloadHash {
		body, bodyHash, contentLength, chunked := decodeChunkedIfNeeded(r)
		return body, bodyHash, contentLength, chunked, nil, nil
	}

	info, err := auth.ParseAuthInfo(r)
	if err != nil {
		return nil, "", 0, false, nil, mapAuthError(err)
	}
	if info.IsPresigned {
		// Presigned validation is based on UNSIGNED-PAYLOAD and does not establish
		// the header-signature seed required by the chunk HMAC chain.
		body, bodyHash, contentLength, chunked := decodeChunkedIfNeeded(r)
		return body, bodyHash, contentLength, chunked, nil, nil
	}

	// Staging consumes the request body before forwarding. Close it on completion
	// and interrupt any blocked read if the request is canceled.
	defer closeReadCloserWhenCanceled(ctx, r.Body)()

	dateText := r.Header.Get("X-Amz-Date")
	if dateText == "" {
		dateText = r.Header.Get("Date")
	}
	requestTime, err := auth.ParseHTTPDate(dateText)
	if err != nil {
		return nil, "", 0, false, nil, mapAuthError(auth.ErrInvalidDate)
	}
	date := info.Date
	if date == "" {
		date = requestTime.UTC().Format("20060102")
	}
	signingKey, err := f.credStore.GetSigningKey(accessKey, date, info.Region)
	if err != nil {
		return nil, "", 0, false, nil, mapAuthError(err)
	}

	staged, err := stageSignedAWSChunkedBody(
		ctx,
		r.Body,
		signingKey,
		requestTime.UTC().Format(auth.TimeFormat),
		date+"/"+info.Region+"/s3/aws4_request",
		info.Signature,
	)
	if err != nil {
		if errors.Is(err, auth.ErrSignatureMismatch) {
			return nil, "", 0, false, nil, mapAuthError(err)
		}
		return nil, "", 0, false, nil, fmt.Errorf("validate signed AWS streaming payload: %w", err)
	}

	decodedLength := int64(-1)
	if value := r.Header.Get("X-Amz-Decoded-Content-Length"); value != "" {
		if parsed, parseErr := strconv.ParseInt(value, 10, 64); parseErr == nil {
			decodedLength = parsed
		}
	}
	return staged, "UNSIGNED-PAYLOAD", decodedLength, true, staged, nil
}
