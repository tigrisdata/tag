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
	"syscall"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/auth"
)

const signedChunkStringToSignPrefix = "AWS4-HMAC-SHA256-PAYLOAD"

var (
	errSignedStreamStagingCapacity = errors.New("signed AWS stream staging capacity is unavailable")
	errSignedStreamLengthMismatch  = errors.New("signed AWS stream decoded length differs from its declaration")
)

// signedStreamStageSpace describes writable capacity on the filesystem that
// holds staged request bodies.
type signedStreamStageSpace struct {
	availableBytes int64
	blockSize      int64
}

type signedStreamStageSpaceReader func(string) (signedStreamStageSpace, error)

// signedStreamStageBudget reserves at most half of the temporary filesystem's
// currently available space for all concurrent signed-stream stages, leaving the
// other half unclaimed by this pool. Reservations stay held until the staged file
// is removed, including while the HTTP transport owns it.
type signedStreamStageBudget struct {
	mu          sync.Mutex
	spaceReader signedStreamStageSpaceReader
	dir         string
	limit       int64
	headroom    int64
	reserved    int64
	unallocated int64
}

func newSignedStreamStageBudget(spaceReader signedStreamStageSpaceReader) *signedStreamStageBudget {
	if spaceReader == nil {
		spaceReader = readSignedStreamStageSpace
	}
	return &signedStreamStageBudget{spaceReader: spaceReader}
}

var defaultSignedStreamStageBudget = newSignedStreamStageBudget(nil)

func readSignedStreamStageSpace(dir string) (signedStreamStageSpace, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return signedStreamStageSpace{}, err
	}
	blockSize := int64(stat.Bsize)
	if blockSize <= 0 {
		return signedStreamStageSpace{}, fmt.Errorf("temporary filesystem reported invalid block size %d", blockSize)
	}
	availableBlocks := stat.Bavail
	if availableBlocks > uint64(math.MaxInt64/blockSize) {
		return signedStreamStageSpace{availableBytes: math.MaxInt64, blockSize: blockSize}, nil
	}
	return signedStreamStageSpace{
		availableBytes: int64(availableBlocks) * blockSize,
		blockSize:      blockSize,
	}, nil
}

func (b *signedStreamStageBudget) reserve(dir string, space signedStreamStageSpace, amount int64) error {
	if amount < 0 {
		return fmt.Errorf("%w: negative reservation %d", errSignedStreamStagingCapacity, amount)
	}
	if amount == 0 {
		return b.checkLocked(dir, space)
	}
	if b.reserved == 0 {
		b.dir = dir
		b.limit = space.availableBytes / 2
		b.headroom = space.availableBytes - b.limit
	} else if b.dir != dir {
		return fmt.Errorf("%w: temporary directory changed while a stage is active", errSignedStreamStagingCapacity)
	}
	if b.limit <= 0 || b.reserved > b.limit || amount > b.limit-b.reserved {
		return errSignedStreamStagingCapacity
	}
	if err := b.checkLocked(dir, space); err != nil {
		return err
	}
	if amount > space.availableBytes-b.headroom-b.unallocated {
		return errSignedStreamStagingCapacity
	}
	b.reserved += amount
	b.unallocated += amount
	return nil
}

func (b *signedStreamStageBudget) checkLocked(dir string, space signedStreamStageSpace) error {
	if b.reserved == 0 {
		return nil
	}
	if b.dir != dir || space.availableBytes < b.headroom || b.unallocated > space.availableBytes-b.headroom {
		return errSignedStreamStagingCapacity
	}
	return nil
}

func (b *signedStreamStageBudget) release(reserved, unallocated int64) {
	if reserved == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reserved -= reserved
	b.unallocated -= unallocated
	if b.reserved == 0 {
		b.dir = ""
		b.limit = 0
		b.headroom = 0
		b.unallocated = 0
	}
}

// signedStreamStageReservation tracks the rounded disk space reserved for one
// staged body and the portion already allocated by writes to its file.
type signedStreamStageReservation struct {
	budget        *signedStreamStageBudget
	dir           string
	blockSize     int64
	reservedBytes int64
	allocated     int64
	written       int64
}

func (r *signedStreamStageReservation) reservePayload(total int64) error {
	if total < 0 {
		return fmt.Errorf("%w: negative decoded length %d", errSignedStreamStagingCapacity, total)
	}
	if total == 0 {
		return nil
	}

	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	space, err := r.budget.spaceReader(r.dir)
	if err != nil {
		return fmt.Errorf("%w: inspect temporary filesystem: %w", errSignedStreamStagingCapacity, err)
	}
	if space.availableBytes < 0 || space.blockSize <= 0 {
		return fmt.Errorf("%w: invalid temporary filesystem capacity", errSignedStreamStagingCapacity)
	}
	if r.blockSize == 0 {
		r.blockSize = space.blockSize
	}
	required, err := roundSignedStreamStageBytes(total, r.blockSize)
	if err != nil {
		return err
	}
	if required < r.reservedBytes {
		return fmt.Errorf("signed-stream reservation shrank from %d to %d bytes", r.reservedBytes, required)
	}
	if err := r.budget.reserve(r.dir, space, required-r.reservedBytes); err != nil {
		return err
	}
	r.reservedBytes = required
	return nil
}

func (r *signedStreamStageReservation) checkAvailable() error {
	if r.reservedBytes == 0 {
		return nil
	}
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()
	space, err := r.budget.spaceReader(r.dir)
	if err != nil {
		return fmt.Errorf("%w: inspect temporary filesystem: %w", errSignedStreamStagingCapacity, err)
	}
	return r.budget.checkLocked(r.dir, space)
}

// write accounts allocated bytes while holding the admission lock across the
// filesystem write, so another reservation cannot double-count that allocation.
func (r *signedStreamStageReservation) write(file *os.File, p []byte) (int, error) {
	r.budget.mu.Lock()
	defer r.budget.mu.Unlock()

	if len(p) == 0 {
		return 0, nil
	}
	writeLength := int64(len(p))
	if writeLength > math.MaxInt64-r.written {
		return 0, errors.New("signed-stream written length overflows int64")
	}
	maxAllocated, err := roundSignedStreamStageBytes(r.written+writeLength, r.blockSize)
	if err != nil {
		return 0, err
	}
	if maxAllocated > r.reservedBytes {
		return 0, fmt.Errorf("signed-stream write would use %d bytes beyond %d-byte reservation", maxAllocated, r.reservedBytes)
	}
	if maxAllocated-r.allocated > r.budget.unallocated {
		return 0, fmt.Errorf("signed-stream write would exceed unallocated staging reservations")
	}

	n, writeErr := file.Write(p)
	if n <= 0 {
		return n, writeErr
	}
	written := int64(n)
	allocated, err := roundSignedStreamStageBytes(r.written+written, r.blockSize)
	if err != nil {
		return n, err
	}
	allocatedDelta := allocated - r.allocated
	r.budget.unallocated -= allocatedDelta
	r.written += written
	r.allocated = allocated
	return n, writeErr
}

func (r *signedStreamStageReservation) release(fileRemoved bool) {
	unallocated := r.reservedBytes - r.allocated
	releasable := unallocated
	if fileRemoved {
		releasable += r.allocated
	}
	r.budget.release(releasable, unallocated)
	r.reservedBytes = 0
	r.allocated = 0
	r.written = 0
}

func roundSignedStreamStageBytes(size, blockSize int64) (int64, error) {
	if size < 0 || blockSize <= 0 {
		return 0, fmt.Errorf("invalid signed-stream size %d or block size %d", size, blockSize)
	}
	if size == 0 {
		return 0, nil
	}
	remainder := size % blockSize
	if remainder == 0 {
		return size, nil
	}
	padding := blockSize - remainder
	if size > math.MaxInt64-padding {
		return 0, errors.New("signed-stream staging size overflows int64")
	}
	return size + padding, nil
}

// stagedSignedChunkedBody owns the verified payload file until the outbound
// request has finished using it. Close is idempotent because the transport and
// the local cleanup path may both close the request body.
type stagedSignedChunkedBody struct {
	*os.File
	path        string
	reservation *signedStreamStageReservation
	closeOnce   sync.Once
	closeErr    error
}

func (b *stagedSignedChunkedBody) Write(p []byte) (int, error) {
	return b.reservation.write(b.File, p)
}

func (b *stagedSignedChunkedBody) Close() error {
	b.closeOnce.Do(func() {
		closeErr := b.File.Close()
		removeErr := os.Remove(b.path)
		removed := removeErr == nil || errors.Is(removeErr, os.ErrNotExist)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
		b.reservation.release(removed)
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
// bytes. It reserves staging space before copying chunk data and does not expose
// the body to the upstream request until the complete chain has been verified.
func stageSignedAWSChunkedBody(ctx context.Context, source io.Reader, signingKey []byte, signingTime, credentialScope, seedSignature string, declaredLength int64, budget *signedStreamStageBudget) (*stagedSignedChunkedBody, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("read signed AWS stream: %w", err)
	}
	if budget == nil {
		budget = defaultSignedStreamStageBudget
	}
	dir := os.TempDir()
	reservation := &signedStreamStageReservation{budget: budget, dir: dir}
	if declaredLength >= 0 {
		if err := reservation.reservePayload(declaredLength); err != nil {
			return nil, fmt.Errorf("reserve temporary storage for signed AWS stream: %w", err)
		}
	}
	file, err := os.CreateTemp(dir, "tag-signed-stream-*")
	if err != nil {
		reservation.release(true)
		return nil, fmt.Errorf("create temporary file for signed AWS stream: %w", err)
	}
	staged := &stagedSignedChunkedBody{File: file, path: file.Name(), reservation: reservation}
	reader := bufio.NewReaderSize(source, awsChunkedReaderBufSize)
	previousSignature := seedSignature
	decodedLength := int64(0)

	for {
		if err := ctx.Err(); err != nil {
			return nil, cleanupFailedSignedStage(staged, fmt.Errorf("read signed AWS stream: %w", err))
		}
		line, err := readSignedChunkLine(reader)
		if err != nil {
			return nil, cleanupFailedSignedStage(staged, fmt.Errorf("read signed AWS chunk header: %w", signedStreamReadError(ctx, err)))
		}
		chunkLength, signature, err := parseSignedChunkHeader(line)
		if err != nil {
			return nil, cleanupFailedSignedStage(staged, err)
		}

		if chunkLength == 0 {
			expected := signedChunkSignature(signingKey, signingTime, credentialScope, previousSignature, sha256HexBytes(nil))
			if !hmac.Equal([]byte(signature), []byte(expected)) {
				return nil, cleanupFailedSignedStage(staged, auth.ErrSignatureMismatch)
			}
			if err := readSignedChunkTerminator(reader); err != nil {
				return nil, cleanupFailedSignedStage(staged, fmt.Errorf("read signed AWS terminal chunk terminator: %w", signedStreamReadError(ctx, err)))
			}
			if err := ctx.Err(); err != nil {
				return nil, cleanupFailedSignedStage(staged, fmt.Errorf("finish signed AWS stream: %w", err))
			}
			if declaredLength >= 0 && decodedLength != declaredLength {
				return nil, cleanupFailedSignedStage(staged, fmt.Errorf("%w: received %d bytes, declared %d", errSignedStreamLengthMismatch, decodedLength, declaredLength))
			}
			break
		}

		if chunkLength > math.MaxInt64-decodedLength {
			return nil, cleanupFailedSignedStage(staged, errors.New("signed AWS stream payload length overflows int64"))
		}
		nextLength := decodedLength + chunkLength
		if declaredLength < 0 || nextLength > declaredLength {
			if err := reservation.reservePayload(nextLength); err != nil {
				return nil, cleanupFailedSignedStage(staged, fmt.Errorf("reserve temporary storage for signed AWS stream: %w", err))
			}
		} else if err := reservation.checkAvailable(); err != nil {
			return nil, cleanupFailedSignedStage(staged, fmt.Errorf("reserve temporary storage for signed AWS stream: %w", err))
		}

		chunkHash := sha256.New()
		written, copyErr := io.CopyN(io.MultiWriter(staged, chunkHash), reader, chunkLength)
		if copyErr != nil {
			return nil, cleanupFailedSignedStage(staged, fmt.Errorf("stage signed AWS chunk data: %w", signedStreamReadError(ctx, copyErr)))
		}
		if written != chunkLength {
			return nil, cleanupFailedSignedStage(staged, io.ErrUnexpectedEOF)
		}
		if err := ctx.Err(); err != nil {
			return nil, cleanupFailedSignedStage(staged, fmt.Errorf("stage signed AWS stream: %w", err))
		}
		decodedLength += written

		expected := signedChunkSignature(signingKey, signingTime, credentialScope, previousSignature, hex.EncodeToString(chunkHash.Sum(nil)))
		if !hmac.Equal([]byte(signature), []byte(expected)) {
			return nil, cleanupFailedSignedStage(staged, auth.ErrSignatureMismatch)
		}
		previousSignature = signature
		if err := readSignedChunkTerminator(reader); err != nil {
			return nil, cleanupFailedSignedStage(staged, fmt.Errorf("read signed AWS chunk terminator: %w", signedStreamReadError(ctx, err)))
		}
	}

	if err := reservation.checkAvailable(); err != nil {
		return nil, cleanupFailedSignedStage(staged, fmt.Errorf("reserve temporary storage for signed AWS stream: %w", err))
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, cleanupFailedSignedStage(staged, fmt.Errorf("rewind verified AWS stream: %w", err))
	}
	return staged, nil
}

func cleanupFailedSignedStage(staged *stagedSignedChunkedBody, cause error) error {
	if cleanupErr := staged.Close(); cleanupErr != nil {
		return errors.Join(cause, fmt.Errorf("clean up signed AWS stream staging file: %w", cleanupErr))
	}
	return cause
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

// stageBudget returns the process-wide reservation pool unless a test supplies
// an isolated pool for deterministic capacity and ownership checks.
func (f *signingForwarder) stageBudget() *signedStreamStageBudget {
	if f.stageBudgetOverride != nil {
		return f.stageBudgetOverride
	}
	return defaultSignedStreamStageBudget
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

	decodedLength := int64(-1)
	if value := r.Header.Get("X-Amz-Decoded-Content-Length"); value != "" {
		if parsed, parseErr := strconv.ParseInt(value, 10, 64); parseErr == nil {
			decodedLength = parsed
		}
	}

	staged, err := stageSignedAWSChunkedBody(
		ctx,
		r.Body,
		signingKey,
		requestTime.UTC().Format(auth.TimeFormat),
		date+"/"+info.Region+"/s3/aws4_request",
		info.Signature,
		decodedLength,
		f.stageBudget(),
	)
	if err != nil {
		if errors.Is(err, auth.ErrSignatureMismatch) {
			return nil, "", 0, false, nil, mapAuthError(err)
		}
		return nil, "", 0, false, nil, fmt.Errorf("validate signed AWS streaming payload: %w", err)
	}

	return staged, "UNSIGNED-PAYLOAD", decodedLength, true, staged, nil
}
