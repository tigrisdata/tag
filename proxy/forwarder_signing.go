package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/auth"
)

// signingForwarder validates incoming request signatures and re-signs requests
// before forwarding to upstream Tigris. This is the default mode where TAG
// acts as a credential-translating proxy.
//
// DoFullObjectRequest is inherited from baseForwarder (always uses SigV4 signing).
type signingForwarder struct {
	baseForwarder
	credStore *auth.CredentialStore
	validator *auth.RequestValidator
}

// validateRequest keeps signing mode's historical payload-hash requirement.
// URL-only presigned GETs are enabled for transparent local authentication, not
// for this forwarder's credential-translating request path.
func (f *signingForwarder) validateRequest(r *http.Request) (*auth.ValidatedRequest, error) {
	if r.Header.Get("X-Amz-Content-Sha256") == "" {
		return nil, auth.ErrMissingContentHash
	}
	return f.validator.Validate(r)
}

// validate validates the incoming request signature and looks up the secret key
// to re-sign with. The validation outcome carries the signing material a signed
// streaming body's chunk chain is verified against, so it must come BEFORE the
// body is decoded.
func (f *signingForwarder) validate(r *http.Request) (*auth.ValidatedRequest, string, error) {
	vr, err := f.validateRequest(r)
	if err != nil {
		log.Warn().Err(err).Str("path", r.URL.Path).Msg("Request signature validation failed")
		return nil, "", mapAuthError(err)
	}
	secretKey, err := f.credStore.GetSecretKey(vr.AccessKey)
	if err != nil {
		return nil, "", mapAuthError(err)
	}
	return vr, secretKey, nil
}

// chunkVerifier implements chunkVerifierProvider: the local tier's PUT path
// decodes the aws-chunked body itself, so it needs the request's chunk chain.
func (f *signingForwarder) chunkVerifier(r *http.Request) (*auth.ChunkVerifier, error) {
	vr, _, err := f.validate(r)
	if err != nil {
		return nil, err
	}
	return vr.ChunkVerifier(), nil
}

// signForward builds the upstream request: the (possibly decoded) body, the
// headers as they will be sent, and a fresh SigV4 signature over them.
func (f *signingForwarder) signForward(ctx context.Context, r *http.Request, body io.Reader, bodyHash string, contentLength int64, chunked bool, accessKey, secretKey string) (*http.Request, error) {
	path := r.URL.Path
	if r.URL.RawQuery != "" {
		path = path + "?" + r.URL.RawQuery
	}
	fwdReq, err := f.signer.SignRequest(ctx, r.Method, path, body, bodyHash, accessKey, secretKey, headersForForwardedSigning(r.Header, chunked))
	if err != nil {
		return nil, err
	}
	prepareForwardedRequest(fwdReq, contentLength, chunked)
	return fwdReq, nil
}

// forwardError maps a chunk-chain verification failure surfaced through the
// upstream transport (the decoded body errors mid-stream, so the exact-length
// upstream request is aborted and upstream rejects the short body) to an auth
// error, so the client is answered SignatureDoesNotMatch rather than a 500.
func forwardError(err error) error {
	if errors.Is(err, auth.ErrChunkSignatureMismatch) {
		return mapAuthError(err)
	}
	return err
}

// decodeBody is decodeChunkedIfNeeded for a validated request, with one
// addition: an EMPTY verified stream is verified right here. A zero decoded
// length is forwarded as http.NoBody (prepareForwardedRequest, so upstream
// sees Content-Length: 0 rather than a chunked transfer), which means nothing
// downstream ever reads the decoder — and the terminal frame's signature,
// the only thing binding an empty body to the request, would go unchecked.
// Draining the decoder reads exactly that frame and verifies it before the
// body is replaced; a failure is the same SignatureDoesNotMatch a non-empty
// stream earns, and the request never reaches upstream.
func decodeBody(r *http.Request, vr *auth.ValidatedRequest) (body io.ReadCloser, bodyHash string, contentLength int64, chunked bool, err error) {
	body, bodyHash, contentLength, chunked = decodeChunkedIfNeeded(r, vr)
	if chunked && contentLength == 0 && streamChunkVerifier(r, vr) != nil {
		if _, err := io.Copy(io.Discard, body); err != nil {
			return nil, "", 0, false, forwardError(err)
		}
	}
	return body, bodyHash, contentLength, chunked, nil
}

// Forward forwards a request to Tigris and writes the response to the client.
// Validates the incoming request signature, re-signs with upstream credentials,
// and streams the response back. If the request uses AWS chunked transfer encoding
// (streaming SigV4), the body is decoded on-the-fly — verifying each chunk's
// signature for the signed marker — and forwarded as UNSIGNED-PAYLOAD.
func (f *signingForwarder) Forward(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	vr, secretKey, err := f.validate(r)
	if err != nil {
		return err
	}
	body, bodyHash, contentLength, chunked, err := decodeBody(r, vr)
	if err != nil {
		return err
	}

	fwdReq, err := f.signForward(ctx, r, body, bodyHash, contentLength, chunked, vr.AccessKey, secretKey)
	if err != nil {
		return err
	}
	return forwardError(f.executeAndStream(w, fwdReq, contentLength, nil))
}

// ForwardTeeingBody forwards a single PutObject while teeing the decoded request body
// into `tee`, so the caller can populate the cache from the bytes TAG already has in hand
// (a write-through tee) instead of a read-back warm-on-write GET. Returns the upstream
// status code and a clone of the response headers (for the ETag) so the caller can build
// cache metadata for the just-written object.
//
// Only the signing forwarder implements this: it decodes any AWS chunked encoding, so it
// sees the assembled object bytes. The transparent forwarder preserves the client's opaque
// (possibly chunked) body and signature, so it can't tee cleanly and falls back to
// warm-on-write. The `tee` writer must never return an error — that would truncate the
// upstream stream via io.TeeReader — so callers pass a capped, non-erroring buffer and
// check overflow out of band. A chunk-signature failure errors the decoded stream, so the
// tee holds an incomplete body and the caller never caches it (upstream rejects the PUT).
func (f *signingForwarder) ForwardTeeingBody(ctx context.Context, w http.ResponseWriter, r *http.Request, tee io.Writer) (int, http.Header, string, string, error) {
	vr, secretKey, err := f.validate(r)
	if err != nil {
		return 0, nil, "", "", err
	}
	body, bodyHash, contentLength, chunked, err := decodeBody(r, vr)
	if err != nil {
		return 0, nil, "", "", err
	}

	// Tee the decoded body into the caller's buffer as it is streamed upstream.
	fwdReq, err := f.signForward(ctx, r, io.TeeReader(body, tee), bodyHash, contentLength, chunked, vr.AccessKey, secretKey)
	if err != nil {
		return 0, nil, "", "", err
	}

	// Return the validated credentials so the caller can HEAD/warm without re-validating.
	status, headers, err := f.executeAndStreamReturningMeta(w, fwdReq, contentLength, nil)
	return status, headers, vr.AccessKey, secretKey, forwardError(err)
}

// ForwardWithCapture forwards the request and captures the response for caching.
// It validates and re-signs like Forward, but also retains the response body.
func (f *signingForwarder) ForwardWithCapture(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
	vr, secretKey, err := f.validate(r)
	if err != nil {
		return nil, err
	}
	body, bodyHash, contentLength, chunked, err := decodeBody(r, vr)
	if err != nil {
		return nil, err
	}

	fwdReq, err := f.signForward(ctx, r, body, bodyHash, contentLength, chunked, vr.AccessKey, secretKey)
	if err != nil {
		return nil, err
	}
	capture, err := f.executeAndCapture(w, fwdReq, contentLength, nil)
	return capture, forwardError(err)
}

// ValidateAndGetCredentials validates the request signature and returns credentials.
// In signing mode, validation is always performed locally — returns AuthValidated on success.
func (f *signingForwarder) ValidateAndGetCredentials(r *http.Request) (AuthResult, string, string, error) {
	vr, secretKey, err := f.validate(r)
	if err != nil {
		return AuthNotValidated, "", "", err
	}
	return AuthValidated, vr.AccessKey, secretKey, nil
}

// DoRequestWithCreds executes a request with pre-validated credentials.
// Returns the raw response for streaming. Caller is responsible for closing the response body.
// If the request uses AWS chunked transfer encoding, the body is decoded on-the-fly.
func (f *signingForwarder) DoRequestWithCreds(ctx context.Context, r *http.Request, accessKey, secretKey string) (*http.Response, error) {
	// The caller validated the headers but did not keep the signing material;
	// a signed streaming body needs it for the chunk chain, so re-validate
	// (one HMAC over the headers) rather than decode unverified.
	var vr *auth.ValidatedRequest
	if r.Header.Get("X-Amz-Content-Sha256") == StreamingPayloadHash {
		var err error
		if vr, _, err = f.validate(r); err != nil {
			return nil, err
		}
	}
	body, bodyHash, contentLength, chunked, err := decodeBody(r, vr)
	if err != nil {
		return nil, err
	}

	fwdReq, err := f.signForward(ctx, r, body, bodyHash, contentLength, chunked, accessKey, secretKey)
	if err != nil {
		return nil, err
	}
	resp, err := f.executeRequest(fwdReq, contentLength, nil)
	return resp, forwardError(err)

}
