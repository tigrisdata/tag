package auth

import (
	"crypto/hmac"
	"encoding/hex"
	"errors"
)

// ErrChunkSignatureMismatch is returned when a chunk of a
// STREAMING-AWS4-HMAC-SHA256-PAYLOAD body carries a signature that does not
// continue the request's SigV4 chunk chain (or carries none at all).
var ErrChunkSignatureMismatch = errors.New("chunk signature does not match")

// streamingAlgorithm is the SigV4 algorithm identifier for signed chunk payloads.
const streamingAlgorithm = "AWS4-HMAC-SHA256-PAYLOAD"

// ChunkVerifier verifies the SigV4 chunk chain of a
// STREAMING-AWS4-HMAC-SHA256-PAYLOAD body. Every chunk's signature is
//
//	HMAC-SHA256(signingKey, "AWS4-HMAC-SHA256-PAYLOAD\n" + <X-Amz-Date> + "\n" +
//	    <credential scope> + "\n" + <previous signature> + "\n" +
//	    sha256("") + "\n" + sha256(<chunk data>))
//
// seeded with the Authorization header's signature; the terminal 0-byte chunk
// is signed the same way over empty data. Because each chunk commits to its
// predecessor, a verified chain binds the whole body to the request signature:
// a replayed header cannot authenticate a different body. A verifier is bound
// to one request and is not safe for concurrent use.
type ChunkVerifier struct {
	signingKey []byte
	prefix     string // algorithm, signing time and credential scope, fixed per request
	previous   string
}

// VerifyChunk checks signature against the chain for a chunk whose SHA-256
// digest is chunkSHA256 and, on success, advances the chain. On mismatch the
// chain is left unchanged and ErrChunkSignatureMismatch is returned.
func (c *ChunkVerifier) VerifyChunk(signature string, chunkSHA256 []byte) error {
	stringToSign := c.prefix + c.previous + "\n" + emptyBodyHash + "\n" + hex.EncodeToString(chunkSHA256)
	expected := hex.EncodeToString(hmacSHA256(c.signingKey, []byte(stringToSign)))
	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return ErrChunkSignatureMismatch
	}
	c.previous = expected
	return nil
}
