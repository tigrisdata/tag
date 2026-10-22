package auth

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// chainSignature is an independent, spec-literal computation of a SigV4
// streaming chunk signature.
func chainSignature(signingKey []byte, signingTime, scope, previous string, chunk []byte) string {
	chunkSum := sha256.Sum256(chunk)
	emptySum := sha256.Sum256(nil)
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256-PAYLOAD", signingTime, scope, previous,
		hex.EncodeToString(emptySum[:]), hex.EncodeToString(chunkSum[:]),
	}, "\n")
	mac := hmac.New(sha256.New, signingKey)
	mac.Write([]byte(stringToSign))
	return hex.EncodeToString(mac.Sum(nil))
}

// validatedStreamingPut signs a STREAMING-AWS4-HMAC-SHA256-PAYLOAD PUT with the
// package's signer, validates it, and returns the outcome plus the chain
// material the test derives on its own.
func validatedStreamingPut(t *testing.T) (*ValidatedRequest, []byte, string, string) {
	t.Helper()
	const accessKey, secretKey, region = "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "us-east-1"
	store := NewCredentialStore()
	store.AddCredential(accessKey, secretKey)

	headers := http.Header{"X-Amz-Decoded-Content-Length": {"11"}}
	signed, err := NewRequestSigner("https://s3.example.com", region).SignRequest(
		t.Context(), http.MethodPut, "/bucket/key", nil, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", accessKey, secretKey, headers)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	req := httptest.NewRequest(http.MethodPut, "/bucket/key", nil)
	req.Header = signed.Header.Clone()
	req.Host = signed.Host

	vr, err := NewRequestValidator(store).Validate(req)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if vr.AccessKey != accessKey {
		t.Fatalf("AccessKey = %q, want %q", vr.AccessKey, accessKey)
	}
	signingTime, err := time.Parse(TimeFormat, signed.Header.Get("X-Amz-Date"))
	if err != nil {
		t.Fatalf("parse X-Amz-Date: %v", err)
	}
	scope := signingTime.Format(shortTimeFormat) + "/" + region + "/s3/aws4_request"
	info, err := ParseAuthInfo(req)
	if err != nil {
		t.Fatalf("ParseAuthInfo: %v", err)
	}
	return vr, deriveSigningKey(secretKey, signingTime.Format(shortTimeFormat), region), signingTime.Format(TimeFormat) + "\x00" + scope, info.Signature
}

func TestChunkVerifier_FollowsChain(t *testing.T) {
	vr, key, timeAndScope, seed := validatedStreamingPut(t)
	signingTime, scope, _ := strings.Cut(timeAndScope, "\x00")

	chunks := [][]byte{[]byte("hello "), []byte("world"), nil}
	v := vr.ChunkVerifier()
	previous := seed
	for i, chunk := range chunks {
		sig := chainSignature(key, signingTime, scope, previous, chunk)
		sum := sha256.Sum256(chunk)
		if err := v.VerifyChunk(sig, sum[:]); err != nil {
			t.Fatalf("chunk %d: %v", i, err)
		}
		previous = sig
	}

	// Replaying an already-verified chunk fails: the chain has moved on.
	first := chainSignature(key, signingTime, scope, seed, chunks[0])
	sum := sha256.Sum256(chunks[0])
	if err := v.VerifyChunk(first, sum[:]); !errors.Is(err, ErrChunkSignatureMismatch) {
		t.Fatalf("replayed chunk: err = %v, want ErrChunkSignatureMismatch", err)
	}
}

func TestChunkVerifier_RejectsTamperedChunkAndKeepsChain(t *testing.T) {
	vr, key, timeAndScope, seed := validatedStreamingPut(t)
	signingTime, scope, _ := strings.Cut(timeAndScope, "\x00")
	v := vr.ChunkVerifier()

	data := []byte("hello world")
	sig := chainSignature(key, signingTime, scope, seed, data)

	tampered := sha256.Sum256(bytes.ToUpper(data))
	if err := v.VerifyChunk(sig, tampered[:]); !errors.Is(err, ErrChunkSignatureMismatch) {
		t.Fatalf("tampered data: err = %v, want ErrChunkSignatureMismatch", err)
	}
	sum := sha256.Sum256(data)
	wrongSig := "0" + sig[1:]
	if sig[0] == '0' {
		wrongSig = "1" + sig[1:]
	}
	if err := v.VerifyChunk(wrongSig, sum[:]); !errors.Is(err, ErrChunkSignatureMismatch) {
		t.Fatalf("tampered signature: err = %v, want ErrChunkSignatureMismatch", err)
	}
	// Wrong key (a replayed header with a body signed by someone else).
	otherSig := chainSignature(deriveSigningKey("not-the-secret", signingTime[:8], "us-east-1"), signingTime, scope, seed, data)
	if err := v.VerifyChunk(otherSig, sum[:]); !errors.Is(err, ErrChunkSignatureMismatch) {
		t.Fatalf("foreign key: err = %v, want ErrChunkSignatureMismatch", err)
	}
	// Failures leave the chain where it was: the genuine chunk still verifies.
	if err := v.VerifyChunk(sig, sum[:]); err != nil {
		t.Fatalf("genuine chunk after failures: %v", err)
	}
}

func TestValidate_FailsWithoutOutcome(t *testing.T) {
	store := NewCredentialStore()
	req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=nobody/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00")
	req.Header.Set("X-Amz-Date", time.Now().UTC().Format(TimeFormat))
	vr, err := NewRequestValidator(store).Validate(req)
	if err == nil || vr != nil {
		t.Fatalf("Validate = (%v, %v), want (nil, error)", vr, err)
	}
}
