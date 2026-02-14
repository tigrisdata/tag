package auth

// KeyProvider abstracts signing key retrieval for SigV4 validation.
// Two implementations exist:
//   - CredentialStore: derives signing keys from raw secrets (signing mode)
//   - DerivedKeyStore: looks up pre-derived signing keys (transparent mode)
type KeyProvider interface {
	// GetSigningKey returns the signing key for the given access key, date, and region.
	// Returns an error wrapping ErrUnknownAccessKey if the access key is not known.
	GetSigningKey(accessKey, date, region string) ([]byte, error)

	// HasKey returns whether any signing key exists for the given access key.
	HasKey(accessKey string) bool
}

// deriveSigningKey derives the SigV4 signing key from a secret key, date, and region.
// This implements the standard AWS SigV4 key derivation:
//
//	kDate    = HMAC-SHA256("AWS4" + secretKey, date)
//	kRegion  = HMAC-SHA256(kDate, region)
//	kService = HMAC-SHA256(kRegion, "s3")
//	kSigning = HMAC-SHA256(kService, "aws4_request")
func deriveSigningKey(secretKey, dateStr, region string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(dateStr))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte(terminationString))
	return kSigning
}
