// Package handlers provides HTTP handlers for the S3 API.
package handlers

import (
	"net/http"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/s3err"
)

// WriteAuthError writes an S3 error response for authentication errors.
// It maps proxy.AuthErrorCode to the appropriate S3 error code.
func WriteAuthError(w http.ResponseWriter, r *http.Request, code int, err error) {
	// Map auth error codes to S3 error codes
	// These constants match proxy.AuthErrorCode values
	const (
		errCodeSignatureMismatch = 0
		errCodeInvalidAccessKey  = 1
		errCodeExpiredRequest    = 2
		errCodeMalformedAuth     = 3
		errCodeInternal          = 4
	)

	var s3Code s3err.ErrorCode
	switch code {
	case errCodeSignatureMismatch:
		s3Code = s3err.ErrSignatureDoesNotMatch
	case errCodeInvalidAccessKey:
		s3Code = s3err.ErrInvalidAccessKeyId
	case errCodeExpiredRequest:
		s3Code = s3err.ErrAccessDenied
	case errCodeMalformedAuth:
		s3Code = s3err.ErrAuthorizationHeaderMalformed
	default:
		s3Code = s3err.ErrInternalError
	}

	log.Warn().Err(err).Str("path", r.URL.Path).Str("s3_code", string(s3Code)).Msg("Auth error")
	s3err.WriteError(w, r, s3Code)
}
