// Package s3err provides S3-compatible error types and response writers.
package s3err

import (
	"encoding/xml"
	"net/http"

	"github.com/rs/zerolog/log"
)

// ErrorCode represents an S3 error code.
type ErrorCode string

// S3 Error codes
const (
	ErrNone                         ErrorCode = ""
	ErrAccessDenied                 ErrorCode = "AccessDenied"
	ErrBucketNotEmpty               ErrorCode = "BucketNotEmpty"
	ErrBucketAlreadyExists          ErrorCode = "BucketAlreadyExists"
	ErrBucketAlreadyOwnedByYou      ErrorCode = "BucketAlreadyOwnedByYou"
	ErrNoSuchBucket                 ErrorCode = "NoSuchBucket"
	ErrNoSuchKey                    ErrorCode = "NoSuchKey"
	ErrNoSuchUpload                 ErrorCode = "NoSuchUpload"
	ErrInvalidAccessKeyId           ErrorCode = "InvalidAccessKeyId"
	ErrSignatureDoesNotMatch        ErrorCode = "SignatureDoesNotMatch"
	ErrInvalidBucketName            ErrorCode = "InvalidBucketName"
	ErrInvalidObjectName            ErrorCode = "InvalidObjectName"
	ErrInvalidPart                  ErrorCode = "InvalidPart"
	ErrInvalidPartNumber            ErrorCode = "InvalidPartNumber"
	ErrInvalidPartOrder             ErrorCode = "InvalidPartOrder"
	ErrInternalError                ErrorCode = "InternalError"
	ErrMalformedXML                 ErrorCode = "MalformedXML"
	ErrMethodNotAllowed             ErrorCode = "MethodNotAllowed"
	ErrNotImplemented               ErrorCode = "NotImplemented"
	ErrRequestTimeout               ErrorCode = "RequestTimeout"
	ErrServiceUnavailable           ErrorCode = "ServiceUnavailable"
	ErrSlowDown                     ErrorCode = "SlowDown"
	ErrInvalidRequest               ErrorCode = "InvalidRequest"
	ErrMissingContentLength         ErrorCode = "MissingContentLength"
	ErrIncompleteBody               ErrorCode = "IncompleteBody"
	ErrInvalidRange                 ErrorCode = "InvalidRange"
	ErrAuthorizationHeaderMalformed ErrorCode = "AuthorizationHeaderMalformed"
	ErrInvalidArgument              ErrorCode = "InvalidArgument"
	ErrBadContentLength             ErrorCode = "BadContentLength"
)

// ErrorResponse represents an S3 error response.
type ErrorResponse struct {
	XMLName   xml.Name  `xml:"Error"`
	Code      ErrorCode `xml:"Code"`
	Message   string    `xml:"Message"`
	Resource  string    `xml:"Resource,omitempty"`
	RequestID string    `xml:"RequestId,omitempty"`
}

// errorInfo holds error details for writing responses.
type errorInfo struct {
	HTTPStatus int
	Code       ErrorCode
	Message    string
}

// errorMap maps error codes to HTTP status and messages.
var errorMap = map[ErrorCode]errorInfo{
	ErrAccessDenied:                 {http.StatusForbidden, ErrAccessDenied, "Access Denied"},
	ErrBucketNotEmpty:               {http.StatusConflict, ErrBucketNotEmpty, "The bucket you tried to delete is not empty"},
	ErrBucketAlreadyExists:          {http.StatusConflict, ErrBucketAlreadyExists, "The requested bucket name is not available"},
	ErrBucketAlreadyOwnedByYou:      {http.StatusConflict, ErrBucketAlreadyOwnedByYou, "Your previous request to create the named bucket succeeded"},
	ErrNoSuchBucket:                 {http.StatusNotFound, ErrNoSuchBucket, "The specified bucket does not exist"},
	ErrNoSuchKey:                    {http.StatusNotFound, ErrNoSuchKey, "The specified key does not exist"},
	ErrNoSuchUpload:                 {http.StatusNotFound, ErrNoSuchUpload, "The specified multipart upload does not exist"},
	ErrInvalidAccessKeyId:           {http.StatusForbidden, ErrInvalidAccessKeyId, "The AWS access key ID you provided does not exist in our records"},
	ErrSignatureDoesNotMatch:        {http.StatusForbidden, ErrSignatureDoesNotMatch, "The request signature we calculated does not match the signature you provided"},
	ErrInvalidBucketName:            {http.StatusBadRequest, ErrInvalidBucketName, "The specified bucket is not valid"},
	ErrInvalidObjectName:            {http.StatusBadRequest, ErrInvalidObjectName, "Object name is not valid"},
	ErrInvalidPart:                  {http.StatusBadRequest, ErrInvalidPart, "One or more of the specified parts could not be found"},
	ErrInvalidPartNumber:            {http.StatusBadRequest, ErrInvalidPartNumber, "The part number you specified is not valid"},
	ErrInvalidPartOrder:             {http.StatusBadRequest, ErrInvalidPartOrder, "The list of parts was not in ascending order"},
	ErrInternalError:                {http.StatusInternalServerError, ErrInternalError, "We encountered an internal error. Please try again."},
	ErrMalformedXML:                 {http.StatusBadRequest, ErrMalformedXML, "The XML you provided was not well-formed"},
	ErrMethodNotAllowed:             {http.StatusMethodNotAllowed, ErrMethodNotAllowed, "The specified method is not allowed against this resource"},
	ErrNotImplemented:               {http.StatusNotImplemented, ErrNotImplemented, "A header you provided implies functionality that is not implemented"},
	ErrRequestTimeout:               {http.StatusBadRequest, ErrRequestTimeout, "Your socket connection to the server was not read from or written to within the timeout period"},
	ErrServiceUnavailable:           {http.StatusServiceUnavailable, ErrServiceUnavailable, "Service is unable to handle request"},
	ErrSlowDown:                     {http.StatusServiceUnavailable, ErrSlowDown, "Please reduce your request rate"},
	ErrInvalidRequest:               {http.StatusBadRequest, ErrInvalidRequest, "Invalid Request"},
	ErrMissingContentLength:         {http.StatusLengthRequired, ErrMissingContentLength, "You must provide the Content-Length HTTP header"},
	ErrIncompleteBody:               {http.StatusBadRequest, ErrIncompleteBody, "You did not provide the number of bytes specified by the Content-Length HTTP header"},
	ErrInvalidRange:                 {http.StatusRequestedRangeNotSatisfiable, ErrInvalidRange, "The requested range is not satisfiable"},
	ErrAuthorizationHeaderMalformed: {http.StatusBadRequest, ErrAuthorizationHeaderMalformed, "The authorization header is malformed"},
	ErrInvalidArgument:              {http.StatusBadRequest, ErrInvalidArgument, "Invalid argument"},
	ErrBadContentLength:             {http.StatusBadRequest, ErrBadContentLength, "The Content-Length you specified is invalid"},
}

// WriteError writes an S3 error response.
func WriteError(w http.ResponseWriter, r *http.Request, code ErrorCode) {
	info, ok := errorMap[code]
	if !ok {
		info = errorInfo{http.StatusInternalServerError, ErrInternalError, "Internal Server Error"}
	}

	WriteErrorWithMessage(w, r, code, info.Message)
}

// WriteErrorWithMessage writes an S3 error response with a custom message.
func WriteErrorWithMessage(w http.ResponseWriter, r *http.Request, code ErrorCode, message string) {
	info, ok := errorMap[code]
	if !ok {
		info = errorInfo{http.StatusInternalServerError, ErrInternalError, message}
	}

	resp := ErrorResponse{
		Code:      code,
		Message:   message,
		Resource:  r.URL.Path,
		RequestID: r.Header.Get("X-Request-ID"),
	}

	log.Debug().
		Int("status", info.HTTPStatus).
		Str("code", string(code)).
		Str("message", message).
		Str("path", r.URL.Path).
		Msg("S3 error response")

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(info.HTTPStatus)
	if err := xml.NewEncoder(w).Encode(resp); err != nil {
		log.Error().Err(err).Msg("Failed to encode error response")
	}
}

// WriteInternalError writes an internal server error response.
func WriteInternalError(w http.ResponseWriter, r *http.Request, err error) {
	log.Error().Err(err).Str("path", r.URL.Path).Msg("Internal error")
	WriteError(w, r, ErrInternalError)
}
