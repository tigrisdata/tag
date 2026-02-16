package auth

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

// ProxySigner computes proxy headers for transparent proxy mode.
// In this mode, TAG forwards the client's original signed request as-is
// and adds proxy headers so Tigris validates the signature against the original host.
type ProxySigner struct {
	accessKey string
	secretKey string
}

// NewProxySigner creates a new proxy signer with TAG's own Tigris credentials.
func NewProxySigner(accessKey, secretKey string) *ProxySigner {
	return &ProxySigner{
		accessKey: accessKey,
		secretKey: secretKey,
	}
}

// ProxyHeaders holds the 4 proxy headers to add to the forwarded request.
type ProxyHeaders struct {
	ForwardedHost  string // X-Tigris-Forwarded-Host
	ProxyAccessKey string // X-Tigris-Proxy-Access-Key
	ProxyTimestamp string // X-Tigris-Proxy-Timestamp
	ProxySignature string // X-Tigris-Proxy-Signature
}

// ComputeProxyHeaders computes the 4 proxy headers for a given request.
// forwardedHost is the original Host header from the client request.
// method is the HTTP method (GET, PUT, etc.).
// path is the URL path (e.g., /bucket/object.jpg).
func (s *ProxySigner) ComputeProxyHeaders(forwardedHost, method, path string) *ProxyHeaders {
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)

	// Build canonical string: forwarded_host\ntimestamp\nmethod\npath
	canonicalString := fmt.Sprintf("%s\n%s\n%s\n%s", forwardedHost, timestamp, method, path)

	// HMAC-SHA256 signature using TAG's secret key
	signature := hex.EncodeToString(hmacSHA256([]byte(s.secretKey), []byte(canonicalString)))

	return &ProxyHeaders{
		ForwardedHost:  forwardedHost,
		ProxyAccessKey: s.accessKey,
		ProxyTimestamp: timestamp,
		ProxySignature: signature,
	}
}

// AccessKey returns the proxy access key.
func (s *ProxySigner) AccessKey() string { return s.accessKey }

// SecretKey returns the proxy secret key.
func (s *ProxySigner) SecretKey() string { return s.secretKey }
