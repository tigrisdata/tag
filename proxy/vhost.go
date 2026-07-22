package proxy

import (
	"net"
	"net/url"
	"strings"
)

// VHostEndpoint prepends the bucket to the endpoint hostname.
// e.g., ("https://t3.storage.dev", "mybucket") -> "https://mybucket.t3.storage.dev"
// Returns the original endpoint unchanged if bucket is empty.
func VHostEndpoint(endpoint, bucket string) string {
	if bucket == "" {
		return endpoint
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return endpoint
	}

	u.Host = bucket + "." + u.Host
	return u.String()
}

// SupportsVHost returns true if the endpoint supports vhost-style addressing
// (bucket.hostname). This requires a real domain name with wildcard DNS.
// IP addresses (127.0.0.1, [::1]) and localhost cannot resolve bucket subdomains.
func SupportsVHost(endpoint string) bool {
	u, err := url.Parse(endpoint)
	if err != nil {
		return false
	}

	host := u.Hostname() // strips port
	if host == "" {
		return false
	}

	// IP addresses don't support wildcard DNS subdomains
	if net.ParseIP(host) != nil {
		return false
	}

	// localhost doesn't support subdomains
	if host == "localhost" {
		return false
	}

	return true
}

// anonymousObjectURL returns the URL for an anonymous (unsigned) GET of bucket/key.
// It uses vhost-style addressing (bucket in the hostname) when the endpoint supports
// it — which Tigris requires for anonymous access to public buckets; a path-style
// anonymous GET 403s even for a publicly-readable object — and path-style otherwise.
// This mirrors the anonymous client-forward path (buildTransparentRequest). Returns ""
// if the endpoint cannot be parsed.
func anonymousObjectURL(endpoint, bucket, key string) string {
	target := endpoint
	path := "/" + bucket + "/" + key
	if bucket != "" && SupportsVHost(endpoint) {
		target = VHostEndpoint(endpoint, bucket)
		path = "/" + key
	}

	u, err := url.Parse(target)
	if err != nil {
		return ""
	}
	u.Path = path
	u.RawQuery = ""
	return u.String()
}

// RemoveBucketPrefix strips the leading /bucket from a path.
// e.g., ("/mybucket/key/path", "mybucket") -> "/key/path"
// Returns "/" if only the bucket was in the path.
// Returns the path unchanged if bucket is empty or path doesn't start with /bucket.
func RemoveBucketPrefix(path, bucket string) string {
	if bucket == "" || path == "" {
		return path
	}

	prefix := "/" + bucket
	if !strings.HasPrefix(path, prefix) {
		return path
	}

	rest := path[len(prefix):]
	if rest == "" {
		return "/"
	}
	if rest[0] != '/' {
		// Path has /bucketXYZ not /bucket/..., don't strip
		return path
	}
	return rest
}
