package proxy

import (
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
