package proxy

import "testing"

func TestVHostEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		bucket   string
		want     string
	}{
		{
			name:     "basic https",
			endpoint: "https://t3.storage.dev",
			bucket:   "mybucket",
			want:     "https://mybucket.t3.storage.dev",
		},
		{
			name:     "with port",
			endpoint: "https://t3.storage.dev:443",
			bucket:   "mybucket",
			want:     "https://mybucket.t3.storage.dev:443",
		},
		{
			name:     "http endpoint",
			endpoint: "http://localhost:8080",
			bucket:   "testbucket",
			want:     "http://testbucket.localhost:8080",
		},
		{
			name:     "trailing slash stripped",
			endpoint: "https://t3.storage.dev/",
			bucket:   "mybucket",
			want:     "https://mybucket.t3.storage.dev/",
		},
		{
			name:     "empty bucket returns original",
			endpoint: "https://t3.storage.dev",
			bucket:   "",
			want:     "https://t3.storage.dev",
		},
		{
			name:     "subdomain endpoint",
			endpoint: "https://fly.storage.tigris.dev",
			bucket:   "assets",
			want:     "https://assets.fly.storage.tigris.dev",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := VHostEndpoint(tt.endpoint, tt.bucket)
			if got != tt.want {
				t.Errorf("VHostEndpoint(%q, %q) = %q, want %q", tt.endpoint, tt.bucket, got, tt.want)
			}
		})
	}
}

func TestSupportsVHost(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		want     bool
	}{
		{name: "tigris domain", endpoint: "https://t3.storage.dev", want: true},
		{name: "tigris subdomain", endpoint: "https://fly.storage.tigris.dev", want: true},
		{name: "domain with port", endpoint: "https://t3.storage.dev:443", want: true},
		{name: "localhost", endpoint: "http://localhost:8080", want: false},
		{name: "ipv4", endpoint: "http://127.0.0.1:8080", want: false},
		{name: "ipv4 no port", endpoint: "http://127.0.0.1", want: false},
		{name: "ipv6 loopback", endpoint: "http://[::1]:8080", want: false},
		{name: "ipv4 address", endpoint: "http://10.0.0.1:9000", want: false},
		{name: "empty", endpoint: "", want: false},
		{name: "invalid url", endpoint: "://bad", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SupportsVHost(tt.endpoint)
			if got != tt.want {
				t.Errorf("SupportsVHost(%q) = %v, want %v", tt.endpoint, got, tt.want)
			}
		})
	}
}

func TestRemoveBucketPrefix(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		bucket string
		want   string
	}{
		{
			name:   "bucket and key",
			path:   "/mybucket/mykey",
			bucket: "mybucket",
			want:   "/mykey",
		},
		{
			name:   "bucket and nested key",
			path:   "/mybucket/path/to/key",
			bucket: "mybucket",
			want:   "/path/to/key",
		},
		{
			name:   "bucket only",
			path:   "/mybucket",
			bucket: "mybucket",
			want:   "/",
		},
		{
			name:   "bucket with trailing slash",
			path:   "/mybucket/",
			bucket: "mybucket",
			want:   "/",
		},
		{
			name:   "empty bucket returns path",
			path:   "/mybucket/key",
			bucket: "",
			want:   "/mybucket/key",
		},
		{
			name:   "empty path returns empty",
			path:   "",
			bucket: "mybucket",
			want:   "",
		},
		{
			name:   "path doesn't match bucket",
			path:   "/otherbucket/key",
			bucket: "mybucket",
			want:   "/otherbucket/key",
		},
		{
			name:   "bucket is prefix of path segment",
			path:   "/mybucketextra/key",
			bucket: "mybucket",
			want:   "/mybucketextra/key",
		},
		{
			name:   "encoded path characters",
			path:   "/mybucket/my%20key",
			bucket: "mybucket",
			want:   "/my%20key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RemoveBucketPrefix(tt.path, tt.bucket)
			if got != tt.want {
				t.Errorf("RemoveBucketPrefix(%q, %q) = %q, want %q", tt.path, tt.bucket, got, tt.want)
			}
		})
	}
}
