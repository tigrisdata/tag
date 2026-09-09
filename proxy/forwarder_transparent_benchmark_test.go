package proxy

import (
	"context"
	"net/http"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/auth"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

const (
	transparentForwarderBenchmarkAccessKey = "AKIAIOSFODNN7EXAMPLE"
	transparentForwarderBenchmarkAuth      = "AWS4-HMAC-SHA256 Credential=" + transparentForwarderBenchmarkAccessKey + "/20260214/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	transparentForwarderBenchmarkUserAgent = "aws-sdk-go-v2/1.36.0 ua/2.1 os/linux lang/go#1.24 md/GOOS#linux md/GOARCH#amd64 api/s3#1.75.0"
)

type transparentForwarderBenchmarkTransport struct{}

func (transparentForwarderBenchmarkTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       http.NoBody,
	}, nil
}

type transparentForwarderBenchmarkResponseWriter struct {
	header http.Header
}

func (w *transparentForwarderBenchmarkResponseWriter) Header() http.Header {
	return w.header
}

func (*transparentForwarderBenchmarkResponseWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func (*transparentForwarderBenchmarkResponseWriter) WriteHeader(int) {}

// BenchmarkServiceHandlePassthroughTransparent measures an authenticated small
// S3 operation with the request headers emitted by a typical AWS SDK client.
// The controlled transport keeps upstream network latency out of request setup.
func BenchmarkServiceHandlePassthroughTransparent(b *testing.B) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	base := newBaseForwarder("https://upstream.example.com", "us-east-1", 1)
	base.httpClient = &http.Client{Transport: transparentForwarderBenchmarkTransport{}}
	forwarder := &transparentForwarder{
		baseForwarder:    base,
		proxySigner:      auth.NewProxySigner(transparentForwarderBenchmarkAccessKey, "benchmark-secret-key"),
		upstreamEndpoint: "https://upstream.example.com",
	}
	forwarder.initInterceptor()
	service := NewService(forwarder, cache.NewDisabledCache(), config.NewDefault())

	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		"http://localhost:8080/benchmark-bucket/small-object.txt?partNumber=1",
		nil,
	)
	if err != nil {
		b.Fatal(err)
	}
	req.Host = "client.example.com"
	req.Header = http.Header{
		"Accept":                      {"*/*"},
		"Accept-Encoding":             {"identity"},
		"Authorization":               {transparentForwarderBenchmarkAuth},
		"Content-Type":                {"application/octet-stream"},
		"User-Agent":                  {transparentForwarderBenchmarkUserAgent},
		"X-Amz-Content-Sha256":        {"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		"X-Amz-Date":                  {"20260214T120000Z"},
		"X-Amz-Expected-Bucket-Owner": {"123456789012"},
		"X-Amz-Request-Payer":         {"requester"},
		"X-Amz-Security-Token":        {"IQoJb3JpZ2luX2VjEJr//////////wEaCXVzLWVhc3QtMSJHMEUCIQDbenchmark-session-token"},
		"X-Amz-User-Agent":            {transparentForwarderBenchmarkUserAgent},
		"X-Custom-Client-Trace":       {"trace-0123456789abcdef"},
	}
	writer := transparentForwarderBenchmarkResponseWriter{header: make(http.Header)}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := service.HandlePassthrough(&writer, req); err != nil {
			b.Fatal(err)
		}
	}

}
