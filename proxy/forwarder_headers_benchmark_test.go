package proxy

import (
	"context"
	"net/http"
	"testing"
)

type copyHeadersBenchmarkShape struct {
	name               string
	ordinary, metadata int
}

var copyHeadersBenchmarkShapes = []copyHeadersBenchmarkShape{
	{name: "ZeroMetadata", ordinary: 10},
	{name: "TypicalMetadata", ordinary: 9, metadata: 1},
	{name: "ManyMetadata", ordinary: 4, metadata: 8},
}

func benchmarkResponseHeaders(shape copyHeadersBenchmarkShape) http.Header {
	headers := make(http.Header, shape.ordinary+shape.metadata)
	for i := 0; i < shape.ordinary; i++ {
		key := "X-Upstream-Header-" + string(rune('A'+i))
		headers[key] = []string{"ordinary-value"}
	}
	for i := 0; i < shape.metadata; i++ {
		key := "X-Amz-Meta-Custom-" + string(rune('A'+i))
		headers[key] = []string{"metadata-value"}
	}
	return headers
}

// BenchmarkCopyHeadersResponseMix measures response-header copying for the
// ordinary response mixes used by the signing-mode forwarding path.
func BenchmarkCopyHeadersResponseMix(b *testing.B) {
	for _, shape := range copyHeadersBenchmarkShapes {
		b.Run(shape.name, func(b *testing.B) {
			src := benchmarkResponseHeaders(shape)
			dst := make(http.Header, len(src))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				clear(dst)
				copyHeaders(dst, src)
			}
			b.StopTimer()
			if len(dst) != len(src) {
				b.Fatalf("copied header count = %d, want %d", len(dst), len(src))
			}
		})
	}
}

// BenchmarkSigningForwarderResponseHeaders measures the complete signing-mode
// forwarding path with controlled upstream responses carrying the same mixes.
func BenchmarkSigningForwarderResponseHeaders(b *testing.B) {
	for _, shape := range copyHeadersBenchmarkShapes {
		b.Run(shape.name, func(b *testing.B) {
			service, incomingRequest := newSigningPassthroughBenchmarkWithResponseHeaders(
				b,
				http.MethodGet,
				signingForwarderBenchmarkGetPath,
				"",
				http.Header{"X-Amz-Expected-Bucket-Owner": {"123456789012"}},
				benchmarkResponseHeaders(shape),
			)
			req := incomingRequest.Clone(context.Background())
			writer := signingForwarderBenchmarkResponseWriter{header: make(http.Header, shape.ordinary+shape.metadata)}

			if err := service.HandlePassthrough(&writer, req); err != nil {
				b.Fatal(err)
			}
			writer.header = make(http.Header, shape.ordinary+shape.metadata)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := service.HandlePassthrough(&writer, req); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
