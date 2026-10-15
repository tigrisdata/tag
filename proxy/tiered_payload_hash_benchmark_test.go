package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// BenchmarkTieredLocalPutPayloadHash measures a 4 MiB local-tier PUT with a
// concrete payload digest through Service.HandlePutObject. The mock
// supplies AuthValidated; the engine stream, digest check, body staging, and
// metadata publication are the production path.
func BenchmarkTieredLocalPutPayloadHash(b *testing.B) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	const (
		bucket = "benchmark-bucket"
		key    = "tiered-local-put"
	)
	body := bytes.Repeat([]byte("x"), 4<<20)
	mock, forwards, _ := tieredMock()
	svc, store := newTieredTestService(mock, 16<<20)
	request := httptest.NewRequest(http.MethodPut, "/"+bucket+"/"+key, bytes.NewReader(body))
	request.ContentLength = int64(len(body))
	request.Header.Set("X-Amz-Content-Sha256", payloadSHA256(body))

	var previousBodyRef string
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if previousBodyRef != "" {
			b.StopTimer()
			if err := store.DeleteBody(context.Background(), bucket, key, previousBodyRef); err != nil {
				b.Fatalf("delete prior benchmark body: %v", err)
			}
			b.StartTimer()
		}

		request.Body = io.NopCloser(bytes.NewReader(body))
		w := httptest.NewRecorder()
		if err := svc.HandlePutObject(w, request); err != nil {
			b.Fatalf("HandlePutObject: %v", err)
		}
		if w.Code != http.StatusOK {
			b.Fatalf("PUT status = %d, want %d: %s", w.Code, http.StatusOK, w.Body.String())
		}

		b.StopTimer()
		meta, found, err := store.GetMeta(context.Background(), bucket, key)
		if err != nil || !found || meta == nil || meta.BodyRef == "" {
			b.Fatalf("published metadata = %+v, found=%v err=%v", meta, found, err)
		}
		previousBodyRef = meta.BodyRef
		b.StartTimer()
	}
	b.StopTimer()
	if got := forwards.Load(); got != 0 {
		b.Fatalf("upstream requests = %d, want zero for local-tier PUTs", got)
	}
}
