package proxy

import (
	"context"
	"net/http"
	runtimemetrics "runtime/metrics"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tag/auth"
)

const (
	transparentAuthBenchmarkAccessKey = "AKIAIOSFODNN7EXAMPLE"
	transparentAuthBenchmarkSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	transparentAuthBenchmarkRegion    = "us-east-1"
	transparentAuthBenchmarkBucket    = "benchmark-bucket"
	transparentAuthBenchmarkCPUMetric = "/cpu/classes/user:cpu-seconds"
)

type transparentAuthBenchmarkCPUCounter struct {
	samples []runtimemetrics.Sample
}

func newTransparentAuthBenchmarkCPUCounter(tb testing.TB) *transparentAuthBenchmarkCPUCounter {
	tb.Helper()
	counter := &transparentAuthBenchmarkCPUCounter{
		samples: []runtimemetrics.Sample{{Name: transparentAuthBenchmarkCPUMetric}},
	}
	_ = counter.nanoseconds(tb)
	return counter
}

func (c *transparentAuthBenchmarkCPUCounter) nanoseconds(tb testing.TB) int64 {
	tb.Helper()
	runtimemetrics.Read(c.samples)
	if c.samples[0].Value.Kind() != runtimemetrics.KindFloat64 {
		tb.Fatalf("runtime metric %q kind = %v, want float64", transparentAuthBenchmarkCPUMetric, c.samples[0].Value.Kind())
	}
	return int64(c.samples[0].Value.Float64() * float64(time.Second))
}

type transparentAuthBenchmarkKeyProvider struct {
	store *auth.DerivedKeyStore
	calls atomic.Uint64
}

func (p *transparentAuthBenchmarkKeyProvider) GetSigningKey(accessKey, date, region string) ([]byte, error) {
	p.calls.Add(1)
	return p.store.GetSigningKey(accessKey, date, region)
}

func (p *transparentAuthBenchmarkKeyProvider) HasKey(accessKey string) bool {
	return p.store.HasKey(accessKey)
}

func newTransparentAuthBenchmarkCase(b *testing.B, grant bool) (*transparentForwarder, *http.Request, *transparentAuthBenchmarkKeyProvider) {
	b.Helper()

	keyStore := auth.NewDerivedKeyStore(auth.DefaultDerivedKeyTTL)
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(transparentAuthBenchmarkAccessKey, transparentAuthBenchmarkSecretKey)

	signer := auth.NewRequestSigner("https://upstream.example.com", transparentAuthBenchmarkRegion)
	req, err := signer.SignRequest(
		context.Background(),
		http.MethodGet,
		"/"+transparentAuthBenchmarkBucket+"/object.txt?partNumber=1",
		nil,
		"",
		transparentAuthBenchmarkAccessKey,
		transparentAuthBenchmarkSecretKey,
		nil,
	)
	if err != nil {
		b.Fatal(err)
	}

	date := req.Header.Get("X-Amz-Date")[:8]
	signingKey, err := credentials.GetSigningKey(transparentAuthBenchmarkAccessKey, date, transparentAuthBenchmarkRegion)
	if err != nil {
		b.Fatal(err)
	}
	keyStore.Store(transparentAuthBenchmarkAccessKey, date, transparentAuthBenchmarkRegion, signingKey)

	keyProvider := &transparentAuthBenchmarkKeyProvider{store: keyStore}
	authzCache := auth.NewAuthzCache(time.Hour)
	if grant {
		authzCache.Grant(transparentAuthBenchmarkAccessKey, transparentAuthBenchmarkBucket)
	}

	forwarder := &transparentForwarder{
		derivedKeyStore: keyStore,
		validator:       auth.NewRequestValidator(keyProvider),
		authzCache:      authzCache,
	}
	return forwarder, req, keyProvider
}

// BenchmarkTransparentForwarderValidateLocally measures the local-auth decision
// for known keys with and without a current bucket authorization grant. The
// validator-calls/op metric makes the avoided signing-key lookup observable while
// ns/op and the allocation metrics cover the complete local decision path.
func BenchmarkTransparentForwarderValidateLocally(b *testing.B) {
	oldLogger := log.Logger
	log.Logger = log.Logger.Level(zerolog.WarnLevel)
	b.Cleanup(func() { log.Logger = oldLogger })

	b.Run("GrantMiss", func(b *testing.B) {
		forwarder, req, keyProvider := newTransparentAuthBenchmarkCase(b, false)
		cpu := newTransparentAuthBenchmarkCPUCounter(b)
		b.ReportAllocs()
		cpuStart := cpu.nanoseconds(b)
		b.ResetTimer()
		for b.Loop() {
			result, err := forwarder.validateLocally(req)
			if err != nil || result != AuthNotValidated {
				b.Fatalf("validateLocally() = (%v, %v), want (AuthNotValidated, nil)", result, err)
			}
		}
		b.StopTimer()
		cpuEnd := cpu.nanoseconds(b)
		b.ReportMetric(float64(cpuEnd-cpuStart)/float64(b.N), "cpu-ns/op")
		b.ReportMetric(float64(keyProvider.calls.Load())/float64(b.N), "validator-calls/op")
	})

	b.Run("GrantHit", func(b *testing.B) {
		forwarder, req, keyProvider := newTransparentAuthBenchmarkCase(b, true)
		cpu := newTransparentAuthBenchmarkCPUCounter(b)
		b.ReportAllocs()
		cpuStart := cpu.nanoseconds(b)
		b.ResetTimer()
		for b.Loop() {
			result, err := forwarder.validateLocally(req)
			if err != nil || result != AuthValidated {
				b.Fatalf("validateLocally() = (%v, %v), want (AuthValidated, nil)", result, err)
			}
		}
		b.StopTimer()
		cpuEnd := cpu.nanoseconds(b)
		b.ReportMetric(float64(cpuEnd-cpuStart)/float64(b.N), "cpu-ns/op")
		b.ReportMetric(float64(keyProvider.calls.Load())/float64(b.N), "validator-calls/op")
	})

	b.Run("GrantMix50_50", func(b *testing.B) {
		missForwarder, missReq, missProvider := newTransparentAuthBenchmarkCase(b, false)
		hitForwarder, hitReq, hitProvider := newTransparentAuthBenchmarkCase(b, true)
		cpu := newTransparentAuthBenchmarkCPUCounter(b)
		b.ReportAllocs()
		iteration := 0
		cpuStart := cpu.nanoseconds(b)
		b.ResetTimer()
		for b.Loop() {
			forwarder, req, want := missForwarder, missReq, AuthNotValidated
			if iteration%2 == 1 {
				forwarder, req, want = hitForwarder, hitReq, AuthValidated
			}
			result, err := forwarder.validateLocally(req)
			if err != nil || result != want {
				b.Fatalf("validateLocally() = (%v, %v), want (%v, nil)", result, err, want)
			}
			iteration++
		}
		b.StopTimer()
		cpuEnd := cpu.nanoseconds(b)
		b.ReportMetric(float64(cpuEnd-cpuStart)/float64(b.N), "cpu-ns/op")
		b.ReportMetric(float64(missProvider.calls.Load()+hitProvider.calls.Load())/float64(b.N), "validator-calls/op")
	})
}
