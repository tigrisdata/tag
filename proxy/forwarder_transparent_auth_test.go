package proxy

import (
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/tigrisdata/tag/auth"
)

const (
	transparentAuthTestAccessKey = "AKIAIOSFODNN7EXAMPLE"
	transparentAuthTestSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	transparentAuthTestRegion    = "us-east-1"
	transparentAuthTestBucket    = "authz-bucket"
)

type transparentAuthTestKeyProvider struct {
	store *auth.DerivedKeyStore
	calls uint64
}

func (p *transparentAuthTestKeyProvider) GetSigningKey(accessKey, date, region string) ([]byte, error) {
	atomic.AddUint64(&p.calls, 1)
	return p.store.GetSigningKey(accessKey, date, region)
}

func (p *transparentAuthTestKeyProvider) HasKey(accessKey string) bool {
	return p.store.HasKey(accessKey)
}

func (p *transparentAuthTestKeyProvider) callCount() uint64 {
	return atomic.LoadUint64(&p.calls)
}

func newTransparentAuthTestFixture(t *testing.T, authzTTL time.Duration) (*transparentForwarder, *transparentAuthTestKeyProvider) {
	t.Helper()

	keyStore := auth.NewDerivedKeyStore(auth.DefaultDerivedKeyTTL)
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(transparentAuthTestAccessKey, transparentAuthTestSecretKey)
	keyProvider := &transparentAuthTestKeyProvider{store: keyStore}
	authzCache := auth.NewAuthzCache(authzTTL)

	forwarder := &transparentForwarder{
		derivedKeyStore: keyStore,
		validator:       auth.NewRequestValidator(keyProvider),
		authzCache:      authzCache,
	}

	for _, request := range []*http.Request{
		newTransparentAuthHeaderRequest(t),
		newTransparentAuthPresignedRequest(t),
	} {
		info, err := auth.ParseAuthInfo(request)
		if err != nil {
			t.Fatalf("ParseAuthInfo() error = %v", err)
		}
		signingKey, err := credentials.GetSigningKey(transparentAuthTestAccessKey, info.Date, info.Region)
		if err != nil {
			t.Fatalf("GetSigningKey() error = %v", err)
		}
		keyStore.Store(info.AccessKey, info.Date, info.Region, signingKey)
	}

	return forwarder, keyProvider
}

func newTransparentAuthHeaderRequest(t *testing.T) *http.Request {
	t.Helper()

	signer := auth.NewRequestSigner("https://upstream.example.com", transparentAuthTestRegion)
	request, err := signer.SignRequest(
		t.Context(),
		http.MethodGet,
		"/"+transparentAuthTestBucket+"/object.txt?partNumber=1",
		nil,
		"",
		transparentAuthTestAccessKey,
		transparentAuthTestSecretKey,
		nil,
	)
	if err != nil {
		t.Fatalf("SignRequest() error = %v", err)
	}
	return request
}

func newTransparentAuthPresignedRequest(t *testing.T) *http.Request {
	t.Helper()

	cfg := aws.Config{
		Region:      transparentAuthTestRegion,
		Credentials: credentials.NewStaticCredentialsProvider(transparentAuthTestAccessKey, transparentAuthTestSecretKey, ""),
	}
	cfg.BaseEndpoint = aws.String("https://upstream.example.com")
	client := s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.UsePathStyle = true
	})
	presigner := s3.NewPresignClient(client)
	presigned, err := presigner.PresignGetObject(
		t.Context(),
		&s3.GetObjectInput{
			Bucket: aws.String(transparentAuthTestBucket),
			Key:    aws.String("object.txt"),
		},
	)
	if err != nil {
		t.Fatalf("PresignGetObject() error = %v", err)
	}

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, presigned.URL, nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	// RequestValidator requires this header before its presigned branch. It is
	// not a signed header, so adding it does not change the presigned signature.
	request.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	return request
}

func invalidHeaderRequest(request *http.Request) *http.Request {
	invalid := request.Clone(request.Context())
	authHeader := invalid.Header.Get("Authorization")
	signatureStart := strings.Index(authHeader, "Signature=")
	invalid.Header.Set("Authorization", authHeader[:signatureStart+len("Signature=")]+strings.Repeat("0", 64))
	return invalid
}

func invalidPresignedRequest(request *http.Request) *http.Request {
	invalid := request.Clone(request.Context())
	query := invalid.URL.Query()
	query.Set("X-Amz-Signature", strings.Repeat("0", 64))
	invalid.URL.RawQuery = query.Encode()
	return invalid
}

func TestTransparentForwarderAuthzMissSkipsLocalValidation(t *testing.T) {
	forwarder, keyProvider := newTransparentAuthTestFixture(t, time.Hour)
	tests := []struct {
		name    string
		request func(*testing.T) *http.Request
	}{
		{
			name:    "valid header signature",
			request: newTransparentAuthHeaderRequest,
		},
		{
			name: "invalid header signature",
			request: func(t *testing.T) *http.Request {
				return invalidHeaderRequest(newTransparentAuthHeaderRequest(t))
			},
		},
		{
			name:    "valid presigned URL",
			request: newTransparentAuthPresignedRequest,
		},
		{
			name: "invalid presigned URL",
			request: func(t *testing.T) *http.Request {
				return invalidPresignedRequest(newTransparentAuthPresignedRequest(t))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			atomic.StoreUint64(&keyProvider.calls, 0)

			result, err := forwarder.validateLocally(tt.request(t))
			if err != nil {
				t.Fatalf("validateLocally() error = %v, want nil", err)
			}
			if result != AuthNotValidated {
				t.Fatalf("validateLocally() result = %v, want AuthNotValidated", result)
			}
			if got := keyProvider.callCount(); got != 0 {
				t.Fatalf("validator key lookups = %d, want 0 on authorization miss", got)
			}
		})
	}
}

func TestTransparentForwarderGrantRequiresLocalValidation(t *testing.T) {
	forwarder, keyProvider := newTransparentAuthTestFixture(t, time.Hour)
	forwarder.authzCache.Grant(transparentAuthTestAccessKey, transparentAuthTestBucket)

	tests := []struct {
		name      string
		request   func(*testing.T) *http.Request
		want      AuthResult
		wantCalls uint64
	}{
		{
			name:      "valid header signature",
			request:   newTransparentAuthHeaderRequest,
			want:      AuthValidated,
			wantCalls: 1,
		},
		{
			name:      "invalid header signature",
			request:   func(t *testing.T) *http.Request { return invalidHeaderRequest(newTransparentAuthHeaderRequest(t)) },
			want:      AuthNotValidated,
			wantCalls: 1,
		},
		{
			name:      "valid presigned URL",
			request:   newTransparentAuthPresignedRequest,
			want:      AuthValidated,
			wantCalls: 1,
		},
		{
			name: "invalid presigned URL",
			request: func(t *testing.T) *http.Request {
				return invalidPresignedRequest(newTransparentAuthPresignedRequest(t))
			},
			want:      AuthNotValidated,
			wantCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			atomic.StoreUint64(&keyProvider.calls, 0)

			result, err := forwarder.validateLocally(tt.request(t))
			if err != nil {
				t.Fatalf("validateLocally() error = %v, want nil", err)
			}
			if result != tt.want {
				t.Fatalf("validateLocally() result = %v, want %v", result, tt.want)
			}
			if got := keyProvider.callCount(); got != tt.wantCalls {
				t.Fatalf("validator key lookups = %d, want %d", got, tt.wantCalls)
			}
		})
	}
}

func TestTransparentForwarderAuthzMissStatesSkipLocalValidation(t *testing.T) {
	tests := []struct {
		name string
		prep func(*transparentForwarder)
	}{
		{
			name: "absent",
		},
		{
			name: "revoked",
			prep: func(forwarder *transparentForwarder) {
				forwarder.authzCache.Grant(transparentAuthTestAccessKey, transparentAuthTestBucket)
				forwarder.authzCache.Revoke(transparentAuthTestAccessKey, transparentAuthTestBucket)
			},
		},
		{
			name: "expired",
			prep: func(forwarder *transparentForwarder) {
				forwarder.authzCache.Grant(transparentAuthTestAccessKey, transparentAuthTestBucket)
				timeout := time.After(2 * time.Second)
				poll := time.NewTicker(time.Millisecond)
				defer poll.Stop()
				for forwarder.authzCache.IsAuthorized(transparentAuthTestAccessKey, transparentAuthTestBucket) {
					select {
					case <-timeout:
						return
					case <-poll.C:
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			forwarder, keyProvider := newTransparentAuthTestFixture(t, time.Millisecond)
			if tt.prep != nil {
				tt.prep(forwarder)
			}
			if forwarder.authzCache.IsAuthorized(transparentAuthTestAccessKey, transparentAuthTestBucket) {
				t.Fatal("authorization grant is still present")
			}

			result, err := forwarder.validateLocally(newTransparentAuthHeaderRequest(t))
			if err != nil {
				t.Fatalf("validateLocally() error = %v, want nil", err)
			}
			if result != AuthNotValidated {
				t.Fatalf("validateLocally() result = %v, want AuthNotValidated", result)
			}
			if got := keyProvider.callCount(); got != 0 {
				t.Fatalf("validator key lookups = %d, want 0", got)
			}
		})
	}
}
