package proxy

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/tigrisdata/tag/auth"
)

// tieredConditionalOrigin evaluates conditions against its own objects, not
// TAG's local tier. It also validates the forwarded caller's signature.
type tieredConditionalOrigin struct {
	mu      sync.Mutex
	objects map[string]string
	puts    int
}

func (o *tieredConditionalOrigin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	credentials := auth.NewCredentialStore()
	credentials.AddCredential(payloadHashTestAccessKey, payloadHashTestSecretKey)
	check := r.Clone(r.Context())
	if host := r.Header.Get("X-Tigris-Forwarded-Host"); host != "" {
		check.Host = host
		check.Header.Set("Host", host)
	}
	if _, err := auth.NewRequestValidator(credentials).Validate(check); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	body, exists := o.objects[r.URL.Path]
	etag := fmt.Sprintf(`"%x"`, md5.Sum([]byte(body)))
	switch r.Method {
	case http.MethodPut:
		o.puts++
		if im := r.Header.Get("If-Match"); im != "" && (!exists || (im != "*" && im != etag)) {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		if inm := r.Header.Get("If-None-Match"); exists && (inm == "*" || inm == etag) {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		o.objects[r.URL.Path] = string(data)
		w.Header().Set("ETag", fmt.Sprintf(`"%x"`, md5.Sum(data)))
	case http.MethodGet:
		if !exists {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("ETag", etag)
		_, _ = io.WriteString(w, body)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func TestTieredLargePutLocalConditions(t *testing.T) {
	for _, flavor := range []payloadHashTestFlavor{payloadHashSigningFlavor, payloadHashTransparentFlavor} {
		for _, condition := range []string{"If-None-Match", "If-Match"} {
			origin := &tieredConditionalOrigin{objects: make(map[string]string)}
			fixture := newTieredPayloadHashFixture(t, flavor, origin)
			old := []byte("local only")
			seed := fixture.do(t, http.MethodPut, "object", old, payloadSHA256(old), "")
			if seed.Code != http.StatusOK {
				t.Fatalf("seed local object: %d %s", seed.Code, seed.Body.String())
			}
			prior, version, found, err := fixture.cache.GetMetaWithVersion(context.Background(), payloadHashTestBucket, "object")
			if err != nil || !found || prior == nil || prior.BodyUpstream {
				t.Fatalf("seed metadata: %+v found=%v err=%v", prior, found, err)
			}
			value := "*"
			if condition == "If-Match" {
				value = prior.ETag
			}
			body := []byte(strings.Repeat("replacement", 128))
			result := fixture.do(t, http.MethodPut, "object", body, payloadSHA256(body), "", http.Header{condition: {value}})
			current, currentVersion, found, err := fixture.cache.GetMetaWithVersion(context.Background(), payloadHashTestBucket, "object")
			if err != nil {
				t.Fatalf("read current metadata: %v", err)
			}
			get := fixture.do(t, http.MethodGet, "object", nil, "UNSIGNED-PAYLOAD", "")
			if condition == "If-None-Match" {
				if result.Code != http.StatusPreconditionFailed || origin.puts != 0 || !found || current == nil || currentVersion != version || get.Code != http.StatusOK || get.Body.String() != string(old) {
					t.Errorf("conditional rejection (%s) = %d, origin PUTs=%d, version=%d->%d, meta=%+v, GET=%d %q; want 412 without forwarding or changing the local object", flavor, result.Code, origin.puts, version, currentVersion, current, get.Code, get.Body.String())
				}
			} else if result.Code != http.StatusOK || origin.puts != 1 || !found || current == nil || !current.BodyUpstream || get.Code != http.StatusOK || get.Body.String() != string(body) {
				t.Errorf("conditional replacement (%s) = %d %s, origin PUTs=%d, meta=%+v, GET=%d %q; want a readable upstream replacement after matching the local ETag", flavor, result.Code, result.Body.String(), origin.puts, current, get.Code, get.Body.String())
			}
		}
	}
}
