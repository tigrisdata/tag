package proxy

import (
	"context"
	"errors"
	"net/http"

	"github.com/tigrisdata/tag/s3err"
)

// originlessForwarder is the RequestForwarder for a TAG with no upstream. It
// serves only what the cache already holds; anything else is a miss, and a miss is
// the final answer.
//
// Every upstream call in the proxy goes through RequestForwarder, so this type is
// where "there is no origin" is expressed for I/O. The policy questions that are
// not I/O — may a miss be filled, may an entry be revalidated — live behind Origin
// (see origin.go) and are what keep these methods from being reached in the first
// place.
//
// Deliberately NOT a blanket null object. Returning a synthetic 404 from every
// method would look tidy and corrupt the cache: a 404 from
// DoConditionalGetRequest reads as "deleted upstream" and would make revalidation
// invalidate a healthy entry. So the serve paths answer NoSuchKey, and the
// origin-only paths return errNoOrigin — loud, not plausible.
type originlessForwarder struct{}

// errNoOrigin marks a call that only makes sense against an upstream. Reaching it
// is a wiring bug — Origin.CanFill/CanRevalidate should have prevented the call —
// so it surfaces as an error rather than an empty-but-successful result that would
// be indistinguishable from a real upstream answer.
var errNoOrigin = errors.New("no upstream configured: this deployment serves only cached objects")

// Forward answers from nothing: with no origin there is no fetch to make.
//
// The code depends on what was asked. Forward is the shared exit for far more
// than object reads — HandlePassthrough routes ListObjects/ListBuckets/CreateBucket
// and the multipart calls through it, and the write handlers land here too. Telling
// a client that `s3 ls` failed because "the specified key does not exist" describes
// the wrong thing entirely, and a rejected PUT is not a missing object.
//
// So: a read of a specific object is a genuine miss and gets NoSuchKey. Everything
// else is an operation this mode does not implement, and says so.
func (originlessForwarder) Forward(_ context.Context, w http.ResponseWriter, r *http.Request) error {
	if isObjectRead(r) {
		s3err.WriteError(w, r, s3err.ErrNoSuchKey)
		return nil
	}
	s3err.WriteError(w, r, s3err.ErrNotImplemented)
	return nil
}

// isObjectRead reports whether the request reads one named object, as opposed to
// listing a bucket or mutating something. Only those can honestly answer NoSuchKey.
func isObjectRead(r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	// A bucket-scoped GET is a list, not an object read.
	_, key := ParseBucketKey(r)
	if key == "" {
		return false
	}
	// ?list-type / ?uploads and friends are bucket operations that can carry a
	// prefix resembling a key.
	q := r.URL.Query()
	for _, listish := range []string{"list-type", "uploads", "versions", "delimiter"} {
		if q.Has(listish) {
			return false
		}
	}
	return true
}

// ForwardWithCapture mirrors Forward. The capture reports the 404 so callers that
// gate cache population on a 2xx skip it, exactly as they would for an upstream
// 404.
func (f originlessForwarder) ForwardWithCapture(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
	rec := &statusRecorder{ResponseWriter: w}
	if err := f.Forward(ctx, rec, r); err != nil {
		return nil, err
	}
	// Report whatever Forward actually wrote, so a caller gating cache population
	// on a 2xx skips it — and so the capture never disagrees with the response.
	return &ResponseCapture{
		StatusCode: rec.status,
		Headers:    w.Header(),
	}, nil
}

// ValidateAndGetCredentials cannot validate a signature: signature validation
// resolves keys against the upstream, and there is none.
//
// Phase 1 keeps the ACL gate in the read paths intact, so this alone does not
// grant access to non-public cached objects — an unsigned request still only
// reads public-read entries. Opening that up is a separate, explicit decision
// (Origin.TrustsUnauthenticated), not a side effect of removing the origin.
func (originlessForwarder) ValidateAndGetCredentials(*http.Request) (AuthResult, string, string, error) {
	return AuthNotValidated, "", "", nil
}

func (originlessForwarder) DoRequestWithCreds(context.Context, *http.Request, string, string) (*http.Response, error) {
	return nil, errNoOrigin
}

func (originlessForwarder) DoFullObjectRequest(context.Context, string, string, string, string) (*http.Response, error) {
	return nil, errNoOrigin
}

func (originlessForwarder) DoAnonymousFullObjectRequest(context.Context, string, string) (*http.Response, error) {
	return nil, errNoOrigin
}

func (originlessForwarder) DoConditionalGetRequest(context.Context, string, string, string, string, string, int64, string) (*http.Response, error) {
	return nil, errNoOrigin
}

func (originlessForwarder) DoConditionalHeadRequest(context.Context, string, string, string, string, string, int64) (*http.Response, error) {
	return nil, errNoOrigin
}

var _ RequestForwarder = originlessForwarder{}
