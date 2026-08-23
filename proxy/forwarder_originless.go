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

// Forward answers from nothing: with no origin there is no fetch to make, so a
// request that reached the forwarder has already missed the cache.
func (originlessForwarder) Forward(_ context.Context, w http.ResponseWriter, r *http.Request) error {
	s3err.WriteError(w, r, s3err.ErrNoSuchKey)
	return nil
}

// ForwardWithCapture mirrors Forward. The capture reports the 404 so callers that
// gate cache population on a 2xx skip it, exactly as they would for an upstream
// 404.
func (f originlessForwarder) ForwardWithCapture(ctx context.Context, w http.ResponseWriter, r *http.Request) (*ResponseCapture, error) {
	if err := f.Forward(ctx, w, r); err != nil {
		return nil, err
	}
	return &ResponseCapture{
		StatusCode: http.StatusNotFound,
		Headers:    http.Header{},
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
