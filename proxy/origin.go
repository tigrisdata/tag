package proxy

// Origin answers the questions that depend on whether TAG has an upstream to
// fall back to.
//
// These are policy, not I/O. Every upstream *call* is already behind
// RequestForwarder, so a mode with no upstream needs no changes in the request
// handlers for the network work itself. What the forwarder cannot express is the
// handful of decisions that are only correct *because* an origin exists — filling
// a miss, revalidating an entry, and (later) trusting an unsigned request. Those
// live here so they are named and testable rather than spelled as scattered
// config lookups.
//
// This renames branching rather than removing it: CanFill() is still a branch.
// The gain is three seams with names that say why, instead of `if cfg.X` in a
// dozen places.
type Origin interface {
	// CanFill reports whether a cache miss can be satisfied from an upstream.
	// When false, a miss is the final answer and must be served as NoSuchKey —
	// there is nowhere else for the bytes to come from.
	CanFill() bool

	// CanRevalidate reports whether a cached entry can be checked against an
	// upstream. When false, a client's Cache-Control: no-cache cannot be honored
	// and must be ignored rather than acted on: both revalidation paths end in a
	// conditional GET or a fall-through to the miss path, and with no upstream the
	// fall-through would discard a perfectly good cached object.
	CanRevalidate() bool

	// AcceptsMutations reports whether writes and deletes can be carried out. A
	// proxying TAG forwards them upstream; phase 1 of origin-less mode has nowhere
	// to put them and must reject them BEFORE any cache invalidation runs — the
	// mutation handlers invalidate first and forward second, so without this gate a
	// rejected PUT (or a 1000-key DeleteObjects) still destroys the cached entries
	// it named. Becomes a local commit in a later phase.
	AcceptsMutations() bool

	// TrustsUnauthenticated reports whether an unsigned request may read cached
	// objects regardless of their ACL, because the deployment's trust boundary is
	// the network rather than the request signature.
	//
	// False in every mode today. Origin-less deployments that front an internal
	// tier will set it, which is why it is stated as a property of the deployment
	// rather than inferred from "no credentials were presented".
	TrustsUnauthenticated() bool
}

// proxyOrigin is TAG's normal mode: an upstream is configured and reachable.
type proxyOrigin struct{}

func (proxyOrigin) CanFill() bool               { return true }
func (proxyOrigin) CanRevalidate() bool         { return true }
func (proxyOrigin) AcceptsMutations() bool      { return true }
func (proxyOrigin) TrustsUnauthenticated() bool { return false }

// noOrigin is origin-less mode: TAG serves only what it already holds. A miss is
// a miss, and mutations are rejected — local writes arrive in a later phase.
type noOrigin struct{}

func (noOrigin) CanFill() bool          { return false }
func (noOrigin) CanRevalidate() bool    { return false }
func (noOrigin) AcceptsMutations() bool { return false }

// Still false: origin-less and auth-less are separate decisions, and conflating
// them here would turn "no upstream" into an ACL bypass.
func (noOrigin) TrustsUnauthenticated() bool { return false }

var (
	_ Origin = proxyOrigin{}
	_ Origin = noOrigin{}
)

// originFor selects the mode from whether an upstream endpoint is configured.
//
// Derived rather than carried in its own flag: a separate boolean could
// contradict the endpoint, and there is no meaningful state where TAG has an
// origin it refuses to use, or lacks one it is expected to reach.
func originFor(upstreamEndpoint string) Origin {
	if upstreamEndpoint == "" {
		return noOrigin{}
	}
	return proxyOrigin{}
}
