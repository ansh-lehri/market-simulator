package conductor

import "errors"

// ErrMarketNotLive is returned by a join flow (canonical, by tag, or
// on-demand, by exact market id) when the resolved market record does
// not exist or is not in Live state. The flow rejects immediately — it
// never waits for a market to come up (the caller is expected to retry
// after the market's own open/warm-up timer fires).
var ErrMarketNotLive = errors.New("conductor: no live market for the join request")

// ErrAlreadyInSession is returned by the per-user front-gate when the
// requesting user already holds a session. The broker's per-account XOR
// is the authoritative backstop; this gate is the cheap pre-I/O check so
// the common duplicate-trigger case never pays an EnsureAccount
// round-trip.
var ErrAlreadyInSession = errors.New("conductor: user already has a live session")
