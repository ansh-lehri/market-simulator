package conductor

import "errors"

// ErrMarketNotLive is returned by the canonical-join flow when no market
// record exists for the requested tag in Live state. The flow rejects
// immediately — it never waits for a market to come up (the caller is
// expected to retry after the market's own open timer fires).
var ErrMarketNotLive = errors.New("conductor: no live canonical market for tag")

// ErrAlreadyInSession is returned by the per-user front-gate when the
// requesting user already holds a session. The broker's per-account XOR
// is the authoritative backstop; this gate is the cheap pre-I/O check so
// the common duplicate-trigger case never pays an EnsureAccount
// round-trip.
var ErrAlreadyInSession = errors.New("conductor: user already has a live session")
