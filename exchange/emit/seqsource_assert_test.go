package emit

import "simulator/exchange/match"

// Compile-time lock: the Emitter is the real SeqSource that replaces match's
// in-package monotonicSeq stub (and, when auction 1.3 lands, the same shape
// there). Kept in _test.go so the production package does not depend on match.
var _ match.SeqSource = (*Emitter)(nil)
