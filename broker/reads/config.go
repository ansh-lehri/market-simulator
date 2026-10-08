package reads

import (
	"errors"
	"fmt"
	"time"
)

// Config carries the one dial every read HTTP handler shares: ReadTimeout
// (BROKER_PHASE6 §0.7, LOCKED).
//
// The handler owns the request deadline, not the funnel. The timeout
// bounds the broker-side round-trip (live reads — A8/A9/A11/A14) AND the
// durable Store query (A10/A16); A14 fans both out on the SAME ctx, so a
// slow shard plus a slow DB cannot combine past the dial.
//
// Value is deliberately not pinned here: it is a bringup / ops concern,
// exactly like broker/transfer's Config.TransferTimeout and broker/order's
// Config.OrderTimeout. The home for the field is fixed by this phase; the
// number tunes with real traffic.
type Config struct {
	ReadTimeout time.Duration
}

// ErrBadConfig is the category sentinel every Config.Validate failure
// wraps. Callers (bringup, tests) dispatch via errors.Is per
// [[feedback_error_category_sentinels]].
var ErrBadConfig = errors.New("broker/reads: bad Config")

// Validate rejects an unusable Config at construction time. A zero or
// negative ReadTimeout would collapse every request into immediate
// context.DeadlineExceeded (surfacing as 504 for live reads or 503 for
// durable reads) — a bringup programming error, not a per-request outcome.
func (cfg Config) Validate() error {
	if cfg.ReadTimeout <= 0 {
		return fmt.Errorf("ReadTimeout=%v (must be > 0): %w", cfg.ReadTimeout, ErrBadConfig)
	}
	return nil
}
