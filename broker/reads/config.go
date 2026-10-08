package reads

import (
	"errors"
	"fmt"
	"time"
)

// Config carries the one dial every read HTTP handler shares — the
// request deadline every handler attaches to its context before
// calling the broker or the store.
//
// Keeping the timeout on the handler (not inside the funnel) means a
// slow shard surfaces as a 504 to the client rather than a hanging
// HTTP connection. The observation endpoint fans two reads out on the
// same ctx, so one dial bounds both legs — a slow shard plus a slow DB
// cannot combine past the dial.
//
// Value is deliberately not pinned here. The field's home is this
// package; the number is a bringup / ops concern that tunes with real
// traffic, same as the dials the transfer and order packages carry.
type Config struct {
	ReadTimeout time.Duration
}

// ErrBadConfig is the category sentinel every Validate failure wraps,
// so callers (bringup, tests) dispatch with a single errors.Is check.
var ErrBadConfig = errors.New("broker/reads: bad Config")

// Validate rejects an unusable Config at construction time. A zero or
// negative ReadTimeout would collapse every request into immediate
// context.DeadlineExceeded, which is a bringup programming error, not
// a per-request outcome.
func (cfg Config) Validate() error {
	if cfg.ReadTimeout <= 0 {
		return fmt.Errorf("ReadTimeout=%v (must be > 0): %w", cfg.ReadTimeout, ErrBadConfig)
	}
	return nil
}
