package order

import (
	"errors"
	"fmt"
	"time"
)

// Config carries the one dial every order HTTP handler shares — the
// per-request timeout the handler owns (BROKER_PHASE5 §4.5, P5-D27).
//
// The timeout bounds the broker-side round-trip only; a well-formed
// handler attaches it to the request's context before calling the broker,
// so a slow shard surfaces as a `504 broker timeout` to the client, not
// as a hanging HTTP connection.
//
// Value is deliberately not pinned here: it is a bringup / ops concern,
// just like broker/transfer's Config.TransferTimeout. The home for the
// field is fixed by this phase; the number tunes with real traffic.
type Config struct {
	OrderTimeout time.Duration
}

// ErrBadConfig is the category sentinel every Config.Validate failure
// wraps. Callers (bringup, tests) dispatch via errors.Is per
// [[feedback_error_category_sentinels]] — one errors.Is call catches any
// future variant of "bad Config" we add.
var ErrBadConfig = errors.New("broker/order: bad Config")

// Validate rejects an unusable Config at construction time. A zero or
// negative OrderTimeout would collapse every request into immediate
// context.DeadlineExceeded (surfacing as 504 to every caller) — a
// bringup programming error, not a per-request outcome.
func (cfg Config) Validate() error {
	if cfg.OrderTimeout <= 0 {
		return fmt.Errorf("OrderTimeout=%v (must be > 0): %w", cfg.OrderTimeout, ErrBadConfig)
	}
	return nil
}
