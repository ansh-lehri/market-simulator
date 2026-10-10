package marketdata

import (
	"errors"
	"fmt"
	"time"
)

// Config carries the dials every marketdata HTTP handler shares. One
// field today (the per-request deadline); WS stage will grow others
// (conn buffer size, lag-eviction threshold, ping interval).
//
// Why the timeout lives on the handler (not inside the engine): a slow
// store surfaces as a 503 to the client rather than a hanging HTTP
// connection, and bounds the engine's one Store round-trip. Same
// discipline as broker/reads/config.go.
type Config struct {
	ReadTimeout time.Duration
}

// ErrBadConfig is the category sentinel every Validate failure wraps so
// callers (bringup, tests) dispatch with a single errors.Is check.
var ErrBadConfig = errors.New("broker/marketdata: bad Config")

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
