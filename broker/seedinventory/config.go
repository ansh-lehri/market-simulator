package seedinventory

import (
	"errors"
	"fmt"
	"time"
)

// Config carries the seed-inventory HTTP handler's one dial,
// SeedTimeout. Mirrors broker/transfer's Config shape (BROKER_SEED_INVENTORY
// §5): the handler owns the request deadline, not the funnel.
//
// Open dial (value): the number is a bringup/ops concern, deliberately
// not pinned here.
type Config struct {
	SeedTimeout time.Duration
}

// ErrBadConfig is the category sentinel for Validate failures. Callers
// (bringup, tests) dispatch via errors.Is.
var ErrBadConfig = errors.New("broker/seedinventory: bad Config")

// Validate rejects an unusable Config at construction time; a zero or
// negative SeedTimeout would collapse every request to immediate
// context.DeadlineExceeded, which is a bringup-time programming error,
// not a per-request outcome.
func (cfg Config) Validate() error {
	if cfg.SeedTimeout <= 0 {
		return fmt.Errorf("SeedTimeout=%v (must be > 0): %w", cfg.SeedTimeout, ErrBadConfig)
	}
	return nil
}
