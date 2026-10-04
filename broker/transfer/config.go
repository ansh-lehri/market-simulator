package transfer

import (
	"errors"
	"fmt"
	"time"
)

// Config carries the transfer HTTP handler's one dial, TransferTimeout.
// Mirrors the shape of Tier-4's readTimeout (BROKER_PHASE3 §1.6): the
// handler owns the request deadline, not the funnel. The timeout bounds
// Broker.Transfer's shard round-trip AND, on a Present:false miss,
// Store.DurableTransfer's SQL round-trip — the same ctx is threaded
// through both so a slow shard + slow DB cannot combine to exceed it.
//
// Open dial (value): the number is a bringup/ops concern, deliberately
// not pinned here. The home for the field is Phase-3 (locked); the
// value tunes with real traffic.
type Config struct {
	TransferTimeout time.Duration
}

// ErrBadConfig is the category sentinel for Validate failures. Callers
// (bringup, tests) dispatch via errors.Is per [[feedback_error_category_sentinels]].
var ErrBadConfig = errors.New("broker/transfer: bad Config")

// Validate rejects an unusable Config at construction time; a zero or
// negative TransferTimeout would collapse every request to immediate
// context.DeadlineExceeded, which is a bringup-time programming error,
// not a per-request outcome.
func (cfg Config) Validate() error {
	if cfg.TransferTimeout <= 0 {
		return fmt.Errorf("TransferTimeout=%v (must be > 0): %w", cfg.TransferTimeout, ErrBadConfig)
	}
	return nil
}
