package wire

import "simulator/exchange/clock"

// controlKind discriminates the payload carried by a controlMessage. One
// kind exists today (session-phase transition fan-out from the monitor);
// closing-auction clear will add a second when the auction package lands.
type controlKind int

const (
	controlSessionTransition controlKind = iota
	// controlClearAuction — deferred, wired in when the auction package
	// lands. Kept as a comment so the extension point is visible.
)

// controlMessage is the internal shard→shard message the monitor sends on
// each shard's dedicated control channel (EXCHANGE_10 §7). Separate from
// contract.InboundMessage because control is a wire-internal concern; per
// feedback_seam_types_immutable, the seam type stays untouched.
type controlMessage struct {
	kind       controlKind
	transition clock.PhaseTransition
}
