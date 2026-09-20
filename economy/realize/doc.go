// Package realize implements Layer 5.4 — realized P&L and settlement.
//
// It is the narrative layer over money movement: it computes how much a
// closed round-trip made or lost after costs, and what shows while a
// position is still open. It does not move money — with one exception,
// session-end auto-square, where it books settlement directly via Tier-1
// hands because no order/Convert exists to route through.
//
// Boundary 1 — owns no lot queue. FIFO lives in Tier-1's Position.OpenLots;
// account.RemovePosition performs the match and returns the matched lots.
// This package consumes them and computes arithmetic; it never holds,
// mutates, or re-derives lot state.
//
// Boundary 2 — 5.4 computes; 5.2 moves money on voluntary closes.
// Auto-square (SettleSessionEnd) is the sole exception.
//
// The one piece of state this package owns is the close-accumulator —
// one entry per open (account, instrument) round-trip in progress,
// flushed as one participant_trades row plus one trade_costs child when
// the position goes flat.
package realize
