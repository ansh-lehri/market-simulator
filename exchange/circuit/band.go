// Package circuit owns the per-instrument circuit band — the frozen price
// range trading is allowed in for the session. See docs/exchange/EXCHANGE_15.
package circuit

import "simulator/contract"

// CircuitBand is the single source of truth for one instrument's circuit
// rule. Constructed once at session start; effectively immutable thereafter.
// Two read surfaces answer both halves of the rule from the same fields, so
// admit-side (gate.Bounds) and clamp-side (match.PriceAllowed) cannot diverge
// (EXCHANGE_15 §6 invariant 1).
type CircuitBand struct {
	reference contract.Ticks
	lower     contract.Ticks
	upper     contract.Ticks
}

// NewCircuitBand computes the frozen bounds once, at session start.
// reference is the instrument's prior close (or seed price for the first
// session), from Layer 7. bandBps is the band half-width in basis points,
// from the rulebook (e.g. 1000 = ±10%). Integer division floors, so the
// bounds are exact ticks (EXCHANGE_15 §6 invariant 3).
func NewCircuitBand(reference contract.Ticks, bandBps int) *CircuitBand {
	if reference <= 0 {
		panic("circuit: reference must be > 0")
	}
	if bandBps < 0 {
		panic("circuit: bandBps must be >= 0")
	}
	delta := reference * contract.Ticks(bandBps) / 10000
	return &CircuitBand{
		reference: reference,
		lower:     reference - delta,
		upper:     reference + delta,
	}
}

// PriceAllowed satisfies match.CircuitClamp. The matcher's walk calls this
// before printing at each level; a trade may print iff price is within
// [lower, upper] inclusive.
func (band *CircuitBand) PriceAllowed(price contract.Ticks) bool {
	return price >= band.lower && price <= band.upper
}

// Bounds satisfies gate.CircuitBounds. The gate's checkCircuitBand rejects a
// limit whose price falls outside the returned edges.
func (band *CircuitBand) Bounds() (lower, upper contract.Ticks) {
	return band.lower, band.upper
}
