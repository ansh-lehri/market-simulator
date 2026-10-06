package funnel

import "simulator/contract"

// MarketPriceCache is the market-data read seam Phase-5 point-1 (market-buy
// pricing) consults BEFORE a reserve. Concrete impl is Tier-5 (B10 tape
// relay), not built; this package only knows the interface and the Phase-5
// stub below. Phase 6's position-mark read (A9) will consume the same seam
// via a sibling LastPrice method — not added here, see BROKER_PHASE5
// orientation §4.
//
// Discipline: an in-memory map lookup on the owner goroutine. Callers (the
// place handler) must not block on this; a cache miss is a legitimate "no
// price available" and surfaces as ReasonNoMarketPrice.
type MarketPriceCache interface {
	// BestAsk returns the current best-ask tick for an instrument, or ok=false
	// when the cache is cold (startup, no print yet, instrument never
	// traded). The place handler rejects a market buy with
	// ReasonNoMarketPrice on ok=false — no fallback, no synthetic price.
	BestAsk(instrument contract.InstrumentID) (contract.Ticks, bool)
}

// nopMarketCache is the Phase-5 production stand-in: every lookup misses.
// Limit orders are unaffected (their reserve path never reads BestAsk); only
// market-buy requests surface ReasonNoMarketPrice to the user until B10
// lands.
//
// Rationale (BROKER_PHASE5 §8, carried): Market-buy runtime is blocked on
// Point 5 (MarketDataCache). The branch is designed against the interface
// now so settlement and limit trading move forward; a real cache is a drop-in
// replacement at the newShard wiring point.
type nopMarketCache struct{}

// BestAsk — always misses.
func (nopMarketCache) BestAsk(_ contract.InstrumentID) (contract.Ticks, bool) {
	return 0, false
}
