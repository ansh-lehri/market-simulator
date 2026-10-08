package funnel

import "simulator/contract"

// MarketPriceCache is the market-data read seam shared by Phase-5 Point-1
// (market-buy pricing) and Phase-6 Point-2 (position mark). Concrete impl
// is Tier-5 (B10 tape relay), not built; this package only knows the
// interface and the production stub below.
//
// Both methods are in-memory map lookups on the owner goroutine —
// callers must not block on them. A cache miss (ok=false) is a
// legitimate "no price available" and both callers degrade honestly:
//
//   - BestAsk  miss → ReasonNoMarketPrice   (Phase-5 market-buy reject)
//   - LastPrice miss → MarkAvailable:false  (Phase-6 A9 degrade)
//
// Both methods are keyed by (ExchangeID, InstrumentID). The broker fans
// out to N exchange instances; L7 stamps ExchangeID at session binding
// and the ingestor goroutine (Phase 7, one per exchange, same pattern
// as the settlement consumer) writes into the cache under its own
// ExchangeID — making the cache implicitly a map[ExchangeID]map
// [InstrumentID]… with single-writer-per-(ExchangeID,InstrumentID) by
// construction. The exchange wire itself does NOT carry ExchangeID
// (verified against contract/); the ingestor stamps it on receipt
// because it knows which Public channel it was wired to. Readers pass
// sess.binding.ExchangeID, which the shard already carries from
// session install.
type MarketPriceCache interface {
	// BestAsk returns the current best-ask tick for an instrument on the
	// named exchange, or ok=false when the cache is cold (startup, no
	// print yet, instrument never traded on that exchange). The place
	// handler rejects a market buy with ReasonNoMarketPrice on ok=false
	// — no fallback, no synthetic price.
	BestAsk(exchange ExchangeID, instrument contract.InstrumentID) (contract.Ticks, bool)

	// LastPrice returns the last-traded tick for an instrument on the
	// named exchange, or ok=false when the cache is cold. Phase-6 A9
	// feeds this into realize.ComputeUnrealizedMark to compute
	// GrossUnrealized; on ok=false the position row still returns
	// (Qty, avg) with MarkAvailable:false.
	LastPrice(exchange ExchangeID, instrument contract.InstrumentID) (contract.Ticks, bool)
}

// nopMarketCache is the production stand-in: every lookup misses on
// both methods. Limit orders are unaffected (their reserve path never
// reads BestAsk); only market-buy requests surface ReasonNoMarketPrice,
// and Phase-6 position marks degrade to MarkAvailable:false until B10
// lands.
//
// Rationale: Market-data runtime is blocked on Tier-5 Point 5 (B10
// MarketDataCache). The interface is designed now so settlement, limit
// trading, and Phase-6 reads move forward; a real impl is a drop-in
// replacement at the NewBroker wiring point. Also substituted
// internally by newShard when the caller passes a nil mktCache —
// Phase-5's place handler dereferences mktCache.BestAsk unconditionally
// and must never see a nil interface.
type nopMarketCache struct{}

// BestAsk — always misses.
func (nopMarketCache) BestAsk(_ ExchangeID, _ contract.InstrumentID) (contract.Ticks, bool) {
	return 0, false
}

// LastPrice — always misses. Phase-6 A9 marks degrade to
// MarkAvailable:false on ok=false, so this is the natural production
// behavior until the Tier-5 cache is wired.
func (nopMarketCache) LastPrice(_ ExchangeID, _ contract.InstrumentID) (contract.Ticks, bool) {
	return 0, false
}
