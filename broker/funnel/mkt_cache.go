package funnel

import "simulator/contract"

// MarketPriceCache is the market-data read seam shared by the market-
// buy price lookup (used at order-place time) and the position mark
// (used by the positions and observation reads). A concrete writer is
// not built yet — the production stand-in below misses on everything,
// and the real cache will be fed by one ingestor goroutine per
// exchange when the tape relay goes live.
//
// Both methods are in-memory map lookups on the owner goroutine —
// callers must not block on them. A miss (ok=false) is a legitimate
// "no price available" and both callers degrade honestly:
//
//   - BestAsk miss  → the market-buy place rejects with "no market price"
//   - LastPrice miss → the mark degrades to MarkAvailable:false; the
//                      position itself still ships
//
// Both methods are keyed by (exchange, instrument). The broker fans
// out to many exchange instances; session install stamps ExchangeID
// on the binding, and when the ingestor goroutine is built it will
// write under its own ExchangeID — so the cache is implicitly
// map[ExchangeID]map[InstrumentID]…, with single-writer-per-
// (ExchangeID, InstrumentID) by construction. The exchange wire does
// NOT carry ExchangeID; the ingestor stamps it on receipt because it
// knows which Public channel it was wired to. Readers pass
// sess.binding.ExchangeID, which the shard already carries from
// session install.
type MarketPriceCache interface {
	// BestAsk returns the current best-ask tick for an instrument on
	// the named exchange, or ok=false when the cache is cold (startup,
	// no print yet, instrument never traded on that exchange). The
	// place handler rejects a market buy with no fallback / no
	// synthetic price when this misses.
	BestAsk(exchange ExchangeID, instrument contract.InstrumentID) (contract.Ticks, bool)

	// LastPrice returns the last-traded tick for an instrument on the
	// named exchange, or ok=false when the cache is cold. The
	// positions read feeds this into the unrealized-mark computation;
	// on ok=false the position row still returns qty + avg with
	// MarkAvailable:false.
	LastPrice(exchange ExchangeID, instrument contract.InstrumentID) (contract.Ticks, bool)
}

// nopMarketCache is the production stand-in: every lookup misses on
// both methods. Limit orders are unaffected (their reserve path never
// reads BestAsk); only market-buy requests surface "no market price",
// and position marks degrade to MarkAvailable:false until the real
// writer lands.
//
// Why ship a stand-in now: order intake, limit trading, and the
// position reads are all ready to move forward; wiring up the real
// cache (one goroutine per exchange, tape-fed) is a separate chunk
// of work. A real impl will drop in at the NewBroker wiring point.
// newShard also substitutes this stub when the caller passes a nil
// mktCache, so the place handler can dereference the interface
// unconditionally without a nil guard at the call site.
type nopMarketCache struct{}

// BestAsk always misses.
func (nopMarketCache) BestAsk(_ ExchangeID, _ contract.InstrumentID) (contract.Ticks, bool) {
	return 0, false
}

// LastPrice always misses. Position marks therefore degrade to
// MarkAvailable:false — the natural production behavior until the
// real cache is wired.
func (nopMarketCache) LastPrice(_ ExchangeID, _ contract.InstrumentID) (contract.Ticks, bool) {
	return 0, false
}
