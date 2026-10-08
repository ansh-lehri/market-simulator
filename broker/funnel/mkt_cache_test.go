package funnel

import "testing"

// nopMarketCache always misses on BOTH BestAsk and LastPrice, across
// every (ExchangeID, InstrumentID) pair we probe — a few exchange ids
// including the empty one, times a few instrument strings including
// the empty one, so a future "defaults for empty id" bug on either
// axis is caught early. Phase-6 A9 position-mark degrades to
// MarkAvailable:false on LastPrice ok=false, so this is the production
// behavior until B10 wires a real cache.
func TestNopMarketCache_AlwaysMisses(t *testing.T) {
	cache := nopMarketCache{}
	exchanges := []ExchangeID{"NSE-canonical", "NSE-ondemand", ""}
	instruments := []string{"ACME", "WIDGET", "", "with spaces"}
	for _, exchange := range exchanges {
		for _, instrument := range instruments {
			if price, ok := cache.BestAsk(exchange, instrument); ok || price != 0 {
				t.Errorf("nopMarketCache.BestAsk(%q, %q) = (%d, %v), want (0, false)",
					exchange, instrument, price, ok)
			}
			if price, ok := cache.LastPrice(exchange, instrument); ok || price != 0 {
				t.Errorf("nopMarketCache.LastPrice(%q, %q) = (%d, %v), want (0, false)",
					exchange, instrument, price, ok)
			}
		}
	}
}

// nopMarketCache implements MarketPriceCache — compile-time guarantee
// the production stand-in stays in sync with the interface. If a
// method is added to MarketPriceCache without an impl here, the file
// fails to compile; the Phase-5 place path and the Phase-6 position-
// mark path both dereference the interface unconditionally and must
// stay safe.
var _ MarketPriceCache = nopMarketCache{}
