package funnel

import "testing"

// nopMarketCache must miss on both BestAsk and LastPrice across every
// (exchange, instrument) pair we probe — a few exchange ids including
// the empty one, times a few instrument strings including the empty
// one — so a future "defaults for empty id" bug on either axis is
// caught early. The positions read degrades to MarkAvailable:false on
// a LastPrice miss, which is the production behavior until the real
// cache is wired.
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

// Compile-time guarantee that the production stand-in stays in sync
// with the MarketPriceCache interface. If a method is added without
// an impl here, the file fails to compile — the market-buy place
// path and the position-mark path both dereference the interface
// unconditionally and must stay safe.
var _ MarketPriceCache = nopMarketCache{}
