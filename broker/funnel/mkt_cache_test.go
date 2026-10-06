package funnel

import "testing"

// nopMarketCache always misses — a few instrument strings including the
// empty one just so a future "defaults for empty id" bug is caught early.
func TestNopMarketCache_AlwaysMisses(t *testing.T) {
	cache := nopMarketCache{}
	for _, instrument := range []string{"ACME", "WIDGET", "", "with spaces"} {
		price, ok := cache.BestAsk(instrument)
		if ok {
			t.Errorf("nopMarketCache.BestAsk(%q) ok=true (price=%d), want false", instrument, price)
		}
		if price != 0 {
			t.Errorf("nopMarketCache.BestAsk(%q) price=%d, want 0 on miss", instrument, price)
		}
	}
}
