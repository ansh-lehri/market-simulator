package realize

import (
	"testing"

	"pgregory.net/rapid"

	"simulator/economy/account"
	"simulator/economy/cost"
)

// P1 — signed P&L is never clamped: Net == Gross − Costs exactly, for any
// combination of matched lots, maker price, and cost total. Costs are
// allowed to exceed gross; Net must go negative unchanged.
func TestProperty_RealizedNet_NeverClamped(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		makerPrice := rapid.Int64Range(1, 1_000_000).Draw(rt, "makerPrice")
		lotCount := rapid.IntRange(0, 8).Draw(rt, "lotCount")

		matchedLots := make([]account.LotMatch, 0, lotCount)
		for lotIndex := 0; lotIndex < lotCount; lotIndex++ {
			matchedLots = append(matchedLots, account.LotMatch{
				EntryPrice: rapid.Int64Range(1, 1_000_000).Draw(rt, "entryPrice"),
				Qty:        rapid.Int64Range(1, 10_000).Draw(rt, "qty"),
			})
		}

		costTotal := rapid.Int64Range(0, 10_000_000_000).Draw(rt, "costTotal")

		input := RealizationInput{
			MakerPrice:  makerPrice,
			MatchedLots: matchedLots,
			Cost:        cost.CostBreakdown{Total: costTotal},
		}

		got := ComputeRealizedPnl(input)

		var wantGross int64
		for _, matched := range matchedLots {
			wantGross += (makerPrice - matched.EntryPrice) * matched.Qty
		}

		if got.Gross != wantGross {
			rt.Fatalf("Gross drift: want %d got %d", wantGross, got.Gross)
		}
		if got.Costs != costTotal {
			rt.Fatalf("Costs drift: want %d got %d", costTotal, got.Costs)
		}
		if got.Net != wantGross-costTotal {
			rt.Fatalf("Net drift: want %d got %d (gross=%d costs=%d)",
				wantGross-costTotal, got.Net, wantGross, costTotal)
		}
	})
}
