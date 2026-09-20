package cost

import (
	"testing"

	"pgregory.net/rapid"
	"simulator/contract"
)

// drawSide picks a valid Side uniformly.
func drawSide(rt *rapid.T) contract.Side {
	return rapid.SampledFrom([]contract.Side{contract.Buy, contract.Sell}).Draw(rt, "Side")
}

// drawFillCostInput generates arbitrary but bounded FillCostInput values.
// FillQty and MakerPrice bounds keep turnover well below int64 max so the
// property under test isn't confounded by overflow — 5.3's semantics do not
// promise anything under overflow, and Stage 2 doesn't add overflow guards.
func drawFillCostInput(rt *rapid.T) FillCostInput {
	return FillCostInput{
		Side:         drawSide(rt),
		FillQty:      rapid.Int64Range(0, 10_000_000).Draw(rt, "FillQty"),
		MakerPrice:   rapid.Int64Range(0, 100_000).Draw(rt, "MakerPrice"),
		IsAutosquare: rapid.Bool().Draw(rt, "IsAutosquare"),
	}
}

// Total is always sum(Lines). This is the only universal contract the
// generic CostBreakdown output makes; every country implementation must
// preserve it.
func TestProperty_FillCost_TotalEqualsSumOfLines(t *testing.T) {
	module := NewIndiaCostModule(testIndiaRates)
	rapid.Check(t, func(rt *rapid.T) {
		input := drawFillCostInput(rt)
		result := module.ComputeFillCost(input)
		if result.Total != sum(result.Lines) {
			rt.Fatalf("Total (%d) != sum(Lines) (%d) for input=%+v, Lines=%v",
				result.Total, sum(result.Lines), input, result.Lines)
		}
	})
}

// STT is sell-only — a buy fill must never carry an "stt" line.
func TestProperty_FillCost_BuyNeverHasSTT(t *testing.T) {
	module := NewIndiaCostModule(testIndiaRates)
	rapid.Check(t, func(rt *rapid.T) {
		input := drawFillCostInput(rt)
		input.Side = contract.Buy
		result := module.ComputeFillCost(input)
		if _, present := result.Lines[lineSTT]; present {
			rt.Fatalf("Buy fill contains stt: input=%+v, Lines=%v", input, result.Lines)
		}
	})
}

// Stamp is buy-only — a sell fill must never carry a "stamp" line.
func TestProperty_FillCost_SellNeverHasStamp(t *testing.T) {
	module := NewIndiaCostModule(testIndiaRates)
	rapid.Check(t, func(rt *rapid.T) {
		input := drawFillCostInput(rt)
		input.Side = contract.Sell
		result := module.ComputeFillCost(input)
		if _, present := result.Lines[lineStamp]; present {
			rt.Fatalf("Sell fill contains stamp: input=%+v, Lines=%v", input, result.Lines)
		}
	})
}

// autosquare_fee is gated by IsAutosquare: absent when the flag is false.
func TestProperty_FillCost_NoAutosquareNeverHasAutosquareFee(t *testing.T) {
	module := NewIndiaCostModule(testIndiaRates)
	rapid.Check(t, func(rt *rapid.T) {
		input := drawFillCostInput(rt)
		input.IsAutosquare = false
		result := module.ComputeFillCost(input)
		if _, present := result.Lines[lineAutosquareFee]; present {
			rt.Fatalf("Non-autosquare fill contains autosquare_fee: input=%+v, Lines=%v", input, result.Lines)
		}
	})
}

// autosquare_fee is present with the injected amount whenever IsAutosquare is set.
func TestProperty_FillCost_AutosquareAlwaysHasAutosquareFee(t *testing.T) {
	module := NewIndiaCostModule(testIndiaRates)
	rapid.Check(t, func(rt *rapid.T) {
		input := drawFillCostInput(rt)
		input.IsAutosquare = true
		result := module.ComputeFillCost(input)
		amount, present := result.Lines[lineAutosquareFee]
		if !present {
			rt.Fatalf("Autosquare fill missing autosquare_fee: input=%+v, Lines=%v", input, result.Lines)
		}
		if amount != testIndiaRates.AutosquareFee {
			rt.Fatalf("autosquare_fee = %d, want %d (injected rate)", amount, testIndiaRates.AutosquareFee)
		}
	})
}

// With non-negative FillQty and MakerPrice, every line amount is >= 0.
// Guards against a sign bug in applyRate or the additive GST base.
func TestProperty_FillCost_NonNegativeLines(t *testing.T) {
	module := NewIndiaCostModule(testIndiaRates)
	rapid.Check(t, func(rt *rapid.T) {
		input := drawFillCostInput(rt)
		result := module.ComputeFillCost(input)
		for lineName, amount := range result.Lines {
			if amount < 0 {
				rt.Fatalf("negative line %q = %d for input=%+v, Lines=%v",
					lineName, amount, input, result.Lines)
			}
		}
	})
}
