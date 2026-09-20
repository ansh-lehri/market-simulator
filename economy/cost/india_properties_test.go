package cost

import (
	"reflect"
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

// drawOrderCostInput generates arbitrary OrderCostInput values.
func drawOrderCostInput(rt *rapid.T) OrderCostInput {
	return OrderCostInput{
		EverFilled: rapid.Bool().Draw(rt, "EverFilled"),
		TotalQty:   rapid.Int64Range(0, 1_000_000_000).Draw(rt, "TotalQty"),
		Side:       drawSide(rt),
	}
}

// Total is always sum(Lines) — the universal contract, extended to the
// order-cost method.
func TestProperty_OrderCost_TotalEqualsSumOfLines(t *testing.T) {
	module := NewIndiaCostModule(testIndiaRates)
	rapid.Check(t, func(rt *rapid.T) {
		input := drawOrderCostInput(rt)
		result := module.ComputeOrderCost(input)
		if result.Total != sum(result.Lines) {
			rt.Fatalf("Total (%d) != sum(Lines) (%d) for input=%+v, Lines=%v",
				result.Total, sum(result.Lines), input, result.Lines)
		}
	})
}

// EverFilled=false must always produce an empty result.
func TestProperty_OrderCost_NeverFilledEmptyResult(t *testing.T) {
	module := NewIndiaCostModule(testIndiaRates)
	rapid.Check(t, func(rt *rapid.T) {
		input := drawOrderCostInput(rt)
		input.EverFilled = false
		result := module.ComputeOrderCost(input)
		if len(result.Lines) != 0 {
			rt.Fatalf("never-filled produced non-empty Lines: %v", result.Lines)
		}
		if result.Total != 0 {
			rt.Fatalf("never-filled Total = %d, want 0", result.Total)
		}
	})
}

// EverFilled=true must always produce exactly two lines: brokerage and gst.
// No stray keys, no missing keys.
func TestProperty_OrderCost_FilledHasExactlyBrokerageAndGST(t *testing.T) {
	module := NewIndiaCostModule(testIndiaRates)
	rapid.Check(t, func(rt *rapid.T) {
		input := drawOrderCostInput(rt)
		input.EverFilled = true
		result := module.ComputeOrderCost(input)
		if len(result.Lines) != 2 {
			rt.Fatalf("expected exactly 2 lines, got %d: %v", len(result.Lines), result.Lines)
		}
		if _, present := result.Lines[lineBrokerage]; !present {
			rt.Fatalf("missing brokerage line: %v", result.Lines)
		}
		if _, present := result.Lines[lineGST]; !present {
			rt.Fatalf("missing gst line: %v", result.Lines)
		}
	})
}

// India ignores TotalQty on ComputeOrderCost — two inputs differing only
// in TotalQty produce identical Lines and Total. Guards against a size-tiered
// brokerage bug slipping in.
func TestProperty_OrderCost_IndiaIgnoresTotalQty(t *testing.T) {
	module := NewIndiaCostModule(testIndiaRates)
	rapid.Check(t, func(rt *rapid.T) {
		everFilled := rapid.Bool().Draw(rt, "EverFilled")
		side := drawSide(rt)
		qtyA := rapid.Int64Range(0, 1_000_000_000).Draw(rt, "QtyA")
		qtyB := rapid.Int64Range(0, 1_000_000_000).Draw(rt, "QtyB")

		resultA := module.ComputeOrderCost(OrderCostInput{EverFilled: everFilled, TotalQty: qtyA, Side: side})
		resultB := module.ComputeOrderCost(OrderCostInput{EverFilled: everFilled, TotalQty: qtyB, Side: side})

		if !reflect.DeepEqual(resultA, resultB) {
			rt.Fatalf("TotalQty leaked into output:\n  qtyA=%d -> %+v\n  qtyB=%d -> %+v",
				qtyA, resultA, qtyB, resultB)
		}
	})
}

// India ignores Side on ComputeOrderCost — buy and sell produce identical
// results for the same EverFilled + TotalQty.
func TestProperty_OrderCost_IndiaIgnoresSide(t *testing.T) {
	module := NewIndiaCostModule(testIndiaRates)
	rapid.Check(t, func(rt *rapid.T) {
		everFilled := rapid.Bool().Draw(rt, "EverFilled")
		qty := rapid.Int64Range(0, 1_000_000_000).Draw(rt, "TotalQty")

		resultBuy := module.ComputeOrderCost(OrderCostInput{EverFilled: everFilled, TotalQty: qty, Side: contract.Buy})
		resultSell := module.ComputeOrderCost(OrderCostInput{EverFilled: everFilled, TotalQty: qty, Side: contract.Sell})

		if !reflect.DeepEqual(resultBuy, resultSell) {
			rt.Fatalf("Side leaked into output:\n  buy  -> %+v\n  sell -> %+v", resultBuy, resultSell)
		}
	})
}
