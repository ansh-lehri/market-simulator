package cost

import (
	"reflect"
	"testing"

	"simulator/contract"
)

// testIndiaRates is the pinned India rulebook used by every India test.
// Values are locked in memory (project_india_rate_locks.md): STT 0.025%,
// Exchange 0.00297% (NSE), SEBI 0.0001%, Stamp 0.003%, GST 18%,
// Brokerage ₹20, AutosquareFee ₹50.
var testIndiaRates = IndiaRates{
	STT:           Rate{Num: 25, Den: 100_000},
	Exchange:      Rate{Num: 297, Den: 10_000_000},
	SEBI:          Rate{Num: 1, Den: 1_000_000},
	Stamp:         Rate{Num: 3, Den: 100_000},
	GST:           Rate{Num: 18, Den: 100},
	Brokerage:     2000,
	AutosquareFee: 5000,
}

func TestComputeFillCost_India(t *testing.T) {
	module := NewIndiaCostModule(testIndiaRates)

	cases := []struct {
		name          string
		input         FillCostInput
		expectedLines map[string]int64
		expectedTotal int64
	}{
		{
			// Golden A — buy, no autosquare, turnover ₹1,00,000 (10_000_000 paise).
			// stamp    = applyRate(10_000_000, {3, 100_000})       = 300
			// exchange = applyRate(10_000_000, {297, 10_000_000})  = 297
			// sebi     = applyRate(10_000_000, {1, 1_000_000})     = 10
			// gst      = applyRate(297 + 10, {18, 100})            = 55
			// Total    = 300 + 297 + 10 + 55                       = 662
			name: "golden_A_buy_no_autosquare",
			input: FillCostInput{
				Side:         contract.Buy,
				FillQty:      1000,
				MakerPrice:   10_000,
				IsAutosquare: false,
			},
			expectedLines: map[string]int64{
				"stamp":    300,
				"exchange": 297,
				"sebi":     10,
				"gst":      55,
			},
			expectedTotal: 662,
		},
		{
			// Golden B — sell, autosquare, same turnover.
			// stt            = applyRate(10_000_000, {25, 100_000})  = 2500
			// exchange       = 297; sebi = 10; autosquare_fee = 5000
			// gst            = applyRate(297 + 10 + 5000, {18, 100}) = 955
			// Total          = 2500 + 297 + 10 + 5000 + 955          = 8762
			name: "golden_B_sell_autosquare",
			input: FillCostInput{
				Side:         contract.Sell,
				FillQty:      1000,
				MakerPrice:   10_000,
				IsAutosquare: true,
			},
			expectedLines: map[string]int64{
				"stt":            2500,
				"exchange":       297,
				"sebi":           10,
				"autosquare_fee": 5000,
				"gst":            955,
			},
			expectedTotal: 8762,
		},
		{
			// Sell, no autosquare — stt present, stamp absent, no autosquare_fee.
			name: "sell_no_autosquare",
			input: FillCostInput{
				Side:         contract.Sell,
				FillQty:      1000,
				MakerPrice:   10_000,
				IsAutosquare: false,
			},
			expectedLines: map[string]int64{
				"stt":      2500,
				"exchange": 297,
				"sebi":     10,
				"gst":      55,
			},
			expectedTotal: 2862,
		},
		{
			// Buy + autosquare — nonsensical per §7 but well-defined:
			// stamp present, stt absent, autosquare_fee present, GST includes it.
			name: "buy_autosquare_edge",
			input: FillCostInput{
				Side:         contract.Buy,
				FillQty:      1000,
				MakerPrice:   10_000,
				IsAutosquare: true,
			},
			expectedLines: map[string]int64{
				"stamp":          300,
				"exchange":       297,
				"sebi":           10,
				"autosquare_fee": 5000,
				"gst":            955,
			},
			expectedTotal: 6562,
		},
		{
			// FillQty=0 -> turnover 0 -> every rate line 0 (kept in map).
			name: "zero_qty_buy",
			input: FillCostInput{
				Side:         contract.Buy,
				FillQty:      0,
				MakerPrice:   10_000,
				IsAutosquare: false,
			},
			expectedLines: map[string]int64{
				"stamp":    0,
				"exchange": 0,
				"sebi":     0,
				"gst":      0,
			},
			expectedTotal: 0,
		},
		{
			// MakerPrice=0 -> turnover 0 -> same shape for sell side.
			name: "zero_price_sell",
			input: FillCostInput{
				Side:         contract.Sell,
				FillQty:      1000,
				MakerPrice:   0,
				IsAutosquare: false,
			},
			expectedLines: map[string]int64{
				"stt":      0,
				"exchange": 0,
				"sebi":     0,
				"gst":      0,
			},
			expectedTotal: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := module.ComputeFillCost(tc.input)
			if !reflect.DeepEqual(result.Lines, tc.expectedLines) {
				t.Errorf("Lines mismatch\n  got:  %v\n  want: %v", result.Lines, tc.expectedLines)
			}
			if result.Total != tc.expectedTotal {
				t.Errorf("Total = %d, want %d", result.Total, tc.expectedTotal)
			}
		})
	}
}

// Purity guarantee: same input -> byte-identical output on every call. The
// method is stateless and pure by construction; this pins that as a
// contract, not a hope.
func TestComputeFillCost_India_Determinism(t *testing.T) {
	module := NewIndiaCostModule(testIndiaRates)
	input := FillCostInput{
		Side:         contract.Sell,
		FillQty:      1000,
		MakerPrice:   10_000,
		IsAutosquare: true,
	}

	first := module.ComputeFillCost(input)
	second := module.ComputeFillCost(input)

	if !reflect.DeepEqual(first, second) {
		t.Fatalf("determinism violated\n  first:  %+v\n  second: %+v", first, second)
	}
}

// Guard against a shared-map bug: each ComputeFillCost invocation must
// return a fresh Lines map; mutating one caller's map must not leak into
// another's.
func TestComputeFillCost_India_ReturnsFreshMap(t *testing.T) {
	module := NewIndiaCostModule(testIndiaRates)
	input := FillCostInput{
		Side:       contract.Buy,
		FillQty:    100,
		MakerPrice: 1000,
	}

	first := module.ComputeFillCost(input)
	second := module.ComputeFillCost(input)

	first.Lines["injected"] = 999
	if _, leaked := second.Lines["injected"]; leaked {
		t.Fatalf("Lines map is shared across invocations — second saw injected key: %v", second.Lines)
	}
}
