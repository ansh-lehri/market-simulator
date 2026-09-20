package cost

import (
	"reflect"
	"testing"

	"simulator/contract"
)

func TestComputeOrderCost_India(t *testing.T) {
	module := NewIndiaCostModule(testIndiaRates)

	cases := []struct {
		name          string
		input         OrderCostInput
		expectedLines map[string]int64
		expectedTotal int64
	}{
		{
			// Golden D — order never filled: empty Lines, Total 0.
			// "Per executed order" means a fully-cancelled-before-any-fill
			// order pays zero brokerage.
			name: "golden_D_never_filled",
			input: OrderCostInput{
				EverFilled: false,
				TotalQty:   1000,
				Side:       contract.Buy,
			},
			expectedLines: map[string]int64{},
			expectedTotal: 0,
		},
		{
			// Golden C — order filled: flat ₹20 + 18% GST on ₹20.
			// brokerage = 2000 paise
			// gst       = applyRate(2000, {18, 100}) = (2000*18 + 50)/100 = 360
			// Total     = 2000 + 360                                     = 2360
			name: "golden_C_filled",
			input: OrderCostInput{
				EverFilled: true,
				TotalQty:   1000,
				Side:       contract.Buy,
			},
			expectedLines: map[string]int64{
				"brokerage": 2000,
				"gst":       360,
			},
			expectedTotal: 2360,
		},
		{
			// India ignores Side on ComputeOrderCost — sell output is
			// identical to buy output.
			name: "filled_sell_same_output",
			input: OrderCostInput{
				EverFilled: true,
				TotalQty:   1000,
				Side:       contract.Sell,
			},
			expectedLines: map[string]int64{
				"brokerage": 2000,
				"gst":       360,
			},
			expectedTotal: 2360,
		},
		{
			// India ignores TotalQty (no size-tiered brokerage) —
			// TotalQty=0 with EverFilled=true still yields the full charge.
			name: "filled_zero_totalqty",
			input: OrderCostInput{
				EverFilled: true,
				TotalQty:   0,
				Side:       contract.Buy,
			},
			expectedLines: map[string]int64{
				"brokerage": 2000,
				"gst":       360,
			},
			expectedTotal: 2360,
		},
		{
			// Never-filled + sell — same zero result as never-filled + buy.
			name: "never_filled_sell",
			input: OrderCostInput{
				EverFilled: false,
				TotalQty:   1000,
				Side:       contract.Sell,
			},
			expectedLines: map[string]int64{},
			expectedTotal: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := module.ComputeOrderCost(tc.input)
			if !reflect.DeepEqual(result.Lines, tc.expectedLines) {
				t.Errorf("Lines mismatch\n  got:  %v\n  want: %v", result.Lines, tc.expectedLines)
			}
			if result.Total != tc.expectedTotal {
				t.Errorf("Total = %d, want %d", result.Total, tc.expectedTotal)
			}
		})
	}
}

// Purity guarantee for ComputeOrderCost — same input, byte-identical output.
func TestComputeOrderCost_India_Determinism(t *testing.T) {
	module := NewIndiaCostModule(testIndiaRates)
	input := OrderCostInput{
		EverFilled: true,
		TotalQty:   500,
		Side:       contract.Sell,
	}

	first := module.ComputeOrderCost(input)
	second := module.ComputeOrderCost(input)

	if !reflect.DeepEqual(first, second) {
		t.Fatalf("determinism violated\n  first:  %+v\n  second: %+v", first, second)
	}
}

// Fresh Lines map per invocation — mutating one caller's result must not
// leak into another's.
func TestComputeOrderCost_India_ReturnsFreshMap(t *testing.T) {
	module := NewIndiaCostModule(testIndiaRates)
	input := OrderCostInput{
		EverFilled: true,
		TotalQty:   100,
		Side:       contract.Buy,
	}

	first := module.ComputeOrderCost(input)
	second := module.ComputeOrderCost(input)

	first.Lines["injected"] = 999
	if _, leaked := second.Lines["injected"]; leaked {
		t.Fatalf("Lines map is shared across invocations — second saw injected key: %v", second.Lines)
	}
}
