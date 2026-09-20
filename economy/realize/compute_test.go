package realize

import (
	"testing"

	"simulator/economy/account"
	"simulator/economy/cost"
)

// G1 — FIFO across two lots.
// matched = [{100,10},{110,5}], MakerPrice=120, cost total 30.
// gross = (120-100)*10 + (120-110)*5 = 200 + 50 = 250; net = 250 - 30 = 220.
func TestComputeRealizedPnl_G1_FIFOAcrossTwoLots(t *testing.T) {
	input := RealizationInput{
		FillQty:    15,
		MakerPrice: 120,
		MatchedLots: []account.LotMatch{
			{EntryPrice: 100, Qty: 10},
			{EntryPrice: 110, Qty: 5},
		},
		Cost: cost.CostBreakdown{
			Lines: map[string]int64{"stt": 20, "exchange": 5, "gst": 5},
			Total: 30,
		},
	}

	got := ComputeRealizedPnl(input)

	if got.Gross != 250 {
		t.Errorf("Gross: want 250, got %d", got.Gross)
	}
	if got.Costs != 30 {
		t.Errorf("Costs: want 30, got %d", got.Costs)
	}
	if got.Net != 220 {
		t.Errorf("Net: want 220, got %d", got.Net)
	}
	if got.Lines["stt"] != 20 || got.Lines["exchange"] != 5 || got.Lines["gst"] != 5 {
		t.Errorf("Lines carried wrong: %v", got.Lines)
	}
}

// G2 — single-lot loss; signed, never clamped.
// matched = [{100,5}], MakerPrice=95, cost total 4.
// gross = (95-100)*5 = -25; net = -25 - 4 = -29.
func TestComputeRealizedPnl_G2_SingleLotLoss_Signed(t *testing.T) {
	input := RealizationInput{
		FillQty:     5,
		MakerPrice:  95,
		MatchedLots: []account.LotMatch{{EntryPrice: 100, Qty: 5}},
		Cost:        cost.CostBreakdown{Total: 4},
	}

	got := ComputeRealizedPnl(input)

	if got.Gross != -25 || got.Costs != 4 || got.Net != -29 {
		t.Errorf("want gross=-25 costs=4 net=-29; got %+v", got)
	}
}

// G3 — zero matched lots edge case (defensive; realize will not normally
// be called with an empty match, but ComputeRealizedPnl must still be pure
// and return the zero triple).
func TestComputeRealizedPnl_G3_ZeroMatchedLots(t *testing.T) {
	input := RealizationInput{
		FillQty:     0,
		MakerPrice:  120,
		MatchedLots: nil,
		Cost:        cost.CostBreakdown{Total: 0},
	}

	got := ComputeRealizedPnl(input)

	if got.Gross != 0 || got.Costs != 0 || got.Net != 0 {
		t.Errorf("want zero triple; got %+v", got)
	}
}

// G4 — multi-lot unrealized mark, gain.
// lots = [{100,10},{110,5}], ltp=115, incurredEntryCosts=8.
// gross = (115-100)*10 + (115-110)*5 = 150 + 25 = 175; net = 175 - 8 = 167.
func TestComputeUnrealizedMark_G4_MultiLotGain(t *testing.T) {
	lots := []account.Lot{
		{EntryPrice: 100, RemainingQty: 10},
		{EntryPrice: 110, RemainingQty: 5},
	}

	gross, net := ComputeUnrealizedMark(lots, 115, 8)

	if gross != 175 {
		t.Errorf("gross: want 175, got %d", gross)
	}
	if net != 167 {
		t.Errorf("net: want 167, got %d", net)
	}
}

// G5 — unrealized mark, loss; signed, never clamped.
// lots = [{100,10}], ltp=90, incurredEntryCosts=3.
// gross = (90-100)*10 = -100; net = -100 - 3 = -103.
func TestComputeUnrealizedMark_G5_Loss_Signed(t *testing.T) {
	lots := []account.Lot{{EntryPrice: 100, RemainingQty: 10}}

	gross, net := ComputeUnrealizedMark(lots, 90, 3)

	if gross != -100 {
		t.Errorf("gross: want -100, got %d", gross)
	}
	if net != -103 {
		t.Errorf("net: want -103, got %d", net)
	}
}

// G6 — weighted-average entry on an open position.
// lots = [{100,5},{120,10}] -> (100*5 + 120*10) / 15 = 1700/15 = 113 (int div).
func TestComputeWeightedAvgEntry_G6_OpenPosition(t *testing.T) {
	lots := []account.Lot{
		{EntryPrice: 100, RemainingQty: 5},
		{EntryPrice: 120, RemainingQty: 10},
	}

	avg, hasOpen := ComputeWeightedAvgEntry(lots)

	if !hasOpen {
		t.Fatal("hasOpen: want true")
	}
	if avg != 113 {
		t.Errorf("avg: want 113, got %d", avg)
	}
}

// G7 — weighted-average entry on a flat/empty position; no divide-by-zero.
func TestComputeWeightedAvgEntry_G7_Flat(t *testing.T) {
	cases := map[string][]account.Lot{
		"nil":                nil,
		"empty":              {},
		"zero_remaining_qty": {{EntryPrice: 100, RemainingQty: 0}},
	}
	for name, lots := range cases {
		avg, hasOpen := ComputeWeightedAvgEntry(lots)
		if hasOpen {
			t.Errorf("%s: hasOpen want false, got true (avg=%d)", name, avg)
		}
		if avg != 0 {
			t.Errorf("%s: avg want 0, got %d", name, avg)
		}
	}
}
