package realize

import (
	"math"
	"reflect"
	"testing"
	"time"

	"simulator/economy/account"
	"simulator/economy/cost"
)

// G8 — one-lot open, two partial sells accumulate to one row on flat.
//
// OpenLots at start: [{100,10}].
// Fill A: sell 4 @ 110 from {100,4}, cost {stt:2, gst:1, total:3}.
// Fill B: sell 6 @ 120 from {100,6}, cost {stt:3, gst:2, total:5}.
//
// After A: QtyClosed=4, GrossAccum=(110-100)*4=40, CostsAccum={stt:2,gst:1}.
// After B: QtyClosed=10, GrossAccum=40+(120-100)*6=160, CostsAccum={stt:5,gst:3}.
//
// Assembled row:
//   EntryPrice = 100 (single-price lot; 100*10/10)
//   ExitPrice  = (110*4 + 120*6)/10 = 1160/10 = 116
//   GrossPnl=160, TotalCosts=8, NetPnl=152, PnlPct=152/1000=0.152
func TestAccumulator_G8_PartialSellsAccumulateToOneRow(t *testing.T) {
	entryTime := time.Date(2026, 1, 15, 9, 30, 0, 0, time.UTC)
	fillATime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	fillBTime := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)

	acc := &RealizationAccumulator{
		AccountID:      "acct-1",
		SessionID:      "sess-1",
		InstrumentID:   "ACME",
		PersistOnClose: true,
	}

	inputA := RealizationInput{
		AccountID: "acct-1", SessionID: "sess-1", InstrumentID: "ACME",
		FillQty:     4,
		MakerPrice:  110,
		MatchedLots: []account.LotMatch{{EntryPrice: 100, Qty: 4, EntryAt: entryTime}},
		Cost:        cost.CostBreakdown{Lines: map[string]int64{"stt": 2, "gst": 1}, Total: 3},
		ExitAt:      fillATime,
		Direction:   "long",
	}
	acc.fold(inputA, ComputeRealizedPnl(inputA))

	if acc.QtyClosed != 4 {
		t.Errorf("after fill A: QtyClosed want 4, got %d", acc.QtyClosed)
	}
	if acc.GrossAccum != 40 {
		t.Errorf("after fill A: GrossAccum want 40, got %d", acc.GrossAccum)
	}
	if acc.CostsTotal != 3 {
		t.Errorf("after fill A: CostsTotal want 3, got %d", acc.CostsTotal)
	}
	if !reflect.DeepEqual(acc.CostsAccum, map[string]int64{"stt": 2, "gst": 1}) {
		t.Errorf("after fill A: CostsAccum wrong: %v", acc.CostsAccum)
	}

	inputB := RealizationInput{
		AccountID: "acct-1", SessionID: "sess-1", InstrumentID: "ACME",
		FillQty:     6,
		MakerPrice:  120,
		MatchedLots: []account.LotMatch{{EntryPrice: 100, Qty: 6, EntryAt: entryTime}},
		Cost:        cost.CostBreakdown{Lines: map[string]int64{"stt": 3, "gst": 2}, Total: 5},
		ExitAt:      fillBTime,
		Direction:   "long",
	}
	acc.fold(inputB, ComputeRealizedPnl(inputB))

	if acc.QtyClosed != 10 {
		t.Errorf("after fill B: QtyClosed want 10, got %d", acc.QtyClosed)
	}
	if acc.GrossAccum != 160 {
		t.Errorf("after fill B: GrossAccum want 160, got %d", acc.GrossAccum)
	}
	if acc.CostsTotal != 8 {
		t.Errorf("after fill B: CostsTotal want 8, got %d", acc.CostsTotal)
	}
	if !reflect.DeepEqual(acc.CostsAccum, map[string]int64{"stt": 5, "gst": 3}) {
		t.Errorf("after fill B: CostsAccum wrong: %v", acc.CostsAccum)
	}

	closed := acc.assemble("trade-1", ExitVoluntary)

	wantSummary := ParticipantTradeRow{
		TradeID:      "trade-1",
		AccountID:    "acct-1",
		SessionID:    "sess-1",
		InstrumentID: "ACME",
		Direction:    "long",
		Quantity:     10,
		EntryPrice:   100,
		ExitPrice:    116,
		EntryAt:      entryTime,
		ExitAt:       fillBTime,
		GrossPnl:     160,
		TotalCosts:   8,
		NetPnl:       152,
		ExitReason:   ExitVoluntary,
	}
	gotSummary := closed.Summary
	if math.Abs(gotSummary.PnlPct-152.0/1000.0) > 1e-9 {
		t.Errorf("PnlPct: want ~0.152, got %v", gotSummary.PnlPct)
	}
	// Zero out PnlPct on both sides for a struct-equality compare of the rest.
	gotSummary.PnlPct = 0
	if !reflect.DeepEqual(gotSummary, wantSummary) {
		t.Errorf("Summary mismatch:\nwant %+v\n got %+v", wantSummary, gotSummary)
	}

	wantCost := TradeCostRow{
		TradeID: "trade-1",
		Lines:   map[string]int64{"stt": 5, "gst": 3},
		Total:   8,
	}
	if !reflect.DeepEqual(closed.Cost, wantCost) {
		t.Errorf("Cost mismatch:\nwant %+v\n got %+v", wantCost, closed.Cost)
	}
}

// G9 — multi-lot round-trip closed in one fill; weighted-average EntryPrice.
//
// OpenLots [{100,10},{120,10}] closed by sell 20 @ 130 in one fill.
// GrossPnl = (130-100)*10 + (130-120)*10 = 300 + 100 = 400
// entryValueSum = 100*10 + 120*10 = 2200; EntryPrice = 2200/20 = 110
// ExitPrice = 130*20/20 = 130
// TotalCosts = 70; NetPnl = 330; PnlPct = 330/2200 = 0.15
func TestAccumulator_G9_MultiLotRoundTrip_WeightedAvgEntry(t *testing.T) {
	entryA := time.Date(2026, 1, 15, 9, 0, 0, 0, time.UTC)
	entryB := time.Date(2026, 1, 15, 9, 30, 0, 0, time.UTC)
	fillTime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

	acc := &RealizationAccumulator{AccountID: "acct", SessionID: "sess", InstrumentID: "X"}
	input := RealizationInput{
		AccountID: "acct", SessionID: "sess", InstrumentID: "X",
		FillQty:    20,
		MakerPrice: 130,
		MatchedLots: []account.LotMatch{
			{EntryPrice: 100, Qty: 10, EntryAt: entryA},
			{EntryPrice: 120, Qty: 10, EntryAt: entryB},
		},
		Cost:      cost.CostBreakdown{Lines: map[string]int64{"stt": 60, "gst": 10}, Total: 70},
		ExitAt:    fillTime,
		Direction: "long",
	}
	acc.fold(input, ComputeRealizedPnl(input))
	closed := acc.assemble("t-9", ExitVoluntary)

	if closed.Summary.EntryPrice != 110 {
		t.Errorf("EntryPrice (weighted): want 110, got %d", closed.Summary.EntryPrice)
	}
	if closed.Summary.ExitPrice != 130 {
		t.Errorf("ExitPrice: want 130, got %d", closed.Summary.ExitPrice)
	}
	if closed.Summary.Quantity != 20 {
		t.Errorf("Quantity: want 20, got %d", closed.Summary.Quantity)
	}
	if closed.Summary.GrossPnl != 400 {
		t.Errorf("GrossPnl: want 400, got %d", closed.Summary.GrossPnl)
	}
	if closed.Summary.TotalCosts != 70 {
		t.Errorf("TotalCosts: want 70, got %d", closed.Summary.TotalCosts)
	}
	if closed.Summary.NetPnl != 330 {
		t.Errorf("NetPnl: want 330, got %d", closed.Summary.NetPnl)
	}
	if math.Abs(closed.Summary.PnlPct-330.0/2200.0) > 1e-9 {
		t.Errorf("PnlPct: want ~0.15, got %v", closed.Summary.PnlPct)
	}
	if !closed.Summary.EntryAt.Equal(entryA) {
		t.Errorf("EntryAt: want earliest %v, got %v", entryA, closed.Summary.EntryAt)
	}
	if !closed.Summary.ExitAt.Equal(fillTime) {
		t.Errorf("ExitAt: want %v, got %v", fillTime, closed.Summary.ExitAt)
	}
}

// G10 — loss round-trip; signed PnlPct.
//
// OpenLots [{100,10}] closed by sell 10 @ 90; cost total 5.
// GrossPnl = (90-100)*10 = -100; NetPnl = -105; PnlPct = -105/1000 = -0.105.
func TestAccumulator_G10_LossRoundTrip_SignedPnlPct(t *testing.T) {
	entry := time.Date(2026, 1, 15, 9, 0, 0, 0, time.UTC)
	exit := time.Date(2026, 1, 15, 11, 0, 0, 0, time.UTC)

	acc := &RealizationAccumulator{AccountID: "a", SessionID: "s", InstrumentID: "Y"}
	input := RealizationInput{
		FillQty:     10,
		MakerPrice:  90,
		MatchedLots: []account.LotMatch{{EntryPrice: 100, Qty: 10, EntryAt: entry}},
		Cost:        cost.CostBreakdown{Total: 5},
		ExitAt:      exit,
		Direction:   "long",
	}
	acc.fold(input, ComputeRealizedPnl(input))
	closed := acc.assemble("t-10", ExitVoluntary)

	if closed.Summary.GrossPnl != -100 {
		t.Errorf("GrossPnl: want -100, got %d", closed.Summary.GrossPnl)
	}
	if closed.Summary.NetPnl != -105 {
		t.Errorf("NetPnl: want -105, got %d", closed.Summary.NetPnl)
	}
	if math.Abs(closed.Summary.PnlPct-(-105.0/1000.0)) > 1e-9 {
		t.Errorf("PnlPct: want ~-0.105, got %v", closed.Summary.PnlPct)
	}
}

// G11 — assemble with ExitAutoSquare; country-specific lines (like
// "autosquare_fee") surface generically in TradeCostRow.Lines. Confirms
// realize never branches on India-named keys — whatever the cost module
// emits flows through.
func TestAccumulator_G11_AutoSquareExitReason_LinesGeneric(t *testing.T) {
	entry := time.Date(2026, 1, 15, 9, 0, 0, 0, time.UTC)
	exit := time.Date(2026, 1, 15, 15, 30, 0, 0, time.UTC)

	acc := &RealizationAccumulator{AccountID: "a", SessionID: "s", InstrumentID: "Z"}
	input := RealizationInput{
		FillQty:     10,
		MakerPrice:  105,
		MatchedLots: []account.LotMatch{{EntryPrice: 100, Qty: 10, EntryAt: entry}},
		Cost: cost.CostBreakdown{
			Lines: map[string]int64{"stt": 26, "autosquare_fee": 5000},
			Total: 5026,
		},
		ExitAt:    exit,
		Direction: "long",
	}
	acc.fold(input, ComputeRealizedPnl(input))
	closed := acc.assemble("t-11", ExitAutoSquare)

	if closed.Summary.ExitReason != ExitAutoSquare {
		t.Errorf("ExitReason: want auto_square, got %s", closed.Summary.ExitReason)
	}
	if closed.Cost.Lines["autosquare_fee"] != 5000 {
		t.Errorf("Lines[autosquare_fee]: want 5000, got %d", closed.Cost.Lines["autosquare_fee"])
	}
	if closed.Cost.Lines["stt"] != 26 {
		t.Errorf("Lines[stt]: want 26, got %d", closed.Cost.Lines["stt"])
	}
	if closed.Cost.Total != 5026 {
		t.Errorf("Cost.Total: want 5026, got %d", closed.Cost.Total)
	}
	if closed.Summary.GrossPnl != 50 {
		t.Errorf("GrossPnl: want 50, got %d", closed.Summary.GrossPnl)
	}
	if closed.Summary.NetPnl != 50-5026 {
		t.Errorf("NetPnl: want %d, got %d", 50-5026, closed.Summary.NetPnl)
	}
}

// G12 — assemble after fold with empty cost breakdown; NetPnl == GrossPnl,
// TradeCostRow.Lines is empty (nil or {}), Total is 0. Also verifies no
// divide-by-zero panic in pnlPct when there is entry basis.
func TestAccumulator_G12_EmptyCostBreakdown(t *testing.T) {
	entry := time.Date(2026, 1, 15, 9, 0, 0, 0, time.UTC)
	exit := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

	acc := &RealizationAccumulator{AccountID: "a", SessionID: "s", InstrumentID: "W"}
	input := RealizationInput{
		FillQty:     5,
		MakerPrice:  110,
		MatchedLots: []account.LotMatch{{EntryPrice: 100, Qty: 5, EntryAt: entry}},
		Cost:        cost.CostBreakdown{},
		ExitAt:      exit,
		Direction:   "long",
	}
	acc.fold(input, ComputeRealizedPnl(input))
	closed := acc.assemble("t-12", ExitVoluntary)

	if closed.Summary.GrossPnl != 50 {
		t.Errorf("GrossPnl: want 50, got %d", closed.Summary.GrossPnl)
	}
	if closed.Summary.TotalCosts != 0 {
		t.Errorf("TotalCosts: want 0, got %d", closed.Summary.TotalCosts)
	}
	if closed.Summary.NetPnl != 50 {
		t.Errorf("NetPnl: want 50 (== GrossPnl when no costs), got %d", closed.Summary.NetPnl)
	}
	if closed.Cost.Total != 0 {
		t.Errorf("Cost.Total: want 0, got %d", closed.Cost.Total)
	}
	if len(closed.Cost.Lines) != 0 {
		t.Errorf("Cost.Lines: want empty, got %v", closed.Cost.Lines)
	}
}

// Fold captures the FIRST non-empty TradeID and Direction; later non-empty
// values do NOT overwrite. Empty values before the first non-empty one
// leave the field empty until a value arrives.
func TestAccumulator_Fold_FirstNonEmptyTradeIDAndDirectionWin(t *testing.T) {
	entry := time.Date(2026, 1, 15, 9, 0, 0, 0, time.UTC)

	acc := &RealizationAccumulator{}

	// Empty first fill leaves both fields empty.
	inputEmpty := RealizationInput{
		FillQty:     1,
		MakerPrice:  100,
		MatchedLots: []account.LotMatch{{EntryPrice: 100, Qty: 1, EntryAt: entry}},
	}
	acc.fold(inputEmpty, ComputeRealizedPnl(inputEmpty))
	if acc.TradeID != "" {
		t.Errorf("after empty fill: want acc.TradeID empty, got %q", acc.TradeID)
	}
	if acc.Direction != "" {
		t.Errorf("after empty fill: want acc.Direction empty, got %q", acc.Direction)
	}

	// First non-empty values stick.
	inputFirst := inputEmpty
	inputFirst.TradeID = "T1"
	inputFirst.Direction = "long"
	acc.fold(inputFirst, ComputeRealizedPnl(inputFirst))
	if acc.TradeID != "T1" {
		t.Errorf("after T1/long: want acc.TradeID=T1, got %q", acc.TradeID)
	}
	if acc.Direction != "long" {
		t.Errorf("after T1/long: want acc.Direction=long, got %q", acc.Direction)
	}

	// Later non-empty values do NOT overwrite.
	inputLater := inputEmpty
	inputLater.TradeID = "T2"
	inputLater.Direction = "short"
	acc.fold(inputLater, ComputeRealizedPnl(inputLater))
	if acc.TradeID != "T1" {
		t.Errorf("first-wins TradeID: want T1, got %q", acc.TradeID)
	}
	if acc.Direction != "long" {
		t.Errorf("first-wins Direction: want long, got %q", acc.Direction)
	}
}
