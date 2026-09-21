package realize

import (
	"testing"
	"time"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/cost"
)

// buildAutosquareCosts returns a spy cost module pre-loaded with a realistic
// India-shaped fill-cost line-set (STT + exchange + SEBI + GST +
// autosquare_fee) and an order-cost (brokerage + GST-on-brokerage) so the
// auto-square tests assert on concrete numbers.
func buildAutosquareCosts(fillTotal int64, fillLines map[string]int64, orderTotal int64, orderLines map[string]int64) *spyCostModule {
	return &spyCostModule{
		fillReturns:  cost.CostBreakdown{Lines: fillLines, Total: fillTotal},
		orderReturns: cost.CostBreakdown{Lines: orderLines, Total: orderTotal},
	}
}

// G20 — auto-square happy path.
//
// Account has one open position: OpenLots=[{100,10}], Available=10, Reserved=0.
// Closing price = 105. Injected CostModule returns:
//   fill  = { stt:26, exchange:3, sebi:1, gst:6, autosquare_fee:5000, total:5036 }
//   order = { brokerage:2000, gst:360, total:2360 }
//
// Expected side-effects:
//   Cash: 0 + 1050 (proceeds) − 5036 (fillCost) − 2360 (brokerage) = −6346
//   Position: fully flat.
//   Fill-cost input carried Side=Sell, FillQty=10, MakerPrice=105, IsAutosquare=true.
//   Order-cost input carried EverFilled=true, TotalQty=10, Side=Sell.
//   One enqueue: ExitReason=auto_square, Qty=10, GrossPnl=50, TotalCosts=5036,
//     NetPnl=−4986, Lines[autosquare_fee]=5000.
//   Accumulator deleted.
func TestSettleSessionEnd_G20_HappyPath(t *testing.T) {
	entryTime := time.Date(2026, 1, 15, 9, 30, 0, 0, time.UTC)

	ledger := newSpyLedger()
	persist := &spyPersistence{}
	costs := buildAutosquareCosts(
		5036, map[string]int64{"stt": 26, "exchange": 3, "sebi": 1, "gst": 6, "autosquare_fee": 5000},
		2360, map[string]int64{"brokerage": 2000, "gst": 360},
	)
	realizer := NewRealizer(ledger, persist, costs)

	acct := &account.Account{
		ID:   "a1",
		Cash: account.Cash{},
		Positions: map[contract.InstrumentID]*account.Position{
			"X": {
				Available: 10,
				OpenLots:  []account.Lot{{EntryPrice: 100, RemainingQty: 10, EntryAt: entryTime}},
			},
		},
		PersistOnClose: true,
	}
	ledger.put(acct)

	realizer.SettleSessionEnd("sess-1", []account.AccountID{"a1"}, map[contract.InstrumentID]int64{"X": 105})

	// Cash
	wantCash := int64(1050 - 5036 - 2360)
	if acct.Cash.Available != wantCash {
		t.Errorf("Cash.Available: want %d, got %d", wantCash, acct.Cash.Available)
	}

	// Position flat
	if acct.Positions["X"].Available != 0 || acct.Positions["X"].Reserved != 0 {
		t.Errorf("Position not flat: %+v", acct.Positions["X"])
	}
	if len(acct.Positions["X"].OpenLots) != 0 {
		t.Errorf("OpenLots not drained: %+v", acct.Positions["X"].OpenLots)
	}

	// Fill-cost call
	if len(costs.fillCalls) != 1 {
		t.Fatalf("want 1 fill-cost call, got %d", len(costs.fillCalls))
	}
	fillInput := costs.fillCalls[0]
	if fillInput.Side != contract.Sell {
		t.Errorf("fill Side: want Sell, got %v", fillInput.Side)
	}
	if fillInput.FillQty != 10 {
		t.Errorf("fill FillQty: want 10, got %d", fillInput.FillQty)
	}
	if fillInput.MakerPrice != 105 {
		t.Errorf("fill MakerPrice: want 105, got %d", fillInput.MakerPrice)
	}
	if !fillInput.IsAutosquare {
		t.Errorf("fill IsAutosquare: want true, got false")
	}

	// Order-cost call
	if len(costs.orderCalls) != 1 {
		t.Fatalf("want 1 order-cost call, got %d", len(costs.orderCalls))
	}
	orderInput := costs.orderCalls[0]
	if !orderInput.EverFilled {
		t.Errorf("order EverFilled: want true, got false")
	}
	if orderInput.TotalQty != 10 || orderInput.Side != contract.Sell {
		t.Errorf("order input wrong: %+v", orderInput)
	}

	// Enqueue
	if len(persist.enqueued) != 1 {
		t.Fatalf("want 1 enqueue, got %d", len(persist.enqueued))
	}
	got := persist.enqueued[0]
	if got.Summary.ExitReason != ExitAutoSquare {
		t.Errorf("ExitReason: want auto_square, got %s", got.Summary.ExitReason)
	}
	if got.Summary.Quantity != 10 {
		t.Errorf("Quantity: want 10, got %d", got.Summary.Quantity)
	}
	if got.Summary.GrossPnl != 50 {
		t.Errorf("GrossPnl: want 50, got %d", got.Summary.GrossPnl)
	}
	if got.Summary.TotalCosts != 5036 {
		t.Errorf("TotalCosts: want 5036 (brokerage NOT here), got %d", got.Summary.TotalCosts)
	}
	if got.Summary.NetPnl != 50-5036 {
		t.Errorf("NetPnl: want %d, got %d", 50-5036, got.Summary.NetPnl)
	}
	if got.Cost.Lines["autosquare_fee"] != 5000 {
		t.Errorf("Lines[autosquare_fee]: want 5000, got %d", got.Cost.Lines["autosquare_fee"])
	}
	if _, exists := realizer.book[accKey{"a1", "X"}]; exists {
		t.Errorf("accumulator not deleted after auto-square flush")
	}
}

// G21 — auto-square when an accumulator already exists (voluntary partial
// sell happened first). The auto-square fold merges into the same
// accumulator; the flushed row combines qty, gross, and costs; ExitReason
// is auto_square (per your decision on mixed round-trips).
func TestSettleSessionEnd_G21_MergesExistingAccumulator(t *testing.T) {
	entryTime := time.Date(2026, 1, 15, 9, 30, 0, 0, time.UTC)
	voluntaryFillTime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

	ledger := newSpyLedger()
	persist := &spyPersistence{}
	costs := buildAutosquareCosts(
		5015, map[string]int64{"stt": 15, "autosquare_fee": 5000},
		0, map[string]int64{},
	)
	realizer := NewRealizer(ledger, persist, costs)

	// Post-voluntary state: 4 sold, 6 remain in Available with a single lot.
	acct := &account.Account{
		ID:   "a1",
		Cash: account.Cash{},
		Positions: map[contract.InstrumentID]*account.Position{
			"X": {
				Available: 6,
				OpenLots:  []account.Lot{{EntryPrice: 100, RemainingQty: 6, EntryAt: entryTime}},
			},
		},
		PersistOnClose: true,
	}
	ledger.put(acct)

	// Prime the accumulator to reflect the earlier voluntary partial sell.
	realizer.OnSellFill(RealizationInput{
		AccountID: "a1", SessionID: "sess-1", InstrumentID: "X",
		FillQty:         4,
		MakerPrice:      110,
		MatchedLots:     []account.LotMatch{{EntryPrice: 100, Qty: 4, EntryAt: entryTime}},
		Cost:            cost.CostBreakdown{Lines: map[string]int64{"stt": 2}, Total: 2},
		ExitAt:          voluntaryFillTime,
		Direction:       "long",
		PositionNowFlat: false,
	})
	if len(persist.enqueued) != 0 {
		t.Fatalf("pre-settlement enqueue leak: %d", len(persist.enqueued))
	}

	realizer.SettleSessionEnd("sess-1", []account.AccountID{"a1"}, map[contract.InstrumentID]int64{"X": 105})

	if len(persist.enqueued) != 1 {
		t.Fatalf("want 1 enqueue, got %d", len(persist.enqueued))
	}
	got := persist.enqueued[0]

	if got.Summary.Quantity != 10 {
		t.Errorf("Quantity: want 10 (4 voluntary + 6 auto-square), got %d", got.Summary.Quantity)
	}
	// gross_voluntary = (110-100)*4 = 40; gross_auto = (105-100)*6 = 30; total 70
	if got.Summary.GrossPnl != 70 {
		t.Errorf("GrossPnl: want 70, got %d", got.Summary.GrossPnl)
	}
	if got.Summary.TotalCosts != 2+5015 {
		t.Errorf("TotalCosts: want %d, got %d", 2+5015, got.Summary.TotalCosts)
	}
	if got.Summary.NetPnl != 70-(2+5015) {
		t.Errorf("NetPnl: want %d, got %d", 70-(2+5015), got.Summary.NetPnl)
	}
	if got.Summary.ExitReason != ExitAutoSquare {
		t.Errorf("ExitReason: mixed round-trip must flush as auto_square, got %s", got.Summary.ExitReason)
	}
	if got.Cost.Lines["stt"] != 2+15 {
		t.Errorf("Lines[stt]: want %d, got %d", 2+15, got.Cost.Lines["stt"])
	}
	if got.Cost.Lines["autosquare_fee"] != 5000 {
		t.Errorf("Lines[autosquare_fee]: want 5000, got %d", got.Cost.Lines["autosquare_fee"])
	}
}

// G22 — on-demand session: auto-square still moves money and computes P&L,
// but NO enqueue happens.
func TestSettleSessionEnd_G22_OnDemandSuppressesEnqueue(t *testing.T) {
	entryTime := time.Date(2026, 1, 15, 9, 30, 0, 0, time.UTC)

	ledger := newSpyLedger()
	persist := &spyPersistence{}
	costs := buildAutosquareCosts(
		100, map[string]int64{"stt": 50, "autosquare_fee": 50},
		20, map[string]int64{"brokerage": 20},
	)
	realizer := NewRealizer(ledger, persist, costs)

	acct := &account.Account{
		ID:   "a2",
		Cash: account.Cash{},
		Positions: map[contract.InstrumentID]*account.Position{
			"X": {
				Available: 5,
				OpenLots:  []account.Lot{{EntryPrice: 100, RemainingQty: 5, EntryAt: entryTime}},
			},
		},
		PersistOnClose: false,
	}
	ledger.put(acct)

	realizer.SettleSessionEnd("sess-1", []account.AccountID{"a2"}, map[contract.InstrumentID]int64{"X": 110})

	if len(persist.enqueued) != 0 {
		t.Errorf("on-demand: want 0 enqueues, got %d", len(persist.enqueued))
	}
	// Cash still moved: 0 + 550 - 100 - 20 = 430
	if acct.Cash.Available != 550-100-20 {
		t.Errorf("Cash still moves on on-demand: want %d, got %d", 550-100-20, acct.Cash.Available)
	}
	// Cost module was called (P&L computed even if not enqueued)
	if len(costs.fillCalls) != 1 {
		t.Errorf("fill-cost still called on on-demand: want 1, got %d", len(costs.fillCalls))
	}
}

// G23 — instruments with zero open qty are skipped entirely: no
// RemovePosition, no cost calls, no enqueue for that instrument.
func TestSettleSessionEnd_G23_ZeroOpenQtySkipped(t *testing.T) {
	entryTime := time.Date(2026, 1, 15, 9, 30, 0, 0, time.UTC)

	ledger := newSpyLedger()
	persist := &spyPersistence{}
	costs := buildAutosquareCosts(
		100, map[string]int64{"autosquare_fee": 100},
		0, map[string]int64{},
	)
	realizer := NewRealizer(ledger, persist, costs)

	acct := &account.Account{
		ID:   "a3",
		Cash: account.Cash{Available: 5000},
		Positions: map[contract.InstrumentID]*account.Position{
			"EMPTY": {Available: 0, Reserved: 0, OpenLots: nil},
			"OPEN": {
				Available: 3,
				OpenLots:  []account.Lot{{EntryPrice: 100, RemainingQty: 3, EntryAt: entryTime}},
			},
		},
		PersistOnClose: true,
	}
	ledger.put(acct)

	realizer.SettleSessionEnd("sess-1", []account.AccountID{"a3"},
		map[contract.InstrumentID]int64{"OPEN": 105, "EMPTY": 999})

	if len(persist.enqueued) != 1 {
		t.Fatalf("want exactly 1 enqueue (for OPEN only), got %d", len(persist.enqueued))
	}
	if persist.enqueued[0].Summary.InstrumentID != "OPEN" {
		t.Errorf("enqueued instrument: want OPEN, got %q", persist.enqueued[0].Summary.InstrumentID)
	}
	if len(costs.fillCalls) != 1 {
		t.Errorf("want 1 fill-cost call (for OPEN only), got %d", len(costs.fillCalls))
	}
}

// G24 — multi-account × multi-instrument settlement: each (account,
// instrument) becomes its own enqueue; no cross-contamination.
func TestSettleSessionEnd_G24_MultiAccountMultiInstrument(t *testing.T) {
	entryTime := time.Date(2026, 1, 15, 9, 30, 0, 0, time.UTC)

	ledger := newSpyLedger()
	persist := &spyPersistence{}
	costs := buildAutosquareCosts(
		10, map[string]int64{"autosquare_fee": 10},
		0, map[string]int64{},
	)
	realizer := NewRealizer(ledger, persist, costs)

	for _, id := range []account.AccountID{"a1", "a2"} {
		ledger.put(&account.Account{
			ID:   id,
			Cash: account.Cash{},
			Positions: map[contract.InstrumentID]*account.Position{
				"X": {Available: 2, OpenLots: []account.Lot{{EntryPrice: 100, RemainingQty: 2, EntryAt: entryTime}}},
				"Y": {Available: 3, OpenLots: []account.Lot{{EntryPrice: 200, RemainingQty: 3, EntryAt: entryTime}}},
			},
			PersistOnClose: true,
		})
	}

	realizer.SettleSessionEnd("sess-1",
		[]account.AccountID{"a1", "a2"},
		map[contract.InstrumentID]int64{"X": 105, "Y": 210},
	)

	if len(persist.enqueued) != 4 {
		t.Fatalf("want 4 enqueues (2 accounts × 2 instruments), got %d", len(persist.enqueued))
	}
	seen := map[string]bool{}
	for _, closed := range persist.enqueued {
		key := string(closed.Summary.AccountID) + "|" + string(closed.Summary.InstrumentID)
		if seen[key] {
			t.Errorf("duplicate enqueue for %s", key)
		}
		seen[key] = true
		if closed.Summary.ExitReason != ExitAutoSquare {
			t.Errorf("%s: want auto_square, got %s", key, closed.Summary.ExitReason)
		}
	}
	for _, expected := range []string{"a1|X", "a1|Y", "a2|X", "a2|Y"} {
		if !seen[expected] {
			t.Errorf("missing enqueue for %s", expected)
		}
	}
}

// G25 — missing closing price for an open instrument panics (settlement-
// driver contract: every open instrument must have a close price).
func TestSettleSessionEnd_G25_PanicOnMissingClosingPrice(t *testing.T) {
	entryTime := time.Date(2026, 1, 15, 9, 30, 0, 0, time.UTC)

	ledger := newSpyLedger()
	persist := &spyPersistence{}
	costs := buildAutosquareCosts(0, nil, 0, nil)
	realizer := NewRealizer(ledger, persist, costs)

	acct := &account.Account{
		ID:   "a1",
		Cash: account.Cash{},
		Positions: map[contract.InstrumentID]*account.Position{
			"X": {Available: 1, OpenLots: []account.Lot{{EntryPrice: 100, RemainingQty: 1, EntryAt: entryTime}}},
		},
		PersistOnClose: true,
	}
	ledger.put(acct)

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("expected panic on missing closing price, got none")
		}
	}()

	realizer.SettleSessionEnd("sess-1", []account.AccountID{"a1"}, map[contract.InstrumentID]int64{})
}

// PanicOnMissingAccount — SettleSessionEnd for an account not in the ledger
// panics (same invariant as OnSellFill).
func TestSettleSessionEnd_PanicOnMissingAccount(t *testing.T) {
	ledger := newSpyLedger()
	persist := &spyPersistence{}
	costs := buildAutosquareCosts(0, nil, 0, nil)
	realizer := NewRealizer(ledger, persist, costs)

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("expected panic on missing account, got none")
		}
	}()

	realizer.SettleSessionEnd("sess-1", []account.AccountID{"missing"}, map[contract.InstrumentID]int64{})
}

// AutoSquareBooksAvailableAndReservedTogether — when a position has both
// Available and Reserved units (e.g. a resting sell for part of the
// position), auto-square books the entire open qty out via MovePosition +
// RemovePosition.
func TestSettleSessionEnd_BooksAvailableAndReservedTogether(t *testing.T) {
	entryTime := time.Date(2026, 1, 15, 9, 30, 0, 0, time.UTC)

	ledger := newSpyLedger()
	persist := &spyPersistence{}
	costs := buildAutosquareCosts(
		10, map[string]int64{"autosquare_fee": 10},
		0, map[string]int64{},
	)
	realizer := NewRealizer(ledger, persist, costs)

	acct := &account.Account{
		ID:   "a1",
		Cash: account.Cash{},
		Positions: map[contract.InstrumentID]*account.Position{
			"X": {
				Available: 4,
				Reserved:  6,
				OpenLots:  []account.Lot{{EntryPrice: 100, RemainingQty: 10, EntryAt: entryTime}},
			},
		},
		PersistOnClose: true,
	}
	ledger.put(acct)

	realizer.SettleSessionEnd("sess-1", []account.AccountID{"a1"}, map[contract.InstrumentID]int64{"X": 105})

	if len(persist.enqueued) != 1 {
		t.Fatalf("want 1 enqueue, got %d", len(persist.enqueued))
	}
	if persist.enqueued[0].Summary.Quantity != 10 {
		t.Errorf("Quantity: want 10 (4 avail + 6 reserved), got %d", persist.enqueued[0].Summary.Quantity)
	}
	if acct.Positions["X"].Available != 0 || acct.Positions["X"].Reserved != 0 {
		t.Errorf("Position not flat: %+v", acct.Positions["X"])
	}
	if len(costs.fillCalls) != 1 || costs.fillCalls[0].FillQty != 10 {
		t.Errorf("fill FillQty must include both buckets: %+v", costs.fillCalls)
	}
}
