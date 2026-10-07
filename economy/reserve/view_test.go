package reserve

import (
	"testing"
	"time"

	"simulator/contract"
	"simulator/economy/account"
)

// -----------------------------------------------------------------------------
// view_test.go — ViewReservation accessor coverage.
//
// Pure-projection accessor, so the test values are what Reserve / Convert /
// Release wrote (those methods are covered by their own goldens elsewhere).
// Each case exercises one lifecycle transition and asserts:
//
//   1. the (view, ok) return matches the book's state after the transition;
//   2. ViewReservation itself causes NO MarkSnapshotDirty call and emits NO
//      event (the pure-read contract).
//
// viewTestExitAt is a fixed timestamp passed to Convert; needs to be a value
// Convert's convertBuy lazy-position-create accepts (any time.Time works) and
// stable across replays. Distinct from testExitAt in convert_test.go only in
// name — using a local to avoid cross-file coupling.
// -----------------------------------------------------------------------------

var viewTestExitAt = time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)

// 6a — unknown order returns (zero, false). Fresh service, no reservations.
func TestViewReservation_UnknownOrder_ReturnsFalse(t *testing.T) {
	h := newHarness()

	view, ok := h.service.ViewReservation("does-not-exist")
	if ok {
		t.Fatalf("ok = true on unknown id, want false (view=%+v)", view)
	}
	if view != (ReservationView{}) {
		t.Errorf("view on false branch = %+v, want zero value", view)
	}
	if len(h.persist.dirtyCalls) != 0 || len(h.events.emitted) != 0 {
		t.Errorf("ViewReservation touched spies: dirty=%v events=%v",
			h.persist.dirtyCalls, h.events.emitted)
	}
}

// 6b — after a buy-limit reserve, the view reflects limit*qty cash reserved,
// zero units, RemainingQty==OrigQty, Side=Buy, IsMarket=false.
func TestViewReservation_AfterReserveBuyLimit(t *testing.T) {
	h := newHarness()
	buildAccountInLedger(h, account.Cash{Available: 1_000_000}, nil)

	mustReserve(t, h, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID,
		Side:         contract.Buy, Qty: 100, LimitPrice: 50,
	})

	view, ok := h.service.ViewReservation(testOrderID)
	if !ok {
		t.Fatal("ok = false after reserve")
	}
	want := ReservationView{
		OrderID: testOrderID, Side: contract.Buy, IsMarket: false,
		LimitPrice:   50,
		ReservedCash: 5_000, ReservedUnits: 0,
		RemainingQty: 100, OrigQty: 100,
	}
	if view != want {
		t.Errorf("view = %+v, want %+v", view, want)
	}
}

// 6c — a buy-market reserve exposes IsMarket=true and ReservedCash = bestAsk*qty
// + slippage_buffer, which is the invariant Phase 5's market-buy golden pins.
func TestViewReservation_AfterReserveBuyMarket(t *testing.T) {
	h := newHarness()
	buildAccountInLedger(h, account.Cash{Available: 1_000_000}, nil)

	mustReserve(t, h, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID,
		Side:         contract.Buy, Qty: 100, IsMarket: true,
		BestAsk: 52, SlippageBuffer: 200,
	})

	view, ok := h.service.ViewReservation(testOrderID)
	if !ok {
		t.Fatal("ok = false after market reserve")
	}
	want := ReservationView{
		OrderID: testOrderID, Side: contract.Buy, IsMarket: true,
		ReservedCash: 52*100 + 200, ReservedUnits: 0,
		RemainingQty: 100, OrigQty: 100,
	}
	if view != want {
		t.Errorf("view = %+v, want %+v", view, want)
	}
}

// 6d — a sell reserve exposes ReservedUnits (not cash) and Side=Sell.
func TestViewReservation_AfterReserveSell(t *testing.T) {
	h := newHarness()
	buildAccountInLedger(h, account.Cash{Available: 1_000_000}, map[contract.InstrumentID]*account.Position{
		testInstrumentID: {Available: 200, OpenLots: []account.Lot{{EntryPrice: 50, RemainingQty: 200}}},
	})

	mustReserve(t, h, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID,
		Side:         contract.Sell, Qty: 100,
	})

	view, ok := h.service.ViewReservation(testOrderID)
	if !ok {
		t.Fatal("ok = false after sell reserve")
	}
	want := ReservationView{
		OrderID: testOrderID, Side: contract.Sell, IsMarket: false,
		ReservedCash: 0, ReservedUnits: 100,
		RemainingQty: 100, OrigQty: 100,
	}
	if view != want {
		t.Errorf("view = %+v, want %+v", view, want)
	}
}

// 6e — a partial Convert decrements RemainingQty and ReservedCash by the fill
// slice, but OrigQty is preserved. This is the "FilledQty derivation" footing
// Phase 6's open-orders read relies on (filled = OrigQty − RemainingQty).
func TestViewReservation_AfterPartialConvert(t *testing.T) {
	h := newHarness() // stub cost module → actualCost moves cash but no cost debit
	buildAccountInLedger(h, account.Cash{Available: 1_000_000}, nil)

	mustReserve(t, h, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID,
		Side:         contract.Buy, Qty: 100, LimitPrice: 50,
	})

	// Convert 30 of 100 at makerPrice=50 → actualCost = 1500 → ReservedCash
	// shrinks 5000→3500, RemainingQty 100→70, OrigQty unchanged.
	if err := h.service.Convert(testOrderID, 30, 50, viewTestExitAt, "trade-1"); err != nil {
		t.Fatalf("Convert err = %v", err)
	}

	view, ok := h.service.ViewReservation(testOrderID)
	if !ok {
		t.Fatal("ok = false after partial convert")
	}
	want := ReservationView{
		OrderID: testOrderID, Side: contract.Buy, IsMarket: false,
		LimitPrice:   50,
		ReservedCash: 3_500, ReservedUnits: 0,
		RemainingQty: 70, OrigQty: 100,
	}
	if view != want {
		t.Errorf("view = %+v, want %+v", view, want)
	}
}

// 6f — a Convert that fully fills a buy-LIMIT drops ReservedCash to 0 at the
// exact maker price, so the shared tail's three-way delete fires and the
// record is removed → view returns false.
func TestViewReservation_AfterFullConvertLimit_RecordGone(t *testing.T) {
	h := newHarness()
	buildAccountInLedger(h, account.Cash{Available: 1_000_000}, nil)

	mustReserve(t, h, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID,
		Side:         contract.Buy, Qty: 100, LimitPrice: 50,
	})
	if err := h.service.Convert(testOrderID, 100, 50, viewTestExitAt, "trade-1"); err != nil {
		t.Fatalf("Convert err = %v", err)
	}

	view, ok := h.service.ViewReservation(testOrderID)
	if ok {
		t.Errorf("ok = true after full limit convert, want false (view=%+v)", view)
	}
}

// 6g — a market-buy Convert that fully fills at a price with slippage residue
// leaves the record in the book: RemainingQty==0, BrokerageCharged==true, but
// ReservedCash still holds the unused buffer. Convert's three-way delete does
// NOT fire; ReleaseExcess is what cleans it up (test 6h). This is the exact
// intermediate state Phase 5's settleMatch sees between its Convert loop and
// the ReleaseExcess call on a market-buy full-fill.
func TestViewReservation_AfterFullConvertMarketBuyWithResidue(t *testing.T) {
	h := newHarness()
	buildAccountInLedger(h, account.Cash{Available: 10_000_000}, nil)

	mustReserve(t, h, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID,
		Side:         contract.Buy, Qty: 50, IsMarket: true,
		BestAsk: 10_021, SlippageBuffer: 1_000,
	})
	// requiredCash = 10021*50 + 1000 = 502050 reserved.
	// Convert 50 at makerPrice=10021 → actualCost=501050 → residue=1000.
	if err := h.service.Convert(testOrderID, 50, 10_021, viewTestExitAt, "trade-1"); err != nil {
		t.Fatalf("Convert err = %v", err)
	}

	view, ok := h.service.ViewReservation(testOrderID)
	if !ok {
		t.Fatal("ok = false after market-buy full convert — record expected to survive until ReleaseExcess")
	}
	want := ReservationView{
		OrderID: testOrderID, Side: contract.Buy, IsMarket: true,
		ReservedCash: 1_000, ReservedUnits: 0,
		RemainingQty: 0, OrigQty: 50,
	}
	if view != want {
		t.Errorf("view = %+v, want %+v", view, want)
	}
}

// 6h — ReleaseExcess on the 6g state removes the record → view returns false.
func TestViewReservation_AfterReleaseExcess_RecordGone(t *testing.T) {
	h := newHarness()
	buildAccountInLedger(h, account.Cash{Available: 10_000_000}, nil)

	mustReserve(t, h, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID,
		Side:         contract.Buy, Qty: 50, IsMarket: true,
		BestAsk: 10_021, SlippageBuffer: 1_000,
	})
	if err := h.service.Convert(testOrderID, 50, 10_021, viewTestExitAt, "trade-1"); err != nil {
		t.Fatalf("Convert err = %v", err)
	}
	if err := h.service.ReleaseExcess(testOrderID); err != nil {
		t.Fatalf("ReleaseExcess err = %v", err)
	}

	if view, ok := h.service.ViewReservation(testOrderID); ok {
		t.Errorf("ok = true after ReleaseExcess, want false (view=%+v)", view)
	}
}

// 6i — a full Release of a never-filled reservation (releaseQty=0 == release
// all) removes the record → view returns false.
func TestViewReservation_AfterFullRelease_RecordGone(t *testing.T) {
	h := newHarness()
	buildAccountInLedger(h, account.Cash{Available: 1_000_000}, nil)

	mustReserve(t, h, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID,
		Side:         contract.Buy, Qty: 100, LimitPrice: 50,
	})
	if err := h.service.Release(testOrderID, 0); err != nil {
		t.Fatalf("Release err = %v", err)
	}

	if view, ok := h.service.ViewReservation(testOrderID); ok {
		t.Errorf("ok = true after full Release, want false (view=%+v)", view)
	}
}

// 6j — a partial Release decrements RemainingQty and ReservedCash by the
// half-up-rounded perQtyCashShare slice; OrigQty is preserved; record stays.
//
// Numbers come from release.go's perQtyCashShare formula:
//   amount = (ReservedCash*releaseQty + RemainingQty/2) / RemainingQty
//          = (5000*30 + 100/2) / 100
//          = (150000 + 50) / 100
//          = 1500
// So ReservedCash: 5000 → 3500, RemainingQty: 100 → 70, OrigQty: 100 (fixed).
func TestViewReservation_AfterPartialRelease(t *testing.T) {
	h := newHarness()
	buildAccountInLedger(h, account.Cash{Available: 1_000_000}, nil)

	mustReserve(t, h, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID,
		Side:         contract.Buy, Qty: 100, LimitPrice: 50,
	})
	if err := h.service.Release(testOrderID, 30); err != nil {
		t.Fatalf("Release err = %v", err)
	}

	view, ok := h.service.ViewReservation(testOrderID)
	if !ok {
		t.Fatal("ok = false after partial Release — record should survive")
	}
	want := ReservationView{
		OrderID: testOrderID, Side: contract.Buy, IsMarket: false,
		LimitPrice:   50,
		ReservedCash: 3_500, ReservedUnits: 0,
		RemainingQty: 70, OrigQty: 100,
	}
	if view != want {
		t.Errorf("view = %+v, want %+v", view, want)
	}
}

// 6k — a consumer that mutates the returned view cannot affect the book.
// Proves the no-shared-pointer guarantee by pulling the view twice with a
// deliberate mutation in between.
func TestViewReservation_ReturnsValueCopy_NoAliasing(t *testing.T) {
	h := newHarness()
	buildAccountInLedger(h, account.Cash{Available: 1_000_000}, nil)

	mustReserve(t, h, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID,
		Side:         contract.Buy, Qty: 100, LimitPrice: 50,
	})

	first, ok := h.service.ViewReservation(testOrderID)
	if !ok {
		t.Fatal("first view not found")
	}

	// Vandalise every mutable field on the returned copy.
	first.OrderID = "vandalised"
	first.Side = contract.Sell
	first.IsMarket = true
	first.LimitPrice = -999
	first.ReservedCash = -999
	first.ReservedUnits = -999
	first.RemainingQty = -999
	first.OrigQty = -999

	second, ok := h.service.ViewReservation(testOrderID)
	if !ok {
		t.Fatal("second view not found")
	}
	want := ReservationView{
		OrderID: testOrderID, Side: contract.Buy, IsMarket: false,
		LimitPrice:   50,
		ReservedCash: 5_000, ReservedUnits: 0,
		RemainingQty: 100, OrigQty: 100,
	}
	if second != want {
		t.Errorf("book state leaked through view mutation: second = %+v, want %+v", second, want)
	}
}

// 6l — ViewReservation calls contribute zero MarkSnapshotDirty calls and zero
// events. Reserve emits one of each; ten subsequent View calls must not
// append anything to either spy.
func TestViewReservation_IsPureRead_NoSpyCalls(t *testing.T) {
	h := newHarness()
	buildAccountInLedger(h, account.Cash{Available: 1_000_000}, nil)

	mustReserve(t, h, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID,
		Side:         contract.Buy, Qty: 100, LimitPrice: 50,
	})

	dirtyBefore := len(h.persist.dirtyCalls)
	eventsBefore := len(h.events.emitted)
	if dirtyBefore != 1 || eventsBefore != 1 {
		t.Fatalf("expected exactly 1 dirty + 1 event from Reserve, got dirty=%d events=%d",
			dirtyBefore, eventsBefore)
	}

	for callIndex := 0; callIndex < 10; callIndex++ {
		if _, ok := h.service.ViewReservation(testOrderID); !ok {
			t.Fatalf("view not found at call #%d", callIndex)
		}
		if _, ok := h.service.ViewReservation("absent-id"); ok {
			t.Fatalf("view ok=true for absent-id at call #%d", callIndex)
		}
	}

	if got := len(h.persist.dirtyCalls); got != dirtyBefore {
		t.Errorf("ViewReservation produced dirty calls: before=%d after=%d",
			dirtyBefore, got)
	}
	if got := len(h.events.emitted); got != eventsBefore {
		t.Errorf("ViewReservation produced events: before=%d after=%d",
			eventsBefore, got)
	}
}

// 6m — ViewReservation projects LimitPrice across both Side × IsMarket
// combinations: limit orders carry the user's tick, market orders carry
// 0. Phase-6 A11 reads this field directly from the view instead of
// back-calculating from ReservedCash (unreliable on sells + market buys).
func TestViewReservation_LimitPrice_ProjectedForAllTypes(t *testing.T) {
	tests := []struct {
		name           string
		side           contract.Side
		isMarket       bool
		limitPrice     int64
		positions      map[contract.InstrumentID]*account.Position
		bestAsk        int64
		slippageBuffer int64
		wantLimitPrice int64
	}{
		{
			name:           "buy-limit view carries LimitPrice",
			side:           contract.Buy,
			isMarket:       false,
			limitPrice:     50,
			wantLimitPrice: 50,
		},
		{
			name:           "buy-market view carries LimitPrice=0",
			side:           contract.Buy,
			isMarket:       true,
			bestAsk:        52,
			slippageBuffer: 200,
			wantLimitPrice: 0,
		},
		{
			name:       "sell-limit view carries LimitPrice",
			side:       contract.Sell,
			isMarket:   false,
			limitPrice: 75,
			positions: map[contract.InstrumentID]*account.Position{
				testInstrumentID: {Available: 200, OpenLots: []account.Lot{{EntryPrice: 50, RemainingQty: 200}}},
			},
			wantLimitPrice: 75,
		},
		{
			name:     "sell-market view carries LimitPrice=0",
			side:     contract.Sell,
			isMarket: true,
			positions: map[contract.InstrumentID]*account.Position{
				testInstrumentID: {Available: 200, OpenLots: []account.Lot{{EntryPrice: 50, RemainingQty: 200}}},
			},
			wantLimitPrice: 0,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			h := newHarness()
			buildAccountInLedger(h, account.Cash{Available: 1_000_000}, testCase.positions)

			mustReserve(t, h, Order{
				OrderID:        testOrderID,
				AccountID:      testAccountID,
				SessionID:      testSessionID,
				InstrumentID:   testInstrumentID,
				Side:           testCase.side,
				Qty:            10,
				IsMarket:       testCase.isMarket,
				LimitPrice:     testCase.limitPrice,
				BestAsk:        testCase.bestAsk,
				SlippageBuffer: testCase.slippageBuffer,
			})

			view, ok := h.service.ViewReservation(testOrderID)
			if !ok {
				t.Fatal("ViewReservation ok=false after admission")
			}
			if view.LimitPrice != testCase.wantLimitPrice {
				t.Errorf("view.LimitPrice = %d, want %d",
					view.LimitPrice, testCase.wantLimitPrice)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Shared helpers (test-local; not reused outside this file).
// -----------------------------------------------------------------------------

// buildAccountInLedger primes an account into the harness ledger under the
// shared testAccountID. Mirrors convert_test's primeAccount but kept
// local-named to avoid cross-file symbol churn.
func buildAccountInLedger(h *harness, cash account.Cash, positions map[contract.InstrumentID]*account.Position) {
	if positions == nil {
		positions = map[contract.InstrumentID]*account.Position{}
	}
	h.ledger.put(&account.Account{ID: testAccountID, Cash: cash, Positions: positions})
}

// mustReserve runs ReserveIfAvailable and t.Fatal's on any error. Threads the
// shared testParticipant so individual tests read linearly.
func mustReserve(t *testing.T, h *harness, order Order) {
	t.Helper()
	if err := h.service.ReserveIfAvailable(Participant{ID: testParticipant}, order); err != nil {
		t.Fatalf("ReserveIfAvailable err = %v", err)
	}
}
