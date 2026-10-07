package reserve

import (
	"reflect"
	"testing"
	"time"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/cost"
	"simulator/economy/realize"
)

// -----------------------------------------------------------------------------
// Stage-3 test fixtures.
//
// indiaRatesFixture pins the India rulebook per memory project_india_rate_locks
// (STT 0.025%, exchange 0.00297% NSE, SEBI 0.0001%, stamp 0.003%, GST 18%,
// brokerage 2000 paise, autosquare_fee 5000 paise). Every rate is expressed
// as a Rate{Num, Den} — applyRate does half-up rounding.
//
// testExitAt is a fixed UTC timestamp for every fill so replays are
// bit-identical and golden assertions can pin the lot's EntryAt.
// -----------------------------------------------------------------------------

var indiaRatesFixture = cost.IndiaRates{
	STT:           cost.Rate{Num: 25, Den: 100_000},     // 0.025%
	Exchange:      cost.Rate{Num: 297, Den: 10_000_000}, // 0.00297%
	SEBI:          cost.Rate{Num: 1, Den: 1_000_000},    // 0.0001%
	Stamp:         cost.Rate{Num: 3, Den: 100_000},      // 0.003%
	GST:           cost.Rate{Num: 18, Den: 100},         // 18%
	Brokerage:     2000,
	AutosquareFee: 5000,
}

var testExitAt = time.Date(2026, 9, 22, 14, 30, 0, 0, time.UTC)

func newIndiaHarness() *harness {
	return newHarnessWithCost(cost.NewIndiaCostModule(indiaRatesFixture))
}

// primeAccount pre-seeds an account into a harness's ledger. Convenience so
// each test reads linearly.
func primeAccount(h *harness, cash account.Cash, positions map[contract.InstrumentID]*account.Position) *account.Account {
	if positions == nil {
		positions = map[contract.InstrumentID]*account.Position{}
	}
	acct := &account.Account{ID: testAccountID, Cash: cash, Positions: positions}
	h.ledger.put(acct)
	return acct
}

// -----------------------------------------------------------------------------
// GOLDEN — buy partial. Hand-computed India cost lines pinned exactly.
//
// Setup: reserve buy 100 units @ limit 10_000 paise (Rs.100/unit) →
// requiredCash = 1_000_000 paise. Then Convert(fillQty=40, makerPrice=10_000).
//
// Expected fill cost (India, buy side, turnover = 400_000 paise):
//   stamp    = round_half_up(400_000 × 0.003%)   = (1_200_000 + 50_000)/100_000 = 12
//   exchange = round_half_up(400_000 × 0.00297%) = (118_800_000 + 5_000_000)/10_000_000 = 12
//   sebi     = round_half_up(400_000 × 0.0001%)  = (400_000 + 500_000)/1_000_000 = 0 (0.4 < 0.5)
//   gstBase  = 12 + 0 = 12
//   gst      = round_half_up(12 × 18%)           = (216 + 50)/100 = 2
//   Total    = 12 + 12 + 0 + 2                    = 26
//
// Ordering (§3.3 LOCKED) is proved by the final state: Settled == actualCost
// (principal moved first), THEN Available shows the additional cost debit
// on top of the reserve-time deduction. RemainingQty=60 (not 0) → no
// brokerage debit at this stage; record retained.
// -----------------------------------------------------------------------------

func TestConvertBuy_Partial_CostOrdering(t *testing.T) {
	h := newIndiaHarness()
	initialAvailable := int64(1_000_000_000)
	primeAccount(h, account.Cash{Available: initialAvailable}, nil)

	if err := h.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
		OrderID:      testOrderID,
		AccountID:    testAccountID,
		SessionID:    testSessionID,
		InstrumentID: testInstrumentID,
		Side:         contract.Buy,
		Qty:          100,
		LimitPrice:   10_000,
	}); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}

	if err := h.service.Convert(testOrderID, 40, 10_000, testExitAt, ""); err != nil {
		t.Fatalf("Convert returned err=%v, want nil", err)
	}

	acct := h.ledger.accounts[testAccountID]
	wantCash := account.Cash{
		Available: initialAvailable - 1_000_000 - 26, // reserve moved 1_000_000 out, then fill cost 26 debited
		Reserved:  600_000,
		Settled:   400_000,
	}
	if acct.Cash != wantCash {
		t.Errorf("Cash = %+v, want %+v", acct.Cash, wantCash)
	}

	position := acct.Positions[testInstrumentID]
	if position == nil {
		t.Fatal("Positions[ACME] nil — expected lazy-init on first buy Convert")
	}
	wantPosition := account.Position{
		Available: 40,
		Reserved:  0,
		OpenLots:  []account.Lot{{EntryPrice: 10_000, EntryAt: testExitAt, RemainingQty: 40}},
	}
	if !reflect.DeepEqual(*position, wantPosition) {
		t.Errorf("Position = %+v, want %+v", *position, wantPosition)
	}

	record := h.service.book.get(testOrderID)
	if record == nil {
		t.Fatal("record deleted after partial — should be retained")
	}
	wantRecord := ReservationRecord{
		OrderID:          testOrderID,
		AccountID:        testAccountID,
		SessionID:        testSessionID,
		InstrumentID:     testInstrumentID,
		Side:             contract.Buy,
		IsMarket:         false,
		LimitPrice:       10_000,
		ReservedCash:     600_000,
		RemainingQty:     60,
		OrigQty:          100,
		EverFilled:       true,
		BrokerageCharged: false,
	}
	if *record != wantRecord {
		t.Errorf("record = %+v, want %+v", *record, wantRecord)
	}

	// Events: 1 from Reserve + 1 from Convert = 2 total; no brokerage event
	// because RemainingQty != 0.
	if len(h.events.emitted) != 2 {
		t.Fatalf("events = %d, want 2 (reserve + convert)", len(h.events.emitted))
	}
	gotConvert, ok := h.events.emitted[1].(ConvertEvent)
	if !ok {
		t.Fatalf("second event type = %T, want ConvertEvent", h.events.emitted[1])
	}
	wantConvert := ConvertEvent{
		OrderID:       testOrderID,
		AccountID:     testAccountID,
		CashDebited:   400_000,
		UnitsCredited: 40,
		Cost: cost.CostBreakdown{
			Lines: map[string]int64{"stamp": 12, "exchange": 12, "sebi": 0, "gst": 2},
			Total: 26,
		},
	}
	if !reflect.DeepEqual(gotConvert, wantConvert) {
		t.Errorf("convert event = %+v, want %+v", gotConvert, wantConvert)
	}

	// Dirty calls: 1 from Reserve + 1 from Convert = 2.
	if len(h.persist.dirtyCalls) != 2 {
		t.Errorf("dirty calls = %d, want 2", len(h.persist.dirtyCalls))
	}
}

// -----------------------------------------------------------------------------
// GOLDEN — full-fill via two Converts. Brokerage fires exactly ONCE at
// the terminal Convert (§3.4 / §5 LOCKED wiring).
//
// Setup: reserve buy 100 @ limit 10_000. Convert(60, 10_000), then Convert(40, 10_000).
//
// Fill cost 1 (turnover 600_000):
//   stamp=18, exchange=18, sebi=1, gst=round((18+1)*18%)=round(3.42)=3 → Total=40
// Fill cost 2 (turnover 400_000): Total=26 (as above)
// Order cost (terminal, EverFilled=true, TotalQty=100):
//   brokerage=2000, gst=round(2000*18%)=(36000+50)/100=360 → Total=2360
//
// Assertions: after fill 1 EverFilled=true BrokerageCharged=false; after
// fill 2 record DELETED (all three zero: RemainingQty, ReservedCash,
// ReservedUnits); brokerage debit fires exactly once; total DebitCash
// invocations = 2 fill costs + 1 order cost = 3.
// -----------------------------------------------------------------------------

func TestConvertBuy_FullFill_MultipleConverts_BrokerageOnce(t *testing.T) {
	h := newIndiaHarness()
	initialAvailable := int64(1_000_000_000)
	primeAccount(h, account.Cash{Available: initialAvailable}, nil)

	if err := h.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID, Side: contract.Buy, Qty: 100, LimitPrice: 10_000,
	}); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}

	// First fill (60 units).
	if err := h.service.Convert(testOrderID, 60, 10_000, testExitAt, ""); err != nil {
		t.Fatalf("Convert #1 err=%v", err)
	}
	afterFirst := h.service.book.get(testOrderID)
	if afterFirst == nil {
		t.Fatal("record dropped after first partial fill")
	}
	if !afterFirst.EverFilled {
		t.Errorf("EverFilled = false after first fill, want true")
	}
	if afterFirst.BrokerageCharged {
		t.Errorf("BrokerageCharged = true after first partial fill — must not fire until terminal")
	}
	if afterFirst.RemainingQty != 40 || afterFirst.ReservedCash != 400_000 {
		t.Errorf("record after fill 1 = {Remaining:%d, Reserved:%d}, want {40, 400000}",
			afterFirst.RemainingQty, afterFirst.ReservedCash)
	}

	// Second fill (40 units, terminal).
	if err := h.service.Convert(testOrderID, 40, 10_000, testExitAt, ""); err != nil {
		t.Fatalf("Convert #2 err=%v", err)
	}
	if record := h.service.book.get(testOrderID); record != nil {
		t.Errorf("record retained after terminal fill: %+v", *record)
	}

	acct := h.ledger.accounts[testAccountID]
	// Cash: initial - 1_000_000 (reserve) - 40 (fill cost 1) - 26 (fill cost 2) - 2360 (brokerage)
	wantAvailable := initialAvailable - 1_000_000 - 40 - 26 - 2360
	wantCash := account.Cash{Available: wantAvailable, Reserved: 0, Settled: 1_000_000}
	if acct.Cash != wantCash {
		t.Errorf("Cash = %+v, want %+v", acct.Cash, wantCash)
	}

	// Events: reserve + convert (fill 1) + convert (fill 2) + convert (brokerage) = 4.
	if len(h.events.emitted) != 4 {
		t.Fatalf("events = %d, want 4 (reserve + fill1 + fill2 + brokerage)", len(h.events.emitted))
	}

	// The 4th event is the brokerage ConvertEvent: money-side fields all zero,
	// Cost carrying the order-level breakdown.
	brokerageEvent, ok := h.events.emitted[3].(ConvertEvent)
	if !ok {
		t.Fatalf("4th event type = %T, want ConvertEvent", h.events.emitted[3])
	}
	wantBrokerage := ConvertEvent{
		OrderID:   testOrderID,
		AccountID: testAccountID,
		Cost: cost.CostBreakdown{
			Lines: map[string]int64{"brokerage": 2000, "gst": 360},
			Total: 2360,
		},
	}
	if !reflect.DeepEqual(brokerageEvent, wantBrokerage) {
		t.Errorf("brokerage event = %+v, want %+v", brokerageEvent, wantBrokerage)
	}
}

// -----------------------------------------------------------------------------
// GOLDEN — sell partial. Verifies the RealizationInput handed to 5.4 is
// shaped correctly: MatchedLots FIFO-consumed from head, PositionNowFlat
// FALSE (units remain), Direction "long" (prototype), ExitAt + TradeID
// pass through from the fill event, Cost = the pre-computed fill breakdown.
//
// Setup: 200 units in two lots [{8000, t1, 100}, {10000, t2, 100}].
// Reserve sell 100. Convert(fillQty=50, makerPrice=12_000, exitAt, "trade-1").
//
// Fill cost (sell, turnover 600_000):
//   stt=150, exchange=18, sebi=1, gst=3 → Total=172.
//
// RemovePosition(50) consumes 50 from head lot → matched = [{8000, t1, 50}];
// head lot retains 50 units → OpenLots = [{8000, t1, 50}, {10000, t2, 100}];
// Position after = {Available:100, Reserved:50} → PositionFlat = false.
// -----------------------------------------------------------------------------

func TestConvertSell_Partial_HandsOffToRealize(t *testing.T) {
	h := newIndiaHarness()
	lotOneAt := time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)
	lotTwoAt := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	primeAccount(h, account.Cash{Available: 100_000_000}, map[contract.InstrumentID]*account.Position{
		testInstrumentID: {
			Available: 200, Reserved: 0,
			OpenLots: []account.Lot{
				{EntryPrice: 8_000, EntryAt: lotOneAt, RemainingQty: 100},
				{EntryPrice: 10_000, EntryAt: lotTwoAt, RemainingQty: 100},
			},
		},
	})

	if err := h.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID, Side: contract.Sell, Qty: 100,
	}); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}

	if err := h.service.Convert(testOrderID, 50, 12_000, testExitAt, "trade-1"); err != nil {
		t.Fatalf("Convert err=%v", err)
	}

	acct := h.ledger.accounts[testAccountID]
	// Cash: 100_000_000 + 600_000 (proceeds) - 172 (fill cost).
	wantCash := account.Cash{Available: 100_000_000 + 600_000 - 172}
	if acct.Cash != wantCash {
		t.Errorf("Cash = %+v, want %+v", acct.Cash, wantCash)
	}

	position := acct.Positions[testInstrumentID]
	wantOpenLots := []account.Lot{
		{EntryPrice: 8_000, EntryAt: lotOneAt, RemainingQty: 50},
		{EntryPrice: 10_000, EntryAt: lotTwoAt, RemainingQty: 100},
	}
	if position.Available != 100 || position.Reserved != 50 {
		t.Errorf("Position = {Available:%d, Reserved:%d}, want {100, 50}", position.Available, position.Reserved)
	}
	if !reflect.DeepEqual(position.OpenLots, wantOpenLots) {
		t.Errorf("OpenLots = %+v, want %+v", position.OpenLots, wantOpenLots)
	}

	record := h.service.book.get(testOrderID)
	if record == nil {
		t.Fatal("record deleted after partial sell — should be retained")
	}
	if record.EverFilled != true || record.BrokerageCharged != false {
		t.Errorf("flags = {Ever:%v, Brok:%v}, want {true, false}", record.EverFilled, record.BrokerageCharged)
	}
	if record.ReservedUnits != 50 || record.RemainingQty != 50 {
		t.Errorf("record = {ReservedUnits:%d, Remaining:%d}, want {50, 50}", record.ReservedUnits, record.RemainingQty)
	}

	// The realizer spy captured exactly one OnSellFill call with the
	// expected shape (this IS the stage-3 handoff assertion).
	if len(h.realiz.fills) != 1 {
		t.Fatalf("realizer OnSellFill calls = %d, want 1", len(h.realiz.fills))
	}
	wantInput := realize.RealizationInput{
		AccountID:       testAccountID,
		SessionID:       testSessionID,
		InstrumentID:    testInstrumentID,
		FillQty:         50,
		MakerPrice:      12_000,
		MatchedLots:     []account.LotMatch{{EntryPrice: 8_000, EntryAt: lotOneAt, Qty: 50}},
		Cost:            cost.CostBreakdown{Lines: map[string]int64{"stt": 150, "exchange": 18, "sebi": 1, "gst": 3}, Total: 172},
		PositionNowFlat: false,
		TradeID:         "trade-1",
		Direction:       "long",
		ExitAt:          testExitAt,
	}
	if !reflect.DeepEqual(h.realiz.fills[0], wantInput) {
		t.Errorf("RealizationInput = %+v, want %+v", h.realiz.fills[0], wantInput)
	}
}

// -----------------------------------------------------------------------------
// GOLDEN — sell terminal. Position drains to flat; brokerage fires once;
// record deleted; PositionNowFlat == TRUE.
//
// Setup: 100 units in one lot [{10000, t1, 100}]. Reserve sell 100.
// Convert(100, 12_000, exitAt, "trade-X").
//
// Fill cost (sell, turnover 1_200_000): stt=300, exchange=36, sebi=1, gst=7,
// Total=344. Order cost = 2360. Position empties fully → PositionFlat=true.
// -----------------------------------------------------------------------------

func TestConvertSell_Terminal_PositionFlat(t *testing.T) {
	h := newIndiaHarness()
	lotOneAt := time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)
	primeAccount(h, account.Cash{Available: 100_000_000}, map[contract.InstrumentID]*account.Position{
		testInstrumentID: {
			Available: 100, Reserved: 0,
			OpenLots: []account.Lot{{EntryPrice: 10_000, EntryAt: lotOneAt, RemainingQty: 100}},
		},
	})

	if err := h.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID, Side: contract.Sell, Qty: 100,
	}); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}

	if err := h.service.Convert(testOrderID, 100, 12_000, testExitAt, "trade-X"); err != nil {
		t.Fatalf("Convert err=%v", err)
	}

	acct := h.ledger.accounts[testAccountID]
	wantAvailable := int64(100_000_000 + 1_200_000 - 344 - 2360)
	wantCash := account.Cash{Available: wantAvailable}
	if acct.Cash != wantCash {
		t.Errorf("Cash = %+v, want %+v", acct.Cash, wantCash)
	}

	position := acct.Positions[testInstrumentID]
	if position.Available != 0 || position.Reserved != 0 || len(position.OpenLots) != 0 {
		t.Errorf("Position after terminal sell = {Available:%d, Reserved:%d, OpenLots:%d}, want all zero",
			position.Available, position.Reserved, len(position.OpenLots))
	}
	if !acct.PositionFlat(testInstrumentID) {
		t.Errorf("PositionFlat = false after terminal sell, want true")
	}

	// Record deleted at terminal (RemainingQty=0, ReservedUnits=0, ReservedCash=0).
	if record := h.service.book.get(testOrderID); record != nil {
		t.Errorf("record retained after terminal fill: %+v", *record)
	}

	// The realizer received PositionNowFlat=true. This is the accumulator-
	// flush trigger — realize's own tests cover what it does with it.
	if len(h.realiz.fills) != 1 {
		t.Fatalf("OnSellFill calls = %d, want 1", len(h.realiz.fills))
	}
	if !h.realiz.fills[0].PositionNowFlat {
		t.Errorf("PositionNowFlat on RealizationInput = false, want true")
	}
	if h.realiz.fills[0].Direction != "long" {
		t.Errorf("Direction = %q, want %q", h.realiz.fills[0].Direction, "long")
	}
	if h.realiz.fills[0].TradeID != "trade-X" {
		t.Errorf("TradeID = %q, want %q", h.realiz.fills[0].TradeID, "trade-X")
	}
	if !h.realiz.fills[0].ExitAt.Equal(testExitAt) {
		t.Errorf("ExitAt = %v, want %v", h.realiz.fills[0].ExitAt, testExitAt)
	}

	// Events: reserve + convert(sell fill) + convert(brokerage) = 3.
	if len(h.events.emitted) != 3 {
		t.Fatalf("events = %d, want 3", len(h.events.emitted))
	}
	brokerageEvent, ok := h.events.emitted[2].(ConvertEvent)
	if !ok {
		t.Fatalf("3rd event = %T, want ConvertEvent", h.events.emitted[2])
	}
	wantBrokerage := ConvertEvent{
		OrderID:   testOrderID,
		AccountID: testAccountID,
		Cost:      cost.CostBreakdown{Lines: map[string]int64{"brokerage": 2000, "gst": 360}, Total: 2360},
	}
	if !reflect.DeepEqual(brokerageEvent, wantBrokerage) {
		t.Errorf("brokerage event = %+v, want %+v", brokerageEvent, wantBrokerage)
	}
}

// -----------------------------------------------------------------------------
// Market-buy residue: RemainingQty reaches 0 but ReservedCash > 0 (unused
// slippage buffer + savings from cheaper fills). The record MUST be
// retained here — ReleaseExcess (stage 4) is what trues it up. Brokerage
// still fires at this Convert (RemainingQty == 0 is the terminal trigger).
// -----------------------------------------------------------------------------

func TestConvertBuy_MarketResidue_RecordRetained(t *testing.T) {
	h := newIndiaHarness()
	primeAccount(h, account.Cash{Available: 100_000_000}, nil)

	if err := h.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID, Side: contract.Buy, Qty: 100,
		IsMarket: true, BestAsk: 10_000, SlippageBuffer: 500,
	}); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}

	// Fill at 9_500 (cheaper than BestAsk 10_000 → savings + buffer stay in Reserved).
	if err := h.service.Convert(testOrderID, 100, 9_500, testExitAt, ""); err != nil {
		t.Fatalf("Convert err=%v", err)
	}

	record := h.service.book.get(testOrderID)
	if record == nil {
		t.Fatal("record deleted despite ReservedCash > 0 — ReleaseExcess would have nothing to true up")
	}
	// requiredCash was 10_000*100 + 500 = 1_000_500; actualCost 9_500*100 = 950_000; residue = 50_500.
	if record.ReservedCash != 50_500 {
		t.Errorf("residue ReservedCash = %d, want 50_500", record.ReservedCash)
	}
	if record.RemainingQty != 0 {
		t.Errorf("RemainingQty = %d, want 0", record.RemainingQty)
	}
	if !record.BrokerageCharged {
		t.Errorf("BrokerageCharged = false at terminal Convert — brokerage must fire once at RemainingQty==0")
	}
}

// -----------------------------------------------------------------------------
// Lazy Position init on the first buy Convert for an instrument the
// account has never held. Reserve inserts the Position{} into
// account.Positions when nil (per stage-3 design decision (a)).
// -----------------------------------------------------------------------------

func TestConvertBuy_FirstBuy_LazyInitsPosition(t *testing.T) {
	h := newIndiaHarness()
	acct := primeAccount(h, account.Cash{Available: 100_000_000}, nil)
	if len(acct.Positions) != 0 {
		t.Fatalf("test seed dirty: Positions len = %d, want 0", len(acct.Positions))
	}

	if err := h.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID, Side: contract.Buy, Qty: 50, LimitPrice: 100,
	}); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}

	if err := h.service.Convert(testOrderID, 50, 100, testExitAt, ""); err != nil {
		t.Fatalf("Convert err=%v", err)
	}

	position := acct.Positions[testInstrumentID]
	if position == nil {
		t.Fatal("Position not lazy-initialised on first buy Convert")
	}
	if position.Available != 50 || position.Reserved != 0 {
		t.Errorf("Position = {Available:%d, Reserved:%d}, want {50, 0}", position.Available, position.Reserved)
	}
	if len(position.OpenLots) != 1 {
		t.Fatalf("OpenLots len = %d, want 1", len(position.OpenLots))
	}
}

// -----------------------------------------------------------------------------
// Panics: missing record; missing account; unknown Side.
// -----------------------------------------------------------------------------

func TestConvert_UnknownOrder_Panics(t *testing.T) {
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected panic on missing record, got none")
		}
		msg, ok := recovered.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", recovered, recovered)
		}
		want := `convert: no reservation record for order "ghost" (invariant broken)`
		if msg != want {
			t.Errorf("panic msg = %q, want %q", msg, want)
		}
	}()
	h := newIndiaHarness()
	_ = h.service.Convert("ghost", 1, 100, testExitAt, "")
}

func TestConvert_UnknownAccount_Panics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on missing account, got none")
		}
	}()
	h := newIndiaHarness()
	// Inject a record whose AccountID has NOT been put in the ledger.
	h.service.book.put(&ReservationRecord{
		OrderID:      testOrderID,
		AccountID:    "orphan-account",
		SessionID:    testSessionID,
		InstrumentID: testInstrumentID,
		Side:         contract.Buy,
		ReservedCash: 1_000,
		RemainingQty: 10,
		OrigQty:      10,
	})
	_ = h.service.Convert(testOrderID, 1, 100, testExitAt, "")
}

func TestConvert_UnknownSide_Panics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on unknown side, got none")
		}
	}()
	h := newIndiaHarness()
	primeAccount(h, account.Cash{Available: 1_000_000}, nil)
	h.service.book.put(&ReservationRecord{
		OrderID:      testOrderID,
		AccountID:    testAccountID,
		SessionID:    testSessionID,
		InstrumentID: testInstrumentID,
		Side:         contract.Side(42),
		ReservedCash: 100,
		RemainingQty: 1,
		OrigQty:      1,
	})
	_ = h.service.Convert(testOrderID, 1, 100, testExitAt, "")
}

// -----------------------------------------------------------------------------
// Determinism replay — same reserve→convert sequence on two fresh
// services yields identical Cash / Positions / book / spy call log.
// Guards against any hidden clock or randomness sneaking into Convert.
// -----------------------------------------------------------------------------

func TestConvert_DeterminismReplay(t *testing.T) {
	run := func() (*harness, *account.Account) {
		h := newIndiaHarness()
		acct := primeAccount(h, account.Cash{Available: 1_000_000_000}, map[contract.InstrumentID]*account.Position{
			testInstrumentID: {
				Available: 100, Reserved: 0,
				OpenLots: []account.Lot{{EntryPrice: 5_000, EntryAt: testExitAt, RemainingQty: 100}},
			},
		})

		// Buy 50 @ limit 8000, fully filled in two Converts.
		if err := h.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
			OrderID: "buy-1", AccountID: testAccountID, SessionID: testSessionID,
			InstrumentID: testInstrumentID, Side: contract.Buy, Qty: 50, LimitPrice: 8_000,
		}); err != nil {
			t.Fatalf("buy-1 reserve failed: %v", err)
		}
		if err := h.service.Convert("buy-1", 30, 8_000, testExitAt, "tr-A"); err != nil {
			t.Fatalf("buy-1 convert #1: %v", err)
		}
		if err := h.service.Convert("buy-1", 20, 8_000, testExitAt, "tr-A"); err != nil {
			t.Fatalf("buy-1 convert #2: %v", err)
		}

		// Sell 40 @ 9000, terminal.
		if err := h.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
			OrderID: "sell-1", AccountID: testAccountID, SessionID: testSessionID,
			InstrumentID: testInstrumentID, Side: contract.Sell, Qty: 40,
		}); err != nil {
			t.Fatalf("sell-1 reserve failed: %v", err)
		}
		if err := h.service.Convert("sell-1", 40, 9_000, testExitAt, "tr-B"); err != nil {
			t.Fatalf("sell-1 convert: %v", err)
		}
		return h, acct
	}

	harnessOne, acctOne := run()
	harnessTwo, acctTwo := run()

	if acctOne.Cash != acctTwo.Cash {
		t.Errorf("Cash diverged: r1=%+v r2=%+v", acctOne.Cash, acctTwo.Cash)
	}
	if !reflect.DeepEqual(acctOne.Positions, acctTwo.Positions) {
		t.Errorf("Positions diverged: r1=%+v r2=%+v", acctOne.Positions, acctTwo.Positions)
	}
	if !reflect.DeepEqual(harnessOne.persist.dirtyCalls, harnessTwo.persist.dirtyCalls) {
		t.Errorf("dirty call log diverged")
	}
	if !reflect.DeepEqual(harnessOne.events.emitted, harnessTwo.events.emitted) {
		t.Errorf("events diverged")
	}
	if !reflect.DeepEqual(harnessOne.realiz.fills, harnessTwo.realiz.fills) {
		t.Errorf("realizer fills diverged")
	}
	// Both runs should have emptied the book at their respective terminals.
	if record := harnessOne.service.book.get("buy-1"); record != nil {
		t.Errorf("book retained buy-1 after full fill: %+v", *record)
	}
	if record := harnessOne.service.book.get("sell-1"); record != nil {
		t.Errorf("book retained sell-1 after terminal: %+v", *record)
	}
}
