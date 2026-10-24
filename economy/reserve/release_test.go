package reserve

import (
	"reflect"
	"testing"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/cost"
)

// -----------------------------------------------------------------------------
// GOLDEN — partial-fill-then-cancel (doc §4.1 canonical scenario).
// This is the invariant's headline case: filled part converted, unfilled
// remainder released, no overlap, brokerage fires exactly once (via
// Release since Convert never saw the terminal state).
//
// Setup: Available=1_000_000_000. Reserve buy 100 @ limit 10_000.
// Convert(40, 10_000). Release(0) — full cancel of the unfilled 60.
//
// Fill cost 1 (turnover 400_000): stamp=12, exchange=12, sebi=0, gst=2 → Total=26.
// Order cost at Release (India, EverFilled=true): brokerage=2000, gst=360 → Total=2360.
//
// requiredCash = 10_000*100/100 = 10_000 rupees; actualCost = 10_000*40/100 = 4_000 rupees.
// After the whole sequence:
//   Cash.Available  = 1_000_000_000 − 10_000 (reserve) + 6_000 (release) − 26 − 2360
//                   = 999_993_614
//   Cash.Reserved   = 0
//   Cash.Settled    = 4_000 (unaffected by Release)
//   Positions[ACME] = {Available:40, Reserved:0, OpenLots:[{10000, exitAt, 40}]}
//   book[testOrderID] = nil (record deleted at full release)
//
// Reserved vs debits/credits disjoint by construction:
//   filled 40 units → 4_000 principal debited to Settled + 26 fill cost to Available
//   unfilled 60 units → 6_000 returned to Available (never overlapped)
// -----------------------------------------------------------------------------

func TestRelease_PartialFillThenCancel_BrokerageOnce(t *testing.T) {
	h := newIndiaHarness()
	initialAvailable := int64(1_000_000_000)
	primeAccount(h, account.Cash{Available: initialAvailable}, nil)

	if err := h.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID, Side: contract.Buy, Qty: 100, LimitPrice: 10_000,
	}); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if err := h.service.Convert(testOrderID, 40, 10_000, testExitAt, ""); err != nil {
		t.Fatalf("convert failed: %v", err)
	}

	// Snapshot the record BEFORE Release so we can capture BrokerageCharged flag
	// as it flipped inside Release. After Release, the record is deleted, so we
	// verify the brokerage debit fired via the event log + final Cash.
	preRelease := *h.service.book.get(testOrderID)
	if preRelease.EverFilled != true || preRelease.BrokerageCharged != false {
		t.Fatalf("pre-Release record = %+v, want EverFilled:true BrokerageCharged:false", preRelease)
	}

	if err := h.service.Release(testOrderID, 0); err != nil {
		t.Fatalf("Release err=%v, want nil", err)
	}

	// Final state.
	acct := h.ledger.accounts[testAccountID]
	wantAvailable := initialAvailable - 10_000 + 6_000 - 26 - 2360
	wantCash := account.Cash{Available: wantAvailable, Reserved: 0, Settled: 4_000}
	if acct.Cash != wantCash {
		t.Errorf("Cash = %+v, want %+v", acct.Cash, wantCash)
	}

	position := acct.Positions[testInstrumentID]
	wantOpenLots := []account.Lot{{EntryPrice: 10_000, EntryAt: testExitAt, RemainingQty: 40}}
	if position.Available != 40 || position.Reserved != 0 || !reflect.DeepEqual(position.OpenLots, wantOpenLots) {
		t.Errorf("Position = %+v, want {40, 0, %+v}", *position, wantOpenLots)
	}

	if record := h.service.book.get(testOrderID); record != nil {
		t.Errorf("record retained after full cancel: %+v", *record)
	}

	// Events: reserve + convert(fill) + convert(brokerage from Release) + release = 4.
	if len(h.events.emitted) != 4 {
		t.Fatalf("events = %d, want 4 (reserve + convertFill + convertBrokerage + release)", len(h.events.emitted))
	}
	brokerageEvent, ok := h.events.emitted[2].(ConvertEvent)
	if !ok {
		t.Fatalf("event[2] = %T, want ConvertEvent (brokerage)", h.events.emitted[2])
	}
	wantBrokerage := ConvertEvent{
		OrderID:   testOrderID,
		AccountID: testAccountID,
		Cost:      cost.CostBreakdown{Lines: map[string]int64{"brokerage": 2000, "gst": 360}, Total: 2360},
	}
	if !reflect.DeepEqual(brokerageEvent, wantBrokerage) {
		t.Errorf("brokerage event = %+v, want %+v", brokerageEvent, wantBrokerage)
	}

	releaseEvent, ok := h.events.emitted[3].(ReleaseEvent)
	if !ok {
		t.Fatalf("event[3] = %T, want ReleaseEvent", h.events.emitted[3])
	}
	wantRelease := ReleaseEvent{
		OrderID:   testOrderID,
		AccountID: testAccountID,
		Cash:      6_000, // exactly the unfilled portion (6_000 = 10_000 reserved − 4_000 converted)
		Units:     0,
	}
	if releaseEvent != wantRelease {
		t.Errorf("release event = %+v, want %+v", releaseEvent, wantRelease)
	}
}

// -----------------------------------------------------------------------------
// A partial-fill-then-cancel on the SELL side. Symmetric to the buy golden:
// filled units delivered out via Convert, unfilled units returned to
// PosAvailable via Release, brokerage fires once at Release.
//
// Setup: 200 units seeded as one lot. Reserve sell 100 → PosAvailable=100,
// PosReserved=100. Convert(40 @ 12_000) → 40 out, matched from head lot.
// Release(0) → the remaining 60 reserved units returned.
// -----------------------------------------------------------------------------

func TestRelease_PartialSellFillThenCancel_BrokerageOnce(t *testing.T) {
	h := newIndiaHarness()
	primeAccount(h, account.Cash{Available: 100_000_000}, map[contract.InstrumentID]*account.Position{
		testInstrumentID: {
			Available: 200, Reserved: 0,
			OpenLots: []account.Lot{{EntryPrice: 8_000, EntryAt: testExitAt, RemainingQty: 200}},
		},
	})

	if err := h.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID, Side: contract.Sell, Qty: 100,
	}); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if err := h.service.Convert(testOrderID, 40, 12_000, testExitAt, "trade-partial"); err != nil {
		t.Fatalf("convert failed: %v", err)
	}
	if err := h.service.Release(testOrderID, 0); err != nil {
		t.Fatalf("Release err=%v", err)
	}

	// Post state: 40 units sold and matched from the 200-unit lot → 160 left
	// unsold. Reserved returned in full.
	acct := h.ledger.accounts[testAccountID]
	position := acct.Positions[testInstrumentID]
	if position.Available != 160 || position.Reserved != 0 {
		t.Errorf("Position = {Available:%d, Reserved:%d}, want {160, 0}", position.Available, position.Reserved)
	}
	wantLots := []account.Lot{{EntryPrice: 8_000, EntryAt: testExitAt, RemainingQty: 160}}
	if !reflect.DeepEqual(position.OpenLots, wantLots) {
		t.Errorf("OpenLots = %+v, want %+v", position.OpenLots, wantLots)
	}

	if record := h.service.book.get(testOrderID); record != nil {
		t.Errorf("record retained: %+v", *record)
	}

	// Brokerage debit surfaces as a ConvertEvent inside the Release path
	// (uniform with Convert's terminal brokerage — see release.go doc block).
	brokerageEvent, ok := h.events.emitted[2].(ConvertEvent)
	if !ok {
		t.Fatalf("event[2] = %T, want ConvertEvent (brokerage)", h.events.emitted[2])
	}
	if brokerageEvent.Cost.Total != 2360 {
		t.Errorf("brokerage total = %d, want 2360", brokerageEvent.Cost.Total)
	}

	releaseEvent, ok := h.events.emitted[3].(ReleaseEvent)
	if !ok {
		t.Fatalf("event[3] = %T, want ReleaseEvent", h.events.emitted[3])
	}
	if releaseEvent.Units != 60 || releaseEvent.Cash != 0 {
		t.Errorf("release event = %+v, want Units:60 Cash:0", releaseEvent)
	}
}

// -----------------------------------------------------------------------------
// Unfilled cancel (EverFilled == false) — zero brokerage on both sides.
// -----------------------------------------------------------------------------

func TestRelease_UnfilledBuyFullCancel_ZeroBrokerage(t *testing.T) {
	h := newIndiaHarness()
	initialAvailable := int64(1_000_000_000)
	primeAccount(h, account.Cash{Available: initialAvailable}, nil)

	if err := h.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID, Side: contract.Buy, Qty: 100, LimitPrice: 10_000,
	}); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if err := h.service.Release(testOrderID, 0); err != nil {
		t.Fatalf("Release err=%v", err)
	}

	acct := h.ledger.accounts[testAccountID]
	// Full return: Available restored exactly, no cost debits.
	if acct.Cash != (account.Cash{Available: initialAvailable}) {
		t.Errorf("Cash after unfilled cancel = %+v, want {Available:%d}", acct.Cash, initialAvailable)
	}
	if record := h.service.book.get(testOrderID); record != nil {
		t.Errorf("record retained: %+v", *record)
	}

	// Events: reserve + release. NO brokerage event.
	if len(h.events.emitted) != 2 {
		t.Fatalf("events = %d, want 2 (reserve + release); brokerage must NOT fire", len(h.events.emitted))
	}
	for index, event := range h.events.emitted {
		if convertEvent, ok := event.(ConvertEvent); ok {
			t.Errorf("event[%d] = ConvertEvent (brokerage) — must not fire on unfilled cancel: %+v", index, convertEvent)
		}
	}
}

func TestRelease_UnfilledSellFullCancel_ZeroBrokerage(t *testing.T) {
	h := newIndiaHarness()
	primeAccount(h, account.Cash{Available: 100_000_000}, map[contract.InstrumentID]*account.Position{
		testInstrumentID: {
			Available: 200, Reserved: 0,
			OpenLots: []account.Lot{{EntryPrice: 8_000, EntryAt: testExitAt, RemainingQty: 200}},
		},
	})

	if err := h.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID, Side: contract.Sell, Qty: 100,
	}); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if err := h.service.Release(testOrderID, 0); err != nil {
		t.Fatalf("Release err=%v", err)
	}

	acct := h.ledger.accounts[testAccountID]
	// Cash untouched (sells never touched Available on the reserve path); position restored.
	if acct.Cash != (account.Cash{Available: 100_000_000}) {
		t.Errorf("Cash mutated by unfilled sell cancel: %+v", acct.Cash)
	}
	position := acct.Positions[testInstrumentID]
	if position.Available != 200 || position.Reserved != 0 {
		t.Errorf("Position = {Available:%d, Reserved:%d}, want {200, 0}", position.Available, position.Reserved)
	}
	if len(h.events.emitted) != 2 {
		t.Fatalf("events = %d, want 2 (reserve + release)", len(h.events.emitted))
	}
	for _, event := range h.events.emitted {
		if _, isConvert := event.(ConvertEvent); isConvert {
			t.Errorf("ConvertEvent emitted on unfilled sell cancel — brokerage must not fire")
		}
	}
}

// -----------------------------------------------------------------------------
// Partial qty-decrease modify — the order KEEPS RESTING. Record retained
// with decremented RemainingQty and Reserved*; brokerage does NOT fire
// (order is not terminal). EverFilled is still false on this test so the
// tail's guard skips naturally; separate tests below cover the retained-
// record-then-later-terminal path.
// -----------------------------------------------------------------------------

func TestRelease_PartialQtyDecreaseBuy(t *testing.T) {
	h := newIndiaHarness()
	initialAvailable := int64(1_000_000_000)
	primeAccount(h, account.Cash{Available: initialAvailable}, nil)

	if err := h.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID, Side: contract.Buy, Qty: 100, LimitPrice: 100,
	}); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	// Reserve moved 100*100/100 = 100 rupees to Reserved.

	if err := h.service.Release(testOrderID, 30); err != nil {
		t.Fatalf("Release err=%v", err)
	}

	acct := h.ledger.accounts[testAccountID]
	// perQtyCashShare(100, 30, 100) = 100*30/100 = 30 (exact).
	wantCash := account.Cash{Available: initialAvailable - 100 + 30, Reserved: 70}
	if acct.Cash != wantCash {
		t.Errorf("Cash = %+v, want %+v", acct.Cash, wantCash)
	}

	record := h.service.book.get(testOrderID)
	if record == nil {
		t.Fatal("record deleted on partial release — should be retained (order still resting)")
	}
	if record.RemainingQty != 70 || record.ReservedCash != 70 {
		t.Errorf("record = {Remaining:%d, ReservedCash:%d}, want {70, 70}", record.RemainingQty, record.ReservedCash)
	}
	if record.EverFilled || record.BrokerageCharged {
		t.Errorf("flags mutated on partial release with no prior fill: {Ever:%v, Brok:%v}", record.EverFilled, record.BrokerageCharged)
	}

	// The release event.
	releaseEvent, ok := h.events.emitted[1].(ReleaseEvent)
	if !ok || releaseEvent.Cash != 30 {
		t.Errorf("release event = %+v, want ReleaseEvent with Cash:30", h.events.emitted[1])
	}
}

func TestRelease_PartialQtyDecreaseSell(t *testing.T) {
	h := newIndiaHarness()
	primeAccount(h, account.Cash{Available: 100_000_000}, map[contract.InstrumentID]*account.Position{
		testInstrumentID: {
			Available: 100, Reserved: 0,
			OpenLots: []account.Lot{{EntryPrice: 8_000, EntryAt: testExitAt, RemainingQty: 100}},
		},
	})

	if err := h.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID, Side: contract.Sell, Qty: 100,
	}); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if err := h.service.Release(testOrderID, 30); err != nil {
		t.Fatalf("Release err=%v", err)
	}

	acct := h.ledger.accounts[testAccountID]
	position := acct.Positions[testInstrumentID]
	if position.Available != 30 || position.Reserved != 70 {
		t.Errorf("Position = {Available:%d, Reserved:%d}, want {30, 70}", position.Available, position.Reserved)
	}

	record := h.service.book.get(testOrderID)
	if record == nil {
		t.Fatal("record deleted on partial sell release")
	}
	if record.RemainingQty != 70 || record.ReservedUnits != 70 {
		t.Errorf("record = {Remaining:%d, ReservedUnits:%d}, want {70, 70}", record.RemainingQty, record.ReservedUnits)
	}
}

// -----------------------------------------------------------------------------
// perQtyCashShare — half-up rounding table. Covers exact division, tie
// cases (.5 rounds up), and just-below / just-above tie.
// -----------------------------------------------------------------------------

func TestPerQtyCashShare_HalfUp(t *testing.T) {
	tests := []struct {
		name         string
		reservedCash int64
		remainingQty int64
		releaseQty   int64
		want         int64
	}{
		{"exact division (no rounding)", 1_000, 100, 30, 300},
		{"exact division, releaseQty=1", 100, 10, 1, 10},
		{"tie 0.5 rounds up", 2, 4, 1, 1},        // 0.5 → 1
		{"tie 2.5 rounds up", 10, 4, 1, 3},       // 2.5 → 3
		{"just below tie 2.25 rounds down", 9, 4, 1, 2},
		{"just above tie 2.75 rounds up", 11, 4, 1, 3},
		{"tie 1.5 rounds up", 3, 2, 1, 2},        // 1.5 → 2
		{"large tie 500.5 rounds up", 1_001, 2, 1, 501},
		{"fraction 0.4 rounds down", 4, 10, 1, 0}, // 0.4 → 0
		{"fraction 0.6 rounds up", 6, 10, 1, 1},   // 0.6 → 1
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			record := &ReservationRecord{
				ReservedCash: testCase.reservedCash,
				RemainingQty: testCase.remainingQty,
			}
			if got := perQtyCashShare(record, testCase.releaseQty); got != testCase.want {
				t.Errorf("perQtyCashShare(reserved=%d, remaining=%d, release=%d) = %d, want %d",
					testCase.reservedCash, testCase.remainingQty, testCase.releaseQty, got, testCase.want)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Release forgivingness: releaseQty >= RemainingQty is treated as full
// release (per doc §4.1 pseudocode's else branch). Not an error.
// -----------------------------------------------------------------------------

func TestRelease_ReleaseQtyExceedingRemaining_TreatedAsFull(t *testing.T) {
	h := newIndiaHarness()
	primeAccount(h, account.Cash{Available: 1_000_000}, nil)

	if err := h.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID, Side: contract.Buy, Qty: 10, LimitPrice: 100,
	}); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}

	// Ask to release 999 when only 10 units are remaining.
	if err := h.service.Release(testOrderID, 999); err != nil {
		t.Fatalf("Release err=%v", err)
	}

	acct := h.ledger.accounts[testAccountID]
	if acct.Cash.Reserved != 0 {
		t.Errorf("Reserved = %d after over-sized release, want 0", acct.Cash.Reserved)
	}
	if record := h.service.book.get(testOrderID); record != nil {
		t.Errorf("record retained: %+v", *record)
	}
}

// -----------------------------------------------------------------------------
// Release panics.
// -----------------------------------------------------------------------------

func TestRelease_UnknownOrder_Panics(t *testing.T) {
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected panic on missing record, got none")
		}
		msg, ok := recovered.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T: %v", recovered, recovered)
		}
		want := `release: no reservation record for order "ghost" (invariant broken)`
		if msg != want {
			t.Errorf("panic msg = %q, want %q", msg, want)
		}
	}()
	h := newIndiaHarness()
	_ = h.service.Release("ghost", 0)
}

func TestRelease_NegativeReleaseQty_Panics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on negative releaseQty, got none")
		}
	}()
	h := newIndiaHarness()
	primeAccount(h, account.Cash{Available: 1_000_000}, nil)
	if err := h.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID, Side: contract.Buy, Qty: 10, LimitPrice: 100,
	}); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	_ = h.service.Release(testOrderID, -1)
}

// -----------------------------------------------------------------------------
// GOLDEN — ReleaseExcess on a market buy with residue.
//
// Setup: Available=100_000_000. Reserve market buy 100 with BestAsk=10_000
// and SlippageBuffer=500 → requiredCash=(10_000*100+500)/100=10_005 rupees.
// Convert(100, 9_500) — fills 5% cheaper than the estimate. After Convert:
//
//   actualCost = 9_500*100/100 = 9_500 → moved Reserved→Settled
//   residue    = 10_005 − 9_500 = 505  (buffer + savings still in Reserved)
//   fill cost  (turnover 950_000): stamp=29, exchange=28, sebi=1, gst=5 → 63
//   order cost (terminal, EverFilled=true): 2360   (fires INSIDE Convert since
//                                                   RemainingQty hit 0)
//   Record retained (ReservedCash=505 > 0) for ReleaseExcess to true up.
//
// ReleaseExcess drains the 505 residue back to Available and deletes the record.
// -----------------------------------------------------------------------------

func TestReleaseExcess_MarketBuyResidue(t *testing.T) {
	h := newIndiaHarness()
	primeAccount(h, account.Cash{Available: 100_000_000}, nil)

	if err := h.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID, Side: contract.Buy, Qty: 100,
		IsMarket: true, BestAsk: 10_000, SlippageBuffer: 500,
	}); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if err := h.service.Convert(testOrderID, 100, 9_500, testExitAt, ""); err != nil {
		t.Fatalf("convert failed: %v", err)
	}

	// Sanity: pre-ReleaseExcess state should show the residue and terminal flags.
	pre := *h.service.book.get(testOrderID)
	if pre.ReservedCash != 505 {
		t.Fatalf("pre-ReleaseExcess ReservedCash = %d, want 505", pre.ReservedCash)
	}
	if !pre.BrokerageCharged || pre.RemainingQty != 0 {
		t.Fatalf("pre-ReleaseExcess record = %+v, want BrokerageCharged:true RemainingQty:0", pre)
	}

	if err := h.service.ReleaseExcess(testOrderID); err != nil {
		t.Fatalf("ReleaseExcess err=%v", err)
	}

	acct := h.ledger.accounts[testAccountID]
	// Available = 100_000_000 − 10_005 (reserve) + 505 (residue back) − 63 (fill cost) − 2360 (brokerage)
	wantAvailable := int64(100_000_000 - 10_005 + 505 - 63 - 2360)
	wantCash := account.Cash{Available: wantAvailable, Reserved: 0, Settled: 9_500}
	if acct.Cash != wantCash {
		t.Errorf("Cash = %+v, want %+v", acct.Cash, wantCash)
	}
	if record := h.service.book.get(testOrderID); record != nil {
		t.Errorf("record retained after ReleaseExcess: %+v", *record)
	}

	// Last event is a ReleaseEvent carrying the residue amount.
	last := h.events.emitted[len(h.events.emitted)-1]
	releaseEvent, ok := last.(ReleaseEvent)
	if !ok {
		t.Fatalf("last event = %T, want ReleaseEvent", last)
	}
	if releaseEvent.Cash != 505 || releaseEvent.Units != 0 {
		t.Errorf("release event = %+v, want Cash:505 Units:0", releaseEvent)
	}
}

// A market buy that filled at EXACTLY best_ask with zero slippage buffer
// leaves ReservedCash==0 after the terminal Convert, so Convert's own
// three-way check deletes the record. ReleaseExcess called after that
// finds no record and must return nil (doc §4.2 explicitly permits this).
func TestReleaseExcess_NoRecord_NoOp(t *testing.T) {
	h := newIndiaHarness()
	// No account primed and no record — ReleaseExcess must not touch the ledger.
	if err := h.service.ReleaseExcess("ghost"); err != nil {
		t.Fatalf("ReleaseExcess err=%v, want nil (no-record path)", err)
	}
	// No side effects.
	if len(h.events.emitted) != 0 {
		t.Errorf("events emitted on no-record ReleaseExcess: %+v", h.events.emitted)
	}
	if len(h.persist.dirtyCalls) != 0 {
		t.Errorf("dirty calls on no-record ReleaseExcess: %+v", h.persist.dirtyCalls)
	}
}

// Calling ReleaseExcess before Convert finishes is an engine wiring bug —
// RemainingQty > 0 means the walk is not done. Panic.
func TestReleaseExcess_IncompleteOrder_Panics(t *testing.T) {
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected panic on incomplete order, got none")
		}
		msg, ok := recovered.(string)
		if !ok {
			t.Fatalf("expected string panic, got %T", recovered)
		}
		want := `release_excess: order "` + testOrderID + `" incomplete (RemainingQty=100, want 0)`
		if msg != want {
			t.Errorf("panic msg = %q, want %q", msg, want)
		}
	}()
	h := newIndiaHarness()
	primeAccount(h, account.Cash{Available: 1_000_000}, nil)
	if err := h.service.ReserveIfAvailable(Participant{ID: testParticipant}, Order{
		OrderID: testOrderID, AccountID: testAccountID, SessionID: testSessionID,
		InstrumentID: testInstrumentID, Side: contract.Buy, Qty: 100, LimitPrice: 100,
	}); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	// No Convert — RemainingQty still 100.
	_ = h.service.ReleaseExcess(testOrderID)
}

// A record with RemainingQty==0 but BrokerageCharged==false is impossible
// under normal flow (Convert would have fired brokerage on hitting the
// terminal state). If it happens, Convert missed its terminal check —
// coupling invariant break.
func TestReleaseExcess_BrokerageNotCharged_Panics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on BrokerageCharged=false at terminal, got none")
		}
	}()
	h := newIndiaHarness()
	// Synthesize the invalid state directly in the book (bypass Convert).
	primeAccount(h, account.Cash{Available: 1_000_000, Reserved: 500}, nil)
	h.service.book.put(&ReservationRecord{
		OrderID:          testOrderID,
		AccountID:        testAccountID,
		SessionID:        testSessionID,
		InstrumentID:     testInstrumentID,
		Side:             contract.Buy,
		IsMarket:         true,
		ReservedCash:     500,
		RemainingQty:     0,
		OrigQty:          10,
		EverFilled:       true,
		BrokerageCharged: false, // <-- the invariant break
	})
	_ = h.service.ReleaseExcess(testOrderID)
}
