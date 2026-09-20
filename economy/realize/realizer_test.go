package realize

import (
	"testing"
	"time"

	"simulator/economy/account"
	"simulator/economy/cost"
)

// spyPersistence records every seam call for assertion. Phase 1's in-memory
// stand-in for the future Redis buffer / Postgres drainer.
type spyPersistence struct {
	enqueued    []ClosedTradeRow
	dirtyCalls  []dirtyCall
}

type dirtyCall struct {
	SessionID SessionID
	AccountID account.AccountID
}

func (spy *spyPersistence) EnqueueClosedTrade(closed ClosedTradeRow) {
	spy.enqueued = append(spy.enqueued, closed)
}

func (spy *spyPersistence) MarkSnapshotDirty(sessionID SessionID, accountID account.AccountID) {
	spy.dirtyCalls = append(spy.dirtyCalls, dirtyCall{SessionID: sessionID, AccountID: accountID})
}

// spyLedger is an in-memory AccountLedger for tests.
type spyLedger struct {
	accounts map[account.AccountID]*account.Account
}

func newSpyLedger() *spyLedger {
	return &spyLedger{accounts: map[account.AccountID]*account.Account{}}
}

func (spy *spyLedger) Get(accountID account.AccountID) *account.Account {
	return spy.accounts[accountID]
}

func (spy *spyLedger) put(a *account.Account) {
	spy.accounts[a.ID] = a
}

// buildSellFill constructs a canonical single-lot sell RealizationInput —
// used across the Stage-3 goldens to keep the test setup terse.
func buildSellFill(
	accountID account.AccountID,
	instrumentID string,
	entryPrice int64, entryAt time.Time,
	fillQty int64, makerPrice int64, exitAt time.Time,
	costLines map[string]int64, costTotal int64,
	positionNowFlat bool,
) RealizationInput {
	return RealizationInput{
		AccountID:    accountID,
		SessionID:    "sess-1",
		InstrumentID: instrumentID,
		FillQty:      fillQty,
		MakerPrice:   makerPrice,
		MatchedLots:  []account.LotMatch{{EntryPrice: entryPrice, Qty: fillQty, EntryAt: entryAt}},
		Cost:         cost.CostBreakdown{Lines: costLines, Total: costTotal},
		ExitAt:       exitAt,
		Direction:    "long",
		PositionNowFlat: positionNowFlat,
	}
}

// G13 — canonical (PersistOnClose=true): two partial sells accumulate,
// flush on flat, EnqueueClosedTrade fires exactly once with the correct
// aggregate. Accumulator deleted after flush. MarkSnapshotDirty never
// called (realize does not touch that method).
func TestRealizer_G13_CanonicalGateFires(t *testing.T) {
	ledger := newSpyLedger()
	persist := &spyPersistence{}
	realizer := NewRealizer(ledger, persist)

	ledger.put(&account.Account{ID: "acct-1", PersistOnClose: true})

	entryTime := time.Date(2026, 1, 15, 9, 30, 0, 0, time.UTC)
	fillATime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	fillBTime := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)

	realizer.OnSellFill(buildSellFill(
		"acct-1", "ACME", 100, entryTime,
		4, 110, fillATime,
		map[string]int64{"stt": 2, "gst": 1}, 3,
		false,
	))
	if len(persist.enqueued) != 0 {
		t.Fatalf("mid-round-trip: want 0 enqueued, got %d", len(persist.enqueued))
	}

	realizer.OnSellFill(buildSellFill(
		"acct-1", "ACME", 100, entryTime,
		6, 120, fillBTime,
		map[string]int64{"stt": 3, "gst": 2}, 5,
		true,
	))

	if len(persist.enqueued) != 1 {
		t.Fatalf("post-flat: want 1 enqueued, got %d", len(persist.enqueued))
	}
	got := persist.enqueued[0]
	if got.Summary.Quantity != 10 {
		t.Errorf("Quantity: want 10, got %d", got.Summary.Quantity)
	}
	if got.Summary.GrossPnl != 160 {
		t.Errorf("GrossPnl: want 160, got %d", got.Summary.GrossPnl)
	}
	if got.Summary.NetPnl != 152 {
		t.Errorf("NetPnl: want 152, got %d", got.Summary.NetPnl)
	}
	if got.Summary.ExitReason != ExitVoluntary {
		t.Errorf("ExitReason: want voluntary, got %s", got.Summary.ExitReason)
	}
	if got.Cost.Lines["stt"] != 5 || got.Cost.Lines["gst"] != 3 {
		t.Errorf("Cost.Lines wrong: %v", got.Cost.Lines)
	}
	if got.Summary.TradeID == "" || got.Cost.TradeID != got.Summary.TradeID {
		t.Errorf("TradeID: want non-empty and matched summary/cost; got summary=%q cost=%q",
			got.Summary.TradeID, got.Cost.TradeID)
	}
	if _, exists := realizer.book[accKey{"acct-1", "ACME"}]; exists {
		t.Errorf("accumulator not deleted after flush")
	}
	if len(persist.dirtyCalls) != 0 {
		t.Errorf("realize must NOT call MarkSnapshotDirty; got %d calls", len(persist.dirtyCalls))
	}
}

// G14 — on-demand (PersistOnClose=false): identical fill sequence to G13,
// no enqueue, but the accumulator is still born, folded, and deleted
// (P&L is computed and available for display; only the write-back differs).
func TestRealizer_G14_OnDemandGateSuppresses(t *testing.T) {
	ledger := newSpyLedger()
	persist := &spyPersistence{}
	realizer := NewRealizer(ledger, persist)

	ledger.put(&account.Account{ID: "acct-2", PersistOnClose: false})

	entryTime := time.Date(2026, 1, 15, 9, 30, 0, 0, time.UTC)
	fillATime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	fillBTime := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)

	realizer.OnSellFill(buildSellFill(
		"acct-2", "ACME", 100, entryTime,
		4, 110, fillATime,
		map[string]int64{"stt": 2, "gst": 1}, 3,
		false,
	))
	// After first partial fill: accumulator must exist (proving it was born
	// and folded even under on-demand).
	if _, exists := realizer.book[accKey{"acct-2", "ACME"}]; !exists {
		t.Fatalf("on-demand: accumulator must be born + folded, not skipped")
	}

	realizer.OnSellFill(buildSellFill(
		"acct-2", "ACME", 100, entryTime,
		6, 120, fillBTime,
		map[string]int64{"stt": 3, "gst": 2}, 5,
		true,
	))

	if len(persist.enqueued) != 0 {
		t.Fatalf("on-demand: want 0 enqueued (gate suppresses), got %d", len(persist.enqueued))
	}
	if _, exists := realizer.book[accKey{"acct-2", "ACME"}]; exists {
		t.Errorf("on-demand: accumulator still deleted after flush")
	}
}

// G15 — PersistOnClose is locked at accumulator birth: flipping the
// account's flag mid-round-trip must NOT change the gate decision on flush.
func TestRealizer_G15_PersistOnCloseLockedAtBirth(t *testing.T) {
	ledger := newSpyLedger()
	persist := &spyPersistence{}
	realizer := NewRealizer(ledger, persist)

	acct := &account.Account{ID: "acct-3", PersistOnClose: true}
	ledger.put(acct)

	entryTime := time.Date(2026, 1, 15, 9, 30, 0, 0, time.UTC)
	fillATime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	fillBTime := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)

	realizer.OnSellFill(buildSellFill(
		"acct-3", "ACME", 100, entryTime,
		4, 110, fillATime,
		map[string]int64{"stt": 2}, 2,
		false,
	))

	// Flip mid-round-trip; the accumulator has already captured true.
	acct.PersistOnClose = false

	realizer.OnSellFill(buildSellFill(
		"acct-3", "ACME", 100, entryTime,
		6, 120, fillBTime,
		map[string]int64{"stt": 3}, 3,
		true,
	))

	if len(persist.enqueued) != 1 {
		t.Errorf("birth-time PersistOnClose must win; want 1 enqueue, got %d", len(persist.enqueued))
	}
}

// G16 — two round-trips on the same account, different instruments: two
// enqueues, two distinct TradeIDs, no cross-contamination in the book.
func TestRealizer_G16_TwoInstrumentsSameAccount(t *testing.T) {
	ledger := newSpyLedger()
	persist := &spyPersistence{}
	realizer := NewRealizer(ledger, persist)

	ledger.put(&account.Account{ID: "acct-4", PersistOnClose: true})

	entry := time.Date(2026, 1, 15, 9, 0, 0, 0, time.UTC)
	exit := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

	realizer.OnSellFill(buildSellFill(
		"acct-4", "X", 100, entry,
		10, 110, exit,
		map[string]int64{"stt": 5}, 5,
		true,
	))
	realizer.OnSellFill(buildSellFill(
		"acct-4", "Y", 200, entry,
		5, 210, exit,
		map[string]int64{"stt": 3}, 3,
		true,
	))

	if len(persist.enqueued) != 2 {
		t.Fatalf("want 2 enqueues, got %d", len(persist.enqueued))
	}
	trade1, trade2 := persist.enqueued[0], persist.enqueued[1]
	if trade1.Summary.TradeID == trade2.Summary.TradeID {
		t.Errorf("TradeIDs must differ across independent round-trips; got %q twice", trade1.Summary.TradeID)
	}
	if trade1.Summary.InstrumentID != "X" || trade2.Summary.InstrumentID != "Y" {
		t.Errorf("instruments swapped: got %q / %q", trade1.Summary.InstrumentID, trade2.Summary.InstrumentID)
	}
	if len(realizer.book) != 0 {
		t.Errorf("both accumulators must be deleted; book has %d entries", len(realizer.book))
	}
}

// G17 — reopen after flat: second round-trip on the SAME instrument uses a
// fresh accumulator; state from round-trip #1 does not leak into #2.
func TestRealizer_G17_ReopenAfterFlat(t *testing.T) {
	ledger := newSpyLedger()
	persist := &spyPersistence{}
	realizer := NewRealizer(ledger, persist)

	ledger.put(&account.Account{ID: "acct-5", PersistOnClose: true})

	entry1 := time.Date(2026, 1, 15, 9, 0, 0, 0, time.UTC)
	exit1 := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	entry2 := time.Date(2026, 1, 15, 11, 0, 0, 0, time.UTC)
	exit2 := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)

	realizer.OnSellFill(buildSellFill(
		"acct-5", "X", 100, entry1,
		10, 110, exit1,
		map[string]int64{"stt": 5}, 5,
		true,
	))
	realizer.OnSellFill(buildSellFill(
		"acct-5", "X", 200, entry2,
		4, 190, exit2,
		map[string]int64{"stt": 2}, 2,
		true,
	))

	if len(persist.enqueued) != 2 {
		t.Fatalf("want 2 enqueues, got %d", len(persist.enqueued))
	}
	trade1, trade2 := persist.enqueued[0], persist.enqueued[1]
	if trade1.Summary.TradeID == trade2.Summary.TradeID {
		t.Errorf("reopened round-trip must get a NEW TradeID; got %q twice", trade1.Summary.TradeID)
	}
	if trade1.Summary.NetPnl != 100-5 || trade2.Summary.NetPnl != -40-2 {
		t.Errorf("no state leak expected; got net #1=%d net #2=%d",
			trade1.Summary.NetPnl, trade2.Summary.NetPnl)
	}
	if !trade1.Summary.EntryAt.Equal(entry1) || !trade2.Summary.EntryAt.Equal(entry2) {
		t.Errorf("EntryAt leaked: #1=%v #2=%v", trade1.Summary.EntryAt, trade2.Summary.EntryAt)
	}
}

// G18 — OnSellFill for an unknown account panics (broken coupling
// invariant, not a business error).
func TestRealizer_G18_PanicOnMissingAccount(t *testing.T) {
	ledger := newSpyLedger()
	persist := &spyPersistence{}
	realizer := NewRealizer(ledger, persist)

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("expected panic on missing account, got none")
		}
	}()

	realizer.OnSellFill(buildSellFill(
		"unknown-acct", "X", 100, time.Now(),
		1, 110, time.Now(),
		nil, 0,
		true,
	))
}

// UpstreamTradeID — when the caller supplies a non-empty TradeID on the
// fill, the enqueued row uses THAT id (generator not invoked).
func TestRealizer_UpstreamTradeIDPreferred(t *testing.T) {
	ledger := newSpyLedger()
	persist := &spyPersistence{}
	realizer := NewRealizer(ledger, persist)

	// Sentinel generator: if it ever runs, tests will notice.
	realizer.nextTradeID = func() TradeID { return "GENERATOR-CALLED" }

	ledger.put(&account.Account{ID: "acct-6", PersistOnClose: true})

	input := buildSellFill(
		"acct-6", "X", 100, time.Date(2026, 1, 15, 9, 0, 0, 0, time.UTC),
		1, 110, time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
		nil, 0,
		true,
	)
	input.TradeID = "upstream-trade-42"
	realizer.OnSellFill(input)

	if len(persist.enqueued) != 1 {
		t.Fatalf("want 1 enqueue, got %d", len(persist.enqueued))
	}
	got := persist.enqueued[0]
	if got.Summary.TradeID != "upstream-trade-42" {
		t.Errorf("upstream TradeID must win; want %q, got %q", "upstream-trade-42", got.Summary.TradeID)
	}
	if got.Cost.TradeID != "upstream-trade-42" {
		t.Errorf("Cost.TradeID: want %q, got %q", "upstream-trade-42", got.Cost.TradeID)
	}
}
