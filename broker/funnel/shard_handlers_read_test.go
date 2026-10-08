package funnel

import (
	"testing"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/reserve"
)

// Tests for the four read handler bodies in shard_handlers.go. Each
// handler is a thin wrapper over a pure helper in reads.go (covered
// deeply in reads_test.go); these tests exercise the handler-only
// parts:
//
//   1. Present:false when there is no live session on this shard —
//      the HTTP layer maps that to 409.
//   2. Present:true + the delegated payload on a live session.
//   3. For positions, the optional Instrument filter is threaded
//      through to the helper verbatim.
//   4. For the observation handler, all three sub-payloads (balance,
//      positions, open orders) land in a single call — the composite
//      must be atomic on the shard.
//
// Uses the goldenHarness from golden_helpers_test.go: a shard with a
// wired reservation service, an empty sessions/tracks pair, and
// seedAccount / seedTrack / seedReservation helpers.

// -----------------------------------------------------------------------------
// handleReadBalance
// -----------------------------------------------------------------------------

func TestHandleReadBalance_NoLiveSession_PresentFalse(t *testing.T) {
	harness := newGoldenHarness(t)
	// No seedAccount — sessions map is empty.

	got := harness.shard.handleReadBalance(ReadBalance{AccountID: harness.accountID})
	if got.Present {
		t.Fatalf("expected Present:false on no live session, got %+v", got)
	}
	if (got != BalanceSnapshot{Present: false}) {
		t.Fatalf("expected zero-valued payload on Present:false, got %+v", got)
	}
}

func TestHandleReadBalance_LiveSession_CopiesCashBuckets(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 10, Reserved: 20, Settled: 30, Bank: 40}, nil)

	got := harness.shard.handleReadBalance(ReadBalance{AccountID: harness.accountID})
	want := BalanceSnapshot{Present: true, Available: 10, Reserved: 20, Settled: 30, Bank: 40}
	if got != want {
		t.Fatalf("handleReadBalance:\n got  = %+v\n want = %+v", got, want)
	}
}

// -----------------------------------------------------------------------------
// handleReadPositions
// -----------------------------------------------------------------------------

func TestHandleReadPositions_NoLiveSession_PresentFalse(t *testing.T) {
	harness := newGoldenHarness(t)

	got := harness.shard.handleReadPositions(ReadPositions{AccountID: harness.accountID})
	if got.Present || got.Positions != nil {
		t.Fatalf("expected Present:false with nil Positions on no live session, got %+v", got)
	}
}

func TestHandleReadPositions_LiveSession_UnfilteredReturnsAllNonFlatRows(t *testing.T) {
	harness := newGoldenHarness(t)
	other := contract.InstrumentID("WIDGET")
	harness.seedAccount(account.Cash{Available: 1_000_000}, map[contract.InstrumentID]*account.Position{
		goldenInstrument: {Available: 10, OpenLots: []account.Lot{lotAt(100, 10)}},
		other:            {Available: 25, OpenLots: []account.Lot{lotAt(200, 25)}},
	})

	got := harness.shard.handleReadPositions(ReadPositions{AccountID: harness.accountID /* no filter */})
	if !got.Present {
		t.Fatalf("expected Present:true on live session, got %+v", got)
	}
	if len(got.Positions) != 2 {
		t.Fatalf("expected 2 rows (unfiltered), got %d: %+v", len(got.Positions), got.Positions)
	}
}

func TestHandleReadPositions_LiveSession_InstrumentFilterThreaded(t *testing.T) {
	harness := newGoldenHarness(t)
	other := contract.InstrumentID("WIDGET")
	harness.seedAccount(account.Cash{Available: 1_000_000}, map[contract.InstrumentID]*account.Position{
		goldenInstrument: {Available: 10, OpenLots: []account.Lot{lotAt(100, 10)}},
		other:            {Available: 25, OpenLots: []account.Lot{lotAt(200, 25)}},
	})

	got := harness.shard.handleReadPositions(ReadPositions{AccountID: harness.accountID, Instrument: goldenInstrument})
	if !got.Present {
		t.Fatalf("expected Present:true, got %+v", got)
	}
	if len(got.Positions) != 1 || got.Positions[0].Instrument != goldenInstrument {
		t.Fatalf("expected one row for %q, got %+v", goldenInstrument, got.Positions)
	}
}

// -----------------------------------------------------------------------------
// handleReadOpenOrders
// -----------------------------------------------------------------------------

func TestHandleReadOpenOrders_NoLiveSession_PresentFalse(t *testing.T) {
	harness := newGoldenHarness(t)

	got := harness.shard.handleReadOpenOrders(ReadOpenOrders{AccountID: harness.accountID})
	if got.Present || got.Orders != nil {
		t.Fatalf("expected Present:false with nil Orders on no live session, got %+v", got)
	}
}

func TestHandleReadOpenOrders_LiveSession_JoinsTrackAndReservation(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 10_000_000}, nil)
	harness.seedReservation(reserve.Order{
		OrderID: "acct-G:1", AccountID: harness.accountID, SessionID: goldenSessionID,
		InstrumentID: goldenInstrument, Side: contract.Buy, Qty: 10, IsMarket: false, LimitPrice: 100,
	})
	harness.seedTrack("acct-G:1", OrderStatusWorking, false)

	got := harness.shard.handleReadOpenOrders(ReadOpenOrders{AccountID: harness.accountID})
	if !got.Present {
		t.Fatalf("expected Present:true, got %+v", got)
	}
	if len(got.Orders) != 1 {
		t.Fatalf("expected 1 open-order row, got %d: %+v", len(got.Orders), got.Orders)
	}
	order := got.Orders[0]
	if order.OrderID != "acct-G:1" || order.Side != contract.Buy || order.LimitPrice != 100 || order.OrigQty != 10 {
		t.Fatalf("join dropped reservation fields: %+v", order)
	}
	if order.Status != OrderStatusWorking || order.Instrument != goldenInstrument {
		t.Fatalf("join dropped track fields: %+v", order)
	}
}

// -----------------------------------------------------------------------------
// handleReadObservation
// -----------------------------------------------------------------------------

func TestHandleReadObservation_NoLiveSession_PresentFalse(t *testing.T) {
	harness := newGoldenHarness(t)

	got := harness.shard.handleReadObservation(ReadObservation{AccountID: harness.accountID})
	if got.Present {
		t.Fatalf("expected Present:false on no live session, got %+v", got)
	}
	// Zero sub-payloads on the no-session branch.
	if got.Balance.Present || got.Positions != nil || got.OpenOrders != nil {
		t.Fatalf("expected zero sub-payloads on Present:false, got %+v", got)
	}
}

// All three sub-payloads must fill in a single call. The composite is
// deliberately not three separate facade calls — this test guards
// against a regression that silently drops a leg.
func TestHandleReadObservation_LiveSession_AllThreeLegsFilled(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 10_000_000, Reserved: 1_000, Settled: 500, Bank: 50_000},
		map[contract.InstrumentID]*account.Position{
			goldenInstrument: {Available: 10, OpenLots: []account.Lot{lotAt(100, 10)}},
		})
	harness.seedReservation(reserve.Order{
		OrderID: "acct-G:1", AccountID: harness.accountID, SessionID: goldenSessionID,
		InstrumentID: goldenInstrument, Side: contract.Buy, Qty: 5, IsMarket: false, LimitPrice: 100,
	})
	harness.seedTrack("acct-G:1", OrderStatusWorking, false)

	got := harness.shard.handleReadObservation(ReadObservation{AccountID: harness.accountID})
	if !got.Present {
		t.Fatalf("expected Present:true, got %+v", got)
	}
	// Balance leg — note: seedReservation above moved 500 (qty=5 × price=100)
	// from Available into Reserved, so the live snapshot reflects post-reserve.
	if !got.Balance.Present {
		t.Fatalf("Balance leg missing Present:true")
	}
	if got.Balance.Available != 10_000_000-500 || got.Balance.Reserved != 1_000+500 || got.Balance.Bank != 50_000 {
		t.Fatalf("Balance leg values dropped: %+v", got.Balance)
	}
	// Positions leg (readPositions is called with "", so all held instruments).
	if len(got.Positions) != 1 || got.Positions[0].Instrument != goldenInstrument {
		t.Fatalf("Positions leg wrong rows: %+v", got.Positions)
	}
	// Open-orders leg.
	if len(got.OpenOrders) != 1 || got.OpenOrders[0].OrderID != "acct-G:1" {
		t.Fatalf("OpenOrders leg wrong rows: %+v", got.OpenOrders)
	}
}
