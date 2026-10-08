package funnel

import (
	"testing"
	"time"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/realize"
	"simulator/economy/reserve"
)

// Tests for the three pure read helpers in reads.go — readBalance,
// readPositions, readOpenOrders. The handler wrappers that stamp
// Present live one layer up in shard_handlers.go and are tested in
// shard_handlers_read_test.go; these tests exercise the helpers
// directly against a seeded shard harness.

func TestReadBalance_CopiesAllFourBuckets(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 1_000_000, Reserved: 250_000, Settled: 125_000, Bank: 7_500_000}, nil)
	session := harness.shard.sessions[harness.accountID]

	got := readBalance(session)
	want := BalanceSnapshot{
		Present:   true,
		Available: 1_000_000,
		Reserved:  250_000,
		Settled:   125_000,
		Bank:      7_500_000,
	}
	if got != want {
		t.Fatalf("readBalance = %+v, want %+v", got, want)
	}
}

// -----------------------------------------------------------------------------
// readPositions
// -----------------------------------------------------------------------------

// lotAt builds a single-lot entry at the given price/qty for position seeding.
// Fixed EntryAt — the helper never consults it and we don't want test-local
// time drift to show up in WeightedAvgEntry math.
func lotAt(entryPrice, qty int64) account.Lot {
	return account.Lot{EntryPrice: entryPrice, EntryAt: time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC), RemainingQty: qty}
}

// seedPositionsAndSession is a thin layer over seedAccount — it builds the
// Positions map, then seedAccount installs both the account AND the
// ownedSession (binding points at goldenExchangeID, same as every other
// golden). Returns the session so tests can hand it straight to readPositions.
func seedPositionsAndSession(t *testing.T, harness *goldenHarness, positions map[contract.InstrumentID]*account.Position) *ownedSession {
	t.Helper()
	harness.seedAccount(account.Cash{Available: 1_000_000}, positions)
	return harness.shard.sessions[harness.accountID]
}

func TestReadPositions_FlatPositionIsSkipped(t *testing.T) {
	harness := newGoldenHarness(t)
	session := seedPositionsAndSession(t, harness, map[contract.InstrumentID]*account.Position{
		goldenInstrument: {Available: 0, Reserved: 0, OpenLots: nil},
	})

	views := readPositions(harness.shard, session, "")
	if len(views) != 0 {
		t.Fatalf("flat position should be skipped, got %d rows: %+v", len(views), views)
	}
}

func TestReadPositions_InstrumentFilter(t *testing.T) {
	harness := newGoldenHarness(t)
	other := contract.InstrumentID("WIDGET")
	session := seedPositionsAndSession(t, harness, map[contract.InstrumentID]*account.Position{
		goldenInstrument: {Available: 10, Reserved: 0, OpenLots: []account.Lot{lotAt(100, 10)}},
		other:            {Available: 25, Reserved: 0, OpenLots: []account.Lot{lotAt(200, 25)}},
	})

	cases := []struct {
		name              string
		filter            InstrumentID
		wantInstruments   []InstrumentID
		wantFlatOnMissing bool
	}{
		{"all", "", []InstrumentID{goldenInstrument, other}, false},
		{"only ACME", goldenInstrument, []InstrumentID{goldenInstrument}, false},
		{"only WIDGET", other, []InstrumentID{other}, false},
		{"unheld instrument → empty slice", InstrumentID("NOT_HELD"), nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			views := readPositions(harness.shard, session, tc.filter)
			if len(views) != len(tc.wantInstruments) {
				t.Fatalf("got %d rows, want %d: %+v", len(views), len(tc.wantInstruments), views)
			}
			got := make(map[InstrumentID]bool, len(views))
			for _, view := range views {
				got[view.Instrument] = true
			}
			for _, want := range tc.wantInstruments {
				if !got[want] {
					t.Errorf("missing row for %q (got %+v)", want, views)
				}
			}
			if tc.wantFlatOnMissing && views == nil {
				t.Fatalf("views slice should be non-nil (empty, not nil) so HTTP shaper serializes []")
			}
		})
	}
}

func TestReadPositions_MarkDegradesOnCacheMiss(t *testing.T) {
	harness := newGoldenHarness(t)
	session := seedPositionsAndSession(t, harness, map[contract.InstrumentID]*account.Position{
		goldenInstrument: {Available: 10, Reserved: 5, OpenLots: []account.Lot{lotAt(100, 10)}},
	})
	// Harness ships with nopMarketCache{} — every LastPrice misses by design.

	views := readPositions(harness.shard, session, "")
	if len(views) != 1 {
		t.Fatalf("want 1 row, got %d: %+v", len(views), views)
	}
	view := views[0]
	wantAvg := int64(100)
	wantView := PositionView{
		Instrument:       goldenInstrument,
		Qty:              10,
		ReservedQty:      5,
		WeightedAvgEntry: wantAvg,
		MarkAvailable:    false,
		LTP:              0,
		GrossUnrealized:  0,
	}
	if view != wantView {
		t.Fatalf("miss branch:\n got  = %+v\n want = %+v", view, wantView)
	}
}

func TestReadPositions_MarkOnCacheHit(t *testing.T) {
	harness := newGoldenHarness(t)
	session := seedPositionsAndSession(t, harness, map[contract.InstrumentID]*account.Position{
		goldenInstrument: {Available: 10, Reserved: 0, OpenLots: []account.Lot{lotAt(100, 10)}},
	})
	// Swap in a stub that answers LastPrice for the golden (exchange, instrument)
	// pair and misses on BestAsk — this test exercises only the LastPrice branch.
	harness.shard.mktCache = &lastPriceStub{exchange: goldenExchangeID, instrument: goldenInstrument, lastPrice: 115}

	views := readPositions(harness.shard, session, "")
	if len(views) != 1 {
		t.Fatalf("want 1 row, got %d", len(views))
	}
	view := views[0]
	gross, _ := realize.ComputeUnrealizedMark([]account.Lot{lotAt(100, 10)}, 115, 0)
	wantView := PositionView{
		Instrument:       goldenInstrument,
		Qty:              10,
		ReservedQty:      0,
		WeightedAvgEntry: 100,
		MarkAvailable:    true,
		LTP:              115,
		GrossUnrealized:  gross,
	}
	if view != wantView {
		t.Fatalf("hit branch:\n got  = %+v\n want = %+v", view, wantView)
	}
}

// A cache keyed on both exchange and instrument must still miss when
// the session's exchange doesn't match, even if the instrument does.
// Guards against a regression that drops the exchange arg from the
// lookup and silently returns a price from the wrong market.
func TestReadPositions_WrongExchangeMisses(t *testing.T) {
	harness := newGoldenHarness(t)
	session := seedPositionsAndSession(t, harness, map[contract.InstrumentID]*account.Position{
		goldenInstrument: {Available: 10, Reserved: 0, OpenLots: []account.Lot{lotAt(100, 10)}},
	})
	harness.shard.mktCache = &lastPriceStub{exchange: "ANOTHER-X", instrument: goldenInstrument, lastPrice: 999}

	views := readPositions(harness.shard, session, "")
	if len(views) != 1 || views[0].MarkAvailable {
		t.Fatalf("expected miss on wrong exchange, got %+v", views)
	}
}

// -----------------------------------------------------------------------------
// readOpenOrders
// -----------------------------------------------------------------------------

func TestReadOpenOrders_FiltersByAccount(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 10_000_000}, nil)
	// This account has one working order.
	harness.seedReservation(reserve.Order{
		OrderID: "acct-G:1", AccountID: harness.accountID, SessionID: goldenSessionID,
		InstrumentID: goldenInstrument, Side: contract.Buy, Qty: 10, IsMarket: false, LimitPrice: 100,
	})
	harness.seedTrack("acct-G:1", OrderStatusWorking, false)
	// A foreign account on the SAME shard has one working order; filter must drop it.
	harness.shard.tracks["acct-FOREIGN:1"] = &OrderTrack{
		OrderID:    "acct-FOREIGN:1",
		AccountID:  AccountID("acct-FOREIGN"),
		Instrument: goldenInstrument,
		Status:     OrderStatusWorking,
	}

	orders := readOpenOrders(harness.shard, harness.accountID)
	if len(orders) != 1 {
		t.Fatalf("want 1 own-account order, got %d: %+v", len(orders), orders)
	}
	if orders[0].OrderID != "acct-G:1" {
		t.Fatalf("got foreign order through filter: %+v", orders[0])
	}
}

// Covers all four Side × IsMarket combinations:
//   - LimitPrice must be 0 exactly when IsMarket is true;
//   - buy reservations hold cash (ReservedCash>0, ReservedUnits==0);
//   - sell reservations hold units (ReservedUnits>0, ReservedCash==0).
// Guards against the join pulling the wrong field for a given combo.
func TestReadOpenOrders_SideAndMarketCrossCover(t *testing.T) {
	cases := []struct {
		name          string
		orderID       OrderID
		side          contract.Side
		isMarket      bool
		qty           int64
		limitPrice    int64
		bestAsk       int64
		seedPosQty    int64
		wantLimit     int64
		wantHasCash   bool
		wantHasUnits  bool
	}{
		{"buy-limit", "acct-G:1", contract.Buy, false, 10, 100, 0, 0, 100, true, false},
		{"buy-market", "acct-G:2", contract.Buy, true, 10, 0, 120, 0, 0, true, false},
		{"sell-limit", "acct-G:3", contract.Sell, false, 10, 100, 0, 10, 100, false, true},
		{"sell-market", "acct-G:4", contract.Sell, true, 10, 0, 0, 10, 0, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			harness := newGoldenHarness(t)
			positions := map[contract.InstrumentID]*account.Position{}
			if tc.seedPosQty > 0 {
				positions[goldenInstrument] = &account.Position{Available: tc.seedPosQty, OpenLots: []account.Lot{lotAt(100, tc.seedPosQty)}}
			}
			harness.seedAccount(account.Cash{Available: 10_000_000}, positions)
			harness.seedReservation(reserve.Order{
				OrderID: tc.orderID, AccountID: harness.accountID, SessionID: goldenSessionID,
				InstrumentID: goldenInstrument, Side: tc.side, Qty: tc.qty,
				IsMarket: tc.isMarket, LimitPrice: tc.limitPrice,
				BestAsk: tc.bestAsk,
			})
			harness.seedTrack(tc.orderID, OrderStatusWorking, tc.side == contract.Buy && tc.isMarket)

			orders := readOpenOrders(harness.shard, harness.accountID)
			if len(orders) != 1 {
				t.Fatalf("want 1 row, got %d: %+v", len(orders), orders)
			}
			view := orders[0]
			if view.OrderID != tc.orderID {
				t.Errorf("OrderID = %q, want %q", view.OrderID, tc.orderID)
			}
			if view.Side != tc.side {
				t.Errorf("Side = %v, want %v", view.Side, tc.side)
			}
			if view.IsMarket != tc.isMarket {
				t.Errorf("IsMarket = %v, want %v", view.IsMarket, tc.isMarket)
			}
			if view.LimitPrice != tc.wantLimit {
				t.Errorf("LimitPrice = %d, want %d", view.LimitPrice, tc.wantLimit)
			}
			if view.OrigQty != tc.qty || view.RemainingQty != tc.qty {
				t.Errorf("Orig/Remaining = (%d,%d), want both %d", view.OrigQty, view.RemainingQty, tc.qty)
			}
			if tc.wantHasCash && view.ReservedCash == 0 {
				t.Errorf("buy reservation should carry ReservedCash > 0 (view=%+v)", view)
			}
			if !tc.wantHasCash && view.ReservedCash != 0 {
				t.Errorf("non-buy reservation should have ReservedCash == 0, got %d", view.ReservedCash)
			}
			if tc.wantHasUnits && view.ReservedUnits == 0 {
				t.Errorf("sell reservation should carry ReservedUnits > 0 (view=%+v)", view)
			}
			if !tc.wantHasUnits && view.ReservedUnits != 0 {
				t.Errorf("non-sell reservation should have ReservedUnits == 0, got %d", view.ReservedUnits)
			}
			if view.Status != OrderStatusWorking {
				t.Errorf("Status = %v, want Working", view.Status)
			}
			if view.CancelInFlight {
				t.Errorf("CancelInFlight should default to false (no cancel punched)")
			}
		})
	}
}

// A track survives but its reservation record has been removed — an
// unusual state (settlement should drop both together), but the helper
// must still ship a row with the track-side fields set and the money /
// qty / side / price fields left at zero rather than silently dropping
// the order from the response.
func TestReadOpenOrders_TrackWithoutReservation_PartialView(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 1_000_000}, nil)
	// No seedReservation — only the track.
	harness.seedTrack("acct-G:1", OrderStatusPartiallyFilled, false)
	harness.shard.tracks["acct-G:1"].CancelInFlight = true

	orders := readOpenOrders(harness.shard, harness.accountID)
	if len(orders) != 1 {
		t.Fatalf("want 1 row (track-only), got %d", len(orders))
	}
	view := orders[0]
	wantView := OpenOrderView{
		OrderID:        "acct-G:1",
		Instrument:     goldenInstrument,
		Status:         OrderStatusPartiallyFilled,
		CancelInFlight: true,
		// money / qty / side / price all zero: no reservation to join against.
	}
	if view != wantView {
		t.Fatalf("partial-view mismatch:\n got  = %+v\n want = %+v", view, wantView)
	}
}

// Test-local MarketPriceCache stub that returns a LastPrice hit. The
// shared harness stub answers BestAsk and misses on LastPrice; keeping
// a separate stub keeps each test's branch isolated.

type lastPriceStub struct {
	exchange   ExchangeID
	instrument contract.InstrumentID
	lastPrice  contract.Ticks
}

func (stub *lastPriceStub) BestAsk(_ ExchangeID, _ contract.InstrumentID) (contract.Ticks, bool) {
	return 0, false
}

func (stub *lastPriceStub) LastPrice(exchange ExchangeID, instrument contract.InstrumentID) (contract.Ticks, bool) {
	if exchange != stub.exchange || instrument != stub.instrument {
		return 0, false
	}
	return stub.lastPrice, true
}
