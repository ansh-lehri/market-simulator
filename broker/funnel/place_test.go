package funnel

import (
	"errors"
	"testing"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/reserve"
)

// -----------------------------------------------------------------------------
// place_test.go — Phase-5 Point-1 handler coverage.
//
// The STAGE-2 golden (place_market_buy_golden_test.go) pins the market-buy
// happy path end-to-end with the "reserved == MaxSpend" invariant.
// This file covers everything that golden does NOT: validateSpec reject
// paths, no-session, cold-mktCache, reserve rejects, sealed-instrument-set,
// limit-buy happy path, sell happy path, pre-punch-failure invariant, and
// the small helpers (reserveReason, tifFor).
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// validateSpec — table driven, Limit × Market cross-coverage
// (per [[feedback_stage_tests_cross_cover_order_types]]).
// -----------------------------------------------------------------------------

func TestValidateSpec_Table(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 1_000_000}, nil)
	liveSession := harness.shard.sessions[harness.accountID]

	cases := []struct {
		name       string
		order      OrderSpec
		wantReason RejectReason
	}{
		{
			name: "happy limit buy",
			order: OrderSpec{
				Instrument: goldenInstrument, Side: contract.Buy, Type: contract.Limit,
				TIF: contract.GTC, Qty: 10, LimitPrice: 100,
			},
			wantReason: ReasonOK,
		},
		{
			name: "happy market buy",
			order: OrderSpec{
				Instrument: goldenInstrument, Side: contract.Buy, Type: contract.Market,
				TIF: contract.IOC, Qty: 10,
			},
			wantReason: ReasonOK,
		},
		{
			name: "happy limit sell",
			order: OrderSpec{
				Instrument: goldenInstrument, Side: contract.Sell, Type: contract.Limit,
				TIF: contract.GTC, Qty: 10, LimitPrice: 100,
			},
			wantReason: ReasonOK,
		},
		{
			name: "happy market sell",
			order: OrderSpec{
				Instrument: goldenInstrument, Side: contract.Sell, Type: contract.Market,
				TIF: contract.IOC, Qty: 10,
			},
			wantReason: ReasonOK,
		},
		{
			name:       "qty zero on limit",
			order:      OrderSpec{Instrument: goldenInstrument, Side: contract.Buy, Type: contract.Limit, Qty: 0, LimitPrice: 100},
			wantReason: ReasonBadQty,
		},
		{
			name:       "qty negative on market",
			order:      OrderSpec{Instrument: goldenInstrument, Side: contract.Buy, Type: contract.Market, Qty: -1},
			wantReason: ReasonBadQty,
		},
		{
			name:       "bad side on limit",
			order:      OrderSpec{Instrument: goldenInstrument, Side: contract.Side(42), Type: contract.Limit, Qty: 10, LimitPrice: 100},
			wantReason: ReasonBadSide,
		},
		{
			name:       "bad side on market",
			order:      OrderSpec{Instrument: goldenInstrument, Side: contract.Side(99), Type: contract.Market, Qty: 10},
			wantReason: ReasonBadSide,
		},
		{
			name:       "limit without price",
			order:      OrderSpec{Instrument: goldenInstrument, Side: contract.Buy, Type: contract.Limit, Qty: 10, LimitPrice: 0},
			wantReason: ReasonBadPrice,
		},
		{
			name:       "limit with negative price",
			order:      OrderSpec{Instrument: goldenInstrument, Side: contract.Sell, Type: contract.Limit, Qty: 10, LimitPrice: -5},
			wantReason: ReasonBadPrice,
		},
		{
			name:       "bad type",
			order:      OrderSpec{Instrument: goldenInstrument, Side: contract.Buy, Type: contract.OrdType(42), Qty: 10},
			wantReason: ReasonBadType,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := harness.shard.validateSpec(liveSession, tc.order)
			if got != tc.wantReason {
				t.Errorf("validateSpec = %v, want %v", got, tc.wantReason)
			}
		})
	}
}

// validateSpec hits the instrument-set guard FIRST: a sealed set that
// excludes the instrument returns ReasonUnknownInstrument regardless of
// how well-formed the rest of the spec is.
func TestValidateSpec_SealedInstruments_RejectsUnknownInstrument(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 1_000_000}, nil)
	liveSession := harness.shard.sessions[harness.accountID]
	liveSession.instruments = sealedInstrumentSet("OTHER") // ACME not allowed

	wellFormed := OrderSpec{
		Instrument: goldenInstrument, Side: contract.Buy, Type: contract.Limit,
		Qty: 10, LimitPrice: 100,
	}
	got := harness.shard.validateSpec(liveSession, wellFormed)
	if got != ReasonUnknownInstrument {
		t.Errorf("validateSpec = %v, want ReasonUnknownInstrument", got)
	}
}

// -----------------------------------------------------------------------------
// handlePlaceOrder reject paths.
//
// Common P5-D6 invariant: ANY pre-punch failure punches nothing, builds
// no OrderTrack, and (except for reserve-after-admit) does not call the
// reservation service.
// -----------------------------------------------------------------------------

// No-session: sessions[AccountID] miss. ReasonNoSession, nothing else.
func TestHandlePlaceOrder_NoSession_RejectsNoSession(t *testing.T) {
	harness := newGoldenHarness(t)
	// Intentionally NO seedAccount — sessions map stays empty.

	result := harness.shard.handlePlaceOrder(PlaceOrder{
		AccountID: harness.accountID,
		Order: OrderSpec{
			Instrument: goldenInstrument, Side: contract.Buy, Type: contract.Limit,
			Qty: 10, LimitPrice: 100,
		},
		ReplyTo: make(chan PlaceResult, 1),
	})

	if result.Status != PlaceRejected || result.Reason != ReasonNoSession {
		t.Errorf("result = %+v, want {PlaceRejected, ReasonNoSession}", result)
	}
	if result.OrderID != "" {
		t.Errorf("OrderID = %q on reject, want empty", result.OrderID)
	}
	assertNoTrackNoPunch(t, harness)
	// Seq must not have bumped — nextOrderID was never called.
	if harness.shard.seq != 0 {
		t.Errorf("seq = %d, want 0 (no-session rejected before nextOrderID)", harness.shard.seq)
	}
}

// Bad spec routes to validateSpec reasons. One case per branch is enough —
// validateSpec itself is table-tested above. Also pins P5-D6: a bad spec
// never calls nextOrderID or ReserveIfAvailable.
func TestHandlePlaceOrder_BadSpec_RoutesToValidateSpecReason(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 1_000_000}, nil)

	result := harness.shard.handlePlaceOrder(PlaceOrder{
		AccountID: harness.accountID,
		Order: OrderSpec{
			Instrument: goldenInstrument, Side: contract.Buy, Type: contract.Limit,
			Qty: 10, LimitPrice: 0, // bad: limit without price
		},
		ReplyTo: make(chan PlaceResult, 1),
	})

	if result.Status != PlaceRejected || result.Reason != ReasonBadPrice {
		t.Errorf("result = %+v, want {PlaceRejected, ReasonBadPrice}", result)
	}
	assertNoTrackNoPunch(t, harness)
	assertNoReserveEvent(t, harness)
	if harness.shard.seq != 0 {
		t.Errorf("seq = %d, want 0 (bad spec rejected before nextOrderID)", harness.shard.seq)
	}
}

// Market-buy with cold mktCache rejects before reserve. seq is NOT bumped
// (nextOrderID runs AFTER the market-buy price lookup).
func TestHandlePlaceOrder_MarketBuyColdCache_RejectsNoMarketPrice(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 10_000_000}, nil)
	// mktCache is nopMarketCache by default — every lookup misses.

	result := harness.shard.handlePlaceOrder(PlaceOrder{
		AccountID: harness.accountID,
		Order: OrderSpec{
			Instrument: goldenInstrument, Side: contract.Buy, Type: contract.Market, Qty: 10,
		},
		ReplyTo: make(chan PlaceResult, 1),
	})

	if result.Status != PlaceRejected || result.Reason != ReasonNoMarketPrice {
		t.Errorf("result = %+v, want {PlaceRejected, ReasonNoMarketPrice}", result)
	}
	assertNoTrackNoPunch(t, harness)
	assertNoReserveEvent(t, harness)
	if harness.shard.seq != 0 {
		t.Errorf("seq = %d, want 0 (cold cache rejected before nextOrderID)", harness.shard.seq)
	}
}

// Reserve-reject (ErrInsufficientFunds): no track built (STEP 5 never
// reached), no punch (STEP 6 never reached). seq DOES bump — the mint
// happened at STEP 3 before Gate 4; the gap is harmless (P5-D4).
func TestHandlePlaceOrder_ReserveInsufficientFunds_NoTrackNoPunch(t *testing.T) {
	harness := newGoldenHarness(t)
	// Available=100 is insufficient for 10 units at 100 (requires 1_000).
	harness.seedAccount(account.Cash{Available: 100}, nil)

	result := harness.shard.handlePlaceOrder(PlaceOrder{
		AccountID: harness.accountID,
		Order: OrderSpec{
			Instrument: goldenInstrument, Side: contract.Buy, Type: contract.Limit,
			Qty: 10, LimitPrice: 100,
		},
		ReplyTo: make(chan PlaceResult, 1),
	})

	if result.Status != PlaceRejected || result.Reason != ReasonInsufficientFunds {
		t.Errorf("result = %+v, want {PlaceRejected, ReasonInsufficientFunds}", result)
	}
	assertNoTrackNoPunch(t, harness)
	assertNoReserveEvent(t, harness)
	if harness.shard.seq != 1 {
		t.Errorf("seq = %d, want 1 (nextOrderID bumped before Gate 4)", harness.shard.seq)
	}
}

// Reserve-reject on a SELL (ErrInsufficientPosition) collapses into
// ReasonInsufficientFunds — P5-D28 explicitly does not split funds-vs-units
// at the funnel/HTTP surface.
func TestHandlePlaceOrder_ReserveInsufficientPosition_CollapsesToInsufficientFunds(t *testing.T) {
	harness := newGoldenHarness(t)
	// Position=5 units — insufficient for sell of 10 units.
	harness.seedAccount(account.Cash{Available: 10_000_000}, map[contract.InstrumentID]*account.Position{
		goldenInstrument: {Available: 5, OpenLots: []account.Lot{{EntryPrice: 100, RemainingQty: 5}}},
	})

	result := harness.shard.handlePlaceOrder(PlaceOrder{
		AccountID: harness.accountID,
		Order: OrderSpec{
			Instrument: goldenInstrument, Side: contract.Sell, Type: contract.Limit,
			Qty: 10, LimitPrice: 100,
		},
		ReplyTo: make(chan PlaceResult, 1),
	})

	if result.Status != PlaceRejected || result.Reason != ReasonInsufficientFunds {
		t.Errorf("result = %+v, want {PlaceRejected, ReasonInsufficientFunds (P5-D28 collapse)}", result)
	}
	assertNoTrackNoPunch(t, harness)
	assertNoReserveEvent(t, harness)
}

// Sealed instrument set → ReasonUnknownInstrument at validateSpec. Pins
// the first-case-wins order of the switch (instrument check runs BEFORE
// Qty/Side/Price checks even when they would also fail).
func TestHandlePlaceOrder_SealedInstrumentSet_RejectsUnknownInstrument(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 10_000_000}, nil)
	harness.shard.sessions[harness.accountID].instruments = sealedInstrumentSet("OTHER")

	result := harness.shard.handlePlaceOrder(PlaceOrder{
		AccountID: harness.accountID,
		Order: OrderSpec{
			Instrument: goldenInstrument, Side: contract.Buy, Type: contract.Limit,
			Qty: 10, LimitPrice: 100,
		},
		ReplyTo: make(chan PlaceResult, 1),
	})

	if result.Status != PlaceRejected || result.Reason != ReasonUnknownInstrument {
		t.Errorf("result = %+v, want {PlaceRejected, ReasonUnknownInstrument}", result)
	}
	assertNoTrackNoPunch(t, harness)
}

// -----------------------------------------------------------------------------
// Happy paths beyond the market-buy golden: limit-buy, limit-sell,
// market-sell.
// -----------------------------------------------------------------------------

// Limit-buy: ReservedCash = LimitPrice × Qty (no slippage buffer path),
// FundedOrder carries LimitPrice, TIF = GTC, STP = CancelIncoming,
// IsMarketBuy = false.
func TestHandlePlaceOrder_HappyLimitBuy_PunchesWithGTC(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 10_000_000}, nil)

	result := harness.shard.handlePlaceOrder(PlaceOrder{
		AccountID: harness.accountID,
		Order: OrderSpec{
			Instrument: goldenInstrument, Side: contract.Buy, Type: contract.Limit,
			Qty: 100, LimitPrice: 500,
		},
		ReplyTo: make(chan PlaceResult, 1),
	})

	if result.Status != PlaceAccepted {
		t.Fatalf("result = %+v, want PlaceAccepted", result)
	}
	wantOrderID := OrderID("acct-G:1")
	if result.OrderID != wantOrderID {
		t.Errorf("OrderID = %q, want %q", result.OrderID, wantOrderID)
	}
	// Track: Working, IsMarketBuy=false.
	track, exists := harness.shard.tracks[wantOrderID]
	if !exists {
		t.Fatalf("track missing")
	}
	if track.Status != OrderStatusWorking || track.IsMarketBuy {
		t.Errorf("track = %+v, want Status=Working IsMarketBuy=false", *track)
	}
	// ReservedCash = 500 * 100 = 50_000 (computed internally by the reserve service from LimitPrice).
	view, viewOK := harness.shard.resv.ViewReservation(wantOrderID)
	if !viewOK {
		t.Fatalf("ViewReservation missing")
	}
	if view.ReservedCash != 500*100 {
		t.Errorf("ReservedCash = %d, want 50000 (limit * qty)", view.ReservedCash)
	}
	if view.IsMarket {
		t.Errorf("ViewReservation.IsMarket = true, want false on limit")
	}
	// Punched FundedOrder: Type=Limit, TIF=GTC, LimitPrice=500, MaxSpend=0
	// (buffer+bestAsk both zero on limit path).
	select {
	case outbound := <-harness.exchange:
		if outbound.Submit == nil {
			t.Fatalf("outbound.Submit nil")
		}
		funded := *outbound.Submit
		if funded.Type != contract.Limit || funded.TIF != contract.GTC {
			t.Errorf("funded = %+v, want Type=Limit TIF=GTC", funded)
		}
		if funded.LimitPrice != 500 {
			t.Errorf("funded.LimitPrice = %d, want 500", funded.LimitPrice)
		}
		if funded.MaxSpend != 0 {
			t.Errorf("funded.MaxSpend = %d, want 0 (limit path, no buffer)", funded.MaxSpend)
		}
		if funded.STP != contract.CancelIncoming {
			t.Errorf("funded.STP = %v, want CancelIncoming", funded.STP)
		}
	default:
		t.Fatalf("no FundedOrder punched")
	}
}

// Limit-sell: ReservedUnits = Qty, no cash reserved, TIF = GTC.
// IsMarketBuy = false (Side=Sell).
func TestHandlePlaceOrder_HappyLimitSell_ReservesUnits(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 10_000}, map[contract.InstrumentID]*account.Position{
		goldenInstrument: {Available: 100, OpenLots: []account.Lot{{EntryPrice: 500, RemainingQty: 100}}},
	})

	result := harness.shard.handlePlaceOrder(PlaceOrder{
		AccountID: harness.accountID,
		Order: OrderSpec{
			Instrument: goldenInstrument, Side: contract.Sell, Type: contract.Limit,
			Qty: 50, LimitPrice: 500,
		},
		ReplyTo: make(chan PlaceResult, 1),
	})

	if result.Status != PlaceAccepted {
		t.Fatalf("result = %+v, want PlaceAccepted", result)
	}
	view, _ := harness.shard.resv.ViewReservation(result.OrderID)
	if view.ReservedUnits != 50 || view.ReservedCash != 0 {
		t.Errorf("view = %+v, want ReservedUnits=50 ReservedCash=0", view)
	}
	if harness.shard.tracks[result.OrderID].IsMarketBuy {
		t.Errorf("IsMarketBuy = true on sell, want false")
	}
}

// Market-sell: cash NOT reserved (sell side), FundedOrder.MaxSpend = 0,
// TIF = IOC, Type = Market. IsMarketBuy = false — only market-BUY gets
// the flag, since only buys reserve cash with a slippage buffer.
func TestHandlePlaceOrder_HappyMarketSell_SkipsMktCache_ReservesUnits(t *testing.T) {
	harness := newGoldenHarness(t)
	harness.seedAccount(account.Cash{Available: 1_000}, map[contract.InstrumentID]*account.Position{
		goldenInstrument: {Available: 100, OpenLots: []account.Lot{{EntryPrice: 500, RemainingQty: 100}}},
	})
	// mktCache stays nop (cold). Market-sell MUST NOT consult it —
	// sells reserve units from the position, not cash from BestAsk.

	result := harness.shard.handlePlaceOrder(PlaceOrder{
		AccountID: harness.accountID,
		Order: OrderSpec{
			Instrument: goldenInstrument, Side: contract.Sell, Type: contract.Market, Qty: 30,
		},
		ReplyTo: make(chan PlaceResult, 1),
	})

	if result.Status != PlaceAccepted {
		t.Fatalf("result = %+v, want PlaceAccepted (market-sell does not consult mktCache)", result)
	}
	track := harness.shard.tracks[result.OrderID]
	if track.IsMarketBuy {
		t.Errorf("IsMarketBuy = true on market-sell, want false (only market-BUY gets the flag)")
	}
	select {
	case outbound := <-harness.exchange:
		funded := *outbound.Submit
		if funded.Type != contract.Market || funded.TIF != contract.IOC {
			t.Errorf("funded = %+v, want Type=Market TIF=IOC", funded)
		}
		if funded.MaxSpend != 0 {
			t.Errorf("funded.MaxSpend = %d, want 0 (sell path has no cash cap)", funded.MaxSpend)
		}
	default:
		t.Fatalf("no FundedOrder punched")
	}
}

// -----------------------------------------------------------------------------
// Helper tests.
// -----------------------------------------------------------------------------

// reserveReason: both reserve-package sentinels → ReasonInsufficientFunds.
func TestReserveReason_BothSentinelsCollapseToInsufficientFunds(t *testing.T) {
	cases := []error{reserve.ErrInsufficientFunds, reserve.ErrInsufficientPosition}
	for _, err := range cases {
		if got := reserveReason(err); got != ReasonInsufficientFunds {
			t.Errorf("reserveReason(%v) = %v, want ReasonInsufficientFunds", err, got)
		}
	}
}

// reserveReason: an unexpected error panics. Pins the "no silent swallow"
// discipline — anything outside the reserve package's two typed sentinels
// is a programming error.
func TestReserveReason_UnexpectedError_Panics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on unexpected reserve error, got none")
		}
	}()
	_ = reserveReason(errors.New("something else"))
}

// tifFor: Limit→GTC, Market→IOC (per Phase-5 §1.3). An unknown OrdType
// value defaults to IOC (the else-branch), which is also the right
// behaviour for a future Stop type that triggers into a Market/Limit — but
// Stop should never reach the exchange directly per contract §3.2.
func TestTifFor_Table(t *testing.T) {
	cases := []struct {
		name    string
		ordType contract.OrdType
		want    contract.TIF
	}{
		{"limit → GTC", contract.Limit, contract.GTC},
		{"market → IOC", contract.Market, contract.IOC},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tifFor(tc.ordType); got != tc.want {
				t.Errorf("tifFor(%v) = %v, want %v", tc.ordType, got, tc.want)
			}
		})
	}
}

// buildReserveOrder: pins the field-level mapping from OrderSpec to
// reserve.Order. Catches a future bug where a renamed OrderSpec field
// silently fails to propagate into the reservation (e.g. LimitPrice
// forgotten on sell path).
func TestBuildReserveOrder_FieldMapping(t *testing.T) {
	spec := OrderSpec{
		Instrument: goldenInstrument, Side: contract.Sell, Type: contract.Market,
		TIF: contract.IOC, Qty: 50, LimitPrice: 0,
	}
	got := buildReserveOrder(spec, "oid-X", goldenAccountID, goldenSessionID, 10_000, 42)
	want := reserve.Order{
		OrderID:        "oid-X",
		AccountID:      goldenAccountID,
		SessionID:      goldenSessionID,
		InstrumentID:   goldenInstrument,
		Side:           contract.Sell,
		Qty:            50,
		IsMarket:       true,
		LimitPrice:     0,
		BestAsk:        10_000,
		SlippageBuffer: 42,
	}
	if got != want {
		t.Errorf("toL5Order = %+v, want %+v", got, want)
	}
}

// -----------------------------------------------------------------------------
// Shared assertion helpers (test-local).
// -----------------------------------------------------------------------------

// assertNoTrackNoPunch pins the P5-D6 pre-punch-failure invariant: on any
// handler reject BEFORE STEP 5, no OrderTrack is created and no
// FundedOrder is sent onto the exchange edge.
func assertNoTrackNoPunch(t *testing.T, harness *goldenHarness) {
	t.Helper()
	if len(harness.shard.tracks) != 0 {
		t.Errorf("tracks map has %d entries on reject, want 0 (P5-D6: no track on pre-punch failure)", len(harness.shard.tracks))
	}
	select {
	case outbound := <-harness.exchange:
		t.Errorf("FundedOrder punched on reject: %+v (P5-D6: nothing punched on pre-punch failure)", outbound)
	default:
	}
}

// assertNoReserveEvent pins that the reservation service was never
// consulted on this reject path. validateSpec + no-session + cold-mktCache
// all reject BEFORE ReserveIfAvailable — so no ReserveEvent should ever
// land on the spy.
func assertNoReserveEvent(t *testing.T, harness *goldenHarness) {
	t.Helper()
	for _, event := range harness.events.emitted {
		if _, ok := event.(reserve.ReserveEvent); ok {
			t.Errorf("ReserveEvent emitted on pre-reserve reject: %+v (reservation service should not have been consulted)", event)
		}
	}
}
