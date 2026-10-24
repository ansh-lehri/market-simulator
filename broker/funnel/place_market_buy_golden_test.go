package funnel

import (
	"testing"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/reserve"
)

// -----------------------------------------------------------------------------
// Stage-2 MARKET-BUY PLACE GOLDEN — RED, pins the reserved == MaxSpend
// invariant that is the whole point of P5-D3 (BROKER_PHASE5 §1.5).
//
// The second half of the orientation's "two risky pieces" is the market-
// buy reservation path. If the broker computes maxSpend wrong, or passes
// a different pair of (BestAsk, SlippageBuffer) into reserve.Order than
// it stamps on FundedOrder.MaxSpend, the "reserved == MaxSpend by
// construction" invariant breaks — the exchange can either outspend what
// was reserved (shortfall) or sit on reserved cash forever (leak).
//
// This file pins the exact Phase-5 defaults (slippageBufferBps=0, so
// buffer=0 for every call) and asserts BOTH halves at once:
//
//   (a) The FundedOrder punched onto the exchange edge carries
//       MaxSpend = bestAsk*qty + buffer.
//   (b) The reservation admitted (via spy event) carries ReservedCash
//       = bestAsk*qty + buffer.
//   (c) Those two numbers ARE EQUAL — the invariant.
//
// Unit note: MaxSpend is in exchange ticks×qty units (bestAsk*qty+buffer);
// ReservedCash is in broker rupee units ((bestAsk*qty+buffer)/100). The
// invariant is MaxSpend/100 == ReservedCash, not literal equality.
//
// Also pins:
//   - OrderID shape "<AccountID>:<seq>" (first mint = "acct-G:1")
//   - OrderTrack created with IsMarketBuy=true and Status=Working
//   - TIF=IOC (market), LimitPrice=0, STP=CancelIncoming on the FundedOrder
//   - BrokerID stamped on the InboundMessage (by s.punch's shared stamp)
//
// Current state: handlePlaceOrder is an empty stub returning PlaceResult{}
// → every assertion fails. Stage 3 implements to green.
//
// ReleaseExcess true-up (the second part of the orientation's "market-buy
// goldens") is covered inside the SETTLE golden #1 (full fill with residue
// returned) — the two concerns live on opposite sides of the exchange and
// test the two call sites.
// -----------------------------------------------------------------------------

func TestPlaceGolden_MarketBuy_ReservedEqualsMaxSpend_WithPhase5Defaults(t *testing.T) {
	harness := newGoldenHarness(t)

	const (
		startingAvailable int64         = 10_000_000
		bestAsk           contract.Ticks = 10_021
		orderQty          contract.Qty   = 50
	)
	// slippageBufferBps=0 → buffer=0.
	// MaxSpend (exchange unit, ticks×qty): bestAsk*qty + buffer = 501050.
	// ReservedCash (broker unit, rupees): MaxSpend / 100 = 5010.
	// Invariant: MaxSpend / 100 == ReservedCash (same budget, two units).
	const (
		expectedBuffer       int64 = 0
		expectedMaxSpend     int64 = int64(bestAsk) * int64(orderQty) // 501050 ticks×qty
		expectedRequiredCash int64 = expectedMaxSpend / 100           // 5010 rupees
	)

	harness.seedAccount(account.Cash{Available: startingAvailable}, nil)
	harness.hotMktCache(bestAsk)
	// Shard dial is zero by default; stated explicitly for the golden log.
	harness.shard.slippageBufferBps = 0

	placeCmd := PlaceOrder{
		AccountID: goldenAccountID,
		SessionID: goldenSessionID,
		Order: OrderSpec{
			Instrument: goldenInstrument,
			Side:       contract.Buy,
			Type:       contract.Market,
			TIF:        contract.IOC,
			Qty:        orderQty,
			LimitPrice: 0,
		},
		ReplyTo: make(chan PlaceResult, 1),
	}
	result := harness.shard.handlePlaceOrder(placeCmd)

	// -------------------------------------------------------------------------
	// 1. PlaceResult shape — accepted, OrderID = "<AccountID>:<seq>" first mint.
	// -------------------------------------------------------------------------
	if result.Status != PlaceAccepted {
		t.Fatalf("PlaceResult.Status = %v, want PlaceAccepted (place-golden pre-impl: expect RED)", result.Status)
	}
	wantOrderID := OrderID("acct-G:1")
	if result.OrderID != wantOrderID {
		t.Errorf("PlaceResult.OrderID = %q, want %q (first nextOrderID mint)", result.OrderID, wantOrderID)
	}
	if result.Reason != ReasonUnknown {
		t.Errorf("PlaceResult.Reason = %v, want ReasonUnknown on accept", result.Reason)
	}

	// -------------------------------------------------------------------------
	// 2. OrderTrack created with IsMarketBuy=true, Status=Working.
	// -------------------------------------------------------------------------
	track, exists := harness.shard.tracks[wantOrderID]
	if !exists {
		t.Fatalf("OrderTrack missing after accepted place")
	}
	wantTrack := OrderTrack{
		OrderID: wantOrderID, AccountID: goldenAccountID, Instrument: goldenInstrument,
		Status: OrderStatusWorking, CancelInFlight: false, IsMarketBuy: true,
	}
	if *track != wantTrack {
		t.Errorf("OrderTrack = %+v, want %+v", *track, wantTrack)
	}

	// -------------------------------------------------------------------------
	// 3. reservation admitted — ReserveEvent carries requiredCash =
	//    bestAsk*qty + buffer. Pins (b).
	// -------------------------------------------------------------------------
	if len(harness.events.emitted) == 0 {
		t.Fatalf("no ReserveEvent emitted — reserve service never admitted")
	}
	reserveEvent, ok := harness.events.emitted[0].(reserve.ReserveEvent)
	if !ok {
		t.Fatalf("first event is %T, want ReserveEvent", harness.events.emitted[0])
	}
	if reserveEvent.Cash != expectedRequiredCash {
		t.Errorf("ReserveEvent.Cash = %d, want %d (bestAsk*qty + buffer = %d*%d + %d)",
			reserveEvent.Cash, expectedRequiredCash, bestAsk, orderQty, expectedBuffer)
	}
	if reserveEvent.OrderID != wantOrderID {
		t.Errorf("ReserveEvent.OrderID = %q, want %q", reserveEvent.OrderID, wantOrderID)
	}

	// -------------------------------------------------------------------------
	// 4. Account state: Available -= requiredCash, Reserved += requiredCash.
	// -------------------------------------------------------------------------
	acct := harness.ledger.Get(goldenAccountID)
	wantCash := account.Cash{
		Available: startingAvailable - expectedRequiredCash,
		Reserved:  expectedRequiredCash,
	}
	if acct.Cash != wantCash {
		t.Errorf("Account.Cash = %+v, want %+v", acct.Cash, wantCash)
	}

	// -------------------------------------------------------------------------
	// 5. View of the reservation record pins ReservedCash + IsMarket.
	// -------------------------------------------------------------------------
	view, viewOK := harness.shard.resv.ViewReservation(wantOrderID)
	if !viewOK {
		t.Fatalf("ViewReservation missing for order %q after admission", wantOrderID)
	}
	if view.ReservedCash != expectedRequiredCash {
		t.Errorf("ViewReservation.ReservedCash = %d, want %d", view.ReservedCash, expectedRequiredCash)
	}
	if !view.IsMarket {
		t.Errorf("ViewReservation.IsMarket = false, want true (market buy)")
	}
	if view.RemainingQty != int64(orderQty) || view.OrigQty != int64(orderQty) {
		t.Errorf("ViewReservation qty: RemainingQty=%d OrigQty=%d, want both %d",
			view.RemainingQty, view.OrigQty, orderQty)
	}

	// -------------------------------------------------------------------------
	// 6. FundedOrder punched — MaxSpend == bestAsk*qty + buffer. Pins (a) and (c).
	// -------------------------------------------------------------------------
	select {
	case outbound := <-harness.exchange:
		if outbound.Kind != contract.SubmitOrder {
			t.Errorf("outbound.Kind = %v, want contract.SubmitOrder", outbound.Kind)
		}
		if outbound.BrokerID != goldenBrokerID {
			t.Errorf("outbound.BrokerID = %q, want %q (stamped by s.punch)", outbound.BrokerID, goldenBrokerID)
		}
		if outbound.Instrument != goldenInstrument {
			t.Errorf("outbound.Instrument = %q, want %q", outbound.Instrument, goldenInstrument)
		}
		if outbound.Submit == nil {
			t.Fatalf("outbound.Submit = nil, want *FundedOrder")
		}
		wantFunded := contract.FundedOrder{
			OrderID:     wantOrderID,
			Participant: goldenParticipantID,
			Instrument:  goldenInstrument,
			Side:        contract.Buy,
			Type:        contract.Market,
			TIF:         contract.IOC,
			Qty:         orderQty,
			LimitPrice:  0,
			MaxSpend:    expectedMaxSpend, // exchange ticks×qty; /100 == reservedCash
			STP:         contract.CancelIncoming,
		}
		if *outbound.Submit != wantFunded {
			t.Errorf("FundedOrder = %+v, want %+v", *outbound.Submit, wantFunded)
		}
	default:
		t.Fatalf("no FundedOrder punched onto exchange edge — handlePlaceOrder didn't send")
	}

	// -------------------------------------------------------------------------
	// 7. Shard seq bumped (first mint set it to 1).
	// -------------------------------------------------------------------------
	if harness.shard.seq != 1 {
		t.Errorf("shard.seq = %d after first place, want 1", harness.shard.seq)
	}
}
