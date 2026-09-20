package wire

import (
	"reflect"
	"testing"

	"simulator/contract"
	"simulator/exchange/book"
	"simulator/exchange/clock"
	"simulator/exchange/emit"
)

// TestHandleCancel_GoldenA_OnlyRestingOrder — Flow C §9.3, the simplest
// shape: cancel the only resting order. The side becomes empty, so quote
// and depth both change; both are emitted before the private Cancelled.
func TestHandleCancel_GoldenA_OnlyRestingOrder(t *testing.T) {
	exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("ACME")})
	setAllShardsPhase(exchange, clock.PhaseContinuous)
	seedResting(t, exchange, &contract.RestingOrder{
		OrderID:     "o-99",
		Participant: "p-1",
		Side:        contract.Buy,
		Price:       9900,
		Remaining:   100,
		Seq:         1,
	})

	publicEvents, privateResults := runAndCollect(t, exchange, []contract.InboundMessage{
		inboundCancel("B1", "ACME", "o-99"),
	})

	wantPrivate := []contract.EngineResult{{
		Outcome:      contract.OutcomeCancelled,
		OrderID:      "o-99",
		BrokerID:     "B1",
		CancelledQty: 100,
	}}
	assertPrivateEqual(t, privateResults, wantPrivate)

	wantPublic := []emit.TapeEvent{
		{Seq: 1, Instrument: "ACME", Kind: emit.KindQuote, Quote: &book.QuoteState{}},
		{Seq: 2, Instrument: "ACME", Kind: emit.KindDepth, Depth: &emit.DepthSnapshot{Bids: []emit.DepthLevel{}, Asks: []emit.DepthLevel{}}},
	}
	assertPublicEqual(t, publicEvents, wantPublic)
}

// TestHandleCancel_GoldenB_TopLevelWithSibling — cancel one of two orders
// at the same top price. Quote's BidQty drops; depth's row shrinks; both
// change.
func TestHandleCancel_GoldenB_TopLevelWithSibling(t *testing.T) {
	exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("ACME")})
	setAllShardsPhase(exchange, clock.PhaseContinuous)
	seedResting(t, exchange, &contract.RestingOrder{
		OrderID: "o-1", Participant: "p-a",
		Side: contract.Buy, Price: 9900, Remaining: 30, Seq: 1,
	})
	seedResting(t, exchange, &contract.RestingOrder{
		OrderID: "o-2", Participant: "p-b",
		Side: contract.Buy, Price: 9900, Remaining: 70, Seq: 2,
	})

	publicEvents, privateResults := runAndCollect(t, exchange, []contract.InboundMessage{
		inboundCancel("B1", "ACME", "o-1"),
	})

	wantPrivate := []contract.EngineResult{{
		Outcome:      contract.OutcomeCancelled,
		OrderID:      "o-1",
		BrokerID:     "B1",
		CancelledQty: 30,
	}}
	assertPrivateEqual(t, privateResults, wantPrivate)

	wantPublic := []emit.TapeEvent{
		{Seq: 1, Instrument: "ACME", Kind: emit.KindQuote, Quote: &book.QuoteState{HasBid: true, BestBid: 9900, BidQty: 70}},
		{Seq: 2, Instrument: "ACME", Kind: emit.KindDepth, Depth: &emit.DepthSnapshot{Bids: []emit.DepthLevel{{Price: 9900, TotalQty: 70}}, Asks: []emit.DepthLevel{}}},
	}
	assertPublicEqual(t, publicEvents, wantPublic)
}

// TestHandleCancel_GoldenC_BelowTop10_NoPublicEvents — 11 bid levels each
// with one order; cancel the 11th-best (price 990). Top 10 (prices
// 1000..991) unchanged → quote unchanged, depth (top-10) unchanged →
// zero public events; only the private Cancelled fires.
func TestHandleCancel_GoldenC_BelowTop10_NoPublicEvents(t *testing.T) {
	exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("ACME")})
	setAllShardsPhase(exchange, clock.PhaseContinuous)
	for level := 0; level < 11; level++ {
		price := contract.Ticks(1000 - level)
		orderID := contract.OrderID("o-" + string(rune('a'+level)))
		seedResting(t, exchange, &contract.RestingOrder{
			OrderID: orderID, Participant: "p",
			Side: contract.Buy, Price: price, Remaining: 5, Seq: contract.Seq(level + 1),
		})
	}

	publicEvents, privateResults := runAndCollect(t, exchange, []contract.InboundMessage{
		inboundCancel("B1", "ACME", "o-k"), // level 11 (price 990)
	})

	wantPrivate := []contract.EngineResult{{
		Outcome:      contract.OutcomeCancelled,
		OrderID:      "o-k",
		BrokerID:     "B1",
		CancelledQty: 5,
	}}
	assertPrivateEqual(t, privateResults, wantPrivate)

	if len(publicEvents) != 0 {
		t.Fatalf("public events: got %d, want 0 (cancel below top 10 must not touch the tape)", len(publicEvents))
	}
}

// TestHandleCancel_GoldenD_UnknownOrderRejected — cancel of a non-existent
// OrderID. Private CancelRejected; no public events.
func TestHandleCancel_GoldenD_UnknownOrderRejected(t *testing.T) {
	exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("ACME")})
	setAllShardsPhase(exchange, clock.PhaseContinuous)
	seedResting(t, exchange, &contract.RestingOrder{
		OrderID: "o-1", Participant: "p-a",
		Side: contract.Buy, Price: 9900, Remaining: 10, Seq: 1,
	})

	publicEvents, privateResults := runAndCollect(t, exchange, []contract.InboundMessage{
		inboundCancel("B1", "ACME", "o-nonexistent"),
	})

	wantPrivate := []contract.EngineResult{{
		Outcome:  contract.OutcomeCancelRejected,
		OrderID:  "o-nonexistent",
		BrokerID: "B1",
		// CancelledQty zero — nothing to release.
	}}
	assertPrivateEqual(t, privateResults, wantPrivate)

	if len(publicEvents) != 0 {
		t.Fatalf("public events: got %d, want 0 (not-found cancel emits nothing public)", len(publicEvents))
	}
}

// TestHandleCancel_GoldenE_TopLevelDrained_QuoteShifts — cancel the sole
// order at the best bid; the next-best level takes over. Quote and depth
// both change.
func TestHandleCancel_GoldenE_TopLevelDrained_QuoteShifts(t *testing.T) {
	exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("ACME")})
	setAllShardsPhase(exchange, clock.PhaseContinuous)
	seedResting(t, exchange, &contract.RestingOrder{
		OrderID: "o-top", Participant: "p-a",
		Side: contract.Buy, Price: 9900, Remaining: 100, Seq: 1,
	})
	seedResting(t, exchange, &contract.RestingOrder{
		OrderID: "o-next", Participant: "p-b",
		Side: contract.Buy, Price: 9800, Remaining: 50, Seq: 2,
	})

	publicEvents, privateResults := runAndCollect(t, exchange, []contract.InboundMessage{
		inboundCancel("B1", "ACME", "o-top"),
	})

	wantPrivate := []contract.EngineResult{{
		Outcome:      contract.OutcomeCancelled,
		OrderID:      "o-top",
		BrokerID:     "B1",
		CancelledQty: 100,
	}}
	assertPrivateEqual(t, privateResults, wantPrivate)

	wantPublic := []emit.TapeEvent{
		{Seq: 1, Instrument: "ACME", Kind: emit.KindQuote, Quote: &book.QuoteState{HasBid: true, BestBid: 9800, BidQty: 50}},
		{Seq: 2, Instrument: "ACME", Kind: emit.KindDepth, Depth: &emit.DepthSnapshot{Bids: []emit.DepthLevel{{Price: 9800, TotalQty: 50}}, Asks: []emit.DepthLevel{}}},
	}
	assertPublicEqual(t, publicEvents, wantPublic)
}

// TestHandleCancel_GoldenF_SellSide — cross-cover for Side: cancel a
// resting sell. Symmetric to Golden A but on the ask side.
func TestHandleCancel_GoldenF_SellSide(t *testing.T) {
	exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("ACME")})
	setAllShardsPhase(exchange, clock.PhaseContinuous)
	seedResting(t, exchange, &contract.RestingOrder{
		OrderID: "o-99", Participant: "p-1",
		Side: contract.Sell, Price: 10500, Remaining: 40, Seq: 1,
	})

	publicEvents, privateResults := runAndCollect(t, exchange, []contract.InboundMessage{
		inboundCancel("B1", "ACME", "o-99"),
	})

	wantPrivate := []contract.EngineResult{{
		Outcome:      contract.OutcomeCancelled,
		OrderID:      "o-99",
		BrokerID:     "B1",
		CancelledQty: 40,
	}}
	assertPrivateEqual(t, privateResults, wantPrivate)

	wantPublic := []emit.TapeEvent{
		{Seq: 1, Instrument: "ACME", Kind: emit.KindQuote, Quote: &book.QuoteState{}},
		{Seq: 2, Instrument: "ACME", Kind: emit.KindDepth, Depth: &emit.DepthSnapshot{Bids: []emit.DepthLevel{}, Asks: []emit.DepthLevel{}}},
	}
	assertPublicEqual(t, publicEvents, wantPublic)
}

// TestHandleCancel_GoldenG_DepthChangesButNotQuote — the critical case
// motivating pre/post comparison. Cancel a mid-book bid: the top level
// (9900/100) is untouched, so quote is unchanged; but depth's row 2 is
// now missing, so depth is emitted alone.
func TestHandleCancel_GoldenG_DepthChangesButNotQuote(t *testing.T) {
	exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("ACME")})
	setAllShardsPhase(exchange, clock.PhaseContinuous)
	seedResting(t, exchange, &contract.RestingOrder{
		OrderID: "o-top", Participant: "p-a",
		Side: contract.Buy, Price: 9900, Remaining: 100, Seq: 1,
	})
	seedResting(t, exchange, &contract.RestingOrder{
		OrderID: "o-mid", Participant: "p-b",
		Side: contract.Buy, Price: 9800, Remaining: 50, Seq: 2,
	})
	seedResting(t, exchange, &contract.RestingOrder{
		OrderID: "o-bot", Participant: "p-c",
		Side: contract.Buy, Price: 9700, Remaining: 30, Seq: 3,
	})

	publicEvents, privateResults := runAndCollect(t, exchange, []contract.InboundMessage{
		inboundCancel("B1", "ACME", "o-mid"),
	})

	wantPrivate := []contract.EngineResult{{
		Outcome:      contract.OutcomeCancelled,
		OrderID:      "o-mid",
		BrokerID:     "B1",
		CancelledQty: 50,
	}}
	assertPrivateEqual(t, privateResults, wantPrivate)

	// Only Depth — no Quote — because the top of book (9900/100) is
	// untouched. Depth Seq=1 (the only public event's Seq).
	wantPublic := []emit.TapeEvent{
		{Seq: 1, Instrument: "ACME", Kind: emit.KindDepth, Depth: &emit.DepthSnapshot{Bids: []emit.DepthLevel{{Price: 9900, TotalQty: 100}, {Price: 9700, TotalQty: 30}}, Asks: []emit.DepthLevel{}}},
	}
	assertPublicEqual(t, publicEvents, wantPublic)
}

// TestHandleCancel_BrokerIDEchoed — both Cancelled and CancelRejected
// echo the inbound BrokerID (Rule 2 across both outcomes).
func TestHandleCancel_BrokerIDEchoed(t *testing.T) {
	exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("ACME")})
	setAllShardsPhase(exchange, clock.PhaseContinuous)
	seedResting(t, exchange, &contract.RestingOrder{
		OrderID: "o-1", Participant: "p", Side: contract.Buy, Price: 100, Remaining: 5, Seq: 1,
	})

	_, privateResults := runAndCollect(t, exchange, []contract.InboundMessage{
		inboundCancel("BrokerA", "ACME", "o-1"),        // Cancelled
		inboundCancel("BrokerB", "ACME", "o-nonexist"), // CancelRejected
	})

	if len(privateResults) != 2 {
		t.Fatalf("private results: got %d, want 2", len(privateResults))
	}
	if privateResults[0].Outcome != contract.OutcomeCancelled || privateResults[0].BrokerID != "BrokerA" {
		t.Fatalf("first result: got outcome=%v brokerID=%q, want Cancelled/BrokerA",
			privateResults[0].Outcome, privateResults[0].BrokerID)
	}
	if privateResults[1].Outcome != contract.OutcomeCancelRejected || privateResults[1].BrokerID != "BrokerB" {
		t.Fatalf("second result: got outcome=%v brokerID=%q, want CancelRejected/BrokerB",
			privateResults[1].Outcome, privateResults[1].BrokerID)
	}
}

// TestHandleCancel_DoesNotAdvanceAdmitSeq — cancel path bypasses the gate,
// so admitSeq must remain untouched no matter how many cancels happen.
func TestHandleCancel_DoesNotAdvanceAdmitSeq(t *testing.T) {
	exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("ACME")})
	setAllShardsPhase(exchange, clock.PhaseContinuous)
	state := exchange.shards[hashInstrument("ACME")%uint32(len(exchange.shards))].instrs["ACME"]

	seedResting(t, exchange, &contract.RestingOrder{
		OrderID: "o-1", Participant: "p", Side: contract.Buy, Price: 100, Remaining: 5, Seq: 42,
	})
	seedResting(t, exchange, &contract.RestingOrder{
		OrderID: "o-2", Participant: "p", Side: contract.Buy, Price: 99, Remaining: 5, Seq: 43,
	})

	runAndCollect(t, exchange, []contract.InboundMessage{
		inboundCancel("B1", "ACME", "o-1"),
		inboundCancel("B1", "ACME", "o-nope"),
		inboundCancel("B1", "ACME", "o-2"),
	})

	if state.admitSeq != 0 {
		t.Fatalf("admitSeq = %d after cancels, want 0 — cancel path must not consume admit-side sequence", state.admitSeq)
	}
}

// TestHandleCancel_SeqNumberingWhenBothEmit — a cancel that changes both
// quote and depth consumes two consecutive Seqs from the emitter, and the
// Depth event's Seq = Quote's Seq + 1.
func TestHandleCancel_SeqNumberingWhenBothEmit(t *testing.T) {
	exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("ACME")})
	setAllShardsPhase(exchange, clock.PhaseContinuous)
	seedResting(t, exchange, &contract.RestingOrder{
		OrderID: "o-1", Participant: "p", Side: contract.Buy, Price: 100, Remaining: 5, Seq: 1,
	})

	publicEvents, _ := runAndCollect(t, exchange, []contract.InboundMessage{
		inboundCancel("B1", "ACME", "o-1"),
	})

	if len(publicEvents) != 2 {
		t.Fatalf("public events: got %d, want 2 (Quote + Depth)", len(publicEvents))
	}
	if publicEvents[0].Kind != emit.KindQuote {
		t.Fatalf("public[0].Kind = %v, want KindQuote", publicEvents[0].Kind)
	}
	if publicEvents[1].Kind != emit.KindDepth {
		t.Fatalf("public[1].Kind = %v, want KindDepth", publicEvents[1].Kind)
	}
	if publicEvents[0].Seq != 1 {
		t.Fatalf("Quote.Seq = %d, want 1", publicEvents[0].Seq)
	}
	if publicEvents[1].Seq != publicEvents[0].Seq+1 {
		t.Fatalf("Depth.Seq = %d, want Quote.Seq + 1 = %d", publicEvents[1].Seq, publicEvents[0].Seq+1)
	}
}

// TestHandleCancel_DeterministicAcrossRuns — mixed submit+cancel sequence
// run twice on fresh exchanges yields byte-identical public and private
// outputs. Locks that cancel adds no non-determinism.
func TestHandleCancel_DeterministicAcrossRuns(t *testing.T) {
	buildAndRun := func() ([]emit.TapeEvent, []contract.EngineResult) {
		exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("ACME")})
		setAllShardsPhase(exchange, clock.PhaseContinuous)
		messages := []contract.InboundMessage{
			inboundSubmit("B1", &contract.FundedOrder{
				OrderID: "o-1", Participant: "p-a", Instrument: "ACME",
				Side: contract.Buy, Type: contract.Limit, TIF: contract.GTC,
				Qty: 20, LimitPrice: 9900,
			}),
			inboundSubmit("B1", &contract.FundedOrder{
				OrderID: "o-2", Participant: "p-a", Instrument: "ACME",
				Side: contract.Buy, Type: contract.Limit, TIF: contract.GTC,
				Qty: 30, LimitPrice: 9800,
			}),
			inboundCancel("B1", "ACME", "o-1"),
			inboundCancel("B1", "ACME", "o-missing"),
			inboundCancel("B1", "ACME", "o-2"),
		}
		return runAndCollect(t, exchange, messages)
	}

	firstPublic, firstPrivate := buildAndRun()
	secondPublic, secondPrivate := buildAndRun()

	if !reflect.DeepEqual(firstPublic, secondPublic) {
		t.Fatalf("public events not deterministic\n  first:  %s\n  second: %s",
			formatPublic(firstPublic), formatPublic(secondPublic))
	}
	if !reflect.DeepEqual(firstPrivate, secondPrivate) {
		t.Fatalf("private results not deterministic\n  first:  %s\n  second: %s",
			formatPrivate(firstPrivate), formatPrivate(secondPrivate))
	}
}
