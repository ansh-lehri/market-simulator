package wire

import (
	"reflect"
	"strconv"
	"testing"

	"simulator/contract"
	"simulator/exchange/book"
	"simulator/exchange/clock"
	"simulator/exchange/emit"
)

// seedResting inserts one resting order into ACME's book. Kept short since
// every golden test seeds the book identically shaped.
func seedResting(t *testing.T, exchange *Exchange, order *contract.RestingOrder) {
	t.Helper()
	exchange.shards[hashInstrument("ACME")%uint32(len(exchange.shards))].instrs["ACME"].book.Insert(order)
}

// TestHandleSubmit_GoldenA_MarketBuyFullyMatches — EXCHANGE_10 §9.1 Flow A.
// One resting sell fully consumed by a market buy: one trade, empty book,
// three public events (trade, quote, depth), one private matched result.
func TestHandleSubmit_GoldenA_MarketBuyFullyMatches(t *testing.T) {
	exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("ACME")})
	setAllShardsPhase(exchange, clock.PhaseContinuous)
	seedResting(t, exchange, &contract.RestingOrder{
		OrderID:     "o-maker",
		Participant: "p-maker",
		Side:        contract.Sell,
		Price:       10000,
		Remaining:   60,
		Seq:         1,
	})

	takerOrder := &contract.FundedOrder{
		OrderID:     "o-42",
		Participant: "p-taker",
		Instrument:  "ACME",
		Side:        contract.Buy,
		Type:        contract.Market,
		Qty:         60,
		MaxSpend:    100_000_000,
	}
	publicEvents, privateResults := runAndCollect(t, exchange, []contract.InboundMessage{
		inboundSubmit("B1", takerOrder),
	})

	wantPrivate := []contract.EngineResult{{
		Outcome:  contract.OutcomeMatched,
		OrderID:  "o-42",
		BrokerID: "B1",
		Match: &contract.MatchResult{
			Trades: []contract.Trade{{
				Seq:              1,
				Instrument:       "ACME",
				Price:            10000,
				Qty:              60,
				TakerOrder:       "o-42",
				MakerOrder:       "o-maker",
				TakerSide:        contract.Buy,
				TakerParticipant: "p-taker",
				MakerParticipant: "p-maker",
			}},
			LastPrice:      10000,
			LastPriceMoved: true,
		},
	}}
	assertPrivateEqual(t, privateResults, wantPrivate)

	wantPublic := []emit.TapeEvent{
		{Seq: 1, Instrument: "ACME", Kind: emit.KindTrade, Trade: &emit.PublicTrade{Price: 10000, Qty: 60}},
		{Seq: 2, Instrument: "ACME", Kind: emit.KindQuote, Quote: &book.QuoteState{HasLastPrice: true, LastPrice: 10000}},
		{Seq: 3, Instrument: "ACME", Kind: emit.KindDepth, Depth: &emit.DepthSnapshot{Bids: []emit.DepthLevel{}, Asks: []emit.DepthLevel{}}},
	}
	assertPublicEqual(t, publicEvents, wantPublic)
}

// TestHandleSubmit_GoldenB_GTCLimitRestsUnmatched — empty book; GTC limit
// buy rests. No trade printed; the change guard fires via Rested != nil.
func TestHandleSubmit_GoldenB_GTCLimitRestsUnmatched(t *testing.T) {
	exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("ACME")})
	setAllShardsPhase(exchange, clock.PhaseContinuous)

	order := &contract.FundedOrder{
		OrderID:     "o-1",
		Participant: "p-1",
		Instrument:  "ACME",
		Side:        contract.Buy,
		Type:        contract.Limit,
		TIF:         contract.GTC,
		Qty:         100,
		LimitPrice:  9900,
	}
	publicEvents, privateResults := runAndCollect(t, exchange, []contract.InboundMessage{
		inboundSubmit("B1", order),
	})

	wantPrivate := []contract.EngineResult{{
		Outcome:  contract.OutcomeMatched,
		OrderID:  "o-1",
		BrokerID: "B1",
		Match: &contract.MatchResult{
			Rested: &contract.RestingOrder{
				OrderID:     "o-1",
				Participant: "p-1",
				BrokerID:    "B1",
				Side:        contract.Buy,
				Price:       9900,
				Remaining:   100,
				Seq:         1,
			},
		},
	}}
	assertPrivateEqual(t, privateResults, wantPrivate)

	wantPublic := []emit.TapeEvent{
		{Seq: 1, Instrument: "ACME", Kind: emit.KindQuote, Quote: &book.QuoteState{HasBid: true, BestBid: 9900, BidQty: 100}},
		{Seq: 2, Instrument: "ACME", Kind: emit.KindDepth, Depth: &emit.DepthSnapshot{Bids: []emit.DepthLevel{{Price: 9900, TotalQty: 100}}, Asks: []emit.DepthLevel{}}},
	}
	assertPublicEqual(t, publicEvents, wantPublic)
}

// TestHandleSubmit_GoldenC_MarketBuyNoLiquidityRejected — no ask side; the
// gate's checkLiquidity rejects. Private has one Rejected result; public
// has zero events.
func TestHandleSubmit_GoldenC_MarketBuyNoLiquidityRejected(t *testing.T) {
	exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("ACME")})
	setAllShardsPhase(exchange, clock.PhaseContinuous)

	order := &contract.FundedOrder{
		OrderID:     "o-1",
		Participant: "p-1",
		Instrument:  "ACME",
		Side:        contract.Buy,
		Type:        contract.Market,
		Qty:         10,
		MaxSpend:    1_000_000,
	}
	publicEvents, privateResults := runAndCollect(t, exchange, []contract.InboundMessage{
		inboundSubmit("B1", order),
	})

	wantPrivate := []contract.EngineResult{{
		Outcome:   contract.OutcomeRejected,
		OrderID:   "o-1",
		BrokerID:  "B1",
		Rejection: &contract.Rejection{Reason: contract.RejectNoLiquidity, Stage: "liquidity"},
	}}
	assertPrivateEqual(t, privateResults, wantPrivate)

	if len(publicEvents) != 0 {
		t.Fatalf("public events: got %d, want 0 (rejection emits nothing public)", len(publicEvents))
	}
}

// TestHandleSubmit_GoldenD_SessionClosedRejected — phase = PhaseClosed;
// gate's checkSessionPhase rejects before any other stage runs.
func TestHandleSubmit_GoldenD_SessionClosedRejected(t *testing.T) {
	exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("ACME")})
	setAllShardsPhase(exchange, clock.PhaseClosed)
	seedResting(t, exchange, &contract.RestingOrder{
		OrderID: "o-maker", Participant: "p-maker",
		Side: contract.Sell, Price: 10000, Remaining: 10, Seq: 1,
	})

	order := &contract.FundedOrder{
		OrderID:     "o-1",
		Participant: "p-1",
		Instrument:  "ACME",
		Side:        contract.Buy,
		Type:        contract.Market,
		Qty:         5,
		MaxSpend:    1_000_000,
	}
	publicEvents, privateResults := runAndCollect(t, exchange, []contract.InboundMessage{
		inboundSubmit("B1", order),
	})

	wantPrivate := []contract.EngineResult{{
		Outcome:   contract.OutcomeRejected,
		OrderID:   "o-1",
		BrokerID:  "B1",
		Rejection: &contract.Rejection{Reason: contract.RejectMarketClosed, Stage: "session_phase"},
	}}
	assertPrivateEqual(t, privateResults, wantPrivate)

	if len(publicEvents) != 0 {
		t.Fatalf("public events: got %d, want 0", len(publicEvents))
	}
}

// TestHandleSubmit_GoldenE_LimitBuyPriceImprovement — cross-cover for
// OrdType (Limit vs Market): buy limit @ 10050 meets ask @ 10000; trade
// prints at MAKER's price (10000), not the taker's. Full fill, no rest.
func TestHandleSubmit_GoldenE_LimitBuyPriceImprovement(t *testing.T) {
	exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("ACME")})
	setAllShardsPhase(exchange, clock.PhaseContinuous)
	seedResting(t, exchange, &contract.RestingOrder{
		OrderID:     "o-maker",
		Participant: "p-maker",
		Side:        contract.Sell,
		Price:       10000,
		Remaining:   30,
		Seq:         1,
	})

	takerOrder := &contract.FundedOrder{
		OrderID:     "o-5",
		Participant: "p-taker",
		Instrument:  "ACME",
		Side:        contract.Buy,
		Type:        contract.Limit,
		TIF:         contract.GTC,
		Qty:         30,
		LimitPrice:  10050,
	}
	publicEvents, privateResults := runAndCollect(t, exchange, []contract.InboundMessage{
		inboundSubmit("B1", takerOrder),
	})

	wantPrivate := []contract.EngineResult{{
		Outcome:  contract.OutcomeMatched,
		OrderID:  "o-5",
		BrokerID: "B1",
		Match: &contract.MatchResult{
			Trades: []contract.Trade{{
				Seq:              1,
				Instrument:       "ACME",
				Price:            10000, // maker's price, not taker's 10050
				Qty:              30,
				TakerOrder:       "o-5",
				MakerOrder:       "o-maker",
				TakerSide:        contract.Buy,
				TakerParticipant: "p-taker",
				MakerParticipant: "p-maker",
			}},
			LastPrice:      10000,
			LastPriceMoved: true,
		},
	}}
	assertPrivateEqual(t, privateResults, wantPrivate)

	wantPublic := []emit.TapeEvent{
		{Seq: 1, Instrument: "ACME", Kind: emit.KindTrade, Trade: &emit.PublicTrade{Price: 10000, Qty: 30}},
		{Seq: 2, Instrument: "ACME", Kind: emit.KindQuote, Quote: &book.QuoteState{HasLastPrice: true, LastPrice: 10000}},
		{Seq: 3, Instrument: "ACME", Kind: emit.KindDepth, Depth: &emit.DepthSnapshot{Bids: []emit.DepthLevel{}, Asks: []emit.DepthLevel{}}},
	}
	assertPublicEqual(t, publicEvents, wantPublic)
}

// TestHandleSubmit_GoldenF_MarketBuyFundedCapAcrossLevels — two levels
// (10@10000, 10@10001); market buy qty 20 with MaxSpend that only covers
// 10 units at level 1 and 5 units at level 2. Trade seq monotonic across
// prints; post-match Quote/Depth get the next two seqs.
func TestHandleSubmit_GoldenF_MarketBuyFundedCapAcrossLevels(t *testing.T) {
	exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("ACME")})
	setAllShardsPhase(exchange, clock.PhaseContinuous)
	seedResting(t, exchange, &contract.RestingOrder{
		OrderID: "o-m1", Participant: "p-m1",
		Side: contract.Sell, Price: 10000, Remaining: 10, Seq: 1,
	})
	seedResting(t, exchange, &contract.RestingOrder{
		OrderID: "o-m2", Participant: "p-m2",
		Side: contract.Sell, Price: 10001, Remaining: 10, Seq: 2,
	})

	// MaxSpend = 10*10000 (level 1) + 5*10001 (level 2) = 150005 exactly.
	takerOrder := &contract.FundedOrder{
		OrderID:     "o-1",
		Participant: "p-taker",
		Instrument:  "ACME",
		Side:        contract.Buy,
		Type:        contract.Market,
		Qty:         20,
		MaxSpend:    150005,
	}
	publicEvents, privateResults := runAndCollect(t, exchange, []contract.InboundMessage{
		inboundSubmit("B1", takerOrder),
	})

	wantPrivate := []contract.EngineResult{{
		Outcome:  contract.OutcomeMatched,
		OrderID:  "o-1",
		BrokerID: "B1",
		Match: &contract.MatchResult{
			Trades: []contract.Trade{
				{
					Seq: 1, Instrument: "ACME", Price: 10000, Qty: 10,
					TakerOrder: "o-1", MakerOrder: "o-m1",
					TakerSide:        contract.Buy,
					TakerParticipant: "p-taker",
					MakerParticipant: "p-m1",
				},
				{
					Seq: 2, Instrument: "ACME", Price: 10001, Qty: 5,
					TakerOrder: "o-1", MakerOrder: "o-m2",
					TakerSide:        contract.Buy,
					TakerParticipant: "p-taker",
					MakerParticipant: "p-m2",
				},
			},
			LastPrice:      10001,
			LastPriceMoved: true,
			CancelledQty:   5,
			CancelReason:   contract.ReasonFundedCap,
		},
	}}
	assertPrivateEqual(t, privateResults, wantPrivate)

	wantPublic := []emit.TapeEvent{
		{Seq: 1, Instrument: "ACME", Kind: emit.KindTrade, Trade: &emit.PublicTrade{Price: 10000, Qty: 10}},
		{Seq: 2, Instrument: "ACME", Kind: emit.KindTrade, Trade: &emit.PublicTrade{Price: 10001, Qty: 5}},
		{Seq: 3, Instrument: "ACME", Kind: emit.KindQuote, Quote: &book.QuoteState{HasAsk: true, BestAsk: 10001, AskQty: 5, HasLastPrice: true, LastPrice: 10001}},
		{Seq: 4, Instrument: "ACME", Kind: emit.KindDepth, Depth: &emit.DepthSnapshot{Bids: []emit.DepthLevel{}, Asks: []emit.DepthLevel{{Price: 10001, TotalQty: 5}}}},
	}
	assertPublicEqual(t, publicEvents, wantPublic)
}

// TestHandleSubmit_BrokerIDEchoedAndAbsentFromPublic — Rule 2 across two
// outcomes: every private result carries the inbound BrokerID; no public
// event exposes any broker identity (public tape is broadcast).
func TestHandleSubmit_BrokerIDEchoedAndAbsentFromPublic(t *testing.T) {
	exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("ACME")})
	setAllShardsPhase(exchange, clock.PhaseContinuous)
	seedResting(t, exchange, &contract.RestingOrder{
		OrderID: "o-maker", Participant: "p-maker",
		Side: contract.Sell, Price: 10000, Remaining: 10, Seq: 1,
	})

	matcher := &contract.FundedOrder{
		OrderID: "o-match", Participant: "p-a", Instrument: "ACME",
		Side: contract.Buy, Type: contract.Market, Qty: 5, MaxSpend: 1_000_000,
	}
	// Second order sees empty ask side → rejected.
	rejecter := &contract.FundedOrder{
		OrderID: "o-reject", Participant: "p-b", Instrument: "ACME",
		Side: contract.Buy, Type: contract.Market, Qty: 999, MaxSpend: 1_000_000,
	}

	_, privateResults := runAndCollect(t, exchange, []contract.InboundMessage{
		inboundSubmit("BrokerA", matcher),
		inboundSubmit("BrokerB", rejecter),
	})

	if len(privateResults) != 2 {
		t.Fatalf("private results: got %d, want 2", len(privateResults))
	}
	if privateResults[0].BrokerID != "BrokerA" {
		t.Fatalf("first result BrokerID = %q, want BrokerA", privateResults[0].BrokerID)
	}
	if privateResults[1].BrokerID != "BrokerB" {
		t.Fatalf("second result BrokerID = %q, want BrokerB", privateResults[1].BrokerID)
	}
	// No BrokerID field on any public tape event type; nothing to scan.
	// (This is a compile-time property of the TapeEvent shape — worth
	// noting so a future field addition is caught by grep, but there's
	// nothing to assert dynamically.)
}

// TestHandleSubmit_DeterministicAcrossRuns — same inbound sequence run twice
// on fresh exchanges yields identical public + private outputs. The wire
// spine has no non-determinism sources of its own (no time, no random);
// this test locks that.
func TestHandleSubmit_DeterministicAcrossRuns(t *testing.T) {
	buildAndRun := func() ([]emit.TapeEvent, []contract.EngineResult) {
		exchange := NewExchange(1, makeSchedule(), []InstrumentSpec{makeSpec("ACME")})
		setAllShardsPhase(exchange, clock.PhaseContinuous)
		seedResting(t, exchange, &contract.RestingOrder{
			OrderID: "o-m1", Participant: "p-m1",
			Side: contract.Sell, Price: 10000, Remaining: 20, Seq: 1,
		})
		messages := []contract.InboundMessage{
			inboundSubmit("B1", &contract.FundedOrder{
				OrderID: "o-1", Participant: "p-a", Instrument: "ACME",
				Side: contract.Buy, Type: contract.Market, Qty: 5, MaxSpend: 1_000_000,
			}),
			inboundSubmit("B1", &contract.FundedOrder{
				OrderID: "o-2", Participant: "p-a", Instrument: "ACME",
				Side: contract.Buy, Type: contract.Limit, TIF: contract.GTC,
				Qty: 10, LimitPrice: 9900,
			}),
			inboundSubmit("B1", &contract.FundedOrder{
				OrderID: "o-3", Participant: "p-a", Instrument: "ACME",
				Side: contract.Buy, Type: contract.Market, Qty: 7, MaxSpend: 1_000_000,
			}),
		}
		return runAndCollect(t, exchange, messages)
	}

	firstPublic, firstPrivate := buildAndRun()
	secondPublic, secondPrivate := buildAndRun()

	if !reflect.DeepEqual(firstPublic, secondPublic) {
		t.Fatalf("public events not deterministic\n  first:  %+v\n  second: %+v", firstPublic, secondPublic)
	}
	if !reflect.DeepEqual(firstPrivate, secondPrivate) {
		t.Fatalf("private results not deterministic\n  first:  %+v\n  second: %+v", firstPrivate, secondPrivate)
	}
}

// assertPrivateEqual and assertPublicEqual: verbose failure output makes
// golden-test diffs readable when a shape drifts.
func assertPrivateEqual(t *testing.T, got, want []contract.EngineResult) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("private results mismatch\n  got:  %s\n  want: %s", formatPrivate(got), formatPrivate(want))
	}
}

func assertPublicEqual(t *testing.T, got, want []emit.TapeEvent) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("public events mismatch\n  got:  %s\n  want: %s", formatPublic(got), formatPublic(want))
	}
}

func formatPrivate(results []contract.EngineResult) string {
	if len(results) == 0 {
		return "(empty)"
	}
	buf := "["
	for i, result := range results {
		if i > 0 {
			buf += ", "
		}
		buf += formatResult(result)
	}
	return buf + "]"
}

func formatResult(result contract.EngineResult) string {
	base := "{Outcome=" + outcomeName(result.Outcome) + " OrderID=" + string(result.OrderID) + " BrokerID=" + string(result.BrokerID)
	if result.Match != nil {
		base += " Match=" + formatMatch(*result.Match)
	}
	if result.Rejection != nil {
		base += " Reject=" + rejectReasonName(result.Rejection.Reason) + "/" + result.Rejection.Stage
	}
	if result.CancelledQty != 0 {
		base += " CancelledQty=" + strconv.Itoa(int(result.CancelledQty))
	}
	return base + "}"
}

func formatMatch(match contract.MatchResult) string {
	buf := "{Trades=["
	for i, trade := range match.Trades {
		if i > 0 {
			buf += ", "
		}
		buf += "{Seq=" + strconv.Itoa(int(trade.Seq)) + " Price=" + strconv.Itoa(int(trade.Price)) + " Qty=" + strconv.Itoa(int(trade.Qty)) + "}"
	}
	buf += "]"
	if match.Rested != nil {
		buf += " Rested=" + string(match.Rested.OrderID) + "@" + strconv.Itoa(int(match.Rested.Price)) + "/" + strconv.Itoa(int(match.Rested.Remaining))
	}
	if match.LastPriceMoved {
		buf += " LastPrice=" + strconv.Itoa(int(match.LastPrice))
	}
	if match.CancelledQty != 0 {
		buf += " CancelledQty=" + strconv.Itoa(int(match.CancelledQty)) + " Reason=" + cancelReasonName(match.CancelReason)
	}
	return buf + "}"
}

func formatPublic(events []emit.TapeEvent) string {
	if len(events) == 0 {
		return "(empty)"
	}
	buf := "["
	for i, event := range events {
		if i > 0 {
			buf += ", "
		}
		buf += formatTapeEvent(event)
	}
	return buf + "]"
}

func formatTapeEvent(event emit.TapeEvent) string {
	prefix := "{Seq=" + strconv.Itoa(int(event.Seq)) + " " + string(event.Instrument) + " "
	switch event.Kind {
	case emit.KindTrade:
		return prefix + "Trade=" + strconv.Itoa(int(event.Trade.Price)) + "/" + strconv.Itoa(int(event.Trade.Qty)) + "}"
	case emit.KindQuote:
		return prefix + "Quote=" + formatQuote(*event.Quote) + "}"
	case emit.KindDepth:
		return prefix + "Depth=" + formatDepth(*event.Depth) + "}"
	default:
		return prefix + "Kind=" + strconv.Itoa(int(event.Kind)) + "}"
	}
}

func formatQuote(quote book.QuoteState) string {
	buf := "{"
	if quote.HasBid {
		buf += "Bid=" + strconv.Itoa(int(quote.BestBid)) + "/" + strconv.Itoa(int(quote.BidQty)) + " "
	}
	if quote.HasAsk {
		buf += "Ask=" + strconv.Itoa(int(quote.BestAsk)) + "/" + strconv.Itoa(int(quote.AskQty)) + " "
	}
	if quote.HasLastPrice {
		buf += "Last=" + strconv.Itoa(int(quote.LastPrice))
	}
	return buf + "}"
}

func formatDepth(depth emit.DepthSnapshot) string {
	buf := "{Bids=["
	for i, level := range depth.Bids {
		if i > 0 {
			buf += ", "
		}
		buf += strconv.Itoa(int(level.Price)) + "/" + strconv.Itoa(int(level.TotalQty))
	}
	buf += "] Asks=["
	for i, level := range depth.Asks {
		if i > 0 {
			buf += ", "
		}
		buf += strconv.Itoa(int(level.Price)) + "/" + strconv.Itoa(int(level.TotalQty))
	}
	return buf + "]}"
}

func outcomeName(outcome contract.Outcome) string {
	switch outcome {
	case contract.OutcomeMatched:
		return "Matched"
	case contract.OutcomeDiverted:
		return "Diverted"
	case contract.OutcomeRejected:
		return "Rejected"
	case contract.OutcomeCancelled:
		return "Cancelled"
	case contract.OutcomeCancelRejected:
		return "CancelRejected"
	case contract.OutcomeMakerFill:
		return "MakerFill"
	default:
		return "?"
	}
}

func rejectReasonName(reason contract.RejectReason) string {
	switch reason {
	case contract.RejectMalformed:
		return "Malformed"
	case contract.RejectMarketClosed:
		return "MarketClosed"
	case contract.RejectNoLiquidity:
		return "NoLiquidity"
	case contract.RejectBadTick:
		return "BadTick"
	case contract.RejectBeyondCircuit:
		return "BeyondCircuit"
	default:
		return "?"
	}
}

func cancelReasonName(reason contract.CancelReason) string {
	switch reason {
	case contract.ReasonNone:
		return "None"
	case contract.ReasonNoLiquidity:
		return "NoLiquidity"
	case contract.ReasonFundedCap:
		return "FundedCap"
	case contract.ReasonCircuitClamp:
		return "CircuitClamp"
	case contract.ReasonIOCLeftover:
		return "IOCLeftover"
	case contract.ReasonSelfMatch:
		return "SelfMatch"
	case contract.ReasonAuctionUnfilled:
		return "AuctionUnfilled"
	default:
		return "?"
	}
}

