package emit

import (
	"reflect"
	"testing"

	"simulator/contract"
	"simulator/exchange/book"
)

// newTestEmitter builds an Emitter wired to buffered channels the tests then
// drain. Buffer size is set per scenario so no goroutines are needed.
func newTestEmitter(instrument contract.InstrumentID, publicBuf, privateBuf int) (*Emitter, chan TapeEvent, chan contract.EngineResult) {
	publicTape := make(chan TapeEvent, publicBuf)
	privateStream := make(chan contract.EngineResult, privateBuf)
	return NewEmitter(instrument, publicTape, privateStream), publicTape, privateStream
}

func TestNewEmitter_NilPublicTape_Panics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on nil publicTape")
		}
	}()
	privateStream := make(chan contract.EngineResult, 1)
	NewEmitter("ACME", nil, privateStream)
}

func TestNewEmitter_NilPrivateStream_Panics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on nil privateStream")
		}
	}()
	publicTape := make(chan TapeEvent, 1)
	NewEmitter("ACME", publicTape, nil)
}

func TestNext_MonotonicFromOne(t *testing.T) {
	emitter, _, _ := newTestEmitter("ACME", 0, 0)
	for want := contract.Seq(1); want <= 5; want++ {
		if got := emitter.Next(); got != want {
			t.Fatalf("Next() call %d: got %d, want %d", want, got, want)
		}
	}
}

func TestNext_PerInstrumentIndependent(t *testing.T) {
	acme, _, _ := newTestEmitter("ACME", 0, 0)
	widget, _, _ := newTestEmitter("WIDGET", 0, 0)

	acme.Next()
	acme.Next()
	acme.Next()

	if got := widget.Next(); got != 1 {
		t.Fatalf("second emitter's Next() should start at 1 (per-instrument counter); got %d", got)
	}
	if got := acme.Next(); got != 4 {
		t.Fatalf("first emitter's Next() should have advanced to 4; got %d", got)
	}
}

// EmitTrades must reuse each Trade.Seq set by the matcher/auction at creation
// — invariant 4. The per-instrument event counter must NOT advance for
// trade-print emission, since the trade already reserved its slot when it was
// stamped via Next() at trade creation.
func TestEmitTrades_ReusesTradeSeqAndDoesNotAdvanceCounter(t *testing.T) {
	emitter, publicTape, _ := newTestEmitter("ACME", 3, 0)

	trades := []contract.Trade{
		{Seq: 7, Instrument: "ACME", Price: 10023, Qty: 5, TakerSide: contract.Buy},
		{Seq: 8, Instrument: "ACME", Price: 10024, Qty: 3, TakerSide: contract.Buy},
		{Seq: 9, Instrument: "ACME", Price: 10025, Qty: 2, TakerSide: contract.Buy},
	}
	emitter.EmitTrades(trades)

	wantSeqs := []contract.Seq{7, 8, 9}
	for index, want := range wantSeqs {
		event := <-publicTape
		if event.Seq != want {
			t.Fatalf("event %d: tape Seq %d, want %d (must reuse Trade.Seq)", index, event.Seq, want)
		}
		if event.Kind != KindTrade {
			t.Fatalf("event %d: Kind %d, want KindTrade", index, event.Kind)
		}
	}

	if got := emitter.Next(); got != 1 {
		t.Fatalf("counter should not advance during EmitTrades; first Next() after emit returned %d, want 1", got)
	}
}

// The public tape carries no participant identity — invariant 1. Trades
// carrying non-empty participant ids must emit a PublicTrade that exposes only
// Price and Qty; comparing to a fresh PublicTrade{Price, Qty} proves the
// emitted value has no other populated field (there is nowhere for the
// participant string to hide in PublicTrade's type).
func TestEmitTrades_AnonymizesPublicPrint(t *testing.T) {
	emitter, publicTape, _ := newTestEmitter("ACME", 1, 0)

	trade := contract.Trade{
		Seq:              42,
		Instrument:       "ACME",
		Price:            10023,
		Qty:              7,
		TakerOrder:       "ord-taker",
		MakerOrder:       "ord-maker",
		TakerSide:        contract.Buy,
		TakerParticipant: "trader-alpha",
		MakerParticipant: "trader-beta",
	}
	emitter.EmitTrades([]contract.Trade{trade})

	event := <-publicTape
	if event.Trade == nil {
		t.Fatal("Trade payload nil")
	}
	want := PublicTrade{Price: 10023, Qty: 7}
	if *event.Trade != want {
		t.Fatalf("PublicTrade = %+v, want %+v (participant fields must not leak)", *event.Trade, want)
	}
	// The Instrument field on the envelope is the routing key, not a
	// participant id — must be set.
	if event.Instrument != "ACME" {
		t.Fatalf("event.Instrument = %q, want ACME", event.Instrument)
	}
}

func TestEmitTrades_EmptySlice_NoSends(t *testing.T) {
	emitter, publicTape, _ := newTestEmitter("ACME", 1, 0)

	emitter.EmitTrades(nil)
	emitter.EmitTrades([]contract.Trade{})

	select {
	case unexpected := <-publicTape:
		t.Fatalf("unexpected tape event on empty EmitTrades: %+v", unexpected)
	default:
	}
	if got := emitter.Next(); got != 1 {
		t.Fatalf("counter should be untouched; got %d, want 1", got)
	}
}

// N trades all at the same price mirror the auction clearing shape — every
// print at P*, Seqs already stamped by AuctionBook.Clear. Emit treats them
// identically to continuous trades.
func TestEmitTrades_UniformPrice_AuctionShape(t *testing.T) {
	emitter, publicTape, _ := newTestEmitter("ACME", 4, 0)

	clearingPrice := contract.Ticks(10050)
	trades := []contract.Trade{
		{Seq: 20, Instrument: "ACME", Price: clearingPrice, Qty: 5, TakerSide: contract.Buy},
		{Seq: 21, Instrument: "ACME", Price: clearingPrice, Qty: 3, TakerSide: contract.Buy},
		{Seq: 22, Instrument: "ACME", Price: clearingPrice, Qty: 8, TakerSide: contract.Buy},
		{Seq: 23, Instrument: "ACME", Price: clearingPrice, Qty: 1, TakerSide: contract.Buy},
	}
	emitter.EmitTrades(trades)

	for index, source := range trades {
		event := <-publicTape
		if event.Seq != source.Seq {
			t.Fatalf("event %d: Seq %d, want %d", index, event.Seq, source.Seq)
		}
		if event.Trade.Price != clearingPrice {
			t.Fatalf("event %d: Price %d, want %d", index, event.Trade.Price, clearingPrice)
		}
		if event.Trade.Qty != source.Qty {
			t.Fatalf("event %d: Qty %d, want %d", index, event.Trade.Qty, source.Qty)
		}
	}
}

func TestEmitQuote_KindSeqAndPayload(t *testing.T) {
	emitter, publicTape, _ := newTestEmitter("ACME", 1, 0)

	quote := book.QuoteState{
		BestBid: 10020, BidQty: 100, HasBid: true,
		BestAsk: 10023, AskQty: 50, HasAsk: true,
		LastPrice: 10022, HasLastPrice: true,
	}
	emitter.EmitQuote(quote)

	event := <-publicTape
	if event.Kind != KindQuote {
		t.Fatalf("Kind %d, want KindQuote", event.Kind)
	}
	if event.Seq != 1 {
		t.Fatalf("Seq %d, want 1", event.Seq)
	}
	if event.Quote == nil {
		t.Fatal("Quote payload nil")
	}
	if *event.Quote != quote {
		t.Fatalf("Quote payload = %+v, want %+v", *event.Quote, quote)
	}
	if event.Trade != nil || event.Depth != nil || event.Candle != nil || event.Session != nil {
		t.Fatalf("only Quote field must be populated; got %+v", event)
	}
	if got := emitter.Next(); got != 2 {
		t.Fatalf("counter should be at 2 after EmitQuote; Next() returned %d", got)
	}
}

func TestEmitDepth_KindSeqAndPayload(t *testing.T) {
	emitter, publicTape, _ := newTestEmitter("ACME", 1, 0)

	depth := DepthSnapshot{
		Bids: []DepthLevel{{Price: 10020, TotalQty: 100}, {Price: 10019, TotalQty: 50}},
		Asks: []DepthLevel{{Price: 10023, TotalQty: 40}, {Price: 10024, TotalQty: 60}},
	}
	emitter.EmitDepth(depth)

	event := <-publicTape
	if event.Kind != KindDepth {
		t.Fatalf("Kind %d, want KindDepth", event.Kind)
	}
	if event.Seq != 1 {
		t.Fatalf("Seq %d, want 1", event.Seq)
	}
	if event.Depth == nil {
		t.Fatal("Depth payload nil")
	}
	if !reflect.DeepEqual(*event.Depth, depth) {
		t.Fatalf("Depth payload = %+v, want %+v", *event.Depth, depth)
	}
	if event.Trade != nil || event.Quote != nil || event.Candle != nil || event.Session != nil {
		t.Fatalf("only Depth field must be populated; got %+v", event)
	}
}

// EmitCandle stamps the candle's Seq from the emitter's counter and forwards
// the same Seq on the envelope, so a consumer sees one number for both.
func TestEmitCandle_StampsSeqAndMatchesEnvelope(t *testing.T) {
	emitter, publicTape, _ := newTestEmitter("ACME", 1, 0)

	candle := Candle{
		Instrument: "ACME",
		Open:       100, High: 110, Low: 95, Close: 105,
		Volume: 500,
	}
	emitter.EmitCandle(candle)

	event := <-publicTape
	if event.Kind != KindCandle {
		t.Fatalf("Kind %d, want KindCandle", event.Kind)
	}
	if event.Seq != 1 {
		t.Fatalf("envelope Seq %d, want 1", event.Seq)
	}
	if event.Candle == nil {
		t.Fatal("Candle payload nil")
	}
	if event.Candle.Seq != 1 {
		t.Fatalf("candle.Seq %d, want 1 (must equal envelope Seq)", event.Candle.Seq)
	}
	if event.Candle.Seq != event.Seq {
		t.Fatalf("candle.Seq (%d) must equal envelope Seq (%d)", event.Candle.Seq, event.Seq)
	}
}

func TestEmitSession_AllKinds(t *testing.T) {
	emitter, publicTape, _ := newTestEmitter("ACME", 3, 0)

	kinds := []SessionEventKind{SessionOpened, AuctionWindowOpened, SessionClosed}
	for _, kind := range kinds {
		emitter.EmitSession(kind)
	}

	for index, wantKind := range kinds {
		event := <-publicTape
		if event.Kind != KindSession {
			t.Fatalf("event %d: Kind %d, want KindSession", index, event.Kind)
		}
		if event.Seq != contract.Seq(index+1) {
			t.Fatalf("event %d: Seq %d, want %d", index, event.Seq, index+1)
		}
		if event.Session == nil {
			t.Fatalf("event %d: Session payload nil", index)
		}
		if event.Session.Kind != wantKind {
			t.Fatalf("event %d: SessionEvent.Kind %d, want %d", index, event.Session.Kind, wantKind)
		}
	}
}

// EmitResult is a byte-for-byte pass-through. Cover all six Outcomes on the
// contract so the auction path (which reaches this via a shard-wrapped
// EngineResult{OutcomeCancelled}) is exercised alongside the continuous
// outcomes.
func TestEmitResult_PassThroughAcrossAllOutcomes(t *testing.T) {
	cases := []struct {
		name   string
		result contract.EngineResult
	}{
		{
			name: "OutcomeMatched",
			result: contract.EngineResult{
				Outcome:  contract.OutcomeMatched,
				OrderID:  "ord-1",
				BrokerID: "brk-1",
				Match: &contract.MatchResult{
					Trades:         []contract.Trade{{Seq: 1, Price: 100, Qty: 3}},
					LastPrice:      100,
					LastPriceMoved: true,
				},
			},
		},
		{
			name: "OutcomeDiverted",
			result: contract.EngineResult{
				Outcome: contract.OutcomeDiverted, OrderID: "ord-2", BrokerID: "brk-1",
			},
		},
		{
			name: "OutcomeRejected",
			result: contract.EngineResult{
				Outcome: contract.OutcomeRejected, OrderID: "ord-3", BrokerID: "brk-1",
				Rejection: &contract.Rejection{Reason: contract.RejectMalformed},
			},
		},
		{
			name: "OutcomeCancelled (auction unfilled — shard wraps Cancelled here)",
			result: contract.EngineResult{
				Outcome: contract.OutcomeCancelled, OrderID: "ord-4", BrokerID: "brk-1",
				CancelledQty: 15,
			},
		},
		{
			name: "OutcomeCancelRejected",
			result: contract.EngineResult{
				Outcome: contract.OutcomeCancelRejected, OrderID: "ord-5", BrokerID: "brk-1",
			},
		},
		{
			name: "OutcomeMakerFill",
			result: contract.EngineResult{
				Outcome: contract.OutcomeMakerFill, OrderID: "ord-6", BrokerID: "brk-1",
				Match: &contract.MatchResult{
					Trades:         []contract.Trade{{Seq: 2, Price: 99, Qty: 4}},
					LastPrice:      99,
					LastPriceMoved: true,
				},
			},
		},
	}

	emitter, _, privateStream := newTestEmitter("ACME", 0, len(cases))
	for _, tc := range cases {
		emitter.EmitResult(tc.result)
	}
	for _, tc := range cases {
		got := <-privateStream
		if !reflect.DeepEqual(got, tc.result) {
			t.Fatalf("%s: received %+v, want %+v", tc.name, got, tc.result)
		}
	}
}

// EmitResult must not touch the public tape or advance the per-instrument
// event counter — the private stream and the public sequence are independent
// concerns.
func TestEmitResult_DoesNotTouchTapeOrCounter(t *testing.T) {
	emitter, publicTape, privateStream := newTestEmitter("ACME", 1, 1)

	emitter.EmitResult(contract.EngineResult{
		Outcome: contract.OutcomeRejected, OrderID: "ord-x", BrokerID: "brk-1",
		Rejection: &contract.Rejection{Reason: contract.RejectMalformed},
	})
	<-privateStream

	select {
	case unexpected := <-publicTape:
		t.Fatalf("EmitResult must not write to publicTape; got %+v", unexpected)
	default:
	}
	if got := emitter.Next(); got != 1 {
		t.Fatalf("EmitResult must not advance counter; Next() returned %d, want 1", got)
	}
}
