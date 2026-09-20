package wire

import (
	"testing"
	"time"

	"simulator/contract"
	"simulator/exchange/emit"
)

// TestVerticalSlice_MarketBuyFullyMatches — Flow A §9.1 through the real
// lifecycle API (Start/Stop). Not a bit-exact golden — the SessionOpened
// event from the real monitor may or may not interleave with the trade
// events. We assert presence + shape of every required event, not their
// exact ordering.
func TestVerticalSlice_MarketBuyFullyMatches(t *testing.T) {
	exchange := NewExchange(1, scheduleActiveNow(), []InstrumentSpec{makeSpec("ACME")})

	// Seed book BEFORE Start — pre-start state access is safe (no
	// goroutines are running yet).
	shard := exchange.shards[hashInstrument("ACME")%uint32(len(exchange.shards))]
	shard.instrs["ACME"].book.Insert(&contract.RestingOrder{
		OrderID:     "o-maker",
		Participant: "p-maker",
		Side:        contract.Sell,
		Price:       10000,
		Remaining:   60,
		Seq:         1,
	})

	getPublic, getPrivate, wait := startStopCollectors(exchange)

	exchange.Start(5 * time.Millisecond)
	// Give the monitor one tick's worth to fire SessionOpened before the
	// order lands (so a downstream broker would see the session opened
	// notification first in typical timing).
	time.Sleep(20 * time.Millisecond)

	exchange.Inbound <- inboundSubmit("B1", &contract.FundedOrder{
		OrderID:     "o-42",
		Participant: "p-taker",
		Instrument:  "ACME",
		Side:        contract.Buy,
		Type:        contract.Market,
		Qty:         60,
		MaxSpend:    100_000_000,
	})
	time.Sleep(20 * time.Millisecond)

	exchange.Stop()
	wait()

	publicEvents := getPublic()
	privateResults := getPrivate()

	// --- private assertions ---
	if len(privateResults) != 1 {
		t.Fatalf("private results: got %d, want 1", len(privateResults))
	}
	result := privateResults[0]
	if result.Outcome != contract.OutcomeMatched {
		t.Fatalf("Outcome = %v, want OutcomeMatched", result.Outcome)
	}
	if result.OrderID != "o-42" || result.BrokerID != "B1" {
		t.Fatalf("OrderID/BrokerID = %q/%q, want o-42/B1", result.OrderID, result.BrokerID)
	}
	if result.Match == nil || len(result.Match.Trades) != 1 {
		t.Fatalf("Match/Trades = %+v, want one trade", result.Match)
	}
	trade := result.Match.Trades[0]
	if trade.Price != 10000 || trade.Qty != 60 || trade.MakerOrder != "o-maker" {
		t.Fatalf("Trade = %+v, want Price=10000 Qty=60 MakerOrder=o-maker", trade)
	}

	// --- public assertions: category counts, not exact order ---
	var sessionOpened, trades, quotes, depths int
	for _, event := range publicEvents {
		switch event.Kind {
		case emit.KindSession:
			if event.Session.Kind == emit.SessionOpened {
				sessionOpened++
			}
		case emit.KindTrade:
			if event.Trade.Price == 10000 && event.Trade.Qty == 60 {
				trades++
			}
		case emit.KindQuote:
			quotes++
		case emit.KindDepth:
			depths++
		}
	}
	if sessionOpened < 1 {
		t.Fatalf("SessionOpened count = %d, want >= 1", sessionOpened)
	}
	if trades != 1 {
		t.Fatalf("Trade count = %d, want 1", trades)
	}
	if quotes < 1 {
		t.Fatalf("Quote count = %d, want >= 1 (post-match snapshot)", quotes)
	}
	if depths < 1 {
		t.Fatalf("Depth count = %d, want >= 1 (post-match snapshot)", depths)
	}
}

// TestVerticalSlice_CancelPathThroughRealAPI — Flow C through Start/Stop.
// Seed a resting bid, cancel it, assert Cancelled result on private and
// Quote/Depth on public.
func TestVerticalSlice_CancelPathThroughRealAPI(t *testing.T) {
	exchange := NewExchange(1, scheduleActiveNow(), []InstrumentSpec{makeSpec("ACME")})

	shard := exchange.shards[hashInstrument("ACME")%uint32(len(exchange.shards))]
	shard.instrs["ACME"].book.Insert(&contract.RestingOrder{
		OrderID:     "o-99",
		Participant: "p-1",
		Side:        contract.Buy,
		Price:       9900,
		Remaining:   100,
		Seq:         1,
	})

	getPublic, getPrivate, wait := startStopCollectors(exchange)

	exchange.Start(5 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	exchange.Inbound <- inboundCancel("B1", "ACME", "o-99")
	time.Sleep(20 * time.Millisecond)

	exchange.Stop()
	wait()

	publicEvents := getPublic()
	privateResults := getPrivate()

	if len(privateResults) != 1 {
		t.Fatalf("private results: got %d, want 1", len(privateResults))
	}
	result := privateResults[0]
	if result.Outcome != contract.OutcomeCancelled {
		t.Fatalf("Outcome = %v, want OutcomeCancelled", result.Outcome)
	}
	if result.OrderID != "o-99" || result.BrokerID != "B1" {
		t.Fatalf("OrderID/BrokerID = %q/%q, want o-99/B1", result.OrderID, result.BrokerID)
	}
	if result.CancelledQty != 100 {
		t.Fatalf("CancelledQty = %d, want 100", result.CancelledQty)
	}

	var quotes, depths int
	for _, event := range publicEvents {
		switch event.Kind {
		case emit.KindQuote:
			quotes++
		case emit.KindDepth:
			depths++
		}
	}
	if quotes < 1 {
		t.Fatalf("Quote count = %d, want >= 1 (post-cancel snapshot)", quotes)
	}
	if depths < 1 {
		t.Fatalf("Depth count = %d, want >= 1 (post-cancel snapshot)", depths)
	}
}
