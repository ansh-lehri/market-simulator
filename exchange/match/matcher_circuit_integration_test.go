package match

import (
	"testing"

	"simulator/contract"
	"simulator/exchange/book"
	"simulator/exchange/circuit"
)

// Consumer-side compile-time assertion. The matcher declares the CircuitClamp
// contract (see matcher.go); circuit.CircuitBand is our chosen provider. If
// circuit ever changes PriceAllowed's signature in a way that no longer fits
// our expectation, the build fails here — surfacing the drift at compile
// time instead of at wiring time. EXCHANGE_12 §10 + EXCHANGE_15 §2.
var _ CircuitClamp = (*circuit.CircuitBand)(nil)

// Wires a real *circuit.CircuitBand into the matcher and runs a walk that
// must halt on the clamp. Proves the two components connect end-to-end, not
// just that their types agree. Distinct from G8 (which uses the test-only
// bandedClamp stub) — this test's purpose is the runtime wiring, not the
// clamp semantics themselves.
func TestMatcher_UsesRealCircuitBand_HaltsOutOfBand(t *testing.T) {
	band := circuit.NewCircuitBand(10000, 100) // ±1% → [9900, 10100]

	orderBook := book.NewBook("ACME")
	orderBook.Insert(&contract.RestingOrder{
		OrderID: "M1", Participant: "P_M1", BrokerID: "B_M",
		Side: contract.Sell, Price: 10000, Remaining: 5, Seq: 1,
	})
	orderBook.Insert(&contract.RestingOrder{
		OrderID: "M2", Participant: "P_M2", BrokerID: "B_M",
		Side: contract.Sell, Price: 10200, Remaining: 5, Seq: 2, // above upper=10100
	})

	matcher := NewMatcher(orderBook, band, &monotonicSeq{})
	order := &contract.FundedOrder{
		OrderID:     "T",
		Participant: "P_T",
		Instrument:  "ACME",
		Side:        contract.Buy,
		Type:        contract.Market,
		TIF:         contract.IOC,
		Qty:         10,
		MaxSpend:    1_000_000,
	}

	result := matcher.Match(order, "B_T", 100)

	if len(result.Trades) != 1 {
		t.Fatalf("Trades: got %d, want 1 (only the in-band level should print)", len(result.Trades))
	}
	if result.Trades[0].Price != 10000 || result.Trades[0].Qty != 5 {
		t.Errorf("trade[0]: got price=%d qty=%d, want price=10000 qty=5",
			result.Trades[0].Price, result.Trades[0].Qty)
	}
	if result.Rested != nil {
		t.Errorf("Rested: got %+v, want nil (market never rests)", result.Rested)
	}
	if result.CancelledQty != 5 {
		t.Errorf("CancelledQty: got %d, want 5", result.CancelledQty)
	}
	if result.CancelReason != contract.ReasonCircuitClamp {
		t.Errorf("CancelReason: got %v, want ReasonCircuitClamp", result.CancelReason)
	}
	if !result.LastPriceMoved || result.LastPrice != 10000 {
		t.Errorf("LastPrice: got (%d, moved=%v), want (10000, true)",
			result.LastPrice, result.LastPriceMoved)
	}

	// The out-of-band maker (M2) must still be resting untouched — the walk
	// halted BEFORE any print at that level.
	quote := orderBook.Quote()
	if !quote.HasAsk || quote.BestAsk != 10200 || quote.AskQty != 5 {
		t.Errorf("M2 must be intact: got (has=%v price=%d qty=%d), want (true, 10200, 5)",
			quote.HasAsk, quote.BestAsk, quote.AskQty)
	}
}
