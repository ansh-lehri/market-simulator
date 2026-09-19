package wire

import (
	"testing"
	"time"

	"simulator/contract"
	"simulator/exchange/emit"
	"simulator/exchange/gate"
)

// makeSpec returns a well-formed InstrumentSpec for tests. Reference and
// BandBps are chosen so a limit at Reference is well inside the band.
func makeSpec(instrument contract.InstrumentID) InstrumentSpec {
	return InstrumentSpec{
		Instrument:    instrument,
		Reference:     10000,
		BandBps:       1000,
		Rules:         gate.Rulebook{TickSize: 1},
		OHLCVInterval: time.Minute,
		OHLCVStart:    time.Unix(0, 0),
	}
}

// newTestState builds an InstrumentState with buffered outbound channels so
// tests that trigger an emit don't block. Buffer size is well above anything
// a single test drives.
func newTestState(t *testing.T) *InstrumentState {
	t.Helper()
	publicTape := make(chan emit.TapeEvent, 64)
	privateStream := make(chan contract.EngineResult, 64)
	return newInstrumentState(makeSpec("ACME"), publicTape, privateStream)
}

// TestNewInstrumentState_AdmitSeqDistinctFromEventSeq confirms the two
// per-instrument counters (EXCHANGE_10 §10 rule 5) are independent: draining
// one does not advance the other.
func TestNewInstrumentState_AdmitSeqDistinctFromEventSeq(t *testing.T) {
	state := newTestState(t)
	for step := 1; step <= 3; step++ {
		if drawn := state.admitSeqSource.Next(); drawn != contract.Seq(step) {
			t.Fatalf("admitSeqSource.Next() step %d = %d, want %d", step, drawn, step)
		}
	}
	for step := 1; step <= 3; step++ {
		if drawn := state.emitter.Next(); drawn != contract.Seq(step) {
			t.Fatalf("emitter.Next() step %d = %d, want %d", step, drawn, step)
		}
	}
}

// TestNewInstrumentState_AdmitSeqSourcePointsAtField locks the pointer-
// stability contract admitSeqCounter relies on: Next() must mutate the
// admitSeq field on InstrumentState, not a stale copy.
func TestNewInstrumentState_AdmitSeqSourcePointsAtField(t *testing.T) {
	state := newTestState(t)
	for step := 0; step < 5; step++ {
		state.admitSeqSource.Next()
	}
	if state.admitSeq != 5 {
		t.Fatalf("state.admitSeq = %d after five Next() calls, want 5 — pointer stability broken", state.admitSeq)
	}
}

// TestNewInstrumentState_MatcherUsesEmitterAsSeqSource proves the matcher is
// wired to the emitter (not to admitSeqSource) at construction time. A market
// buy against a seeded ask produces one trade; Trade.Seq must be 1 (the
// emitter's first Next()), and admitSeq must not have advanced.
func TestNewInstrumentState_MatcherUsesEmitterAsSeqSource(t *testing.T) {
	state := newTestState(t)
	resting := &contract.RestingOrder{
		OrderID:     "maker-1",
		Participant: "maker-participant",
		Side:        contract.Sell,
		Price:       10000,
		Remaining:   10,
		Seq:         1,
	}
	state.book.Insert(resting)

	taker := &contract.FundedOrder{
		OrderID:     "taker-1",
		Participant: "taker-participant",
		Side:        contract.Buy,
		Type:        contract.Market,
		Qty:         5,
		MaxSpend:    1_000_000,
	}
	result := state.matcher.Match(taker, "B1", 1)

	if len(result.Trades) != 1 {
		t.Fatalf("expected 1 trade, got %d", len(result.Trades))
	}
	if result.Trades[0].Seq != 1 {
		t.Fatalf("Trade.Seq = %d, want 1 (from emitter's first Next())", result.Trades[0].Seq)
	}
	if state.admitSeq != 0 {
		t.Fatalf("state.admitSeq = %d, want 0 — matcher must not consume the admit-side counter", state.admitSeq)
	}
}

// TestNewInstrumentState_BandSharedBetweenBoundsAndClamp verifies that
// state.band answers both surfaces (Bounds and PriceAllowed) consistently
// from the same object — the EXCHANGE_15 §6 invariant 1 that admit-side and
// clamp-side cannot diverge.
func TestNewInstrumentState_BandSharedBetweenBoundsAndClamp(t *testing.T) {
	state := newTestState(t)
	lower, upper := state.band.Bounds()

	for _, price := range []contract.Ticks{lower, (lower + upper) / 2, upper} {
		if !state.band.PriceAllowed(price) {
			t.Fatalf("PriceAllowed(%d) = false, but price is inside Bounds [%d, %d]", price, lower, upper)
		}
	}
	if state.band.PriceAllowed(lower - 1) {
		t.Fatalf("PriceAllowed(%d) = true, but lower is %d", lower-1, lower)
	}
	if state.band.PriceAllowed(upper + 1) {
		t.Fatalf("PriceAllowed(%d) = true, but upper is %d", upper+1, upper)
	}
}

// TestNewInstrumentState_DeferredAuctionWired confirms the stand-in reaches
// gate-side callers through the auction field: a routed order panics with
// the deferred-auction message, not a nil-pointer dereference.
func TestNewInstrumentState_DeferredAuctionWired(t *testing.T) {
	state := newTestState(t)
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatalf("state.auction.Add did not panic — deferred auction not wired")
		}
	}()
	order := &contract.FundedOrder{OrderID: "o-1"}
	state.auction.Add(order, 1, "B1")
}

// TestNewInstrumentState_RulesAndInstrumentEchoed sanity-checks the tiny
// pass-through fields.
func TestNewInstrumentState_RulesAndInstrumentEchoed(t *testing.T) {
	state := newTestState(t)
	if state.instrument != "ACME" {
		t.Fatalf("instrument = %q, want %q", state.instrument, "ACME")
	}
	if state.rules.TickSize != 1 {
		t.Fatalf("rules.TickSize = %d, want 1", state.rules.TickSize)
	}
}
