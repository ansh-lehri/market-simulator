package wire

import (
	"testing"
	"time"

	"simulator/contract"
	"simulator/exchange/clock"
	"simulator/exchange/emit"
)

// TestHandleControl_EmitsSessionEventPerInstrument — a session-transition
// control message emits one SessionEvent on every instrument's emitter in
// sorted-instrument order.
func TestHandleControl_EmitsSessionEventPerInstrument(t *testing.T) {
	exchange := buildShardWithInstruments(t, "AAA", "BBB", "CCC")
	shard := exchange.shards[0]

	publicEvents, _ := runShardOnly(t, exchange, func() {
		shard.control <- controlMessage{
			kind: controlSessionTransition,
			transition: clock.PhaseTransition{
				From: clock.PhaseClosed,
				To:   clock.PhaseContinuous,
				At:   time.Unix(0, 0),
			},
		}
	})

	if len(publicEvents) != 3 {
		t.Fatalf("public events: got %d, want 3 (one SessionEvent per instrument)", len(publicEvents))
	}
	wantOrder := []contract.InstrumentID{"AAA", "BBB", "CCC"}
	for i, event := range publicEvents {
		if event.Kind != emit.KindSession {
			t.Fatalf("public[%d].Kind = %v, want KindSession", i, event.Kind)
		}
		if event.Instrument != wantOrder[i] {
			t.Fatalf("public[%d].Instrument = %q, want %q (sorted iteration)",
				i, event.Instrument, wantOrder[i])
		}
		if event.Session.Kind != emit.SessionOpened {
			t.Fatalf("public[%d].Session.Kind = %v, want SessionOpened", i, event.Session.Kind)
		}
	}
}

// TestHandleControl_UnknownPhaseIgnored — a transition to an unmapped
// phase (a Phase value outside the enum) emits nothing rather than
// panicking or producing a garbage event.
func TestHandleControl_UnknownPhaseIgnored(t *testing.T) {
	exchange := buildShardWithInstruments(t, "AAA")
	shard := exchange.shards[0]

	publicEvents, _ := runShardOnly(t, exchange, func() {
		shard.control <- controlMessage{
			kind: controlSessionTransition,
			transition: clock.PhaseTransition{
				From: clock.PhaseClosed,
				To:   clock.Phase(999), // outside the enum
				At:   time.Unix(0, 0),
			},
		}
	})

	if len(publicEvents) != 0 {
		t.Fatalf("public events: got %d, want 0 (unmappable phase should emit nothing)", len(publicEvents))
	}
}

// TestSessionEventKindFor — the mapping table.
func TestSessionEventKindFor(t *testing.T) {
	cases := []struct {
		phase   clock.Phase
		want    emit.SessionEventKind
		mapOK   bool
		comment string
	}{
		{clock.PhaseContinuous, emit.SessionOpened, true, "continuous → opened"},
		{clock.PhaseClosingAuction, emit.AuctionWindowOpened, true, "closing-auction → auction window opened"},
		{clock.PhaseClosed, emit.SessionClosed, true, "closed → closed"},
		{clock.Phase(42), 0, false, "unknown → not mappable"},
	}
	for _, tc := range cases {
		got, ok := sessionEventKindFor(tc.phase)
		if ok != tc.mapOK {
			t.Fatalf("%s: mappable = %v, want %v", tc.comment, ok, tc.mapOK)
		}
		if ok && got != tc.want {
			t.Fatalf("%s: kind = %v, want %v", tc.comment, got, tc.want)
		}
	}
}

// TestHandleControl_PanicsOnUnknownControlKind — invariant violation:
// wire must not deliver a controlMessage with a kind the shard cannot
// dispatch. The panic makes any future extension gap loud.
func TestHandleControl_PanicsOnUnknownControlKind(t *testing.T) {
	exchange := buildShardWithInstruments(t, "AAA")
	shard := exchange.shards[0]

	panicked := make(chan any, 1)
	shardDone := make(chan struct{})
	go func() {
		defer close(shardDone)
		defer func() { panicked <- recover() }()
		shard.run()
	}()

	shard.control <- controlMessage{kind: controlKind(42)}

	<-shardDone
	recovered := <-panicked
	if recovered == nil {
		t.Fatalf("shard.run did not panic on unknown controlKind")
	}
}
