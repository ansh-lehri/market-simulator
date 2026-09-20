package wire

import (
	"testing"
	"time"

	"simulator/contract"
	"simulator/exchange/clock"
	"simulator/exchange/emit"
)

// TestStage5_TransitionFiresSessionEventPerInstrument — end-to-end
// exercise: build a two-shard exchange with four instruments, wire the
// real monitor to a hand-driven ticker, drive one tick that crosses
// Closed→Continuous, then shut down. Assert exactly four SessionOpened
// events land on the public tape (one per instrument), no duplicates.
func TestStage5_TransitionFiresSessionEventPerInstrument(t *testing.T) {
	specs := []InstrumentSpec{makeSpec("AAA"), makeSpec("BBB"), makeSpec("CCC"), makeSpec("DDD")}
	exchange := NewExchange(2, scheduleWithBoundaries(), specs)

	monitorTicker := make(chan time.Time, 1)
	exchange.monitor.ticker = monitorTicker

	// Start collectors.
	publicChan := make(chan []emit.TapeEvent, 1)
	privateChan := make(chan []contract.EngineResult, 1)
	go func() {
		var collected []emit.TapeEvent
		for event := range exchange.Public {
			collected = append(collected, event)
		}
		publicChan <- collected
	}()
	go func() {
		var collected []contract.EngineResult
		for result := range exchange.Private {
			collected = append(collected, result)
		}
		privateChan <- collected
	}()

	// Start every shard's run().
	shardDone := make([]chan struct{}, len(exchange.shards))
	for i, shard := range exchange.shards {
		i, shard := i, shard
		shardDone[i] = make(chan struct{})
		go func() {
			defer close(shardDone[i])
			shard.run()
		}()
	}

	// Start monitor.
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		exchange.monitor.run()
	}()

	// Drive one transition (Closed → Continuous) and let it propagate.
	monitorTicker <- time.Unix(0, 0).Add(1500 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	// Shutdown ordering (Stage 6 preview):
	// 1. Close monitor ticker → monitor exits.
	// 2. Close each shard inbound → shards exit.
	// 3. Close public+private → collectors exit.
	close(monitorTicker)
	<-monitorDone

	for _, shard := range exchange.shards {
		close(shard.inbound)
	}
	for _, done := range shardDone {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("shard did not exit within 1s of inbound close")
		}
	}
	close(exchange.public)
	close(exchange.private)
	publicEvents := <-publicChan
	<-privateChan

	// Assert exactly 4 SessionOpened events, one per instrument.
	sessionEvents := make(map[contract.InstrumentID]int)
	for _, event := range publicEvents {
		if event.Kind != emit.KindSession {
			continue
		}
		if event.Session.Kind != emit.SessionOpened {
			t.Fatalf("expected only SessionOpened events, got %v for %q", event.Session.Kind, event.Instrument)
		}
		sessionEvents[event.Instrument]++
	}
	wantInstruments := []contract.InstrumentID{"AAA", "BBB", "CCC", "DDD"}
	for _, id := range wantInstruments {
		count := sessionEvents[id]
		if count != 1 {
			t.Fatalf("instrument %q got %d SessionOpened events, want 1", id, count)
		}
	}
	if len(sessionEvents) != len(wantInstruments) {
		t.Fatalf("session events landed on %d instruments, want %d", len(sessionEvents), len(wantInstruments))
	}
}

// TestStage5_ClockNotAdvancedByCurrent — belt-and-braces: after the
// exchange is built, Current() alone returns PhaseClosed. Only the
// monitor's CheckTransition drives the phase field.
func TestStage5_ClockNotAdvancedByCurrent(t *testing.T) {
	exchange := NewExchange(1, scheduleWithBoundaries(), []InstrumentSpec{makeSpec("AAA")})
	for i := 0; i < 10; i++ {
		if got := exchange.clock.Current(); got != clock.PhaseClosed {
			t.Fatalf("Current() call %d = %v, want PhaseClosed (no monitor tick yet)", i, got)
		}
	}
}
