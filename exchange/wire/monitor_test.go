package wire

import (
	"testing"
	"time"

	"simulator/exchange/clock"
)

// scheduleWithBoundaries returns a SessionSchedule for tests: Closed at
// t=0, Continuous at t=1s, Closed at t=2s. Small enough to drive with a
// few ticks; ends in Closed so NewSessionClock's terminal-phase assertion
// passes.
func scheduleWithBoundaries() clock.SessionSchedule {
	base := time.Unix(0, 0)
	return clock.SessionSchedule{
		Boundaries: []clock.PhaseBoundary{
			{At: base.Add(-time.Second), Phase: clock.PhaseClosed},
			{At: base.Add(1 * time.Second), Phase: clock.PhaseContinuous},
			{At: base.Add(2 * time.Second), Phase: clock.PhaseClosed},
		},
	}
}

// TestMonitor_BroadcastsOnTransition — driving the ticker past two
// boundaries produces exactly two control messages per shard.
func TestMonitor_BroadcastsOnTransition(t *testing.T) {
	sessionClock := clock.NewSessionClock(scheduleWithBoundaries())
	shardControl := make(chan controlMessage, 4)
	monitor := newMonitor(sessionClock, []chan<- controlMessage{shardControl})

	tickCh := make(chan time.Time, 3)
	monitor.ticker = tickCh

	done := make(chan struct{})
	go func() {
		defer close(done)
		monitor.run()
	}()

	base := time.Unix(0, 0)
	tickCh <- base                     // no transition (still Closed)
	tickCh <- base.Add(1500 * time.Millisecond) // crosses to Continuous
	tickCh <- base.Add(2500 * time.Millisecond) // crosses to Closed
	close(tickCh)
	<-done

	if len(shardControl) != 2 {
		t.Fatalf("shard control: got %d messages, want 2", len(shardControl))
	}
	first := <-shardControl
	if first.transition.From != clock.PhaseClosed || first.transition.To != clock.PhaseContinuous {
		t.Fatalf("first transition = %v→%v, want Closed→Continuous",
			first.transition.From, first.transition.To)
	}
	second := <-shardControl
	if second.transition.From != clock.PhaseContinuous || second.transition.To != clock.PhaseClosed {
		t.Fatalf("second transition = %v→%v, want Continuous→Closed",
			second.transition.From, second.transition.To)
	}
}

// TestMonitor_NoBroadcastWhenPhaseUnchanged — ticks that don't cross a
// boundary produce zero control messages.
func TestMonitor_NoBroadcastWhenPhaseUnchanged(t *testing.T) {
	sessionClock := clock.NewSessionClock(scheduleWithBoundaries())
	shardControl := make(chan controlMessage, 4)
	monitor := newMonitor(sessionClock, []chan<- controlMessage{shardControl})

	tickCh := make(chan time.Time, 3)
	monitor.ticker = tickCh

	done := make(chan struct{})
	go func() {
		defer close(done)
		monitor.run()
	}()

	base := time.Unix(0, 0)
	tickCh <- base                        // still Closed
	tickCh <- base.Add(-500 * time.Millisecond) // still Closed
	tickCh <- base                        // still Closed
	close(tickCh)
	<-done

	if len(shardControl) != 0 {
		t.Fatalf("shard control: got %d messages, want 0 (no boundary crossed)", len(shardControl))
	}
}

// TestMonitor_BroadcastsToAllShards — with multiple shards, every shard's
// control channel receives every transition, exactly once.
func TestMonitor_BroadcastsToAllShards(t *testing.T) {
	sessionClock := clock.NewSessionClock(scheduleWithBoundaries())
	const numShards = 3
	controls := make([]chan controlMessage, numShards)
	sendEnds := make([]chan<- controlMessage, numShards)
	for i := 0; i < numShards; i++ {
		controls[i] = make(chan controlMessage, 4)
		sendEnds[i] = controls[i]
	}
	monitor := newMonitor(sessionClock, sendEnds)

	tickCh := make(chan time.Time, 1)
	monitor.ticker = tickCh

	done := make(chan struct{})
	go func() {
		defer close(done)
		monitor.run()
	}()

	tickCh <- time.Unix(0, 0).Add(1500 * time.Millisecond) // Closed→Continuous
	close(tickCh)
	<-done

	for i := 0; i < numShards; i++ {
		if len(controls[i]) != 1 {
			t.Fatalf("shard %d control: got %d messages, want 1", i, len(controls[i]))
		}
		msg := <-controls[i]
		if msg.transition.To != clock.PhaseContinuous {
			t.Fatalf("shard %d transition To = %v, want PhaseContinuous", i, msg.transition.To)
		}
	}
}

// TestMonitor_ExitsWhenTickerCloses — closing the ticker terminates
// monitor.run() so no leaked goroutine.
func TestMonitor_ExitsWhenTickerCloses(t *testing.T) {
	sessionClock := clock.NewSessionClock(scheduleWithBoundaries())
	monitor := newMonitor(sessionClock, nil)
	tickCh := make(chan time.Time)
	monitor.ticker = tickCh

	done := make(chan struct{})
	go func() {
		defer close(done)
		monitor.run()
	}()

	close(tickCh)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("monitor.run did not exit within 1s of ticker close")
	}
}
