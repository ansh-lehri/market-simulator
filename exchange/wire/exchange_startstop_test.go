package wire

import (
	"sync"
	"testing"
	"time"

	"simulator/contract"
	"simulator/exchange/clock"
	"simulator/exchange/emit"
)

// scheduleActiveNow returns a schedule where PhaseContinuous is already
// active at time.Now() and closes an hour later. Tests that Start the real
// monitor use this so the very first tick observes the transition
// Closed→Continuous.
func scheduleActiveNow() clock.SessionSchedule {
	now := time.Now()
	return clock.SessionSchedule{
		Boundaries: []clock.PhaseBoundary{
			{At: now.Add(-time.Second), Phase: clock.PhaseContinuous},
			{At: now.Add(time.Hour), Phase: clock.PhaseClosed},
		},
	}
}

// startStopCollectors spawns drain goroutines on Public and Private that
// close their local slices when the exchange closes those channels
// (during Stop). Returns getters that safely read the collected slices
// after Stop has returned. Only safe to read after Stop.
func startStopCollectors(exchange *Exchange) (
	getPublic func() []emit.TapeEvent,
	getPrivate func() []contract.EngineResult,
	waitAllDrained func(),
) {
	var mu sync.Mutex
	var publicEvents []emit.TapeEvent
	var privateResults []contract.EngineResult
	var collectorsWG sync.WaitGroup
	collectorsWG.Add(2)
	go func() {
		defer collectorsWG.Done()
		for event := range exchange.Public {
			mu.Lock()
			publicEvents = append(publicEvents, event)
			mu.Unlock()
		}
	}()
	go func() {
		defer collectorsWG.Done()
		for result := range exchange.Private {
			mu.Lock()
			privateResults = append(privateResults, result)
			mu.Unlock()
		}
	}()
	getPublic = func() []emit.TapeEvent {
		mu.Lock()
		defer mu.Unlock()
		out := make([]emit.TapeEvent, len(publicEvents))
		copy(out, publicEvents)
		return out
	}
	getPrivate = func() []contract.EngineResult {
		mu.Lock()
		defer mu.Unlock()
		out := make([]contract.EngineResult, len(privateResults))
		copy(out, privateResults)
		return out
	}
	waitAllDrained = func() { collectorsWG.Wait() }
	return
}

// TestExchange_StartStop_HappyPath — Start with a fast tick, sleep briefly,
// Stop. The single assertion is that both calls return cleanly; the -race
// detector catches any concurrency bug.
func TestExchange_StartStop_HappyPath(t *testing.T) {
	exchange := NewExchange(2, scheduleActiveNow(), []InstrumentSpec{makeSpec("AAA"), makeSpec("BBB")})
	_, _, wait := startStopCollectors(exchange)

	exchange.Start(5 * time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	exchange.Stop()
	wait()
}

// TestExchange_Start_Idempotent — a second Start is a no-op. If it wasn't,
// we'd allocate a second set of tickers and leak goroutines; Stop would
// then wait on more Done calls than were registered (blocking forever).
func TestExchange_Start_Idempotent(t *testing.T) {
	exchange := NewExchange(1, scheduleActiveNow(), []InstrumentSpec{makeSpec("AAA")})
	_, _, wait := startStopCollectors(exchange)

	exchange.Start(5 * time.Millisecond)
	exchange.Start(5 * time.Millisecond) // no-op
	time.Sleep(20 * time.Millisecond)
	exchange.Stop()
	wait()
}

// TestExchange_Stop_Idempotent — a second Stop is a no-op. Without the
// sync.Once guard, we'd double-close monitorStop / inbound / public /
// private and panic.
func TestExchange_Stop_Idempotent(t *testing.T) {
	exchange := NewExchange(1, scheduleActiveNow(), []InstrumentSpec{makeSpec("AAA")})
	_, _, wait := startStopCollectors(exchange)

	exchange.Start(5 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	exchange.Stop()
	exchange.Stop() // no-op
	wait()
}

// TestExchange_StopWithoutStart — Stop on a fresh exchange must be a no-op,
// not a panic. Public/Private are left open (no writer ever started).
func TestExchange_StopWithoutStart(t *testing.T) {
	exchange := NewExchange(1, scheduleActiveNow(), []InstrumentSpec{makeSpec("AAA")})
	exchange.Stop() // must not panic
}

// TestExchange_SendAfterStopPanics — after Stop, sending on Inbound panics
// (send on closed channel). Documented contract; test locks it.
func TestExchange_SendAfterStopPanics(t *testing.T) {
	exchange := NewExchange(1, scheduleActiveNow(), []InstrumentSpec{makeSpec("AAA")})
	_, _, wait := startStopCollectors(exchange)

	exchange.Start(5 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	exchange.Stop()
	wait()

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatalf("expected panic on send after Stop")
		}
	}()
	exchange.Inbound <- contract.InboundMessage{
		Kind:       contract.SubmitOrder,
		BrokerID:   "B1",
		Instrument: "AAA",
		Submit:     &contract.FundedOrder{OrderID: "o-1", Instrument: "AAA"},
	}
}

// TestExchange_MonitorEmitsSessionOpenedFromRealClock — proof that the real
// monitor goroutine + real time.Ticker + real *SessionClock actually
// observe the transition and fan out. Uses a schedule with Continuous
// already active so the very first tick crosses Closed→Continuous.
func TestExchange_MonitorEmitsSessionOpenedFromRealClock(t *testing.T) {
	exchange := NewExchange(1, scheduleActiveNow(), []InstrumentSpec{makeSpec("AAA")})
	getPublic, _, wait := startStopCollectors(exchange)

	exchange.Start(5 * time.Millisecond)
	time.Sleep(50 * time.Millisecond) // more than enough for ≥1 tick
	exchange.Stop()
	wait()

	publicEvents := getPublic()
	sessionOpenedSeen := false
	for _, event := range publicEvents {
		if event.Kind == emit.KindSession && event.Session.Kind == emit.SessionOpened && event.Instrument == "AAA" {
			sessionOpenedSeen = true
			break
		}
	}
	if !sessionOpenedSeen {
		t.Fatalf("SessionOpened event for AAA not seen on public tape; got %d events", len(publicEvents))
	}
}
