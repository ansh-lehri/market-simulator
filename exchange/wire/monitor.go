package wire

import (
	"log/slog"
	"time"

	"simulator/exchange/clock"
)

// Monitor is the single owner of clock.CheckTransition (EXCHANGE_10 §7,
// EXCHANGE_18 §7 invariant 2). It ticks on its own ticker, asks the clock
// whether a phase boundary was crossed, and — if so — broadcasts one
// controlMessage to every shard so each can fan the transition out to its
// instruments' emitters.
//
// Sole caller of CheckTransition is what keeps the phase's atomic.Int32 a
// one-writer/many-readers structure; do not add another call site.
type Monitor struct {
	sessionClock *clock.SessionClock
	controlOut   []chan<- controlMessage
	ticker       <-chan time.Time
}

func newMonitor(sessionClock *clock.SessionClock, controlOut []chan<- controlMessage) *Monitor {
	return &Monitor{
		sessionClock: sessionClock,
		controlOut:   controlOut,
	}
}

// run reads ticks off ticker until it closes. On each tick it consults the
// clock; a transition triggers one control message per shard. No transition
// → no broadcast (the "no no-op fan-out" rule that would otherwise emit
// duplicate session events).
func (monitor *Monitor) run() {
	slog.Debug("monitor.run starting")
	for now := range monitor.ticker {
		transition := monitor.sessionClock.CheckTransition(now)
		if transition == nil {
			continue
		}
		slog.Info("session phase transition",
			"from", transition.From,
			"to", transition.To,
			"at", transition.At,
		)
		message := controlMessage{
			kind:       controlSessionTransition,
			transition: *transition,
		}
		for _, out := range monitor.controlOut {
			out <- message
		}
	}
	slog.Debug("monitor.run stopped")
}
