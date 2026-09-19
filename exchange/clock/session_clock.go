package clock

import (
	"fmt"
	"sync/atomic"
	"time"
)

// SessionClock is the passive phase calculator (EXCHANGE_18). It holds the
// SessionSchedule handed in by Layer 7 and a single atomic.Int32 carrying
// the last-computed Phase. checkTransition is the sole writer of current;
// Current is a plain atomic load read by every shard-side consumer.
type SessionClock struct {
	schedule SessionSchedule
	current  atomic.Int32
}

// NewSessionClock returns a clock initialised to PhaseClosed. It panics on
// a malformed schedule: an empty slice, boundaries not sorted ascending by
// At, or a terminal boundary whose phase is not PhaseClosed. Malformed
// input is an invariant violation from Layer 7, not a business error, so
// it is dev-fatal per the project's error policy.
func NewSessionClock(schedule SessionSchedule) *SessionClock {
	assertScheduleWellFormed(schedule)
	clock := &SessionClock{schedule: schedule}
	clock.current.Store(int32(PhaseClosed))
	return clock
}

// Current implements SessionPhase. It is an atomic load of the
// last-computed phase; it never reads the OS clock and never scans the
// schedule. The monitor keeps the field fresh via checkTransition.
func (clock *SessionClock) Current() Phase {
	return Phase(clock.current.Load())
}

// checkTransition compares now against the schedule, updates the stored
// phase if a boundary was crossed since the last call, and returns the
// transition (or nil when unchanged). It is the sole writer of current;
// the wiring's monitor is the sole caller.
func (clock *SessionClock) checkTransition(now time.Time) *PhaseTransition {
	want := clock.phaseAt(now)
	old := Phase(clock.current.Load())
	if want == old {
		return nil
	}
	clock.current.Store(int32(want))
	return &PhaseTransition{From: old, To: want, At: now}
}

// phaseAt returns the phase of the latest boundary whose At is at or
// before now, or PhaseClosed if now precedes the first boundary. It scans
// forward so a delayed tick that crosses several boundaries still lands
// on the correct net phase (invariant 6).
func (clock *SessionClock) phaseAt(now time.Time) Phase {
	result := PhaseClosed
	for _, boundary := range clock.schedule.Boundaries {
		if boundary.At.After(now) {
			break
		}
		result = boundary.Phase
	}
	return result
}

func assertScheduleWellFormed(schedule SessionSchedule) {
	if len(schedule.Boundaries) == 0 {
		panic("clock: SessionSchedule.Boundaries is empty")
	}
	for index := 1; index < len(schedule.Boundaries); index++ {
		previous := schedule.Boundaries[index-1].At
		current := schedule.Boundaries[index].At
		if !previous.Before(current) {
			panic(fmt.Sprintf(
				"clock: SessionSchedule.Boundaries not strictly ascending at index %d (%s !< %s)",
				index, previous, current,
			))
		}
	}
	terminal := schedule.Boundaries[len(schedule.Boundaries)-1].Phase
	if terminal != PhaseClosed {
		panic(fmt.Sprintf(
			"clock: SessionSchedule terminal boundary phase = %d, want PhaseClosed",
			terminal,
		))
	}
}
