package clock

import "time"

// PhaseBoundary marks the absolute instant at which the session enters a
// given phase. Layer 7 composes At from rulebook wall-clock configuration
// (assumed time.UTC) with today's session date before handing the
// SessionSchedule to the exchange.
type PhaseBoundary struct {
	At    time.Time
	Phase Phase
}

// SessionSchedule is the data the clock executes verbatim. Boundaries are
// sorted ascending by At; the first boundary marks session start and the
// final boundary is PhaseClosed. Layer 7 guarantees these shape
// requirements; NewSessionClock asserts them.
type SessionSchedule struct {
	Boundaries []PhaseBoundary
}

// PhaseTransition is the value CheckTransition returns when the phase
// changes. At is the now argument passed to CheckTransition — the instant
// the transition was detected, not the boundary's own At.
type PhaseTransition struct {
	From Phase
	To   Phase
	At   time.Time
}
