package clock

// Phase names the session's trading mode. Owned here because the clock is
// the source of truth for what phase the session is in; consumers (the
// admission gate, others later) only read it.
type Phase int

const (
	PhaseClosed         Phase = iota // outside session -> consumers reject
	PhaseContinuous                  // normal trading  -> consumers route to matcher
	PhaseClosingAuction              // collection window -> consumers route to auction batch
)

// SessionPhase is the current-phase read a consumer consults on every
// order. *SessionClock is its production implementation; consumers may
// stub it in their own tests to avoid depending on this package's driver.
type SessionPhase interface {
	Current() Phase
}
