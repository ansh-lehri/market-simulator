package clock

import (
	"testing"
	"time"
)

// sessionStart is the shared base instant for all table rows below. Using
// a fixed UTC point keeps expected phases pinnable to arithmetic offsets.
var sessionStart = time.Date(2026, 9, 19, 18, 0, 0, 0, time.UTC)

// canonicalSchedule mirrors EXCHANGE_18 §1's illustrative canonical
// schedule: continuous open at t0, closing-auction window at t0+5h15m,
// closed at t0+5h23m.
func canonicalSchedule() SessionSchedule {
	return SessionSchedule{
		Boundaries: []PhaseBoundary{
			{At: sessionStart, Phase: PhaseContinuous},
			{At: sessionStart.Add(5*time.Hour + 15*time.Minute), Phase: PhaseClosingAuction},
			{At: sessionStart.Add(5*time.Hour + 23*time.Minute), Phase: PhaseClosed},
		},
	}
}

// Compile-time proof: *SessionClock satisfies SessionPhase.
var _ SessionPhase = (*SessionClock)(nil)

func TestPhaseAt_TableSpansEachBoundary(t *testing.T) {
	clock := NewSessionClock(canonicalSchedule())
	auctionOpen := sessionStart.Add(5*time.Hour + 15*time.Minute)
	sessionEnd := sessionStart.Add(5*time.Hour + 23*time.Minute)

	rows := []struct {
		name string
		now  time.Time
		want Phase
	}{
		{"one second before first boundary", sessionStart.Add(-time.Second), PhaseClosed},
		{"exactly at first boundary", sessionStart, PhaseContinuous},
		{"mid continuous", sessionStart.Add(2 * time.Hour), PhaseContinuous},
		{"one nanosecond before auction", auctionOpen.Add(-time.Nanosecond), PhaseContinuous},
		{"exactly at auction boundary", auctionOpen, PhaseClosingAuction},
		{"mid closing-auction", auctionOpen.Add(4 * time.Minute), PhaseClosingAuction},
		{"one nanosecond before close", sessionEnd.Add(-time.Nanosecond), PhaseClosingAuction},
		{"exactly at close boundary", sessionEnd, PhaseClosed},
		{"one hour after close", sessionEnd.Add(time.Hour), PhaseClosed},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			got := clock.phaseAt(row.now)
			if got != row.want {
				t.Fatalf("phaseAt(%s) = %d, want %d", row.now, got, row.want)
			}
		})
	}
}

func TestNewSessionClock_StartsClosed(t *testing.T) {
	clock := NewSessionClock(canonicalSchedule())
	if got := clock.Current(); got != PhaseClosed {
		t.Fatalf("Current() at construction = %d, want PhaseClosed", got)
	}
}

func TestCurrent_NeverAdvancesWithoutCheckTransition(t *testing.T) {
	// The schedule's first boundary is already in the past relative to
	// this now, yet Current must stay PhaseClosed until the monitor
	// calls CheckTransition. Enforces invariant 3 (Current reads no
	// OS clock and does not recompute).
	clock := NewSessionClock(canonicalSchedule())
	if got := clock.Current(); got != PhaseClosed {
		t.Fatalf("Current() = %d before any CheckTransition, want PhaseClosed", got)
	}
}

func TestCheckTransition_ReportsBoundaryCrossings(t *testing.T) {
	clock := NewSessionClock(canonicalSchedule())
	auctionOpen := sessionStart.Add(5*time.Hour + 15*time.Minute)
	sessionEnd := sessionStart.Add(5*time.Hour + 23*time.Minute)

	steps := []struct {
		name        string
		now         time.Time
		wantNil     bool
		wantFrom    Phase
		wantTo      Phase
		wantCurrent Phase
	}{
		{
			name:        "before session start: no change",
			now:         sessionStart.Add(-time.Second),
			wantNil:     true,
			wantCurrent: PhaseClosed,
		},
		{
			name:        "cross into continuous",
			now:         sessionStart,
			wantFrom:    PhaseClosed,
			wantTo:      PhaseContinuous,
			wantCurrent: PhaseContinuous,
		},
		{
			name:        "still continuous: no change",
			now:         sessionStart.Add(time.Hour),
			wantNil:     true,
			wantCurrent: PhaseContinuous,
		},
		{
			name:        "cross into closing-auction",
			now:         auctionOpen,
			wantFrom:    PhaseContinuous,
			wantTo:      PhaseClosingAuction,
			wantCurrent: PhaseClosingAuction,
		},
		{
			name:        "same instant again: no change",
			now:         auctionOpen,
			wantNil:     true,
			wantCurrent: PhaseClosingAuction,
		},
		{
			name:        "cross into closed",
			now:         sessionEnd,
			wantFrom:    PhaseClosingAuction,
			wantTo:      PhaseClosed,
			wantCurrent: PhaseClosed,
		},
		{
			name:        "after close: no change",
			now:         sessionEnd.Add(time.Hour),
			wantNil:     true,
			wantCurrent: PhaseClosed,
		},
	}

	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			transition := clock.CheckTransition(step.now)
			if step.wantNil {
				if transition != nil {
					t.Fatalf("CheckTransition(%s) = %+v, want nil", step.now, transition)
				}
			} else {
				if transition == nil {
					t.Fatalf("CheckTransition(%s) = nil, want %d -> %d", step.now, step.wantFrom, step.wantTo)
				}
				if transition.From != step.wantFrom || transition.To != step.wantTo {
					t.Fatalf("transition = %d -> %d, want %d -> %d",
						transition.From, transition.To, step.wantFrom, step.wantTo)
				}
				if !transition.At.Equal(step.now) {
					t.Fatalf("transition.At = %s, want %s", transition.At, step.now)
				}
			}
			if got := clock.Current(); got != step.wantCurrent {
				t.Fatalf("Current() after step = %d, want %d", got, step.wantCurrent)
			}
		})
	}
}

func TestCheckTransition_DelayedTickAcrossPhases(t *testing.T) {
	// Two delayed ticks that each cross multiple boundaries. Fresh
	// clock -> mid-auction lands on ClosingAuction (crossed two
	// boundaries). A second tick well past close lands on Closed
	// (crossed one more). Both transitions reflect net phase, not
	// intermediate boundaries.
	clock := NewSessionClock(canonicalSchedule())
	midAuction := sessionStart.Add(5*time.Hour + 20*time.Minute)
	afterClose := sessionStart.Add(6 * time.Hour)

	first := clock.CheckTransition(midAuction)
	if first == nil {
		t.Fatalf("first CheckTransition = nil, want Closed -> ClosingAuction")
	}
	if first.From != PhaseClosed || first.To != PhaseClosingAuction {
		t.Fatalf("first transition = %d -> %d, want %d -> %d",
			first.From, first.To, PhaseClosed, PhaseClosingAuction)
	}

	second := clock.CheckTransition(afterClose)
	if second == nil {
		t.Fatalf("second CheckTransition = nil, want ClosingAuction -> Closed")
	}
	if second.From != PhaseClosingAuction || second.To != PhaseClosed {
		t.Fatalf("second transition = %d -> %d, want %d -> %d",
			second.From, second.To, PhaseClosingAuction, PhaseClosed)
	}
}

func TestCheckTransition_DelayedTickThroughToTerminalReturnsNil(t *testing.T) {
	// Fresh clock (init PhaseClosed) + single tick past the terminal
	// PhaseClosed boundary: phaseAt returns PhaseClosed, so the net
	// phase is unchanged and CheckTransition must report nil. This
	// pins invariant 6's corner: no spurious same-phase transition.
	clock := NewSessionClock(canonicalSchedule())
	afterClose := sessionStart.Add(6 * time.Hour)

	transition := clock.CheckTransition(afterClose)
	if transition != nil {
		t.Fatalf("CheckTransition(%s) = %+v, want nil (net phase unchanged: Closed -> Closed)",
			afterClose, transition)
	}
	if got := clock.Current(); got != PhaseClosed {
		t.Fatalf("Current() = %d, want PhaseClosed", got)
	}
}

func TestNewSessionClock_PanicsOnMalformedSchedule(t *testing.T) {
	base := sessionStart

	cases := []struct {
		name     string
		schedule SessionSchedule
	}{
		{
			name:     "empty boundaries",
			schedule: SessionSchedule{},
		},
		{
			name: "non-ascending At",
			schedule: SessionSchedule{
				Boundaries: []PhaseBoundary{
					{At: base, Phase: PhaseContinuous},
					{At: base.Add(-time.Minute), Phase: PhaseClosingAuction},
					{At: base.Add(time.Hour), Phase: PhaseClosed},
				},
			},
		},
		{
			name: "duplicate At (not strictly ascending)",
			schedule: SessionSchedule{
				Boundaries: []PhaseBoundary{
					{At: base, Phase: PhaseContinuous},
					{At: base, Phase: PhaseClosingAuction},
					{At: base.Add(time.Hour), Phase: PhaseClosed},
				},
			},
		},
		{
			name: "terminal is not PhaseClosed",
			schedule: SessionSchedule{
				Boundaries: []PhaseBoundary{
					{At: base, Phase: PhaseContinuous},
					{At: base.Add(time.Hour), Phase: PhaseClosingAuction},
				},
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("NewSessionClock did not panic for %s", testCase.name)
				}
			}()
			_ = NewSessionClock(testCase.schedule)
		})
	}
}
