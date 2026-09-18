Exchange 1.8 Session-Phase / Clock — Implementation

Prerequisites: EXCHANGE_00_ARCHITECTURE.md, and the Phase type + SessionPhase interface from EXCHANGE_14_ADMISSION_GATE.md (1.8 is their owner and implementer). Uses those directly.

What 1.8 owns: knowing what phase the trading session is in, and detecting when it changes. It implements the SessionPhase.Current() Phase interface the admission gate depends on, holds the session's phase schedule (supplied as data by Layer 7), and reports phase transitions so the exchange wiring can act on them (clear the auction, emit session open/close on the tape).

It is a pure phase calculator. It knows nothing about auctions, the tape, the book, or money. It answers "what phase?" and announces "the phase just changed." What to do about a change lives in the exchange wiring, not here (decision: Option A — report, don't act).

0. Design stance — config-driven, zero domain knowledge

The component contains no market-specific logic — no country names, no hardcoded hours, no "if NSE." It executes a schedule it is handed. The schedule is just data: a sorted list of "at this instant, the phase becomes X." Different sessions are different schedules of the same shape:

Canonical live session: Layer 7 reads the rulebook (session start, end, phase windows) and computes tonight's absolute boundary instants.
On-demand session (started anytime): Layer 7 computes boundaries on the fly from "now" — either equal divisions, or a single continuous phase with no auction.

Either way Layer 7 produces the same SessionSchedule structure and hands it to the exchange at session setup. The exchange monitors real time against those boundaries. The component does not know or care how the boundaries were derived.

Out of scope (explicitly): time-compressed replay. Replay is a chart-playback feature (TradingView-style), not a session the exchange runs, and is not in the prototype. Because of that, the earlier "injected/accelerated clock" machinery is not built — the exchange monitors ordinary real time. The monitor is kept behind a thin seam (§4) so a future accelerated driver could be substituted without touching phase logic, but nothing here is designed around simulated time.

1. The schedule — data supplied by Layer 7
go
type PhaseBoundary struct {
    At    Time   // absolute instant this phase begins (Layer 7 computes these)
    Phase Phase
}

type SessionSchedule struct {
    Boundaries []PhaseBoundary // sorted ascending by At; first boundary is session start
}

Illustrative canonical schedule (values are config, not baked in):

[ {18:00:00, PhaseContinuous},
  {23:15:00, PhaseClosingAuction},
  {23:23:00, PhaseClosed} ]

On-demand example (no auction phase):

[ {now, PhaseContinuous},
  {now+35m, PhaseClosed} ]

Requirements on a well-formed schedule (Layer 7 guarantees; the exchange may assert): boundaries sorted ascending by At, the first boundary marks session start, and the final boundary is PhaseClosed.

2. The component
go
type PhaseTransition struct {
    From Phase
    To   Phase
    At   Time // the instant the transition was detected
}

type SessionClock struct {
    schedule SessionSchedule
    current  atomic.Int32 // holds a Phase; atomic because MANY shard workers read it
                          // concurrently while the monitor (one writer) updates it.
                          // PhaseClosed before session start.
}

func NewSessionClock(s SessionSchedule) *SessionClock {
    c := &SessionClock{schedule: s}
    c.current.Store(int32(PhaseClosed)) // closed until the first boundary is reached
    return c
}

// Current implements SessionPhase (the gate's collaborator). An atomic load of the
// last-computed phase — it does NOT read the clock or recompute; it returns whatever
// the most recent checkTransition established.
func (c *SessionClock) Current() Phase { return Phase(c.current.Load()) }

Why atomic (sharding consequence). The session clock is global — one clock shared by every instrument. The exchange runs many shard workers (one pool, instruments partitioned across it), and every shard's admission gate calls Current() on every order. So current is read concurrently by many goroutines while the monitor (a single writer) updates it on a transition. A single enum behind an atomic.Int32 makes this lock-free and correct: the monitor Stores on transition, every gate Loads. This is the one legitimate piece of cross-goroutine shared state in the exchange; everything else is single-writer-per-shard. Current() never touches the OS clock — the monitor (§4) keeps the value fresh — so the hot admission path pays only an atomic load.

3. Detecting a transition
go
// checkTransition compares real 'now' against the schedule, updates current phase if
// a boundary was crossed since the last call, and returns the transition (or nil if
// the phase is unchanged). It is the only writer of c.current.
func (c *SessionClock) checkTransition(now Time) *PhaseTransition

Logic:

checkTransition(now):
    want := c.phaseAt(now)            // latest boundary whose At <= now; PhaseClosed if before session start
    old  := Phase(c.current.Load())
    if want == old:
        return nil                    // no change
    c.current.Store(int32(want))      // single writer (the monitor); gates Load concurrently
    return &PhaseTransition{From: old, To: want, At: now}

phaseAt(now):
    result := PhaseClosed           // before the first boundary, the market is closed
    for b in schedule.Boundaries:   // sorted ascending
        if b.At <= now:
            result = b.Phase
        else:
            break
    return result

phaseAt picks the phase of the latest boundary already reached. Because the monitor calls checkTransition frequently (§4), each call normally advances the phase by at most one boundary — but phaseAt is defined to be correct even if several boundaries were crossed between calls (it lands on the latest), so a delayed tick can never skip the component into an inconsistent state; it just reports the net current phase. (If a future need arises to fire side-effects for each skipped boundary, the monitor would call more often; for the prototype's 1-second cadence over minute/hour-scale boundaries this never happens.)

4. The monitor — the thin, replaceable driver

SessionClock is passive: it never owns a timer or a goroutine. Something must call checkTransition on a cadence. That something is the monitor, which lives in the exchange's session goroutine and, for the prototype, is a 1-second real-time ticker (decision). Keeping the driver separate from the clock is what lets a future accelerated driver replace it without changing any phase logic.

// inside the exchange session goroutine, alongside inbound-order handling:
ticker := every 1 second
on each tick (now = real current time):
    transition := clock.checkTransition(now)
    if transition != nil:
        handleTransition(transition)   // Option A: the WIRING acts (§5), not the clock

Running the monitor inside the exchange goroutine keeps everything single-threaded: phase changes and order handling never race, so current needs no lock. The ticker fires regardless of order flow, so the market still closes on schedule during a quiet final minute (a purely order-driven clock would freeze in silence — avoided).

5. Acting on a transition — the wiring (Option A)

The clock reports; the exchange wiring acts. The clock holds no reference to the auction (1.3) or the emitter (1.9) and knows nothing about them. The wiring's handleTransition maps a transition to consequences:

handleTransition(t):
    switch:
      t.To == PhaseContinuous  and t.From == PhaseClosed:
          emitter.SessionOpened()                 // 1.9 tape event

      t.To == PhaseClosingAuction:
          emitter.AuctionWindowOpened()           // 1.9 tape event
          // (the gate already diverts orders to the auction batch by reading Current())

      t.To == PhaseClosed and t.From == PhaseClosingAuction:
          result := auction.Clear()               // 1.3 computes clearing price, matches the batch
          route result.trades to tape + broker     // via 1.9
          emitter.SessionClosed()                 // 1.9 tape event

      t.To == PhaseClosed:                         // close with no auction phase
          emitter.SessionClosed()

Why this and not the clock calling these itself: the clock's one job is knowing the phase. All "what happens at a boundary" logic lives in one place — the wiring, which is already the loop coordinating every component. Adding "close also emits a final settlement snapshot" later is a one-line edit here; the clock never changes and stays trivially testable (feed it times, assert the reported transitions). This is the same knowing-vs-acting separation the book/matcher follow.

The Continuous→Closed auction-clear coupling is the single most important consequence: the session clock's close transition is what fires the auction clear. The clock doesn't know that; the wiring does.

6. Where 1.8 sits in the flow
The admission gate (1.4) calls clock.Current() in checkSessionPhase on every order — routing it to matching (continuous), the auction batch (closing-auction), or rejection (closed).
The monitor advances the phase on its 1-second cadence and hands transitions to the wiring.
The wiring turns a close transition into the auction clear (1.3) and tape events (1.9).
7. Invariants 1.8 must hold
The market is PhaseClosed before the first boundary and after the last — phaseAt returns PhaseClosed for any now before session start.
current is only written by checkTransition — a single writer (the monitor); many shard gates read it concurrently via Current(). The atomic.Int32 makes that safe and lock-free.
Current() never reads the OS clock — the hot admission path pays only a field read; the monitor is what keeps the field fresh.
The clock holds no reference to any other component — no auction, emitter, book, or money. It reports transitions; the wiring acts (Option A).
The schedule is data, executed verbatim — no market-specific logic in the component; different markets are different schedules.
A delayed tick never corrupts phase — phaseAt lands on the latest reached boundary, so a late checkTransition reports the correct net phase rather than skipping into an inconsistent one.
8. Deferred / handed to other components
Schedule construction — Layer 7 builds SessionSchedule (from rulebook for canonical, on-the-fly for on-demand) and hands it to the exchange at session setup. 1.8 only consumes it.
Auction clear — 1.3 implements auction.Clear(); 1.8's close transition is the trigger, invoked by the wiring, not by 1.8.
Tape events (session opened/closed, auction window opened) — shaped and emitted by 1.9; 1.8 only reports the phase transition that prompts them.
Time-compressed replay — not an exchange session in the prototype; the monitor seam (§4) is where a future accelerated driver would substitute, with no change to the phase logic.