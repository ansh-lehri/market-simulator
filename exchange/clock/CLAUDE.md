# clock — session-phase calculator (1.8)

## Spec
`docs/exchange/EXCHANGE_00` + `EXCHANGE_18` (this component).
This component implements an interface owned elsewhere, and its driver lives
elsewhere — read both so you build the right half:
- `docs/exchange/EXCHANGE_14_ADMISSION_GATE.md` — owns the `Phase` type and the
  `SessionPhase` interface (`Current() Phase`); this component implements it and
  replaces the gate's phase stub. Match the signature verbatim.
- `docs/exchange/EXCHANGE_10_WIRING_SHARDING.md` — the per-second MONITOR that calls
  `checkTransition` and the `handleTransition` that acts on a transition live in the
  WIRING, not here. This component is passive: it reports, it does not act (Option A).

## What this package owns
Knowing what phase the session is in and detecting when it changes. Holds the
`SessionSchedule` (data from Layer 7), implements `Current() Phase` (an atomic load,
no clock read), and computes transitions via `checkTransition(now)` / `phaseAt(now)`.
A pure phase calculator: it knows nothing about auctions, the tape, the book, or money.

## Invariants (EXCHANGE_18 §7)
1. The market is `PhaseClosed` before the first boundary and after the last —
   `phaseAt` returns `PhaseClosed` for any `now` before session start.
2. `current` is only written by `checkTransition` — a single writer (the monitor);
   many shard gates read it concurrently via `Current()`. The `atomic.Int32` makes
   that safe and lock-free.
3. `Current()` never reads the OS clock — the hot admission path pays only a field
   read; the monitor is what keeps the field fresh.
4. The clock holds no reference to any other component — no auction, emitter, book, or
   money. It reports transitions; the wiring acts (Option A).
5. The schedule is data, executed verbatim — no market-specific logic in the
   component; different markets are different schedules.
6. A delayed tick never corrupts phase — `phaseAt` lands on the latest reached
   boundary, so a late `checkTransition` reports the correct net phase.

## Deps
- `time` (stdlib) — absolute instants; Layer 7 hands boundaries as UTC-composed
  `time.Time`, the clock only compares.
- No other exchange packages. This package OWNS `Phase` + `SessionPhase`;
  consumers import them from here.

## Do NOT
- Build the monitor, the ticker, or `handleTransition` — those are the wiring's
  (Option A: report, don't act).
- Hold a reference to the auction, emitter, book, or any other component.
- Read the OS clock inside `Current()` — the monitor feeds `checkTransition`.

## Testing (table)
`phaseAt` before / at / after each boundary; `checkTransition` returns a transition on
change and nil on no-change; a delayed tick crossing several boundaries lands on the
latest phase (invariant 6). Feed times, assert phases and reported transitions. Not
golden-first.