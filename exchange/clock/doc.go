// Package clock implements the exchange's session-phase calculator (1.8).
//
// It owns a SessionSchedule handed in by Layer 7 and reports the current
// Phase to consumers via Current(). CheckTransition, driven by the
// wiring's per-second monitor, is the sole writer of the phase field
// (an atomic.Int32 read concurrently by every shard-side consumer).
//
// The package is passive: it reports phase transitions, it does not act.
// The auction clear, tape events, and the ticker itself live in the
// exchange wiring (Option A per EXCHANGE_18 §4-§5). It holds no reference
// to the auction, emitter, book, or money.
//
// Time discipline: PhaseBoundary.At and the now argument are absolute
// time.Time instants. Layer 7 composes wall-clock configuration
// (18:00:00, ...) with the session date in time.UTC when building the
// SessionSchedule; the clock only compares instants and never inspects
// Location.
package clock
