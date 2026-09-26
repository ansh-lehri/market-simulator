package capstone

import "simulator/economy/reserve"

// nopEvents is the capstone's stand-in EventSink — Emit does nothing.
// The event system (delivery, buffering, consumers) is deferred per
// reserve/CLAUDE.md §9; reserve already wires the Emit call sites, but
// the system itself is unbuilt. This nop satisfies the seam so wiring
// can compose without pulling in a real sink.
//
// Value type (not pointer): zero-cost, no state, safe to hand into
// NewReservationService as nopEvents{}.
type nopEvents struct{}

var _ reserve.EventSink = nopEvents{}

// Emit drops every event on the floor. When the event system lands,
// this stand-in gets replaced by the real sink at the wiring root; no
// caller (in reserve or elsewhere) has to change.
func (sink nopEvents) Emit(event reserve.Event) {}
