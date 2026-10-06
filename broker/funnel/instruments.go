package funnel

import "simulator/contract"

// InstrumentSet is the per-session allowlist of instruments the session may
// trade (BROKER_PHASE5 §1.4 P5-D9 / §8). validateSpec consults it first:
//
//	if !sess.knownInstrument(order.Instrument) { return ReasonUnknownInstrument }
//
// A sealed set is the real policy — session-install will supply a snapshot
// of the live instrument universe at bind time (cross-point, carried). This
// package only knows the interface and the current default below.
//
// Contract: Contains must be allocation-free and non-blocking (owner-goroutine
// hot path, called once per PlaceOrder). A real impl is likely a backing
// map[InstrumentID]struct{}; the interface keeps the storage out of the
// funnel.
type InstrumentSet interface {
	Contains(instrument contract.InstrumentID) bool
}

// permissiveInstruments is the current default: every instrument is known.
// handleInstallSession installs it when InstallSession.Instruments is nil,
// so existing lifecycle plumbing works unchanged until session-install
// populates the field. A permissive stub is also the right production
// posture while the real instrument universe is unbuilt — rejecting every
// order on an empty sealed set would be strictly worse than letting
// admission (Gate 4) decide.
//
// Tests that need the ReasonUnknownInstrument branch can swap in a sealed
// stub by direct field write on the ownedSession (same package).
type permissiveInstruments struct{}

// Contains — always true.
func (permissiveInstruments) Contains(_ contract.InstrumentID) bool { return true }
