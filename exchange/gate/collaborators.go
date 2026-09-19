package gate

import "simulator/contract"

// Phase names the session's trading mode. Owned here because the gate is
// SessionPhase's consumer; implemented by the session-clock component (1.8).
type Phase int

const (
	PhaseClosed         Phase = iota // outside session -> reject
	PhaseContinuous                  // normal trading -> route to matcher
	PhaseClosingAuction              // collection window -> route to auction batch
)

// SessionPhase is the current-phase read the gate consults on every order.
// Implemented by session (1.8, later); stubbed in gate/stubs_test.go for
// standalone build.
type SessionPhase interface {
	Current() Phase
}

// AuctionBatch collects a diverted order into the current closing-auction
// batch; the collected orders match later, at auction clear. Implemented by
// auction (1.3, later); stubbed in gate/stubs_test.go.
//
// admissionSeq and brokerID are threaded in alongside the order because
// contract.FundedOrder carries neither: seam types stay untouched (per
// feedback_seam_types_immutable), and the auction needs both at clear time —
// brokerID to route each per-order EngineResult back to its broker,
// admissionSeq for time-priority in heavy-side allocation.
type AuctionBatch interface {
	Add(order *contract.FundedOrder, admissionSeq contract.Seq, brokerID contract.BrokerID)
}

// CircuitBounds is the reject-half of the circuit rule (EXCHANGE_15). The
// gate's checkCircuitBand reads Bounds() and rejects a limit priced outside
// [lower, upper]. Implemented by circuit (1.5, later); stubbed in
// gate/stubs_test.go.
//
// This deviates from EXCHANGE_14 §4's concrete *CircuitBand type: the circuit
// package is not built, and gate follows the same interface-first pattern
// used for SessionPhase and AuctionBatch. The real *CircuitBand satisfies
// this shape when it arrives.
type CircuitBounds interface {
	Bounds() (lower, upper contract.Ticks)
}

// Rulebook carries the injected configuration the gate reads. Present at
// session start, never phoned; the gate reads only TickSize.
type Rulebook struct {
	TickSize contract.Ticks
}
