package gate

import "simulator/contract"

// Phase + SessionPhase are owned by the clock package (1.8) — the source of
// truth for what phase the session is in — and imported by consumers from
// there. Gate references them via clock.Phase / clock.SessionPhase in
// context.go and check_session_phase.go.

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
// [lower, upper]. Implemented by circuit (1.5) — *circuit.CircuitBand
// satisfies this shape (see gate/circuit_wiring_test.go for the
// consumer-side assertion). Stubbed in gate/stubs_test.go for unit tests.
type CircuitBounds interface {
	Bounds() (lower, upper contract.Ticks)
}

// Rulebook carries the injected configuration the gate reads. Present at
// session start, never phoned; the gate reads only TickSize.
type Rulebook struct {
	TickSize contract.Ticks
}
