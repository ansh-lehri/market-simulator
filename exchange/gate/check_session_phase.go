package gate

import "simulator/contract"

// checkSessionPhase reads the global session clock and either routes the
// order or rejects it. PhaseContinuous routes to the matcher (default);
// PhaseClosingAuction routes to the auction batch and lets the remaining
// stages run (auction orders still face tick and band — EXCHANGE_14 §8
// invariant 5); PhaseClosed is a hard reject. The defensive default catches
// any future Phase value a SessionPhase implementation might return that
// this pipeline does not yet handle (PhaseOpeningAuction is reserved in
// EXCHANGE_14 §3).
func checkSessionPhase(order *contract.FundedOrder, brokerID contract.BrokerID, ctx *gateCtx) stageResult {
	switch ctx.phase.Current() {
	case PhaseContinuous:
		ctx.route = routeMatch
		return cont()
	case PhaseClosingAuction:
		ctx.route = routeAuction
		return cont()
	case PhaseClosed:
		return reject(order, brokerID, contract.RejectMarketClosed, "session_phase")
	default:
		return reject(order, brokerID, contract.RejectMarketClosed, "session_phase")
	}
}
