package gate

import (
	"simulator/contract"
	"simulator/exchange/clock"
)

// checkSessionPhase reads the global session clock and either routes the
// order or rejects it. clock.PhaseContinuous routes to the matcher
// (default); clock.PhaseClosingAuction routes to the auction batch and lets
// the remaining stages run (auction orders still face tick and band —
// EXCHANGE_14 §8 invariant 5); clock.PhaseClosed is a hard reject. The
// defensive default catches any future Phase value a clock.SessionPhase
// implementation might return that this pipeline does not yet handle
// (PhaseOpeningAuction is reserved in EXCHANGE_14 §3).
func checkSessionPhase(order *contract.FundedOrder, brokerID contract.BrokerID, ctx *gateCtx) stageResult {
	switch ctx.phase.Current() {
	case clock.PhaseContinuous:
		ctx.route = routeMatch
		return cont()
	case clock.PhaseClosingAuction:
		ctx.route = routeAuction
		return cont()
	case clock.PhaseClosed:
		return reject(order, brokerID, contract.RejectMarketClosed, "session_phase")
	default:
		return reject(order, brokerID, contract.RejectMarketClosed, "session_phase")
	}
}
