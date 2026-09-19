package gate

import "simulator/contract"

// routeOrder is the terminal stage: the single exit from the pipeline.
// Reads ctx.route (set by checkSessionPhase) and forwards the stamped
// order to the continuous matcher or the closing-auction batch, wrapping
// the result in an EngineResult. Always stops the pipeline
// (EXCHANGE_14 §8 invariant 7).
//
// brokerID and ctx.admissionSeq are passed to matcher.Match / auction.Add
// because contract.FundedOrder carries neither and the seam types stay
// untouched (feedback_seam_types_immutable).
func routeOrder(order *contract.FundedOrder, brokerID contract.BrokerID, ctx *gateCtx) stageResult {
	switch ctx.route {
	case routeMatch:
		matchResult := ctx.matcher.Match(order, brokerID, ctx.admissionSeq)
		return stageResult{
			stop: true,
			outcome: &contract.EngineResult{
				Outcome:  contract.OutcomeMatched,
				OrderID:  order.OrderID,
				BrokerID: brokerID,
				Match:    &matchResult,
			},
		}
	case routeAuction:
		ctx.auction.Add(order, ctx.admissionSeq, brokerID)
		return stageResult{
			stop: true,
			outcome: &contract.EngineResult{
				Outcome:  contract.OutcomeDiverted,
				OrderID:  order.OrderID,
				BrokerID: brokerID,
			},
		}
	default:
		// Unreachable in the assembled pipeline: checkSessionPhase sets
		// ctx.route to one of the two valid targets before routeOrder runs,
		// and it is the only writer of ctx.route. A default panic here
		// surfaces any future bug that runs routeOrder without a routed
		// context (per root CLAUDE.md: invariant violation → panic).
		panic("routeOrder: ctx.route not set by checkSessionPhase")
	}
}
