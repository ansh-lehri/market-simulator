package gate

import "simulator/contract"

// pipeline is the ordered list of admission stages. Naming is descriptive
// (EXCHANGE_14 §8 invariant 6) and the order matters: checkSessionPhase
// sets ctx.route before checkLiquidity reads it; stampSequence is the sole
// call site of ctx.seq.Next() and sits immediately before routeOrder, so no
// Seq is ever spent on a rejected order (invariants 1, 2). routeOrder is
// last and always terminates (invariant 7).
var pipeline = []Stage{
	checkStructural,
	checkSessionPhase,
	checkLiquidity,
	checkTickGrid,
	checkCircuitBand,
	stampSequence,
	routeOrder,
}

// RunPipeline runs one FundedOrder through the seven admission stages and
// returns the single EngineResult the shard emits. brokerID enters here
// (from the InboundMessage envelope) because contract.FundedOrder does not
// carry it (feedback_seam_types_immutable) — every stage receives it and
// every EngineResult echoes it (EXCHANGE_14 §8 invariant 3).
func RunPipeline(order *contract.FundedOrder, brokerID contract.BrokerID, ctx *gateCtx) contract.EngineResult {
	for _, stage := range pipeline {
		result := stage(order, brokerID, ctx)
		if result.stop {
			return *result.outcome
		}
	}
	// Unreachable: routeOrder is the last stage and always sets stop=true.
	// A panic here surfaces any future edit that reorders the slice and
	// removes the terminal guarantee (root CLAUDE.md: invariant violation).
	panic("gate.RunPipeline: pipeline did not terminate — routeOrder must be last and always stop")
}
