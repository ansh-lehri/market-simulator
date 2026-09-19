package gate

import "simulator/contract"

// stampSequence assigns the order's time-priority admission sequence. Only
// survivors of the earlier gates reach here, so no Seq is spent on a
// rejected order (EXCHANGE_14 §8 invariant 1). Single stamping site
// feeding both routing forks (invariant 2).
//
// The value is written to ctx.admissionSeq — NOT onto the order — because
// contract.FundedOrder carries no Seq field and the seam types stay
// untouched (feedback_seam_types_immutable). routeOrder reads it and
// passes it into matcher.Match / auction.Add.
func stampSequence(order *contract.FundedOrder, brokerID contract.BrokerID, ctx *gateCtx) stageResult {
	ctx.admissionSeq = ctx.seq.Next()
	return cont()
}
