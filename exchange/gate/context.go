package gate

import (
	"simulator/contract"
	"simulator/exchange/book"
	"simulator/exchange/match"
)

// routeTarget is the fork checkSessionPhase sets and routeOrder reads.
type routeTarget int

const (
	routeMatch   routeTarget = iota // -> continuous matcher (default)
	routeAuction                    // -> closing-auction batch
)

// gateCtx bundles the collaborators plus the small pipeline working state
// each stage passes to the next. One gateCtx is built per submission by the
// shard, pointing at the target instrument's per-instrument collaborators
// alongside the shared session phase and rulebook.
//
// Note on seq: the SeqSource wired here is the per-instrument admission
// sequence (order time priority in the book), NOT the emitter Seq the
// matcher uses to stamp Trade.Seq. Both share the match.SeqSource shape
// (Next() Seq); the wiring layer passes distinct concrete counters.
type gateCtx struct {
	// Session-level (shared across instruments):
	phase SessionPhase
	rules Rulebook

	// Per-instrument (wiring supplies these from InstrumentState):
	book    *book.Book
	band    CircuitBounds
	seq     match.SeqSource
	matcher *match.Matcher
	auction AuctionBatch

	// Pipeline working state (mutated by earlier stages, read by later ones):
	route        routeTarget  // set by checkSessionPhase
	admissionSeq contract.Seq // set by stampSequence; read by routeOrder
}

// stageResult is the control signal every stage returns: either continue to
// the next stage (stop == false), or terminate with a finalised EngineResult
// (stop == true, outcome non-nil).
type stageResult struct {
	stop    bool
	outcome *contract.EngineResult
}

// Stage inspects the order + brokerID + context and returns a stageResult.
// brokerID is threaded through because contract.FundedOrder does not carry
// it (per feedback_seam_types_immutable): it arrives on the InboundMessage
// envelope, must be echoed on every EngineResult (EXCHANGE_14 §8 invariant
// 3), and must be passed to matcher.Match / auction.Add in routeOrder.
type Stage func(order *contract.FundedOrder, brokerID contract.BrokerID, ctx *gateCtx) stageResult

// cont signals "stage passed; run the next one."
func cont() stageResult {
	return stageResult{stop: false}
}

// reject builds a terminal Rejected outcome and stops the pipeline. Echoes
// OrderID and BrokerID as EXCHANGE_14 §8 invariant 3 requires. stageName is
// the descriptive Rejection.Stage string ("structural", "session_phase",
// "liquidity", "tick_grid", "circuit_band") — diagnostic only, not
// logic-bearing.
func reject(order *contract.FundedOrder, brokerID contract.BrokerID, reason contract.RejectReason, stageName string) stageResult {
	return stageResult{
		stop: true,
		outcome: &contract.EngineResult{
			Outcome:   contract.OutcomeRejected,
			OrderID:   order.OrderID,
			BrokerID:  brokerID,
			Rejection: &contract.Rejection{Reason: reason, Stage: stageName},
		},
	}
}
