package gate

import (
	"simulator/contract"
	"simulator/exchange/book"
	"simulator/exchange/clock"
	"simulator/exchange/match"
)

// Admit is the exchange-side entry point into the admission pipeline. It
// builds the internal gateCtx from the caller's collaborators and delegates
// to RunPipeline. gateCtx stays unexported (implementation detail of the
// pipeline); Admit is the exported surface the wiring layer calls per
// SubmitOrder. The order and brokerID enter here from the InboundMessage
// envelope (EXCHANGE_10 §2); the remaining args mirror the pre-pipeline
// fields of gateCtx (phase, rules, book, band, seq, matcher, auction).
func Admit(
	order *contract.FundedOrder,
	brokerID contract.BrokerID,
	phase clock.SessionPhase,
	rules Rulebook,
	orderBook *book.Book,
	band CircuitBounds,
	seq match.SeqSource,
	matcher *match.Matcher,
	auction AuctionBatch,
) contract.EngineResult {
	ctx := &gateCtx{
		phase:   phase,
		rules:   rules,
		book:    orderBook,
		band:    band,
		seq:     seq,
		matcher: matcher,
		auction: auction,
	}
	return RunPipeline(order, brokerID, ctx)
}
