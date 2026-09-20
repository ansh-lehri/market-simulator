package wire

import (
	"log/slog"

	"simulator/contract"
	"simulator/exchange/book"
	"simulator/exchange/emit"
	"simulator/exchange/gate"
)

// depthN is the top-of-book depth width emitted on the public tape
// (EXCHANGE_10 §6.1 pins N = 10).
const depthN = 10

// handleSubmit runs one SubmitOrder end-to-end (EXCHANGE_10 §6.1):
// admission via gate.Admit → private EngineResult always → if matched,
// public trades + OHLCV ingest + change-only public quote/depth.
//
// The change guard "len(match.Trades) > 0 || match.Rested != nil" honours
// emit invariant 6 (no no-op quote/depth snapshots): the book is only
// touched when at least one trade printed OR a GTC-limit remainder was
// inserted; in either case the top-of-book state changed.
func (shard *Shard) handleSubmit(state *InstrumentState, brokerID contract.BrokerID, order *contract.FundedOrder) {
	slog.Debug("handleSubmit",
		"shardID", shard.id,
		"brokerID", brokerID,
		"instrument", order.Instrument,
		"orderID", order.OrderID,
		"orderType", order.Type,
		"side", order.Side,
		"qty", order.Qty,
	)

	result := gate.Admit(
		order,
		brokerID,
		shard.phase,
		state.rules,
		state.book,
		state.band,
		state.admitSeqSource,
		state.matcher,
		state.auction,
	)
	state.emitter.EmitResult(result)

	if result.Outcome == contract.OutcomeRejected {
		slog.Info("submit rejected",
			"brokerID", brokerID,
			"instrument", order.Instrument,
			"orderID", order.OrderID,
			"reason", result.Rejection.Reason,
			"stage", result.Rejection.Stage,
		)
		return
	}
	if result.Outcome != contract.OutcomeMatched {
		// OutcomeDiverted cannot occur while the auction package is
		// deferred (deferredAuctionBatch.Add panics). Any other outcome
		// on the submit path is a gate contract violation.
		return
	}

	match := result.Match
	state.emitter.EmitTrades(match.Trades)
	for _, trade := range match.Trades {
		state.ohlcv.IngestTrade(trade.Price, trade.Qty)
	}
	if len(match.Trades) > 0 || match.Rested != nil {
		state.emitter.EmitQuote(state.book.Quote())
		state.emitter.EmitDepth(depthSnapshotFrom(state.book))
	}
}

// depthSnapshotFrom builds one emit.DepthSnapshot from two book.TopN calls.
// Lives in wire because this is where book (returning book.LevelSnapshot)
// and emit (consuming emit.DepthLevel) meet — book does not import emit.
func depthSnapshotFrom(orderBook *book.Book) emit.DepthSnapshot {
	return emit.DepthSnapshot{
		Bids: convertDepthLevels(orderBook.TopN(contract.Buy, depthN)),
		Asks: convertDepthLevels(orderBook.TopN(contract.Sell, depthN)),
	}
}

func convertDepthLevels(levels []book.LevelSnapshot) []emit.DepthLevel {
	converted := make([]emit.DepthLevel, len(levels))
	for index, level := range levels {
		converted[index] = emit.DepthLevel{Price: level.Price, TotalQty: level.TotalQty}
	}
	return converted
}
