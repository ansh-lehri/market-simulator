package wire

import (
	"log/slog"
	"reflect"

	"simulator/contract"
)

// handleCancel runs one CancelOrder end-to-end (EXCHANGE_10 §6.2, Flow C
// §9.3). Cancel is a pure book operation — it bypasses the gate and the
// matcher; its only failure is not-found (a normal outcome).
//
// Emit order (opposite of handleSubmit): public quote → public depth →
// private result. See doc §6.2. Each public emit is change-guarded via a
// pre/post comparison so the tape sees no no-op snapshots (emit invariant
// 6). Cancel of an order below top-10 leaves both quote and depth
// unchanged and emits nothing public — that case is exercised by Golden C.
func (shard *Shard) handleCancel(state *InstrumentState, brokerID contract.BrokerID, req *contract.CancelRequest) {
	slog.Debug("handleCancel",
		"shardID", shard.id,
		"brokerID", brokerID,
		"instrument", state.instrument,
		"orderID", req.OrderID,
	)

	preQuote := state.book.Quote()
	preDepth := depthSnapshotFrom(state.book)

	removed, found := state.book.Cancel(req.OrderID)
	if !found {
		slog.Info("cancel rejected — order not found",
			"brokerID", brokerID,
			"instrument", state.instrument,
			"orderID", req.OrderID,
		)
		state.emitter.EmitResult(contract.EngineResult{
			Outcome:  contract.OutcomeCancelRejected,
			OrderID:  req.OrderID,
			BrokerID: brokerID,
		})
		return
	}

	postQuote := state.book.Quote()
	quoteChanged := postQuote != preQuote
	if quoteChanged {
		state.emitter.EmitQuote(postQuote)
	}
	postDepth := depthSnapshotFrom(state.book)
	depthChanged := !reflect.DeepEqual(postDepth, preDepth)
	if depthChanged {
		state.emitter.EmitDepth(postDepth)
	}

	slog.Info("cancel succeeded",
		"brokerID", brokerID,
		"instrument", state.instrument,
		"orderID", req.OrderID,
		"cancelledQty", removed.Remaining,
		"quoteChanged", quoteChanged,
		"depthChanged", depthChanged,
	)

	state.emitter.EmitResult(contract.EngineResult{
		Outcome:      contract.OutcomeCancelled,
		OrderID:      req.OrderID,
		BrokerID:     brokerID,
		CancelledQty: removed.Remaining,
	})
}
