package funnel

import (
	"log/slog"

	"simulator/contract"
)

// handleCancel is the cancel path (BROKER_PHASE5 §2.3). It runs on the
// owner goroutine, so sessions / tracks / persister touches are
// single-writer safe without a lock.
//
// The whole body is validate -> mark in-flight -> punch. NOTHING RELEASES
// HERE (P5-D10). Release waits for the exchange's echoed CancelledQty on
// the settlement path (settleCancelled in Stage 5) because the exchange
// is authoritative on how much is still cancellable — a fill may have
// beaten the cancel, in which case we must release only what was
// actually pulled, not what we assumed was resting.
//
// Status check (Working / PartiallyFilled) is a BEST-EFFORT pre-filter:
// our view may lag the exchange, but the fast-fail saves the user a
// round-trip for the obviously-terminal case. The exchange is the real
// authority — it may return CancelRejected (OutcomeCancelRejected) even
// when we thought the order was Working.
//
// Idempotency (P5-D11): a repeated cancel while CancelInFlight is set
// returns CancelPending WITHOUT re-punching. Re-punching could race the
// first echo and release twice.
//
// CancelPending is the HTTP 202 outcome — "the cancel was accepted
// locally, exchange will tell us the real outcome asynchronously". It
// never means "cancelled."
func (ownerShard *shard) handleCancel(cmd CancelOrder) CancelResult {
	slog.Debug("funnel: handleCancel begin",
		"account_id", cmd.AccountID,
		"order_id", cmd.OrderID,
	)

	liveSession, isLive := ownerShard.sessions[cmd.AccountID]
	if !isLive {
		slog.Info("funnel: handleCancel rejected — no session",
			"account_id", cmd.AccountID,
			"order_id", cmd.OrderID,
		)
		return CancelResult{Status: CancelFailed, Reason: ReasonNoSession}
	}

	// Ownership guard: track must exist AND belong to the account that
	// requested the cancel. A cross-account cancel (someone else trying
	// to cancel this account's order) returns the same reason as a
	// completely unknown order so a probing caller cannot distinguish
	// "doesn't exist" from "belongs to someone else".
	track, exists := ownerShard.tracks[cmd.OrderID]
	if !exists || track.AccountID != cmd.AccountID {
		slog.Warn("funnel: handleCancel rejected — unknown order (or cross-account)",
			"account_id", cmd.AccountID,
			"order_id", cmd.OrderID,
			"track_exists", exists,
		)
		return CancelResult{Status: CancelFailed, Reason: ReasonUnknownOrder}
	}

	// State guard: terminal states cannot be cancelled. The exchange
	// result that terminated the order already released any unfilled
	// slice (settle path in Stage 5); re-punching a cancel for a
	// terminal order would land as OutcomeCancelRejected anyway.
	if track.Status != OrderStatusWorking && track.Status != OrderStatusPartiallyFilled {
		slog.Warn("funnel: handleCancel rejected — not cancellable (terminal)",
			"account_id", cmd.AccountID,
			"order_id", cmd.OrderID,
			"status", track.Status,
		)
		return CancelResult{Status: CancelFailed, Reason: ReasonNotCancellable}
	}

	// Idempotent: a cancel punch is already in flight, so punching
	// another one would race the first's echo. Return the same
	// CancelPending without touching state or sending to the exchange.
	if track.CancelInFlight {
		slog.Info("funnel: handleCancel idempotent — already in-flight",
			"account_id", cmd.AccountID,
			"order_id", cmd.OrderID,
		)
		return CancelResult{Status: CancelPending, OrderID: cmd.OrderID}
	}

	// Mark the flag + persist dirty BEFORE the punch so a crash between
	// mark and send still leaves the flag set — recovery sees the flag
	// and skips re-punching (which would otherwise land as a second
	// cancel on the exchange's book). Same access path as handleTransfer
	// (Phase 3): goes through bundle.Persist, the shard-owned persistence
	// component.
	track.CancelInFlight = true
	ownerShard.bundle.Persist.MarkSnapshotDirty(liveSession.binding.SessionID, cmd.AccountID)

	// Punch via s.punch (BrokerID stamped inside). contract.CancelOrder
	// is qualified because the broker command type CancelOrder shares
	// the bare name with the contract.InboundKind constant.
	ownerShard.punch(liveSession, contract.InboundMessage{
		Kind:       contract.CancelOrder,
		Instrument: track.Instrument,
		Cancel:     &contract.CancelRequest{OrderID: cmd.OrderID},
	})

	slog.Info("funnel: handleCancel accepted — punch sent",
		"account_id", cmd.AccountID,
		"order_id", cmd.OrderID,
		"instrument", track.Instrument,
	)
	return CancelResult{Status: CancelPending, OrderID: cmd.OrderID}
}
