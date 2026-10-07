package funnel

import (
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"simulator/contract"
	"simulator/economy/realize"
)

// handleApplyResult is the settlement body (BROKER_PHASE5 §3). It is a
// PUSH command (no reply) delivered from the exchange's Private channel
// by the settlement consumer after an EngineResult arrives for one of
// this shard's orders.
//
// Six outcomes, one dispatch — the money side (Convert/Release/
// ReleaseExcess) lives in the reserve service; the broker side updates
// OrderTrack.Status and runs finalizeTrack on terminal transitions. The
// two-record discipline (BROKER_PHASE5 P5-D1) holds throughout: no
// reservation money-state is mirrored onto OrderTrack, no exchange-seam
// state (OrderID / Status / CancelInFlight / IsMarketBuy) is mirrored
// onto the reservation record.
//
// A missing track on any Status-mutating branch is a programming error
// (the broker's reserve-before-punch rule guarantees a track exists
// before any result could come back) — a nil-deref panic surfaces the
// bug. The one exception is settleCancelRejected, where channel order
// legitimately means the terminating result already finalized the track.
//
// An unknown Outcome panics for the same reason dispatch's default case
// does: an EXTENSION of the contract Outcome enum needs a branch here,
// and swallowing it silently would be a data-loss bug.
func (ownerShard *shard) handleApplyResult(cmd ApplyResult) {
	result := cmd.Result
	slog.Debug("funnel: handleApplyResult begin",
		"account_id", cmd.AccountID,
		"order_id", result.OrderID,
		"outcome", result.Outcome,
	)

	switch result.Outcome {
	case contract.OutcomeMatched:
		ownerShard.settleMatch(cmd.AccountID, result)
	case contract.OutcomeMakerFill:
		ownerShard.settleMakerFill(cmd.AccountID, result)
	case contract.OutcomeDiverted:
		ownerShard.markDiverted(result)
	case contract.OutcomeRejected:
		ownerShard.settleRejected(cmd.AccountID, result)
	case contract.OutcomeCancelled:
		ownerShard.settleCancelled(cmd.AccountID, result)
	case contract.OutcomeCancelRejected:
		ownerShard.settleCancelRejected(result)
	default:
		panic(fmt.Sprintf("funnel: handleApplyResult: unknown outcome %v for order %q", result.Outcome, result.OrderID))
	}
}

// convertTrades drives one reserve.Convert per fill print in the match
// (BROKER_PHASE5 §3.3). Shared by settleMatch and settleMakerFill — both
// outcomes carry a MatchResult.Trades slice; the only difference between
// them is terminal-status handling, which lives in the caller.
//
// Convert's shared tail (economy/reserve/convert.go §3.5) manages the
// reservation record: it debits principal, delivers units, shrinks
// Reserved* and RemainingQty, fires the per-fill cost debit, and — when
// RemainingQty hits 0 — the terminal brokerage debit (once per order,
// guarded by BrokerageCharged). The broker does none of that money math;
// it only directs.
//
// exitAt is wall-clock time.Now() because the contract carries no
// timestamp on EngineResult / Trade (the contract package never imports
// "time"). A future exchange fill-timestamp would replace this; a
// session-local candle-time alignment is a carried item (BROKER_PHASE5
// §8).
//
// tradeID is "<OrderID>:<Trade.Seq>" — Trade.Seq is per-matcher so an
// OrderID prefix disambiguates fills across matchers for the realizer's
// participant_trades row.
//
// Errors are intentionally ignored: Convert returns a nil-except-on-
// panic error (its only failure modes are invariant violations which it
// panics on, matching BROKER_PHASE5 §3.4 "ignored errors match the
// doc"). The broker's job here is to call per fill, not to interpret.
func (ownerShard *shard) convertTrades(result contract.EngineResult) {
	fillAt := time.Now()
	for _, trade := range result.Match.Trades {
		_ = ownerShard.resv.Convert(result.OrderID, trade.Qty, trade.Price, fillAt, tradeID(result.OrderID, trade.Seq))
	}
}

// tradeID formats the realizer's TradeID as "<OrderID>:<Seq>". The
// prefix is the broker's own minted OrderID (already "<AccountID>:<seq>"
// per nextOrderID); the suffix is the exchange's Trade.Seq which is
// per-matcher — the OrderID prefix disambiguates fills from different
// matchers in realize's participant_trades buffer.
//
// Pure: no I/O, no state, no clock. Called inside convertTrades per
// print, so a stable byte-identical output is required for determinism.
func tradeID(orderID OrderID, seq contract.Seq) realize.TradeID {
	return realize.TradeID(orderID + ":" + strconv.FormatUint(uint64(seq), 10))
}

// settleMatch handles OutcomeMatched (BROKER_PHASE5 §3.4 case 1). A
// Matched result is the one-shot MatchResult: every print the matcher
// made for one incoming order, batched into Match.Trades, plus possibly
// a Rested remainder (GTC limit landed on the book), plus possibly a
// CancelledQty (non-resting leftover).
//
// Five branches (DEVIATION from the Phase-5 doc, which collapsed cases 3
// and 4 into one; see cross-doc note in BROKER_PHASE5 §9):
//
//  1. Pure rest: no fills, remainder rested. Status stays Working
//     (no money moved; reservation untouched).
//
//  2. Partial fill + remainder rests: at least one fill, non-nil
//     Rested. Status PartiallyFilled; the reservation keeps the
//     RemainingQty slice reserved for the resting remainder.
//
//  3. Zero-fill cancel: no fills, non-zero CancelledQty (self-match,
//     no-liquidity, IOC-leftover, circuit-clamp). Release the full
//     cancelled qty; Status Cancelled — the order never settled
//     anything.
//
//  4. Fills + cancelled leftover: at least one fill, non-zero
//     CancelledQty (market-buy funded-cap, auction unfilled, ...).
//     Release the cancelled slice; Status Filled — the order partially
//     settled, and the leftover was cancelled rather than resting.
//
//  5. Clean full fill: no Rested, no CancelledQty. Status Filled.
//
// Market-buy residue true-up (ReleaseExcess) fires only when
// IsMarketBuy && Status == Filled: a market buy's slippage buffer may
// leave a positive ReservedCash residue after the last fill which the
// true-up returns. Limit orders drain exactly via Convert's maker-price
// shrink and have no residue; sells never reserve cash.
func (ownerShard *shard) settleMatch(accountID AccountID, result contract.EngineResult) {
	match := result.Match
	track := ownerShard.tracks[result.OrderID]
	ownerShard.convertTrades(result)

	switch {
	case len(match.Trades) == 0 && match.Rested != nil:
		track.Status = OrderStatusWorking
	case match.Rested != nil:
		track.Status = OrderStatusPartiallyFilled
	case len(match.Trades) == 0 && match.CancelledQty > 0:
		_ = ownerShard.resv.Release(result.OrderID, match.CancelledQty)
		track.Status = OrderStatusCancelled
	case match.CancelledQty > 0:
		_ = ownerShard.resv.Release(result.OrderID, match.CancelledQty)
		track.Status = OrderStatusFilled
	default:
		track.Status = OrderStatusFilled
	}

	if track.IsMarketBuy && track.Status == OrderStatusFilled {
		_ = ownerShard.resv.ReleaseExcess(result.OrderID)
	}

	slog.Info("funnel: settleMatch applied",
		"account_id", accountID,
		"order_id", result.OrderID,
		"status", track.Status,
		"trades", len(match.Trades),
		"cancelled_qty", match.CancelledQty,
		"cancel_reason", match.CancelReason,
		"rested", match.Rested != nil,
		"is_market_buy", track.IsMarketBuy,
	)
	ownerShard.finalizeTrack(accountID, result.OrderID)
}

// settleMakerFill handles OutcomeMakerFill (BROKER_PHASE5 §3.4 case 2).
// A maker fill is INCREMENTAL — the exchange delivers one EngineResult
// per fill against our resting order over time, in contrast to the one-
// shot Matched. Each delivery has Match.Trades (usually one print,
// occasionally batched) but NEVER a Rested or CancelledQty (that was
// pinned at the original Matched when the order rested).
//
// Completion is read from the reservation record via ViewReservation,
// NEVER mirrored on OrderTrack (two-record division). The source of
// truth for "did this fill drain the order" is Convert's shared tail: at
// RemainingQty == 0 Convert fires the terminal brokerage and may remove
// the record. We then see:
//
//   - ViewReservation returns (zero, false) → record removed → Filled.
//   - ViewReservation returns a view with RemainingQty > 0 → more fills
//     expected → PartiallyFilled, track retained for the next event.
//
// A resting maker's reservation drains exactly on each Convert (maker
// price == resting price == reserved price), so there is no residue to
// true up — ReleaseExcess is never called here.
func (ownerShard *shard) settleMakerFill(accountID AccountID, result contract.EngineResult) {
	track := ownerShard.tracks[result.OrderID]
	ownerShard.convertTrades(result)

	view, hasRecord := ownerShard.resv.ViewReservation(result.OrderID)
	if !hasRecord || view.RemainingQty == 0 {
		track.Status = OrderStatusFilled
		slog.Info("funnel: settleMakerFill filled",
			"account_id", accountID,
			"order_id", result.OrderID,
			"trades", len(result.Match.Trades),
		)
		ownerShard.finalizeTrack(accountID, result.OrderID)
		return
	}

	track.Status = OrderStatusPartiallyFilled
	slog.Info("funnel: settleMakerFill partial",
		"account_id", accountID,
		"order_id", result.OrderID,
		"trades", len(result.Match.Trades),
		"remaining_qty", view.RemainingQty,
	)
}

// markDiverted handles OutcomeDiverted (BROKER_PHASE5 §3.4 case 3). The
// exchange has accepted the order into the closing auction batch; no
// trades yet, no payload. The reservation is HELD across the (possibly
// unbounded) gap until the auction clears; a second EngineResult will
// arrive later with the SAME OrderID and Outcome Matched carrying the
// clearing fills + ReasonAuctionUnfilled remainder, routed back through
// settleMatch above (goldens pin the branch reuse).
//
// Zero reserve-service calls: no money moves on the ACK. Status stays
// Working (semantically, the order IS working — it is sitting in a
// batch). No finalize — the track must survive for the clearing event.
func (ownerShard *shard) markDiverted(result contract.EngineResult) {
	track := ownerShard.tracks[result.OrderID]
	track.Status = OrderStatusWorking
	slog.Info("funnel: markDiverted — holding reservation for auction clear",
		"order_id", result.OrderID,
	)
}

// settleRejected handles OutcomeRejected (BROKER_PHASE5 §3.4 case 4).
// The exchange refused the order at admission (bad tick, bad qty,
// participant unauthorized, ...). Nothing entered the book, nothing
// filled; release the ENTIRE reservation and finalize.
//
// Release(orderID, 0) = release-all (economy/reserve/release.go §4.1
// semantic). Status Rejected is terminal → finalizeTrack deletes the
// OrderTrack.
func (ownerShard *shard) settleRejected(accountID AccountID, result contract.EngineResult) {
	_ = ownerShard.resv.Release(result.OrderID, 0)
	track := ownerShard.tracks[result.OrderID]
	track.Status = OrderStatusRejected

	reason := contract.RejectReason(0)
	stage := ""
	if result.Rejection != nil {
		reason = result.Rejection.Reason
		stage = result.Rejection.Stage
	}
	slog.Warn("funnel: settleRejected — admission failed",
		"account_id", accountID,
		"order_id", result.OrderID,
		"reject_reason", reason,
		"reject_stage", stage,
	)
	ownerShard.finalizeTrack(accountID, result.OrderID)
}

// settleCancelled handles OutcomeCancelled (BROKER_PHASE5 §3.4 case 5).
// Our cancel punch won: the exchange echoes CancelledQty — the qty that
// was still cancellable at the moment the exchange pulled it. The race
// discipline (BROKER_PHASE5 P5-D18) is: release EXACTLY the echoed qty,
// NEVER the qty we assumed was resting. A fill may have beaten the
// cancel; the exchange's number is the only authoritative figure.
//
// CancelInFlight is cleared alongside the Status flip so a defensive
// reader never sees a "cancelled but still waiting on an ack" row.
func (ownerShard *shard) settleCancelled(accountID AccountID, result contract.EngineResult) {
	_ = ownerShard.resv.Release(result.OrderID, result.CancelledQty)
	track := ownerShard.tracks[result.OrderID]
	track.CancelInFlight = false
	track.Status = OrderStatusCancelled

	slog.Info("funnel: settleCancelled — cancel won",
		"account_id", accountID,
		"order_id", result.OrderID,
		"cancelled_qty", result.CancelledQty,
	)
	ownerShard.finalizeTrack(accountID, result.OrderID)
}

// settleCancelRejected handles OutcomeCancelRejected (BROKER_PHASE5
// §3.4 case 6 / P5-D21). The exchange found no cancellable qty — the
// order was already filled, already cancelled, or never entered the
// book. Channel order guarantees the terminating result that drove the
// order terminal was processed before this CancelRejected (both come
// over the same Private channel), so the OrderTrack is normally gone by
// now. If present, clear the flag; otherwise do nothing.
//
// No reserve-service call (nothing to release), no finalize (either the
// track is already gone from the terminating result's finalize or it is
// still live and some other state owns its lifecycle), no mark-dirty
// (same reason — no state change the broker owns here).
func (ownerShard *shard) settleCancelRejected(result contract.EngineResult) {
	track, exists := ownerShard.tracks[result.OrderID]
	if !exists {
		slog.Debug("funnel: settleCancelRejected — track already gone (expected under channel order)",
			"order_id", result.OrderID,
		)
		return
	}
	track.CancelInFlight = false
	slog.Info("funnel: settleCancelRejected — flag cleared",
		"order_id", result.OrderID,
	)
}

// finalizeTrack is the terminal-track cleanup (BROKER_PHASE5 §3.4
// finalizeTrack). Called ONLY on branches that drive the Status
// terminal (filled / cancelled / rejected). Delete the track if its
// Status is terminal — a defensive guard that preserves non-terminal
// tracks (settleMatch's pure-rest and partial-rest branches route
// through here but their Status is Working / PartiallyFilled, so the
// delete is skipped).
//
// MarkSnapshotDirty is the broker-side durability touch: the account's
// snapshot changed (track deleted / status flipped). The reserve
// service ALREADY marked dirty on its own money mutation (Convert /
// Release / ReleaseExcess all call persist.MarkSnapshotDirty with the
// reservation record's own SessionID), so this call is additive — the
// dirty map is set-semantic (persistence.DirtyKey), so a double-mark
// deduplicates.
//
// Session resolution: MarkSnapshotDirty's signature is (sessionID,
// accountID) (deviation from BROKER_PHASE5 §3.4's aid-only pseudo-code
// which doesn't match the real persistence.Component API). If the
// session was torn down between place-time and this settlement (user
// disconnect, lifecycle teardown), skip the mark — the reserve
// service's internal mark covers the money truth and OrderTrack's own
// durability is a carried item (BROKER_PHASE5 §8, B9).
func (ownerShard *shard) finalizeTrack(accountID AccountID, orderID OrderID) {
	if track, exists := ownerShard.tracks[orderID]; exists && isTerminal(track.Status) {
		delete(ownerShard.tracks, orderID)
	}
	liveSession, isLive := ownerShard.sessions[accountID]
	if !isLive {
		slog.Warn("funnel: finalizeTrack — no live session, skipping dirty mark",
			"account_id", accountID,
			"order_id", orderID,
		)
		return
	}
	ownerShard.bundle.Persist.MarkSnapshotDirty(liveSession.binding.SessionID, accountID)
}
