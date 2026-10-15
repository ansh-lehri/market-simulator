package funnel

import (
	"fmt"
	"log/slog"
)

// dispatch is the type-switch that routes an inbound command to the
// matching handler (BROKER_TIER0 §Point-4). Request/response cases send
// the returned result on the command's ReplyTo channel; push cases run
// and return nothing. The default case panics — an unknown command is a
// programming error (someone added a Command type without adding its
// case here), not a runtime condition to recover from.
//
// dispatch is a shard method (not a free function) so it has closure
// over the shard's owned state — no argument threading, no shared
// registry.
func (ownerShard *shard) dispatch(cmd Command) {
	switch typedCmd := cmd.(type) {
	case PlaceOrder:
		typedCmd.ReplyTo <- ownerShard.handlePlaceOrder(typedCmd)
	case CancelOrder:
		typedCmd.ReplyTo <- ownerShard.handleCancel(typedCmd)
	case Transfer:
		typedCmd.ReplyTo <- ownerShard.handleTransfer(typedCmd)
	case ReadBalance:
		typedCmd.ReplyTo <- ownerShard.handleReadBalance(typedCmd)
	case ReadPositions:
		typedCmd.ReplyTo <- ownerShard.handleReadPositions(typedCmd)
	case ReadOpenOrders:
		typedCmd.ReplyTo <- ownerShard.handleReadOpenOrders(typedCmd)
	case ReadObservation:
		typedCmd.ReplyTo <- ownerShard.handleReadObservation(typedCmd)
	case ApplyResult:
		ownerShard.handleApplyResult(typedCmd)
	case FireStop:
		ownerShard.handleFireStop(typedCmd)
	case InstallSession:
		// Phase-2 Point 3 — the first command crossing the funnel from
		// off-shard orchestration to the owner goroutine. Handler is
		// pure in-memory (XOR + map write + reply); body lives in
		// install_session.go.
		ownerShard.handleInstallSession(typedCmd)
	case UninstallSession:
		// Canonical session teardown's detach-keep step. Pure in-memory
		// (SessionID match + map move + reply); body lives in
		// uninstall_session.go. Account state is NOT touched — persist +
		// unload happen later via FinalizeMarket at the market's close.
		ownerShard.handleUninstallSession(typedCmd)
	case FinalizeMarketOnShard:
		// Market-close sweep across every account bound to one
		// ExchangeID. Persists via lifecycle.SessionSave and unloads.
		// Body lives in finalize_market.go.
		ownerShard.handleFinalizeMarketOnShard(typedCmd)
	default:
		panic(fmt.Sprintf("funnel: shard dispatch: unknown command type %T", cmd))
	}
}

// The seven handler bodies below are DELIBERATELY EMPTY SEAMS. Each
// belongs to a later tier and will be filled by that tier's stage; the
// Tier-0 funnel exists so those bodies have a machine to run inside.
//
// Phase-0 arrangement (temporary, per the "compile-time echo" decision):
// request/response handlers return a zero-value result so that dispatch's
// `c.ReplyTo <- handler(c)` send is well-typed and the reply-mechanism
// tests (reply-happy, reply-post-timeout) can exercise the round-trip
// without any real body existing yet. When the owning tier lands, the
// handler body is replaced with real work and the empty return is gone.
// Push handlers already have nothing to return, so the arrangement is
// vacuous for them.
//
// Do NOT extend these bodies to do any account, cost, reservation,
// realization, or exchange work in this package — that logic lives in
// the owning tier, not here.

// handlePlaceOrder — Tier-1 order-intake body — lives in place.go
// (BROKER_PHASE5 §1.3). Dispatch above calls the method; this file no
// longer holds its stub.

// handleCancel — Tier-1 cancel body — lives in cancel.go (BROKER_PHASE5
// §2.3). Dispatch above calls the method; this file no longer holds its
// stub.

// handleTransfer is the Tier-3 in-session bank-move body (BROKER_PHASE3
// §2.2, P3-D5/D6/D9). Runs on the owner goroutine; three outcomes:
//
//   - !live                       → TransferResult{Present:false}      (HTTP falls to durable)
//   - MoveCash err (live-reject)  → TransferResult{Present:true, Err}   (HTTP → 422 / 500)
//   - ok                          → TransferResult{Present:true, A, B}  (HTTP → 200)
//
// Four load-bearing properties live in the sequence below:
//
//  1. The account is reached via sess.account.Account.Cash — the
//     LoadedAccount envelope is a value on ownedSession, but the inner
//     Account is a pointer and s.sessions is pointer-valued, so the
//     MoveCash mutation lands on the live account (P3-D6).
//  2. MoveCash fails BEFORE mutating on amount<=0 / illegal edge /
//     insufficient source (economy/account/cash.go) — on Err we are
//     guaranteed nothing moved, so returning (Err, no balances) is the
//     correct report and no rollback is needed.
//  3. MarkSnapshotDirty is unconditional on the ok path (P3-D9, mode-
//     blind). On-demand isolation is enforced later at session_save
//     via PersistOnClose=false; gating here would be wrong.
//  4. The post-move balances are read from the SAME in-memory Cash we
//     just mutated — single-writer makes that safe without a lock.
//
// Logging: debug on entry + idle miss (routine), warn on live-reject
// (user-visible failure, nothing broken), info on success. No log inside
// MoveCash — the hand must stay pure/replayable (root logging rule).
func (ownerShard *shard) handleTransfer(cmd Transfer) TransferResult {
	slog.Debug("funnel: handleTransfer begin",
		"account_id", cmd.AccountID,
		"from", cmd.From,
		"to", cmd.To,
		"amount", cmd.Amount,
	)

	sess, live := ownerShard.sessions[cmd.AccountID]
	if !live {
		slog.Debug("funnel: handleTransfer not live (routing to durable)",
			"account_id", cmd.AccountID,
		)
		return TransferResult{Present: false}
	}

	if err := sess.account.Account.Cash.MoveCash(cmd.From, cmd.To, cmd.Amount); err != nil {
		slog.Warn("funnel: handleTransfer rejected",
			"account_id", cmd.AccountID,
			"session_id", sess.binding.SessionID,
			"from", cmd.From,
			"to", cmd.To,
			"amount", cmd.Amount,
			"err", err,
		)
		return TransferResult{Present: true, Err: err}
	}

	ownerShard.bundle.Persist.MarkSnapshotDirty(sess.binding.SessionID, cmd.AccountID)

	slog.Info("funnel: handleTransfer ok",
		"account_id", cmd.AccountID,
		"session_id", sess.binding.SessionID,
		"from", cmd.From,
		"to", cmd.To,
		"amount", cmd.Amount,
		"post_available", sess.account.Account.Cash.Available,
		"post_bank", sess.account.Account.Cash.Bank,
	)
	return TransferResult{
		Present:   true,
		Available: sess.account.Account.Cash.Available,
		Bank:      sess.account.Account.Cash.Bank,
	}
}

// handleReadBalance is the live-balance read on the owner goroutine.
// Thin wrapper over readBalance (reads.go): if the account has no live
// session on this shard, return Present:false (the HTTP layer maps
// that to 409); otherwise return the four-bucket copy-out as a value,
// never a pointer into the owned Account.
func (ownerShard *shard) handleReadBalance(cmd ReadBalance) BalanceSnapshot {
	slog.Debug("funnel: handleReadBalance begin", "account_id", cmd.AccountID)

	session, live := ownerShard.sessions[cmd.AccountID]
	if !live {
		slog.Debug("funnel: handleReadBalance not live", "account_id", cmd.AccountID)
		return BalanceSnapshot{Present: false}
	}
	snapshot := readBalance(session)
	slog.Debug("funnel: handleReadBalance ok",
		"account_id", cmd.AccountID,
		"session_id", session.binding.SessionID,
		"available", snapshot.Available,
		"reserved", snapshot.Reserved,
		"settled", snapshot.Settled,
		"bank", snapshot.Bank,
	)
	return snapshot
}

// handleReadPositions is the live-positions read. Thin wrapper over
// readPositions (reads.go) that threads the optional Instrument filter
// through from the command. Mark degrade (MarkAvailable:false on a
// market-price cache miss) is handled by readPositions and is the
// production behavior until the tape relay goes live.
func (ownerShard *shard) handleReadPositions(cmd ReadPositions) PositionsSnapshot {
	slog.Debug("funnel: handleReadPositions begin",
		"account_id", cmd.AccountID,
		"instrument", cmd.Instrument,
	)

	session, live := ownerShard.sessions[cmd.AccountID]
	if !live {
		slog.Debug("funnel: handleReadPositions not live", "account_id", cmd.AccountID)
		return PositionsSnapshot{Present: false}
	}
	snapshot := PositionsSnapshot{
		Present:   true,
		Positions: readPositions(ownerShard, session, cmd.Instrument),
	}
	slog.Debug("funnel: handleReadPositions ok",
		"account_id", cmd.AccountID,
		"session_id", session.binding.SessionID,
		"instrument_filter", cmd.Instrument,
		"row_count", len(snapshot.Positions),
	)
	return snapshot
}

// handleReadOpenOrders is the live-open-orders read. Thin wrapper over
// readOpenOrders (reads.go). The account's working set is filtered out
// of the shard-wide tracks map; terminal orders are already deleted at
// settlement, so what survives is open-set by construction.
func (ownerShard *shard) handleReadOpenOrders(cmd ReadOpenOrders) OpenOrdersSnapshot {
	slog.Debug("funnel: handleReadOpenOrders begin", "account_id", cmd.AccountID)

	session, live := ownerShard.sessions[cmd.AccountID]
	if !live {
		slog.Debug("funnel: handleReadOpenOrders not live", "account_id", cmd.AccountID)
		return OpenOrdersSnapshot{Present: false}
	}
	snapshot := OpenOrdersSnapshot{
		Present: true,
		Orders:  readOpenOrders(ownerShard, cmd.AccountID),
	}
	slog.Debug("funnel: handleReadOpenOrders ok",
		"account_id", cmd.AccountID,
		"session_id", session.binding.SessionID,
		"row_count", len(snapshot.Orders),
	)
	return snapshot
}

// handleReadObservation is the composite live snapshot — balance +
// positions + working orders in one visit to the owning shard. The
// three helpers run back-to-back inside this one dispatch so the
// balance, positions, and orders rows cannot interleave with a
// settlement fill. The durable recent-closed-trades half is fanned
// out by the HTTP handler, not here.
func (ownerShard *shard) handleReadObservation(cmd ReadObservation) ObservationLive {
	slog.Debug("funnel: handleReadObservation begin", "account_id", cmd.AccountID)

	session, live := ownerShard.sessions[cmd.AccountID]
	if !live {
		slog.Debug("funnel: handleReadObservation not live", "account_id", cmd.AccountID)
		return ObservationLive{Present: false}
	}
	observation := ObservationLive{
		Present:    true,
		Balance:    readBalance(session),
		Positions:  readPositions(ownerShard, session, ""),
		OpenOrders: readOpenOrders(ownerShard, cmd.AccountID),
	}
	slog.Debug("funnel: handleReadObservation ok",
		"account_id", cmd.AccountID,
		"session_id", session.binding.SessionID,
		"position_rows", len(observation.Positions),
		"order_rows", len(observation.OpenOrders),
	)
	return observation
}

// handleApplyResult — Tier-1 settlement body — lives in settle.go
// (BROKER_PHASE5 §3). Dispatch above calls the method; this file no
// longer holds its stub.

// handleFireStop is the Tier-2 stop-fire seam. Push command; no reply.
func (ownerShard *shard) handleFireStop(cmd FireStop) {
	_ = cmd
}
