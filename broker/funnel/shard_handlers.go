package funnel

import "fmt"

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

// handlePlaceOrder is the Tier-1 order-intake seam.
func (ownerShard *shard) handlePlaceOrder(cmd PlaceOrder) PlaceResult {
	_ = cmd
	return PlaceResult{}
}

// handleCancel is the Tier-1 cancel seam.
func (ownerShard *shard) handleCancel(cmd CancelOrder) CancelResult {
	_ = cmd
	return CancelResult{}
}

// handleTransfer is the Tier-3 bank-move seam — the in-session Stage-1
// branch of the Phase-3 design (BROKER_PHASE3 §2.2). Phase-3 Stage A
// leaves the body as the Phase-0 empty echo so dispatch stays well-typed;
// Stage B fills the real body (XOR + MoveCash + MarkSnapshotDirty +
// mode-blind {Available, Bank} reply).
//
// The zero value returned here is TransferResult{Present:false}; no HTTP
// handler exists in Stage A, so nothing acts on it. The off-shard Stage-2
// durable transfer (store.DurableTransfer) is composed by the Phase-3
// HTTP handler, not here (P3-D4).
func (ownerShard *shard) handleTransfer(cmd Transfer) TransferResult {
	_ = cmd
	return TransferResult{}
}

// handleReadBalance is the Tier-4 live-balance-read seam. The copy-out
// contract lives here (BROKER_TIER0 §Point-5 read rule): the handler
// returns a by-value snapshot, never a pointer into the owned Account.
func (ownerShard *shard) handleReadBalance(cmd ReadBalance) BalanceSnapshot {
	_ = cmd
	return BalanceSnapshot{}
}

// handleReadPositions is the Tier-4 live-positions-read seam. Same
// copy-out contract as handleReadBalance.
func (ownerShard *shard) handleReadPositions(cmd ReadPositions) PositionsSnapshot {
	_ = cmd
	return PositionsSnapshot{}
}

// handleApplyResult is the Tier-1 settlement seam — applies a
// contract.EngineResult against the account. Push command; no reply.
func (ownerShard *shard) handleApplyResult(cmd ApplyResult) {
	_ = cmd
}

// handleFireStop is the Tier-2 stop-fire seam. Push command; no reply.
func (ownerShard *shard) handleFireStop(cmd FireStop) {
	_ = cmd
}
