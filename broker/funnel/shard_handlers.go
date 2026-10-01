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
	case BankMove:
		typedCmd.ReplyTo <- ownerShard.handleBankMove(typedCmd)
	case ReadBalance:
		typedCmd.ReplyTo <- ownerShard.handleReadBalance(typedCmd)
	case ReadPositions:
		typedCmd.ReplyTo <- ownerShard.handleReadPositions(typedCmd)
	case ApplyResult:
		ownerShard.handleApplyResult(typedCmd)
	case FireStop:
		ownerShard.handleFireStop(typedCmd)
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

// handleBankMove is the Tier-3 bank-move seam (Stage 1 of the transfer
// design — the Stage-2 durableTransfer is off-shard, on the Store).
func (ownerShard *shard) handleBankMove(cmd BankMove) BankResult {
	_ = cmd
	return BankResult{}
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
