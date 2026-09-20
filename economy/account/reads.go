package account

import "simulator/contract"

// -----------------------------------------------------------------------------
// The two hot-path reads (items 14 / 15). Both are per-order and read the
// in-memory working copy — NO store hop, ever. The store read that makes
// this safe already happened once, at session init (Redis snapshot →
// in-memory), off the hot path. See LAYER_5_1_READ_OPERATIONS.md §3/§4.
//
// Neither read mutates, locks, or decides policy. Decisions (can this order
// reserve?) belong to the Tier-2 caller.
// -----------------------------------------------------------------------------

// ReadAvailableCash returns live spendable trading-account cash for a buy
// fundability check. Returns the RAW signed value — Option B allows Available
// to go negative (DebitCash may drive it below zero on fees), and clamping to
// 0 here would lie to the reserve check and mask fee-driven debt.
//
// Panics if the working state is not primed: reaching this read with a nil
// Account means an order routed to the gate before session init completed —
// an ordering bug in the wiring, not something a caller can recover from.
func (account *Account) ReadAvailableCash() int64 {
	if account == nil {
		panic("ReadAvailableCash: working state not primed (order before session init)")
	}
	return account.Cash.Available
}

// ReadAvailablePosition returns live sellable units of one instrument for a
// sell fundability check. Missing instrument → 0 (a legitimate "hold none",
// not an error): the sell reserve then refuses correctly (0 < sellQty)
// without any special-casing. Always ≥ 0 — no primitive drives position
// Available negative.
//
// Panics on a nil Account for the same reason as ReadAvailableCash.
func (account *Account) ReadAvailablePosition(instrumentID contract.InstrumentID) int64 {
	if account == nil {
		panic("ReadAvailablePosition: working state not primed (order before session init)")
	}
	position, exists := account.Positions[instrumentID]
	if !exists {
		return 0
	}
	return position.Available
}
