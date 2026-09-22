package reserve

import (
	"fmt"

	"simulator/contract"
	"simulator/economy/account"
)

// Release (doc §4.1) frees reservation that will NOT become a fill. Called
// by the engine on every terminal-non-fill state: cancel, expire-at-close,
// IOC/market remainder, FOK reject, auction lapse, quantity-decrease modify
// (partial). Never touches already-Converted portions — filled principal
// and per-fill costs stay put; only the still-Reserved slice moves back.
//
// releaseQty semantics (doc §4.1):
//   - releaseQty == 0        → release ALL remaining (the common cancel case).
//   - releaseQty ≥ Remaining → release all (forgiving: caller can pass a
//                              size that exceeds Remaining without erroring).
//   - 0 < releaseQty < Remaining → partial release for a qty-decrease
//                              modify. The order KEEPS RESTING with a
//                              decremented RemainingQty; the record is
//                              retained.
//
// Brokerage tail (doc §4.1 / §5 LOCKED): if the order ever filled but the
// per-executed-order charge has not fired yet (order terminated via
// cancel, not via RemainingQty == 0 in Convert), Release charges it once
// via chargeOrderCost. Guarded by BrokerageCharged so the debit fires
// exactly once regardless of which method sees the terminal state.
// A never-filled order (EverFilled == false) pays ZERO — the guard skips
// naturally.
//
// Doc pseudocode emits a ReleaseEvent for the brokerage debit; we emit
// a ConvertEvent instead (via the shared chargeOrderCost helper) for
// uniformity with Convert's terminal path — same debit shape, same
// event shape, one code path. The choice is internal to this package;
// consumers of the event stream cannot yet exist (system deferred §9).
//
// Panic policy: same as Convert. Missing record → panic (engine can only
// Release what was Reserved); hand refusals after positive record values
// → panic (§2.7 no-debt invariant break); unknown Side → panic. All
// caught at the broker session funnel (not yet built).
func (service *ReservationService) Release(orderID OrderID, releaseQty int64) error {
	record := service.book.get(orderID)
	if record == nil {
		panic(fmt.Sprintf("release: no reservation record for order %q (invariant broken)", orderID))
	}
	resolvedAccount := service.mustGetAccount(record.AccountID)

	if releaseQty < 0 {
		// Direction is carried by the method (Release is "give back reserved"),
		// so a negative amount is meaningless — same rule as the 5.1 hands.
		panic(fmt.Sprintf("release: releaseQty=%d must be >= 0 on order %q", releaseQty, orderID))
	}

	var releasedCash, releasedUnits int64
	switch record.Side {
	case contract.Buy:
		releasedCash = service.releaseBuy(record, resolvedAccount, releaseQty)
	case contract.Sell:
		releasedUnits = service.releaseSell(record, resolvedAccount, releaseQty)
	default:
		panic(fmt.Sprintf("release: unknown Side %v on record %q", record.Side, orderID))
	}

	// Partial-fill-then-cancel brokerage. `EverFilled` was set by Convert on
	// the first fill; `!BrokerageCharged` means Convert did not see the
	// terminal state (order was cancelled before RemainingQty hit 0).
	// chargeOrderCost handles the DebitCash + ConvertEvent + logging.
	if record.EverFilled && !record.BrokerageCharged {
		service.chargeOrderCost(record, resolvedAccount)
		record.BrokerageCharged = true
	}

	// Reduce or delete the record. Partial = order KEEPS RESTING (time
	// priority intact); full = terminal, record gone.
	isPartial := releaseQty > 0 && releaseQty < record.RemainingQty
	if isPartial {
		record.RemainingQty -= releaseQty
	} else {
		service.book.remove(orderID)
	}

	service.events.Emit(ReleaseEvent{
		OrderID:   record.OrderID,
		AccountID: record.AccountID,
		Cash:      releasedCash,
		Units:     releasedUnits,
	})
	service.persist.MarkSnapshotDirty(record.SessionID, record.AccountID)

	service.logger.Info("release",
		"order_id", record.OrderID,
		"account_id", record.AccountID,
		"session_id", record.SessionID,
		"instrument", record.InstrumentID,
		"side", record.Side,
		"release_qty", releaseQty,
		"released_cash", releasedCash,
		"released_units", releasedUnits,
		"remaining_qty", record.RemainingQty,
		"partial", isPartial,
		"brokerage_charged", record.BrokerageCharged,
	)
	return nil
}

// releaseBuy handles the Buy branch of Release. Full release drains
// ReservedCash to zero; partial release moves a half-up-rounded
// proportional slice (perQtyCashShare) and mutates ReservedCash by the
// same amount so record and Cash.Reserved stay in lock-step.
func (service *ReservationService) releaseBuy(
	record *ReservationRecord,
	resolvedAccount *account.Account,
	releaseQty int64,
) int64 {
	amount := record.ReservedCash
	if releaseQty > 0 && releaseQty < record.RemainingQty {
		amount = perQtyCashShare(record, releaseQty)
	}
	if err := resolvedAccount.Cash.MoveCash(account.Reserved, account.Available, amount); err != nil {
		panic(fmt.Sprintf(
			"release buy: MoveCash(Reserved→Available, %d) failed on order %q: %v (Reserved-bucket coupling invariant broken)",
			amount, record.OrderID, err))
	}
	record.ReservedCash -= amount
	return amount
}

// releaseSell handles the Sell branch of Release. Full release drains
// ReservedUnits to zero; partial release moves exactly releaseQty units
// (no rounding — units are integer already).
func (service *ReservationService) releaseSell(
	record *ReservationRecord,
	resolvedAccount *account.Account,
	releaseQty int64,
) int64 {
	units := record.ReservedUnits
	if releaseQty > 0 && releaseQty < record.RemainingQty {
		units = releaseQty
	}
	position := resolvedAccount.Positions[record.InstrumentID]
	if position == nil {
		panic(fmt.Sprintf(
			"release sell: Positions[%q] nil on order %q — reservation exists but position missing (coupling invariant broken)",
			record.InstrumentID, record.OrderID))
	}
	if err := position.MovePosition(account.PosReserved, account.PosAvailable, units); err != nil {
		panic(fmt.Sprintf(
			"release sell: MovePosition(PosReserved→PosAvailable, %d) failed on order %q: %v",
			units, record.OrderID, err))
	}
	record.ReservedUnits -= units
	return units
}

// perQtyCashShare returns the half-up-rounded proportional slice of
// ReservedCash for a partial buy release of `releaseQty` units out of the
// record's RemainingQty. Caller must have already checked
// 0 < releaseQty < RemainingQty (full-release paths use ReservedCash
// directly).
//
// Formula (half-up rounding on integer division): (a*b + c/2) / c where
// a = ReservedCash, b = releaseQty, c = RemainingQty. The "+ c/2" step is
// what pushes a fractional 0.5 up to 1 rather than truncating to 0 —
// matches applyRate's rounding convention in the cost engine so both
// packages are uniform.
//
// A note on drift: sequential partials round each slice independently, so
// tiny per-slice rounding can accumulate. The final full-release path is
// the safety net — it releases whatever remains in ReservedCash exactly,
// so Reserved always drains to zero at terminal. See stage 4 tests.
func perQtyCashShare(record *ReservationRecord, releaseQty int64) int64 {
	return (record.ReservedCash*releaseQty + record.RemainingQty/2) / record.RemainingQty
}

// ReleaseExcess (doc §4.2) is the market-buy true-up on completion.
// After a market buy is FULLY filled (RemainingQty == 0), the reservation
// may still hold a positive ReservedCash residue — the unused slippage
// buffer plus savings from fills cheaper than best_ask. That residue must
// be returned so nothing leaks.
//
// Not called for limit orders (Convert trues them to zero via the exact
// maker=limit price) — this method is specifically the market-buy buffer
// return. A resting limit that finishes with ReservedCash != 0 would be
// a Convert bug, not something ReleaseExcess should fix.
//
// Preconditions (asserted, dev-fatal):
//   - RemainingQty must be 0 (order fully executed). Otherwise Convert
//     hasn't finished; calling ReleaseExcess here is an engine wiring bug.
//   - BrokerageCharged must be true (Convert's terminal check should
//     have fired brokerage when RemainingQty hit 0). If not, Convert's
//     terminal detection missed — coupling invariant break.
//
// nil-record is a clean no-op: Convert may have deleted the record at
// an exact-zero market-buy (fill matched best_ask exactly with zero
// slippage buffer — unusual but possible). §4.2 explicitly permits this.
func (service *ReservationService) ReleaseExcess(orderID OrderID) error {
	record := service.book.get(orderID)
	if record == nil {
		// Convert cleared the record at exact zero — nothing to true up.
		return nil
	}
	if record.RemainingQty != 0 {
		panic(fmt.Sprintf(
			"release_excess: order %q incomplete (RemainingQty=%d, want 0)",
			orderID, record.RemainingQty))
	}
	if !record.BrokerageCharged {
		panic(fmt.Sprintf(
			"release_excess: BrokerageCharged=false on completed order %q — Convert's terminal check missed (§3.4 invariant broken)",
			orderID))
	}

	resolvedAccount := service.mustGetAccount(record.AccountID)

	residue := record.ReservedCash
	if residue > 0 {
		if err := resolvedAccount.Cash.MoveCash(account.Reserved, account.Available, residue); err != nil {
			panic(fmt.Sprintf(
				"release_excess: MoveCash(Reserved→Available, %d) failed on order %q: %v",
				residue, orderID, err))
		}
		record.ReservedCash = 0
	}
	service.book.remove(orderID)

	service.events.Emit(ReleaseEvent{
		OrderID:   orderID,
		AccountID: record.AccountID,
		Cash:      residue,
	})
	service.persist.MarkSnapshotDirty(record.SessionID, record.AccountID)

	service.logger.Info("release_excess",
		"order_id", orderID,
		"account_id", record.AccountID,
		"session_id", record.SessionID,
		"instrument", record.InstrumentID,
		"residue_cash", residue,
	)
	return nil
}
