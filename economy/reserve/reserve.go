package reserve

import (
	"fmt"

	"simulator/contract"
	"simulator/economy/account"
)

// ReserveIfAvailable is the engine's ONE synchronous consult at Admission
// Gate 4 (doc §2). Atomic check-and-reserve, single-goroutine, no lock.
//
// The read-compare vs hand-guard relationship (doc §2.4, resolved):
//
//   - The explicit ReadAvailable* compare decides the return. If the
//     compare fails, ReserveIfAvailable returns a typed sentinel
//     (ErrInsufficientFunds / ErrInsufficientPosition) with NO state
//     mutation — the gate wants the decision BEFORE any move.
//   - The hand's sufficiency guard is a never-should-fire backstop. If
//     the compare said yes but the hand refuses, the read and the move
//     disagreed within one goroutine (impossible under single-writer)
//     = programming error = panic, per root policy.
//
// Option B (signed Available): ReadAvailableCash may return negative,
// and the compare (available < requiredCash with requiredCash > 0)
// correctly fails on a negative balance — "no buying power while
// underwater" falls out for free.
//
// Item 32 (per-archetype Gate-4 rule) is DEFERRED; every participant is
// treated identically here. Participant is a passthrough value carrier,
// carried into boundary logs for correlation but not branched on.
//
// Returns nil on admission; a typed sentinel on rejection. Callers
// dispatch with errors.Is on the sentinel.
func (service *ReservationService) ReserveIfAvailable(participant Participant, order Order) error {
	resolvedAccount := service.mustGetAccount(order.AccountID)

	switch order.Side {
	case contract.Buy:
		return service.reserveBuy(participant, order, resolvedAccount)
	case contract.Sell:
		return service.reserveSell(participant, order, resolvedAccount)
	}
	panic(fmt.Sprintf("reserve: unknown Side %v on order %q", order.Side, order.OrderID))
}

// reserveBuy handles the Buy branch of ReserveIfAvailable. Kept private
// so the branch split is not a public API — the engine sees only
// ReserveIfAvailable.
func (service *ReservationService) reserveBuy(
	participant Participant,
	order Order,
	resolvedAccount *account.Account,
) error {
	// 1. Compute required cash. Reserve READS the buffer (BestAsk +
	//    SlippageBuffer are engine-supplied on Order); it does not set
	//    policy on either.
	// Prices are stored as integer ticks (100 ticks = 1 rupee). Cash
	// buckets are in rupees (coins). Divide by 100 to convert tick×qty
	// into rupees so the comparison against availableCash is unit-safe.
	var requiredCash int64
	if order.IsMarket {
		requiredCash = (order.BestAsk*order.Qty + order.SlippageBuffer) / 100
	} else {
		requiredCash = order.LimitPrice * order.Qty / 100
	}
	// A sub-rupee order (price*qty < 100 ticks = ₹1) rounds to zero cash
	// required after the tick→rupee conversion. Reject it: MoveCash(0) panics
	// and a zero-cost buy would be a free position — both wrong.
	if requiredCash == 0 {
		service.logger.Info("reserve rejected: order value below minimum (sub-rupee)",
			"order_id", order.OrderID,
			"account_id", order.AccountID,
			"session_id", order.SessionID,
			"instrument", order.InstrumentID,
			"limit_price", order.LimitPrice,
			"qty", order.Qty,
		)
		return ErrInsufficientFunds
	}

	// 2. DECISION read (hot-path, in-memory, no store hop). Option B: a
	//    negative Available correctly fails any positive requirement.
	availableCash := resolvedAccount.ReadAvailableCash()
	if availableCash < requiredCash {
		service.logger.Info("reserve rejected: insufficient funds",
			"order_id", order.OrderID,
			"account_id", order.AccountID,
			"session_id", order.SessionID,
			"instrument", order.InstrumentID,
			"side", "Buy",
			"is_market", order.IsMarket,
			"required_cash", requiredCash,
			"available_cash", availableCash,
			"participant_id", participant.ID,
		)
		return ErrInsufficientFunds
	}

	// 3. RESERVE via the hand. The read proved sufficiency, so this
	//    MUST succeed; an error here = read/move disagreed within one
	//    goroutine = programming-error panic (root policy).
	if err := resolvedAccount.Cash.MoveCash(account.Available, account.Reserved, requiredCash); err != nil {
		panic(fmt.Sprintf(
			"reserve: MoveCash(Available→Reserved, %d) failed after positive read-check on order %q: %v",
			requiredCash, order.OrderID, err,
		))
	}

	// 4. Write the reservation record (in the BOOK, NOT the Account).
	service.book.put(&ReservationRecord{
		OrderID:      order.OrderID,
		AccountID:    order.AccountID,
		SessionID:    order.SessionID,
		InstrumentID: order.InstrumentID,
		Side:         contract.Buy,
		IsMarket:     order.IsMarket,
		LimitPrice:   order.LimitPrice,
		ReservedCash: requiredCash,
		RemainingQty: order.Qty,
		OrigQty:      order.Qty,
	})

	// 5. Emit money-event. In-memory event stream (system deferred §9).
	service.events.Emit(ReserveEvent{
		OrderID:   order.OrderID,
		AccountID: order.AccountID,
		Cash:      requiredCash,
	})

	// 6. Async snapshot mark — flags {cash split + this new reservation}
	//    dirty for the background snapshotter. NON-BLOCKING.
	service.persist.MarkSnapshotDirty(order.SessionID, order.AccountID)

	service.logger.Info("reserve admitted: buy",
		"order_id", order.OrderID,
		"account_id", order.AccountID,
		"session_id", order.SessionID,
		"instrument", order.InstrumentID,
		"is_market", order.IsMarket,
		"qty", order.Qty,
		"required_cash", requiredCash,
		"participant_id", participant.ID,
	)
	return nil
}

// reserveSell handles the Sell branch. Symmetric to reserveBuy but
// reserving position units instead of cash.
func (service *ReservationService) reserveSell(
	participant Participant,
	order Order,
	resolvedAccount *account.Account,
) error {
	// 1. Required is a POSITION, not cash.
	// 2. DECISION read (hot-path, in-memory). Missing instrument → 0 →
	//    correctly rejects (0 < any positive Qty) without special-casing.
	availablePosition := resolvedAccount.ReadAvailablePosition(order.InstrumentID)
	if availablePosition < order.Qty {
		service.logger.Info("reserve rejected: insufficient position",
			"order_id", order.OrderID,
			"account_id", order.AccountID,
			"session_id", order.SessionID,
			"instrument", order.InstrumentID,
			"side", "Sell",
			"required_units", order.Qty,
			"available_units", availablePosition,
			"participant_id", participant.ID,
		)
		return ErrInsufficientPosition
	}

	// 3. RESERVE units via the hand. Must succeed — read proved it.
	//    The map lookup that ReadAvailablePosition ≥ Qty > 0 implies must
	//    also succeed; a nil position here would be the read/move
	//    disagreement panic.
	position := resolvedAccount.Positions[order.InstrumentID]
	if position == nil {
		panic(fmt.Sprintf(
			"reserve: Positions[%q] nil after positive read-check on order %q",
			order.InstrumentID, order.OrderID,
		))
	}
	if err := position.MovePosition(account.PosAvailable, account.PosReserved, order.Qty); err != nil {
		panic(fmt.Sprintf(
			"reserve: MovePosition(PosAvailable→PosReserved, %d) failed after positive read-check on order %q: %v",
			order.Qty, order.OrderID, err,
		))
	}

	// 4. Reservation record.
	service.book.put(&ReservationRecord{
		OrderID:       order.OrderID,
		AccountID:     order.AccountID,
		SessionID:     order.SessionID,
		InstrumentID:  order.InstrumentID,
		Side:          contract.Sell,
		IsMarket:      order.IsMarket,
		LimitPrice:    order.LimitPrice,
		ReservedUnits: order.Qty,
		RemainingQty:  order.Qty,
		OrigQty:       order.Qty,
	})

	// 5. Emit position-lock reserve event.
	service.events.Emit(ReserveEvent{
		OrderID:   order.OrderID,
		AccountID: order.AccountID,
		Units:     order.Qty,
	})

	// 6. Async snapshot mark.
	service.persist.MarkSnapshotDirty(order.SessionID, order.AccountID)

	service.logger.Info("reserve admitted: sell",
		"order_id", order.OrderID,
		"account_id", order.AccountID,
		"session_id", order.SessionID,
		"instrument", order.InstrumentID,
		"qty", order.Qty,
		"participant_id", participant.ID,
	)
	return nil
}
