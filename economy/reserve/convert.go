package reserve

import (
	"fmt"
	"time"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/cost"
	"simulator/economy/realize"
)

// Convert (doc §3) is per-fill lifecycle bookkeeping — called by the
// engine on EVERY fill (every partial). It turns reservation into actual
// settlement, incrementally, and holds the core invariant (§0.1) at
// every instant: reservation covers exactly the still-unfilled quantity;
// debits/credits cover exactly the filled quantity; the two never
// overlap.
//
// Signature note (matches the augmented shape adopted in stage 3, doc
// §3.5 updated to match): the spec's pseudocode is Convert(order_id,
// fill_qty, maker_price); the Go signature adds:
//
//   - exitAt: the fill's timestamp. CreditPosition needs it for the new
//     lot's EntryAt; 5.4 tracks the max ExitAt across a round-trip's
//     folded fills. Passed in so this package holds no clock — replay is
//     bit-identical.
//   - tradeID: the upstream fill event's trade id, may be empty. The
//     realizer's accumulator keeps the first non-empty across a round-
//     trip; when all fills were empty, realize generates one at flush.
//
// Direction is HARDCODED to "long" inside the sell branch's
// RealizationInput (prototype is long-only; per handover note 12, only
// the caller changes when shorts land — not realize's compute).
//
// Panic policy (same as ReserveIfAvailable, root: "invariant violation
// → panic; a hand failing its guard after the caller already proved the
// precondition is a programming error"):
//   - no reservation record for orderID (engine can only Convert what it
//     previously Reserved — a missing record is a broker/engine wiring
//     bug);
//   - a hand rejects after the record's own values proved sufficiency
//     (Reserved-underflow, coupling break) — read/move disagreed within
//     one goroutine, impossible under single-writer;
//   - unknown Side on the record (defensive).
//
// The broker session funnel's deferred recover() (not built yet) is the
// process-safe catch for any of these; the process does not crash — the
// session is torn down cleanly and the user notified.
func (service *ReservationService) Convert(
	orderID OrderID,
	fillQty int64,
	makerPrice int64,
	exitAt time.Time,
	tradeID realize.TradeID,
) error {
	record := service.book.get(orderID)
	if record == nil {
		panic(fmt.Sprintf("convert: no reservation record for order %q (invariant broken)", orderID))
	}
	resolvedAccount := service.mustGetAccount(record.AccountID)

	switch record.Side {
	case contract.Buy:
		service.convertBuy(record, resolvedAccount, fillQty, makerPrice, exitAt)
	case contract.Sell:
		service.convertSell(record, resolvedAccount, fillQty, makerPrice, exitAt, tradeID)
	default:
		panic(fmt.Sprintf("convert: unknown Side %v on record %q", record.Side, orderID))
	}

	// Shared tail — brokerage-once-at-terminal (§3.4 / §5 locked wiring),
	// then delete the record if fully drained.
	//
	// EverFilled is set on ANY Convert (a Convert means at least one fill
	// happened). RemainingQty == 0 with !BrokerageCharged is the terminal
	// signal in the Convert path; Release fires the same guard on the
	// partial-fill-then-cancel path (stage 4), so BrokerageCharged makes
	// the debit fire exactly once regardless of which method sees the end.
	//
	// The three-way delete condition (RemainingQty == 0 AND both Reserved*
	// zero) skips the market-buy residue case: after the last fill of a
	// market buy, RemainingQty is 0 but ReservedCash may still be positive
	// (unused slippage buffer + savings). That residue is ReleaseExcess's
	// job (stage 4); the record must survive Convert to be true'd up.
	record.EverFilled = true
	if record.RemainingQty == 0 && !record.BrokerageCharged {
		service.chargeOrderCost(record, resolvedAccount)
		record.BrokerageCharged = true
	}
	// For a terminal limit buy, sweep any residual reserved cash (integer-
	// truncation rounding across multiple fills, or below-limit price
	// savings) back to Available so the three-way delete fires cleanly.
	// Market buys deliberately retain their buffer here — ReleaseExcess is
	// their true-up path.
	if !record.IsMarket && record.RemainingQty == 0 && record.ReservedCash > 0 {
		if err := resolvedAccount.Cash.MoveCash(account.Reserved, account.Available, record.ReservedCash); err != nil {
			panic(fmt.Sprintf(
				"convert: sweep limit buy residue MoveCash(Reserved→Available, %d) failed on order %q: %v",
				record.ReservedCash, orderID, err))
		}
		record.ReservedCash = 0
	}
	if record.RemainingQty == 0 && record.ReservedCash == 0 && record.ReservedUnits == 0 {
		service.book.remove(orderID)
	}
	return nil
}

// convertBuy handles the Buy branch of Convert (§3.5). Order matters
// (§3.3 LOCKED): principal move + position credit FIRST, cost debit
// AFTER. Friction rides on top of settled principal.
func (service *ReservationService) convertBuy(
	record *ReservationRecord,
	resolvedAccount *account.Account,
	fillQty int64,
	makerPrice int64,
	exitAt time.Time,
) {
	// Tick×qty → rupees: same unit conversion as at reserve time.
	actualCost := makerPrice * fillQty / 100

	// 1. Principal debit: Reserved → Settled, at MAKER price (universal core
	//    invariant: every fill executes at the resting/maker order's price).
	//    A refusal here means ReservedCash < actualCost — the engine handed
	//    us a fill exceeding what we reserved, which is the "no debt by
	//    construction" invariant break.
	if err := resolvedAccount.Cash.MoveCash(account.Reserved, account.Settled, actualCost); err != nil {
		panic(fmt.Sprintf(
			"convert buy: MoveCash(Reserved→Settled, %d) failed on order %q: %v (§2.7 no-debt invariant broken)",
			actualCost, record.OrderID, err))
	}

	// 2. Deliver units. Lazy-init the Position entry on first-buy Convert
	//    for this instrument (stage-3 design decision (a) — reserve directs
	//    the hand; account exposes no ensure-position helper today, so the
	//    map insertion happens here). CreditPosition then owns everything
	//    else (Available += units, append lot, §0.6 coupling preserved).
	position := resolvedAccount.Positions[record.InstrumentID]
	if position == nil {
		if resolvedAccount.Positions == nil {
			resolvedAccount.Positions = map[contract.InstrumentID]*account.Position{}
		}
		position = &account.Position{}
		resolvedAccount.Positions[record.InstrumentID] = position
	}
	if err := position.CreditPosition(fillQty, makerPrice, exitAt); err != nil {
		panic(fmt.Sprintf(
			"convert buy: CreditPosition(units=%d, entryPrice=%d) failed on order %q: %v",
			fillQty, makerPrice, record.OrderID, err))
	}

	// 3. Shrink the reservation to cover only still-unfilled qty.
	record.ReservedCash -= actualCost
	record.RemainingQty -= fillQty

	// 4. COST STACK (5.3): per-fill turnover charges. Ordered AFTER the
	//    principal move + position credit, consistent with §3.3.
	fillCost := service.costs.ComputeFillCost(cost.FillCostInput{
		Side:         contract.Buy,
		FillQty:      fillQty,
		MakerPrice:   makerPrice,
		IsAutosquare: false,
	})
	if fillCost.Total > 0 {
		if err := resolvedAccount.Cash.DebitCash(account.Available, fillCost.Total); err != nil {
			panic(fmt.Sprintf(
				"convert buy: DebitCash(fill cost %d) failed on order %q: %v",
				fillCost.Total, record.OrderID, err))
		}
	}

	// 5. Emit money-event (in-memory stream; system deferred §9).
	service.events.Emit(ConvertEvent{
		OrderID:       record.OrderID,
		AccountID:     record.AccountID,
		CashDebited:   actualCost,
		UnitsCredited: fillQty,
		Cost:          fillCost,
	})

	// 6. Async snapshot mark — cash split + position lots + shrunk
	//    reservation all changed.
	service.persist.MarkSnapshotDirty(record.SessionID, record.AccountID)

	service.logger.Info("convert buy fill",
		"order_id", record.OrderID,
		"account_id", record.AccountID,
		"session_id", record.SessionID,
		"instrument", record.InstrumentID,
		"fill_qty", fillQty,
		"maker_price", makerPrice,
		"actual_cost", actualCost,
		"fill_cost_total", fillCost.Total,
		"remaining_qty", record.RemainingQty,
	)
}

// convertSell handles the Sell branch of Convert (§3.5). Symmetric to
// convertBuy but delivering position units out (RemovePosition returns
// the matched FIFO lots for 5.4) and crediting proceeds to Available.
// The realization handoff to 5.4 is the ONE seam out of reserve besides
// the engine and 5.1.
func (service *ReservationService) convertSell(
	record *ReservationRecord,
	resolvedAccount *account.Account,
	fillQty int64,
	makerPrice int64,
	exitAt time.Time,
	tradeID realize.TradeID,
) {
	// Tick×qty → rupees: prices are tick integers; cash is in rupees.
	proceeds := makerPrice * fillQty / 100

	// 1. Deliver units OUT of Reserved (gone). RemovePosition returns the
	//    matched lots for 5.4 to price against MakerPrice. Position must
	//    exist — reserve took units to Reserved at admission — its absence
	//    is a coupling invariant break.
	position := resolvedAccount.Positions[record.InstrumentID]
	if position == nil {
		panic(fmt.Sprintf(
			"convert sell: Positions[%q] nil on order %q — reservation exists but position missing (coupling invariant broken)",
			record.InstrumentID, record.OrderID))
	}
	matched, err := position.RemovePosition(fillQty)
	if err != nil {
		panic(fmt.Sprintf(
			"convert sell: RemovePosition(%d) failed on order %q: %v",
			fillQty, record.OrderID, err))
	}

	// 2. Credit proceeds to AVAILABLE — immediately re-tradeable (STL-2,
	//    no T+1). CreditCash targets only Available; the hand's legal-target
	//    guard enforces that.
	if err := resolvedAccount.Cash.CreditCash(account.Available, proceeds); err != nil {
		panic(fmt.Sprintf(
			"convert sell: CreditCash(Available, %d) failed on order %q: %v",
			proceeds, record.OrderID, err))
	}

	// 3. Shrink the reservation.
	record.ReservedUnits -= fillQty
	record.RemainingQty -= fillQty

	// 4. COST STACK: sell-side charges (STT etc.), debit AFTER proceeds land.
	fillCost := service.costs.ComputeFillCost(cost.FillCostInput{
		Side:         contract.Sell,
		FillQty:      fillQty,
		MakerPrice:   makerPrice,
		IsAutosquare: false,
	})
	if fillCost.Total > 0 {
		if err := resolvedAccount.Cash.DebitCash(account.Available, fillCost.Total); err != nil {
			panic(fmt.Sprintf(
				"convert sell: DebitCash(fill cost %d) failed on order %q: %v",
				fillCost.Total, record.OrderID, err))
		}
	}

	// 5. Emit convert event.
	service.events.Emit(ConvertEvent{
		OrderID:        record.OrderID,
		AccountID:      record.AccountID,
		UnitsDelivered: fillQty,
		CashCredited:   proceeds,
		Cost:           fillCost,
	})

	// 6. Hand off to 5.4 (item 30, doc §3.5 step 6). PositionNowFlat is
	//    read AFTER RemovePosition mutated the position — that is the
	//    post-fill snapshot 5.4 uses as the round-trip-flush trigger.
	//    Direction is hardcoded "long" for the long-only prototype (per
	//    handover note 12); when shorts land, only THIS caller changes.
	service.realiz.OnSellFill(realize.RealizationInput{
		AccountID:       record.AccountID,
		SessionID:       record.SessionID,
		InstrumentID:    record.InstrumentID,
		FillQty:         fillQty,
		MakerPrice:      makerPrice,
		MatchedLots:     matched,
		Cost:            fillCost,
		PositionNowFlat: resolvedAccount.PositionFlat(record.InstrumentID),
		TradeID:         tradeID,
		Direction:       "long",
		ExitAt:          exitAt,
	})

	// 7. Async snapshot mark.
	service.persist.MarkSnapshotDirty(record.SessionID, record.AccountID)

	service.logger.Info("convert sell fill",
		"order_id", record.OrderID,
		"account_id", record.AccountID,
		"session_id", record.SessionID,
		"instrument", record.InstrumentID,
		"fill_qty", fillQty,
		"maker_price", makerPrice,
		"proceeds", proceeds,
		"fill_cost_total", fillCost.Total,
		"remaining_qty", record.RemainingQty,
	)
}

// chargeOrderCost is the shared terminal-brokerage debit. Called from
// Convert's shared tail (RemainingQty == 0), and re-used in stage 4 by
// Release for the partial-fill-then-cancel path — both routes guard on
// !BrokerageCharged so it fires exactly once regardless of which method
// sees the terminal state (§3.4 / §5 LOCKED).
//
// India rulebook: per-executed-order brokerage is a flat ₹20 (2000 paise)
// plus 18% GST on the ₹20 = 2360 paise. ComputeOrderCost returns Total=0
// when EverFilled == false (never-filled order pays zero), but this
// helper is only called when the caller has already confirmed a fill
// occurred — hence EverFilled: true is passed unconditionally.
func (service *ReservationService) chargeOrderCost(
	record *ReservationRecord,
	resolvedAccount *account.Account,
) {
	orderCost := service.costs.ComputeOrderCost(cost.OrderCostInput{
		EverFilled: true,
		TotalQty:   record.OrigQty,
		Side:       record.Side,
	})
	if orderCost.Total > 0 {
		if err := resolvedAccount.Cash.DebitCash(account.Available, orderCost.Total); err != nil {
			panic(fmt.Sprintf(
				"convert: DebitCash(order cost %d) failed on order %q: %v",
				orderCost.Total, record.OrderID, err))
		}
	}
	// Emit a ConvertEvent carrying only the order-cost breakdown (all
	// money-side fields zero — this event is the brokerage debit, not a
	// fill). Consumers distinguish by inspecting Cost.Total vs the fill-
	// side fields.
	service.events.Emit(ConvertEvent{
		OrderID:   record.OrderID,
		AccountID: record.AccountID,
		Cost:      orderCost,
	})
	service.logger.Info("convert: order-level brokerage debited",
		"order_id", record.OrderID,
		"account_id", record.AccountID,
		"session_id", record.SessionID,
		"instrument", record.InstrumentID,
		"orig_qty", record.OrigQty,
		"side", record.Side,
		"order_cost_total", orderCost.Total,
	)
}
