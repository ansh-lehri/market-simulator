package realize

import (
	"fmt"
	"log/slog"
	"sort"
	"time"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/cost"
)

// SettleSessionEnd force-closes every still-open position for the supplied
// accounts at the engine's official closing prices, adds the country's
// auto-square fee via the injected CostModule, folds through the same
// realization path a voluntary close uses, and (subject to the
// PersistOnClose gate captured on the accumulator at birth) enqueues one
// ClosedTradeRow per round-trip with ExitReason = auto_square.
//
// This is the ONE place realize commands Tier-1 hands directly (no order,
// no Convert, no reservation). External settlement driver (Layer 7-core)
// supplies the roster and per-instrument closing prices; realize reads
// the open-lot detail from the ledger's live Positions.
//
// Panics:
//   - unknown account in the ledger,
//   - missing closing price for an instrument with an open position,
//   - any Tier-1 hand failure after the input was just read from state
//     (broken coupling invariant under single-writer).
func (realizer *Realizer) SettleSessionEnd(
	sessionID SessionID,
	accountIDs []account.AccountID,
	closingPrice map[contract.InstrumentID]int64,
) {
	settledAt := time.Now()
	slog.Info("realize: session-end settlement started",
		"session", sessionID,
		"account_count", len(accountIDs),
		"instrument_count", len(closingPrice),
	)

	for _, accountID := range accountIDs {
		accountRecord := realizer.ledger.Get(accountID)
		if accountRecord == nil {
			panic(fmt.Sprintf("realize: SettleSessionEnd for unknown account %q", accountID))
		}

		instrumentIDs := sortedInstruments(accountRecord.Positions)
		for _, instrumentID := range instrumentIDs {
			realizer.settlePosition(sessionID, accountRecord, instrumentID, closingPrice, settledAt)
		}
	}

	slog.Info("realize: session-end settlement complete", "session", sessionID)
}

// settlePosition auto-squares one (account, instrument) round-trip. Broken
// out for clarity; still runs on the caller's goroutine (single-writer).
func (realizer *Realizer) settlePosition(
	sessionID SessionID,
	accountRecord *account.Account,
	instrumentID contract.InstrumentID,
	closingPrice map[contract.InstrumentID]int64,
	settledAt time.Time,
) {
	position := accountRecord.Positions[instrumentID]
	openQty := position.Available + position.Reserved
	if openQty == 0 {
		return
	}

	exitPrice, priceKnown := closingPrice[instrumentID]
	if !priceKnown {
		panic(fmt.Sprintf(
			"realize: SettleSessionEnd missing closing price for instrument %q (account %q)",
			instrumentID, accountRecord.ID,
		))
	}

	// 1. Book units out via Tier-1 hands. RemovePosition consumes from
	// Reserved, so any Available portion moves to Reserved first.
	if position.Available > 0 {
		if err := position.MovePosition(account.PosAvailable, account.PosReserved, position.Available); err != nil {
			panic(fmt.Sprintf(
				"realize: MovePosition failed during auto-square (account %q instrument %q available %d): %v",
				accountRecord.ID, instrumentID, position.Available, err,
			))
		}
	}
	matchedLots, removeErr := position.RemovePosition(openQty)
	if removeErr != nil {
		panic(fmt.Sprintf(
			"realize: RemovePosition failed during auto-square (account %q instrument %q qty %d): %v",
			accountRecord.ID, instrumentID, openQty, removeErr,
		))
	}

	// 2. Proceeds credit.
	proceeds := exitPrice * openQty
	if err := accountRecord.Cash.CreditCash(account.Available, proceeds); err != nil {
		panic(fmt.Sprintf(
			"realize: CreditCash proceeds failed during auto-square (account %q amount %d): %v",
			accountRecord.ID, proceeds, err,
		))
	}

	// 3. Per-fill cost with IsAutosquare = true (country's auto-square fee
	// line, e.g. India's CST-5, is applied inside ComputeFillCost).
	fillCost := realizer.costs.ComputeFillCost(cost.FillCostInput{
		Side:         contract.Sell,
		FillQty:      openQty,
		MakerPrice:   exitPrice,
		IsAutosquare: true,
	})
	if fillCost.Total > 0 {
		if err := accountRecord.Cash.DebitCash(account.Available, fillCost.Total); err != nil {
			panic(fmt.Sprintf(
				"realize: DebitCash fillCost failed during auto-square (account %q amount %d): %v",
				accountRecord.ID, fillCost.Total, err,
			))
		}
	}

	// 4. Same realization path as a voluntary close — fold through the
	// (possibly pre-existing) round-trip accumulator.
	input := RealizationInput{
		AccountID:       accountRecord.ID,
		SessionID:       sessionID,
		InstrumentID:    instrumentID,
		FillQty:         openQty,
		MakerPrice:      exitPrice,
		MatchedLots:     matchedLots,
		Cost:            fillCost,
		PositionNowFlat: true,
		ExitAt:          settledAt,
		Direction:       "long",
	}
	result := ComputeRealizedPnl(input)

	key := accKey{Account: accountRecord.ID, Instrument: instrumentID}
	accumulator := realizer.book[key]
	if accumulator == nil {
		accumulator = &RealizationAccumulator{
			AccountID:      accountRecord.ID,
			SessionID:      sessionID,
			InstrumentID:   instrumentID,
			PersistOnClose: accountRecord.PersistOnClose,
		}
		realizer.book[key] = accumulator
		slog.Debug("realize: auto-square accumulator born",
			"account", accountRecord.ID,
			"instrument", instrumentID,
			"persist_on_close", accountRecord.PersistOnClose,
		)
	}
	accumulator.fold(input, result)

	// 5. Order-level brokerage (forced order is an executed order).
	orderCost := realizer.costs.ComputeOrderCost(cost.OrderCostInput{
		EverFilled: true,
		TotalQty:   openQty,
		Side:       contract.Sell,
	})
	if orderCost.Total > 0 {
		if err := accountRecord.Cash.DebitCash(account.Available, orderCost.Total); err != nil {
			panic(fmt.Sprintf(
				"realize: DebitCash brokerage failed during auto-square (account %q amount %d): %v",
				accountRecord.ID, orderCost.Total, err,
			))
		}
	}

	// 6. Assemble with ExitAutoSquare, gate on birth-captured
	// PersistOnClose, enqueue, delete.
	tradeID := accumulator.TradeID
	if tradeID == "" {
		tradeID = realizer.nextTradeID()
	}
	closed := accumulator.assemble(tradeID, ExitAutoSquare)
	delete(realizer.book, key)

	slog.Info("realize: auto-square flushed",
		"account", accountRecord.ID,
		"instrument", instrumentID,
		"session", sessionID,
		"trade_id", tradeID,
		"open_qty", openQty,
		"exit_price", exitPrice,
		"gross", closed.Summary.GrossPnl,
		"total_costs", closed.Summary.TotalCosts,
		"net", closed.Summary.NetPnl,
		"order_brokerage", orderCost.Total,
		"persist_on_close", accumulator.PersistOnClose,
	)

	if !accumulator.PersistOnClose {
		return
	}
	realizer.persist.EnqueueClosedTrade(closed)
	slog.Info("realize: auto-square trade enqueued",
		"account", accountRecord.ID,
		"instrument", instrumentID,
		"trade_id", tradeID,
	)
}

// sortedInstruments returns the map's keys in deterministic order so
// per-account iteration is replayable (map iteration is randomized).
func sortedInstruments(positions map[contract.InstrumentID]*account.Position) []contract.InstrumentID {
	instrumentIDs := make([]contract.InstrumentID, 0, len(positions))
	for id := range positions {
		instrumentIDs = append(instrumentIDs, id)
	}
	sort.Strings(instrumentIDs)
	return instrumentIDs
}
