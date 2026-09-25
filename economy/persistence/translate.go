package persistence

import "simulator/economy/realize"

// translateClosedTrade is the ONE translation hop the seam performs (§4). It
// unpacks a realize.ClosedTradeRow into a flat TradeInsert whose universal
// (participant_trades) fields map 1:1 to schema §2.2 columns and whose cost
// payload is carried through opaquely — country-agnostic here.
//
// The country-specific mapping (line-name → trade_costs column, unknown-key
// rejection per D1, brokerage=0 per D2) is the CountryCostAdapter's job at
// INSERT time (Stage 2), not this function's.
//
// Cost.Lines is defensively cloned so an owner mutating the source map after
// enqueue cannot corrupt a row already sitting on the write-behind buffer.
func translateClosedTrade(closed realize.ClosedTradeRow) TradeInsert {
	summary := closed.Summary
	return TradeInsert{
		TradeID:      summary.TradeID,
		AccountID:    summary.AccountID,
		SessionID:    summary.SessionID,
		InstrumentID: summary.InstrumentID,
		Direction:    summary.Direction,
		Quantity:     summary.Quantity,
		EntryPrice:   summary.EntryPrice,
		ExitPrice:    summary.ExitPrice,
		EntryAt:      summary.EntryAt,
		ExitAt:       summary.ExitAt,
		GrossPnl:     summary.GrossPnl,
		TotalCosts:   summary.TotalCosts,
		NetPnl:       summary.NetPnl,
		PnlPct:       summary.PnlPct,
		ExitReason:   string(summary.ExitReason),
		CostLines:    cloneCostLines(closed.Cost.Lines),
		CostTotal:    closed.Cost.Total,
	}
}

// cloneCostLines returns a defensive copy of a cost-lines map. A nil source
// returns nil (not an empty map) so downstream can distinguish "not set"
// from "explicitly empty" if that ever matters.
func cloneCostLines(source map[string]int64) map[string]int64 {
	if source == nil {
		return nil
	}
	copy := make(map[string]int64, len(source))
	for lineName, amount := range source {
		copy[lineName] = amount
	}
	return copy
}
