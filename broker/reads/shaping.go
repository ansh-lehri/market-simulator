package reads

import (
	"simulator/broker/funnel"
	"simulator/broker/store"
	"simulator/contract"
)

// Response shapers for every read endpoint. Each converts a funnel view
// struct (or a store row) into a map ready for json.Encode, with the
// field names and value shapes the clients consume.
//
// Three rules bake in here:
//   - snake_case keys everywhere;
//   - no "unit" / currency field — all money is a bare int64;
//   - enum ints turn into stable lowercase tags ("buy"/"sell",
//     "working"/"filled"/…) rather than their numeric values.
//
// Shapes are map[string]any rather than per-endpoint structs because the
// composite observation endpoint merges three of these legs into one
// body, and the maps compose cleanly.

func shapeBalance(snapshot funnel.BalanceSnapshot) map[string]any {
	return map[string]any{
		"available": snapshot.Available,
		"reserved":  snapshot.Reserved,
		"settled":   snapshot.Settled,
		"bank":      snapshot.Bank,
	}
}

// shapePositions returns an empty slice (not nil) for a flat account so
// the encoder emits `"positions":[]` rather than `"positions":null`.
func shapePositions(views []funnel.PositionView) []map[string]any {
	rows := make([]map[string]any, 0, len(views))
	for _, view := range views {
		rows = append(rows, map[string]any{
			"instrument":         string(view.Instrument),
			"qty":                view.Qty,
			"reserved_qty":       view.ReservedQty,
			"weighted_avg_entry": view.WeightedAvgEntry,
			"mark_available":     view.MarkAvailable,
			"ltp":                view.LTP,
			"gross_unrealized":   view.GrossUnrealized,
		})
	}
	return rows
}

// shapeOrders derives filled_qty at the edge rather than mirroring it on
// the funnel view — the funnel ships orig/remaining raw, and
// OrigQty-RemainingQty is cheap to compute here.
func shapeOrders(views []funnel.OpenOrderView) []map[string]any {
	rows := make([]map[string]any, 0, len(views))
	for _, view := range views {
		rows = append(rows, map[string]any{
			"order_id":         string(view.OrderID),
			"instrument":       string(view.Instrument),
			"side":             sideString(view.Side),
			"is_market":        view.IsMarket,
			"limit_price":      view.LimitPrice,
			"orig_qty":         view.OrigQty,
			"remaining_qty":    view.RemainingQty,
			"filled_qty":       view.OrigQty - view.RemainingQty,
			"reserved_cash":    view.ReservedCash,
			"reserved_units":   view.ReservedUnits,
			"status":           statusString(view.Status),
			"cancel_in_flight": view.CancelInFlight,
		})
	}
	return rows
}

// shapeLines maps a slice of cost lines (STT, exchange, SEBI, …) into
// the response list. Only non-zero lines reach us from the Store (the
// mapper drops zeros), so no filtering here.
func shapeLines(lines []store.Line) []map[string]any {
	rows := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		rows = append(rows, map[string]any{
			"name":   line.Name,
			"amount": line.Amount,
		})
	}
	return rows
}

// shapeCostBreakdowns maps per-trade cost rows for the paginated costs
// endpoint. ExitAt is deliberately NOT in the body — it's the
// pagination key the handler uses to form next_before, not something
// the client needs per row.
func shapeCostBreakdowns(breakdowns []store.TradeCostBreakdown) []map[string]any {
	rows := make([]map[string]any, 0, len(breakdowns))
	for _, breakdown := range breakdowns {
		rows = append(rows, map[string]any{
			"trade_id": breakdown.TradeID,
			"lines":    shapeLines(breakdown.Lines),
			"total":    breakdown.Total,
		})
	}
	return rows
}

// shapeTradeViews shapes the on-demand in-session trade log ([]funnel.TradeView)
// into the same wire format as shapeTrades, so the client sees an identical
// recent_trades shape regardless of session mode.
func shapeTradeViews(views []funnel.TradeView) []map[string]any {
	trades := make([]map[string]any, 0, len(views))
	for _, view := range views {
		trades = append(trades, map[string]any{
			"trade_id":    view.TradeID,
			"instrument":  view.InstrumentID,
			"direction":   view.Direction,
			"quantity":    view.Quantity,
			"entry_price": view.EntryPrice,
			"exit_price":  view.ExitPrice,
			"entry_at":    view.EntryAt,
			"exit_at":     view.ExitAt,
			"gross_pnl":   view.GrossPnl,
			"total_costs": view.TotalCosts,
			"net_pnl":     view.NetPnl,
			"pnl_pct":     view.PnlPct,
			"exit_reason": view.ExitReason,
		})
	}
	return trades
}

func shapeTrades(rows []store.TradeRow) []map[string]any {
	trades := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		trades = append(trades, map[string]any{
			"trade_id":    row.TradeID,
			"instrument":  row.InstrumentID,
			"direction":   row.Direction,
			"quantity":    row.Quantity,
			"entry_price": row.EntryPrice,
			"exit_price":  row.ExitPrice,
			"entry_at":    row.EntryAt,
			"exit_at":     row.ExitAt,
			"gross_pnl":   row.GrossPnl,
			"total_costs": row.TotalCosts,
			"net_pnl":     row.NetPnl,
			"pnl_pct":     row.PnlPct,
			"exit_reason": row.ExitReason,
		})
	}
	return trades
}

// sideString returns "unknown" on an out-of-range value so a malformed
// view surfaces loudly in the response body rather than silently
// stringifying as "buy".
func sideString(side contract.Side) string {
	switch side {
	case contract.Buy:
		return "buy"
	case contract.Sell:
		return "sell"
	default:
		return "unknown"
	}
}

// statusString mirrors sideString: the zero-value OrderStatusUnknown
// stringifies as "unknown" so a bug-shaped track is visible in the body.
func statusString(status funnel.OrderStatus) string {
	switch status {
	case funnel.OrderStatusWorking:
		return "working"
	case funnel.OrderStatusPartiallyFilled:
		return "partially_filled"
	case funnel.OrderStatusFilled:
		return "filled"
	case funnel.OrderStatusCancelled:
		return "cancelled"
	case funnel.OrderStatusRejected:
		return "rejected"
	default:
		return "unknown"
	}
}
