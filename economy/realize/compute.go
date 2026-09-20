package realize

import "simulator/economy/account"

// ComputeRealizedPnl prices one sell fill against its FIFO-matched lots.
// Pure: no mutation, no I/O, owns no lot queue, moves no money. Signed
// throughout — a loss is negative and is never clamped. The Lines map is
// carried through by reference for the three-line view.
func ComputeRealizedPnl(input RealizationInput) RealizationResult {
	var gross int64
	for _, matched := range input.MatchedLots {
		gross += (input.MakerPrice - matched.EntryPrice) * matched.Qty
	}
	costs := input.Cost.Total
	return RealizationResult{
		Gross: gross,
		Costs: costs,
		Net:   gross - costs,
		Lines: input.Cost.Lines,
	}
}

// ComputeUnrealizedMark returns the live paper P&L on an open position,
// marked to LTP, net of the entry-side turnover costs already incurred.
// Exit costs are NOT applied (nothing has been sold); brokerage is excluded
// (order-level). Pure read-time computation; never stored.
func ComputeUnrealizedMark(lots []account.Lot, ltp int64, incurredEntryCosts int64) (gross, net int64) {
	for _, lot := range lots {
		gross += (ltp - lot.EntryPrice) * lot.RemainingQty
	}
	net = gross - incurredEntryCosts
	return gross, net
}

// ComputeWeightedAvgEntry returns the qty-weighted mean entry price across
// still-open lots — the "buy average" shown next to an open position.
// Display only; FIFO remains the realization basis. Returns (0, false)
// when flat (no divide-by-zero).
func ComputeWeightedAvgEntry(lots []account.Lot) (avg int64, hasOpen bool) {
	var valueSum, qtySum int64
	for _, lot := range lots {
		valueSum += lot.EntryPrice * lot.RemainingQty
		qtySum += lot.RemainingQty
	}
	if qtySum == 0 {
		return 0, false
	}
	return valueSum / qtySum, true
}
