package realize

import (
	"simulator/contract"
	"simulator/economy/account"
	"time"
)

// RealizationAccumulator is the one piece of state realize owns — one entry
// per open (account, instrument) round-trip in progress. Partial sells fold
// into it until the position goes flat; then assemble emits one
// ClosedTradeRow and the entry is deleted (by the Realizer, Stage 3).
type RealizationAccumulator struct {
	AccountID    account.AccountID
	SessionID    SessionID
	InstrumentID contract.InstrumentID

	QtyClosed  int64
	GrossAccum int64
	CostsAccum map[string]int64
	CostsTotal int64

	entryValueSum int64
	entryQtySum   int64
	exitValueSum  int64
	exitQtySum    int64

	FirstEntryAt time.Time
	ExitAt       time.Time

	// TradeID is the first non-empty id observed across the round-trip's
	// fills; stays empty when no upstream id was supplied. The Realizer
	// resolves the fallback (its generator) before calling assemble.
	TradeID TradeID

	// Direction — first non-empty value observed on a folded fill's
	// RealizationInput. Realize does not infer it; the caller supplies it.
	Direction string

	// PersistOnClose is read from the account at accumulator birth and
	// carried per-flush so the gate decision is local (spec §7).
	PersistOnClose bool
}

// fold merges one per-fill realization into the round-trip accumulator.
// Signed throughout; no clamping. Line names are pass-through from
// result.Lines; values accumulate.
func (acc *RealizationAccumulator) fold(input RealizationInput, result RealizationResult) {
	acc.QtyClosed += input.FillQty
	acc.GrossAccum += result.Gross
	acc.CostsTotal += result.Costs

	for name, amount := range result.Lines {
		if acc.CostsAccum == nil {
			acc.CostsAccum = map[string]int64{}
		}
		acc.CostsAccum[name] += amount
	}

	for _, matched := range input.MatchedLots {
		acc.entryValueSum += matched.EntryPrice * matched.Qty
		acc.entryQtySum += matched.Qty
		if acc.FirstEntryAt.IsZero() || matched.EntryAt.Before(acc.FirstEntryAt) {
			acc.FirstEntryAt = matched.EntryAt
		}
	}

	acc.exitValueSum += input.MakerPrice * input.FillQty
	acc.exitQtySum += input.FillQty

	if input.ExitAt.After(acc.ExitAt) {
		acc.ExitAt = input.ExitAt
	}

	if acc.TradeID == "" && input.TradeID != "" {
		acc.TradeID = input.TradeID
	}

	if acc.Direction == "" && input.Direction != "" {
		acc.Direction = input.Direction
	}
}

// assemble turns the flushed accumulator into one ClosedTradeRow — summary
// row plus itemized cost child, linked by tradeID. The Realizer supplies
// tradeID (falling back to its generator if the accumulator never saw one)
// and the exitReason (voluntary vs auto_square).
func (acc *RealizationAccumulator) assemble(tradeID TradeID, exitReason ExitReason) ClosedTradeRow {
	var entryPrice int64
	if acc.entryQtySum > 0 {
		entryPrice = acc.entryValueSum / acc.entryQtySum
	}
	var exitPrice int64
	if acc.exitQtySum > 0 {
		exitPrice = acc.exitValueSum / acc.exitQtySum
	}
	net := acc.GrossAccum - acc.CostsTotal

	summary := ParticipantTradeRow{
		TradeID:      tradeID,
		AccountID:    acc.AccountID,
		SessionID:    acc.SessionID,
		InstrumentID: acc.InstrumentID,
		Direction:    acc.Direction,
		Quantity:     acc.QtyClosed,
		EntryPrice:   entryPrice,
		ExitPrice:    exitPrice,
		EntryAt:      acc.FirstEntryAt,
		ExitAt:       acc.ExitAt,
		GrossPnl:     acc.GrossAccum,
		TotalCosts:   acc.CostsTotal,
		NetPnl:       net,
		PnlPct:       pnlPct(net, acc.entryValueSum),
		ExitReason:   exitReason,
	}
	costChild := TradeCostRow{
		TradeID: tradeID,
		Lines:   acc.CostsAccum,
		Total:   acc.CostsTotal,
	}
	return ClosedTradeRow{Summary: summary, Cost: costChild}
}

// pnlPct returns net divided by entry-value basis; zero when no basis
// (e.g. an accumulator with no folded lots). Signed — a loss returns
// negative.
func pnlPct(net int64, entryValueSum int64) float64 {
	if entryValueSum == 0 {
		return 0
	}
	return float64(net) / float64(entryValueSum)
}
