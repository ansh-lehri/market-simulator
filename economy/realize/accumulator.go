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
func (accumulator *RealizationAccumulator) fold(input RealizationInput, result RealizationResult) {
	accumulator.QtyClosed += input.FillQty
	accumulator.GrossAccum += result.Gross
	accumulator.CostsTotal += result.Costs

	for name, amount := range result.Lines {
		if accumulator.CostsAccum == nil {
			accumulator.CostsAccum = map[string]int64{}
		}
		accumulator.CostsAccum[name] += amount
	}

	for _, matched := range input.MatchedLots {
		accumulator.entryValueSum += matched.EntryPrice * matched.Qty
		accumulator.entryQtySum += matched.Qty
		if accumulator.FirstEntryAt.IsZero() || matched.EntryAt.Before(accumulator.FirstEntryAt) {
			accumulator.FirstEntryAt = matched.EntryAt
		}
	}

	accumulator.exitValueSum += input.MakerPrice * input.FillQty
	accumulator.exitQtySum += input.FillQty

	if input.ExitAt.After(accumulator.ExitAt) {
		accumulator.ExitAt = input.ExitAt
	}

	if accumulator.TradeID == "" && input.TradeID != "" {
		accumulator.TradeID = input.TradeID
	}

	if accumulator.Direction == "" && input.Direction != "" {
		accumulator.Direction = input.Direction
	}
}

// assemble turns the flushed accumulator into one ClosedTradeRow — summary
// row plus itemized cost child, linked by tradeID. The Realizer supplies
// tradeID (falling back to its generator if the accumulator never saw one)
// and the exitReason (voluntary vs auto_square).
func (accumulator *RealizationAccumulator) assemble(tradeID TradeID, exitReason ExitReason) ClosedTradeRow {
	var entryPrice int64
	if accumulator.entryQtySum > 0 {
		entryPrice = accumulator.entryValueSum / accumulator.entryQtySum
	}
	var exitPrice int64
	if accumulator.exitQtySum > 0 {
		exitPrice = accumulator.exitValueSum / accumulator.exitQtySum
	}
	net := accumulator.GrossAccum - accumulator.CostsTotal

	summary := ParticipantTradeRow{
		TradeID:      tradeID,
		AccountID:    accumulator.AccountID,
		SessionID:    accumulator.SessionID,
		InstrumentID: accumulator.InstrumentID,
		Direction:    accumulator.Direction,
		Quantity:     accumulator.QtyClosed,
		EntryPrice:   entryPrice,
		ExitPrice:    exitPrice,
		EntryAt:      accumulator.FirstEntryAt,
		ExitAt:       accumulator.ExitAt,
		GrossPnl:     accumulator.GrossAccum,
		TotalCosts:   accumulator.CostsTotal,
		NetPnl:       net,
		PnlPct:       pnlPct(net, accumulator.entryValueSum),
		ExitReason:   exitReason,
	}
	costChild := TradeCostRow{
		TradeID: tradeID,
		Lines:   accumulator.CostsAccum,
		Total:   accumulator.CostsTotal,
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
