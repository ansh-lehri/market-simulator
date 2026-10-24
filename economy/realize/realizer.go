package realize

import (
	"fmt"
	"log/slog"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/cost"
)

// accKey addresses one open round-trip: (account, instrument).
type accKey struct {
	Account    account.AccountID
	Instrument contract.InstrumentID
}

// maxLiveClosedTrades is the ring-buffer capacity for on-demand sessions.
// On-demand trades are never sent to Postgres (PersistOnClose=false), so
// they accumulate here and are served to the observation endpoint directly.
const maxLiveClosedTrades = 50

// Realizer is realize's live driver — it owns the close-accumulator book,
// consults the account ledger at accumulator birth (for the PersistOnClose
// gate), and hands closed rows to the persistence seam.
//
// Single-writer assumed (the broker funnel enforces it later); no mutex,
// no goroutine.
type Realizer struct {
	book             map[accKey]*RealizationAccumulator
	ledger           AccountLedger
	persist          Persistence
	costs            cost.CostModule
	nextTradeID      func() TradeID
	liveClosedTrades []ClosedTradeRow // ring for on-demand (PersistOnClose=false) sessions
}

// NewRealizer builds a Realizer wired to the given ledger, persistence
// seam, and cost module. Costs is required — OnSellFill does not touch it
// (voluntary-close costs arrive on RealizationInput.Cost), but
// SettleSessionEnd calls ComputeFillCost / ComputeOrderCost itself.
// The TradeID fallback generator defaults to a crypto/rand hex string;
// tests may replace realizer.nextTradeID directly (same-package access).
func NewRealizer(ledger AccountLedger, persist Persistence, costs cost.CostModule) *Realizer {
	return &Realizer{
		book:        map[accKey]*RealizationAccumulator{},
		ledger:      ledger,
		persist:     persist,
		costs:       costs,
		nextTradeID: defaultTradeIDGenerator,
	}
}

// OnSellFill is the handoff called by the sell branch of 5.2's Convert per
// sell fill. It runs the per-fill FIFO realization, folds it into the
// round-trip accumulator, and on PositionNowFlat assembles + (subject to
// the PersistOnClose gate) enqueues one ClosedTradeRow, then drops the
// accumulator.
//
// Panics if the account is not in the ledger — that is a broken coupling
// invariant, not a business error.
func (realizer *Realizer) OnSellFill(input RealizationInput) {
	key := accKey{Account: input.AccountID, Instrument: input.InstrumentID}
	result := ComputeRealizedPnl(input)

	accumulator := realizer.book[key]
	if accumulator == nil {
		accountRecord := realizer.ledger.Get(input.AccountID)
		if accountRecord == nil {
			panic(fmt.Sprintf("realize: OnSellFill for unknown account %q (instrument %q)",
				input.AccountID, input.InstrumentID))
		}
		accumulator = &RealizationAccumulator{
			AccountID:      input.AccountID,
			SessionID:      input.SessionID,
			InstrumentID:   input.InstrumentID,
			PersistOnClose: accountRecord.PersistOnClose,
		}
		realizer.book[key] = accumulator
		slog.Debug("realize: accumulator born",
			"account", input.AccountID,
			"instrument", input.InstrumentID,
			"session", input.SessionID,
			"persist_on_close", accountRecord.PersistOnClose,
		)
	}

	accumulator.fold(input, result)
	slog.Debug("realize: per-fill fold",
		"account", input.AccountID,
		"instrument", input.InstrumentID,
		"fill_qty", input.FillQty,
		"maker_price", input.MakerPrice,
		"gross", result.Gross,
		"costs", result.Costs,
		"net", result.Net,
	)

	if !input.PositionNowFlat {
		return
	}

	tradeID := accumulator.TradeID
	if tradeID == "" {
		tradeID = realizer.nextTradeID()
	}
	closed := accumulator.assemble(tradeID, ExitVoluntary)
	delete(realizer.book, key)

	slog.Info("realize: round-trip flushed",
		"account", input.AccountID,
		"instrument", input.InstrumentID,
		"session", input.SessionID,
		"trade_id", tradeID,
		"qty", closed.Summary.Quantity,
		"gross", closed.Summary.GrossPnl,
		"total_costs", closed.Summary.TotalCosts,
		"net", closed.Summary.NetPnl,
		"exit_reason", closed.Summary.ExitReason,
		"persist_on_close", accumulator.PersistOnClose,
	)

	if !accumulator.PersistOnClose {
		realizer.appendLiveTrade(closed)
		slog.Info("realize: closed trade buffered (on-demand)",
			"account", input.AccountID,
			"instrument", input.InstrumentID,
			"trade_id", tradeID,
			"live_buffer_len", len(realizer.liveClosedTrades),
		)
		return
	}

	realizer.persist.EnqueueClosedTrade(closed)
	slog.Info("realize: closed trade enqueued",
		"account", input.AccountID,
		"instrument", input.InstrumentID,
		"trade_id", tradeID,
	)
}

// appendLiveTrade adds a closed trade to the on-demand ring buffer, evicting
// the oldest entry when the buffer is full.
func (realizer *Realizer) appendLiveTrade(row ClosedTradeRow) {
	realizer.liveClosedTrades = append(realizer.liveClosedTrades, row)
	if len(realizer.liveClosedTrades) > maxLiveClosedTrades {
		realizer.liveClosedTrades = realizer.liveClosedTrades[1:]
	}
}

// RecentLiveTrades returns up to n of the most recent on-demand closed trades,
// newest first. Returns nil when nothing has been buffered (canonical sessions
// never call appendLiveTrade, so the slice stays nil for them).
func (realizer *Realizer) RecentLiveTrades(n int) []ClosedTradeRow {
	src := realizer.liveClosedTrades
	if len(src) == 0 {
		return nil
	}
	if n > len(src) {
		n = len(src)
	}
	result := make([]ClosedTradeRow, n)
	for i := 0; i < n; i++ {
		result[i] = src[len(src)-1-i]
	}
	return result
}
