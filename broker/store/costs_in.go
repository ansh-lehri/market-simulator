package store

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"simulator/country/india"
)

// Line is one named cost entry inside a per-trade or aggregate cost
// breakdown. Country-agnostic shape (BROKER_READ_SURFACES §8.3): Name is
// a line name a country's cost module emits (India: "stt", "exchange",
// ...), Amount is integer coins. Only non-zero lines are surfaced —
// zero-value lines are dropped (per-trade: brokerage is always 0 under
// D2, so it never appears; aggregate: a line that was never charged for
// this account sums to 0 and drops too).
type Line struct {
	Name   string
	Amount int64
}

// TradeCostBreakdown is one closed trade's cost view. Total is the
// trade_costs.total column verbatim — the stored total, never
// recomputed (BROKER_PHASE1 §3.6).
type TradeCostBreakdown struct {
	TradeID string
	Lines   []Line
	Total   int64
}

// CostAggregate is the account-level sum across every closed trade.
// TradeCount is count(*); Lines are per-line sums with zeros dropped;
// Total is Σ trade_costs.total.
type CostAggregate struct {
	TradeCount int
	Lines      []Line
	Total      int64
}

// selectCostsPerTradeINSQL — per-trade India cost breakdown, paginated.
//
// The join through participant_trades is MANDATORY: trade_costs has no
// account_id column (PK = trade_id; 1:1 child). pt.account_id is the
// filter and pt.exit_at is the keyset-pagination key.
//
// Column SELECT list is built from country/india constants — the ONE
// place India column knowledge lives in this file (quarantine rule,
// P1-D7). `brokerage` is intentionally not SELECTed: it is always 0 at
// the per-trade level under D2 and has no India Line* name to map to.
const selectCostsPerTradeINSQL = `SELECT pt.trade_id,
	tc.` + india.ColumnSTT + `,
	tc.` + india.ColumnExchange + `,
	tc.` + india.ColumnSEBI + `,
	tc.` + india.ColumnStamp + `,
	tc.` + india.ColumnGST + `,
	tc.` + india.ColumnAutosquareFee + `,
	tc.` + india.ColumnTotal + `
FROM ` + india.TradeCostsTable + ` tc
JOIN participant_trades pt ON tc.trade_id = pt.trade_id
WHERE pt.account_id = $1 AND ($2::timestamptz IS NULL OR pt.exit_at < $2)
ORDER BY pt.exit_at DESC
LIMIT $3`

// selectCostsAggregateINSQL — account-level sum across India cost columns
// via the same mandatory participant_trades join. count(*) + per-line
// sums + Σ total in ONE round trip. coalesce guards empty-aggregate NULLs.
const selectCostsAggregateINSQL = `SELECT count(*),
	coalesce(sum(tc.` + india.ColumnSTT + `), 0),
	coalesce(sum(tc.` + india.ColumnExchange + `), 0),
	coalesce(sum(tc.` + india.ColumnSEBI + `), 0),
	coalesce(sum(tc.` + india.ColumnStamp + `), 0),
	coalesce(sum(tc.` + india.ColumnGST + `), 0),
	coalesce(sum(tc.` + india.ColumnAutosquareFee + `), 0),
	coalesce(sum(tc.` + india.ColumnTotal + `), 0)
FROM ` + india.TradeCostsTable + ` tc
JOIN participant_trades pt ON tc.trade_id = pt.trade_id
WHERE pt.account_id = $1`

// mapIndiaCostsToLines is THE one shared column→[]Line mapper for the
// India cost stack. Reads six per-line amounts and emits a []Line in
// stable column order, dropping any zero-value line.
//
// Stable order matters: Phase-6 response shaping (and any human reading
// a cost sheet) expects a predictable ordering. Column order in the
// SELECTs above matches this append order.
func mapIndiaCostsToLines(stt, exchange, sebi, stamp, gst, autosquare int64) []Line {
	lines := make([]Line, 0, 6)
	if stt != 0 {
		lines = append(lines, Line{Name: india.LineSTT, Amount: stt})
	}
	if exchange != 0 {
		lines = append(lines, Line{Name: india.LineExchange, Amount: exchange})
	}
	if sebi != 0 {
		lines = append(lines, Line{Name: india.LineSEBI, Amount: sebi})
	}
	if stamp != 0 {
		lines = append(lines, Line{Name: india.LineStamp, Amount: stamp})
	}
	if gst != 0 {
		lines = append(lines, Line{Name: india.LineGST, Amount: gst})
	}
	if autosquare != 0 {
		lines = append(lines, Line{Name: india.LineAutosquareFee, Amount: autosquare})
	}
	return lines
}

// CostsPerTradeIN returns the India per-trade cost breakdowns for an
// account, newest-first by exit_at, keyset-paginated on exit_at.
//
// Return semantics (BROKER_PHASE1 §3 + §3.6):
//   - requestedLimit <= 0 → ([], nil), no SQL issued.
//   - requestedLimit > maxRecent → capped at maxRecent.
//   - zero rows → ([], nil); empty ≠ error.
//   - infra/driver failure → ErrLoadFailed-wrapped.
//
// Country-quarantine (P1-D7): this method is the one place that knows
// the India trade_costs column names at the read seam; everything
// upstream (Phase 6's handler) sees generic []Line.
func (store *Store) CostsPerTradeIN(ctx context.Context, accountID AccountID, requestedLimit int, before *time.Time) ([]TradeCostBreakdown, error) {
	if requestedLimit <= 0 {
		slog.Debug("broker/store: CostsPerTradeIN limit<=0 shortcut",
			"account_id", accountID,
			"requested_limit", requestedLimit,
		)
		return []TradeCostBreakdown{}, nil
	}
	limit := cappedLimit(requestedLimit)

	var beforeArg any
	if before == nil {
		beforeArg = nil
	} else {
		beforeArg = *before
	}

	rows, queryErr := store.pool.Query(ctx, selectCostsPerTradeINSQL, accountID, beforeArg, limit)
	if queryErr != nil {
		slog.Error("broker/store: CostsPerTradeIN query failed",
			"account_id", accountID,
			"limit", limit,
			"before_null", before == nil,
			"err", queryErr,
		)
		return nil, fmt.Errorf("broker/store: CostsPerTradeIN query: %w (%v)", ErrLoadFailed, queryErr)
	}
	defer rows.Close()

	breakdowns := make([]TradeCostBreakdown, 0, limit)
	for rows.Next() {
		var tradeID string
		var sttAmount, exchangeAmount, sebiAmount, stampAmount, gstAmount, autosquareAmount, totalAmount int64
		scanErr := rows.Scan(
			&tradeID,
			&sttAmount, &exchangeAmount, &sebiAmount,
			&stampAmount, &gstAmount, &autosquareAmount,
			&totalAmount,
		)
		if scanErr != nil {
			return nil, fmt.Errorf("broker/store: CostsPerTradeIN scan: %w (%v)", ErrLoadFailed, scanErr)
		}
		breakdowns = append(breakdowns, TradeCostBreakdown{
			TradeID: tradeID,
			Lines:   mapIndiaCostsToLines(sttAmount, exchangeAmount, sebiAmount, stampAmount, gstAmount, autosquareAmount),
			Total:   totalAmount,
		})
	}
	if iterErr := rows.Err(); iterErr != nil {
		return nil, fmt.Errorf("broker/store: CostsPerTradeIN iter: %w (%v)", ErrLoadFailed, iterErr)
	}

	slog.Debug("broker/store: CostsPerTradeIN ok",
		"account_id", accountID,
		"limit", limit,
		"before_null", before == nil,
		"returned", len(breakdowns),
	)
	return breakdowns, nil
}

// CostsAggregateIN returns the India account-level cost aggregate: sum
// of each India cost line + Σ total + count(*) of trades. A new account
// with zero trades returns a zero-filled aggregate (NOT an error) —
// count(*) always yields a row, coalesce handles NULL sums.
//
// Country-quarantine: identical rule to CostsPerTradeIN. Infra failure
// → ErrLoadFailed-wrapped.
func (store *Store) CostsAggregateIN(ctx context.Context, accountID AccountID) (CostAggregate, error) {
	var tradeCount int
	var sttAmount, exchangeAmount, sebiAmount, stampAmount, gstAmount, autosquareAmount, totalAmount int64

	scanErr := store.pool.QueryRow(ctx, selectCostsAggregateINSQL, accountID).Scan(
		&tradeCount,
		&sttAmount, &exchangeAmount, &sebiAmount,
		&stampAmount, &gstAmount, &autosquareAmount,
		&totalAmount,
	)
	if scanErr != nil {
		slog.Error("broker/store: CostsAggregateIN query failed",
			"account_id", accountID,
			"err", scanErr,
		)
		return CostAggregate{}, fmt.Errorf("broker/store: CostsAggregateIN: %w (%v)", ErrLoadFailed, scanErr)
	}

	aggregate := CostAggregate{
		TradeCount: tradeCount,
		Lines:      mapIndiaCostsToLines(sttAmount, exchangeAmount, sebiAmount, stampAmount, gstAmount, autosquareAmount),
		Total:      totalAmount,
	}
	slog.Debug("broker/store: CostsAggregateIN ok",
		"account_id", accountID,
		"trade_count", aggregate.TradeCount,
		"total", aggregate.Total,
	)
	return aggregate, nil
}
