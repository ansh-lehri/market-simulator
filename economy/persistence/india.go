package persistence

import (
	"context"
	"fmt"

	"simulator/country/india"
)

// indiaCostInsertSQL is the fixed INSERT statement for the India trade_costs
// row. Column order matches the placeholders 1..9. Idempotent via
// ON CONFLICT (trade_id) DO NOTHING so drainer replay is safe.
//
// Table + column names come from country/india — the single India
// identifier package. Both are string constants, so concatenation into a
// const SQL literal remains compile-time.
const indiaCostInsertSQL = `INSERT INTO ` + india.TradeCostsTable + ` (
	trade_id, ` + india.ColumnBrokerage + `, ` + india.ColumnSTT + `, ` + india.ColumnExchange + `, ` + india.ColumnSEBI + `, ` + india.ColumnStamp + `, ` + india.ColumnGST + `, ` + india.ColumnAutosquareFee + `, ` + india.ColumnTotal + `
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (trade_id) DO NOTHING`

// IndiaCostAdapter implements CountryCostAdapter for the India stack. Pure
// value; no state.
type IndiaCostAdapter struct{}

// Compile-time assertion: IndiaCostAdapter satisfies CountryCostAdapter.
var _ CountryCostAdapter = IndiaCostAdapter{}

// FlattenCostColumns maps a CostBreakdown Lines map to the India trade_costs
// column layout (schema §2.3). Every India column appears in the returned
// map — missing lines default to 0. Brokerage is always 0 (D2: order-level,
// separate path; cost/realize never emit "brokerage" in TradeCostRow.Lines).
// Unknown keys are ErrUnknownCostLine (D1: no silent drop).
func (IndiaCostAdapter) FlattenCostColumns(lines map[string]int64, total int64) (map[string]int64, error) {
	columns := map[string]int64{
		india.ColumnBrokerage:     0, // D2
		india.ColumnSTT:           0,
		india.ColumnExchange:      0,
		india.ColumnSEBI:          0,
		india.ColumnStamp:         0,
		india.ColumnGST:           0,
		india.ColumnAutosquareFee: 0,
		india.ColumnTotal:         total,
	}
	for lineName, amount := range lines {
		switch lineName {
		case india.LineSTT:
			columns[india.ColumnSTT] = amount
		case india.LineExchange:
			columns[india.ColumnExchange] = amount
		case india.LineSEBI:
			columns[india.ColumnSEBI] = amount
		case india.LineStamp:
			columns[india.ColumnStamp] = amount
		case india.LineGST:
			columns[india.ColumnGST] = amount
		case india.LineAutosquareFee:
			columns[india.ColumnAutosquareFee] = amount
		default:
			return nil, fmt.Errorf("%w: %q", ErrUnknownCostLine, lineName)
		}
	}
	return columns, nil
}

// InsertCostRow writes one India trade_costs row via the given executor.
// FlattenCostColumns validates the map first; any unknown key surfaces as
// ErrUnknownCostLine and no SQL is issued.
func (adapter IndiaCostAdapter) InsertCostRow(
	ctx context.Context,
	executor CostRowExecutor,
	tradeID string,
	lines map[string]int64,
	total int64,
) error {
	columns, err := adapter.FlattenCostColumns(lines, total)
	if err != nil {
		return err
	}
	_, err = executor.Exec(ctx, indiaCostInsertSQL,
		tradeID,
		columns[india.ColumnBrokerage],
		columns[india.ColumnSTT],
		columns[india.ColumnExchange],
		columns[india.ColumnSEBI],
		columns[india.ColumnStamp],
		columns[india.ColumnGST],
		columns[india.ColumnAutosquareFee],
		columns[india.ColumnTotal],
	)
	if err != nil {
		return fmt.Errorf("persistence: india InsertCostRow %s: %w", tradeID, err)
	}
	return nil
}
