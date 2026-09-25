package persistence

import "fmt"

// India-specific cost-line names, as emitted by cost/india.go via
// CostBreakdown.Lines. Persistence duplicates them (rather than importing
// from cost) to keep the country-adapter self-contained; if they ever drift
// from cost's, an integration test at Stage 5 will catch the mismatch.
// A future refactor may host the canonical India strings in a shared
// per-country package.
const (
	indiaLineSTT           = "stt"
	indiaLineExchange      = "exchange"
	indiaLineSEBI          = "sebi"
	indiaLineStamp         = "stamp"
	indiaLineGST           = "gst"
	indiaLineAutosquareFee = "autosquare_fee"
)

// India-specific trade_costs column names, matching schema §2.3 verbatim.
// These are the only strings persistence uses when it builds a SQL INSERT
// for the India cost row.
const (
	indiaColumnBrokerage     = "brokerage"
	indiaColumnSTT           = "stt"
	indiaColumnExchange      = "exchange"
	indiaColumnSEBI          = "sebi"
	indiaColumnStamp         = "stamp"
	indiaColumnGST           = "gst"
	indiaColumnAutosquareFee = "autosquare_fee"
	indiaColumnTotal         = "total"
)

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
		indiaColumnBrokerage:     0, // D2
		indiaColumnSTT:           0,
		indiaColumnExchange:      0,
		indiaColumnSEBI:          0,
		indiaColumnStamp:         0,
		indiaColumnGST:           0,
		indiaColumnAutosquareFee: 0,
		indiaColumnTotal:         total,
	}
	for lineName, amount := range lines {
		switch lineName {
		case indiaLineSTT:
			columns[indiaColumnSTT] = amount
		case indiaLineExchange:
			columns[indiaColumnExchange] = amount
		case indiaLineSEBI:
			columns[indiaColumnSEBI] = amount
		case indiaLineStamp:
			columns[indiaColumnStamp] = amount
		case indiaLineGST:
			columns[indiaColumnGST] = amount
		case indiaLineAutosquareFee:
			columns[indiaColumnAutosquareFee] = amount
		default:
			return nil, fmt.Errorf("%w: %q", ErrUnknownCostLine, lineName)
		}
	}
	return columns, nil
}
