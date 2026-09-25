package persistence

// CountryCostAdapter is the plug for a country-specific trade_costs mapping.
// The interface exists because trade_costs' column layout is country-specific
// (schema §2.3 is India today; LAYER_5_2_SCHEMA §4 flags a forward-seam split
// that puts US on its own table with different columns — SEC/TAF/FINRA). The
// generic persistence core carries an opaque CostLines map on TradeInsert;
// this adapter is the ONE place that map becomes country-shaped columns.
//
// One method for Stage 1. Stage 2 (SQL layer) will extend the interface with
// what it actually needs to build INSERT statements (ordered column list,
// table name, and — likely — a full InsertCostRow method that takes a pgx.Tx).
// Keeping the surface minimal until the concrete need lands.
//
// Adapters are pure values. No I/O, no state beyond the country's fixed
// column layout. Injected into Component at NewComponent (Stage 3).
type CountryCostAdapter interface {
	// FlattenCostColumns maps a generic CostBreakdown Lines map to the
	// country's trade_costs column layout. The returned map is
	// column-name → coin-value, including every column the country's schema
	// requires (missing lines default to 0) plus the universal "total"
	// column (denormalized == participant_trades.total_costs, schema §2.3).
	//
	// Contract:
	//   - unknown line keys → ErrUnknownCostLine (D1: never silently dropped)
	//   - country's fixed columns present with correct defaults
	//     (India example: brokerage=0 per D2)
	//   - map iteration order is not meaningful; the SQL layer imposes
	//     column order via its own list
	FlattenCostColumns(lines map[string]int64, total int64) (columns map[string]int64, err error)
}
