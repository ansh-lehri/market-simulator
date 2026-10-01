// Package india owns every India-specific identifier the system stores or
// reads: the trade_costs column names, the CostBreakdown line names the
// 5.3 India cost module emits, and the India trade_costs table name.
//
// Single source of truth. Both the write path (economy/persistence/india.go
// — IndiaCostAdapter.FlattenCostColumns / InsertCostRow) and the read path
// (broker/store/costs_in.go — CostsPerTradeIN / CostsAggregateIN) import
// their India strings from here. If a constant drifts, both sides break
// together — never silently diverge.
//
// Country-quarantine rule (project CLAUDE.md: country-specifics stay
// country-specific). Nothing above an India-aware caller is allowed to
// name an India string; a generic cost shape stays map[string]int64 /
// []Line all the way down, and only this package names the India keys.
// Adding a US stack later is a sibling package country/us — the generic
// callers do not change.
package india
