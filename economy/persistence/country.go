package persistence

import (
	"context"

	"github.com/jackc/pgx/v5/pgconn"
)

// CostRowExecutor is the narrow SQL surface a country adapter uses to write
// its trade_costs row. pgx.Tx satisfies it (Exec matches exactly). Kept as
// an interface so unit tests can inject a fake without a live database, and
// so an adapter never depends on more of pgx than it needs.
type CostRowExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// CountryCostAdapter is the plug for a country-specific trade_costs mapping.
// The interface exists because trade_costs' column layout is country-specific
// (schema §2.3 is India today; LAYER_5_2_SCHEMA §4 flags a forward-seam split
// that puts US on its own table with different columns — SEC/TAF/FINRA). The
// generic persistence core carries an opaque CostLines map on TradeInsert;
// this adapter is the ONE place that map becomes country-shaped columns AND
// the ONE place SQL text with country columns is written.
//
// Adapters are pure values with no state beyond their fixed column layout.
// Injected into Component at NewComponent (Stage 3).
type CountryCostAdapter interface {
	// FlattenCostColumns maps a generic CostBreakdown Lines map to the
	// country's trade_costs column layout. Column-name → coin-value,
	// including every column the country requires (missing lines default
	// to 0) plus the universal "total" column (denormalized ==
	// participant_trades.total_costs, schema §2.3).
	//
	// Contract:
	//   - unknown line keys → ErrUnknownCostLine (D1: never silently dropped)
	//   - country's fixed columns present with correct defaults
	//     (India: brokerage=0 per D2)
	FlattenCostColumns(lines map[string]int64, total int64) (columns map[string]int64, err error)

	// InsertCostRow writes ONE trade_costs row inside the given executor
	// (typically a pgx.Tx inside insertClosedTrade's transaction). The
	// SQL statement — table name + column list — is country-specific and
	// lives ENTIRELY inside the adapter; the generic SQL layer never
	// references a country column. Idempotent: uses
	// ON CONFLICT (trade_id) DO NOTHING so drainer replay is safe.
	//
	// Returns ErrUnknownCostLine (via FlattenCostColumns) when lines has
	// an unrecognized key; any SQL/driver error is wrapped with context.
	InsertCostRow(ctx context.Context, executor CostRowExecutor, tradeID string, lines map[string]int64, total int64) error
}
