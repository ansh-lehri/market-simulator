package india

// India trade_costs column names (migrations/0003_create_trade_costs.sql).
// Written by economy/persistence/india.go's IndiaCostAdapter at drain time;
// read by broker/store/costs_in.go's CostsPerTradeIN / CostsAggregateIN.
// Values match the DDL verbatim — a drift here surfaces at test time via
// country/india/india_test.go's value-pin test (and immediately at runtime
// as a Postgres "column does not exist" error).
const (
	ColumnBrokerage     = "brokerage"
	ColumnSTT           = "stt"
	ColumnExchange      = "exchange"
	ColumnSEBI          = "sebi"
	ColumnStamp         = "stamp"
	ColumnGST           = "gst"
	ColumnAutosquareFee = "autosquare_fee"
	ColumnTotal         = "total"
)
