package india

// TradeCostsTable is the Postgres table name the India trade_costs row
// lands in (migrations/0003_create_trade_costs.sql). Written by the India
// adapter's INSERT in economy/persistence/india.go; read by the India
// cost accessors in broker/store/costs_in.go.
//
// When schema §4's country-table split lands, India keeps "trade_costs"
// (the original table) and a US adapter names its own ("us_trade_costs"
// in a future country/us package).
const TradeCostsTable = "trade_costs"
