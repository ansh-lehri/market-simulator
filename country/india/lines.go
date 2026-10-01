package india

// India CostBreakdown line names, as emitted by economy/cost (5.3 India
// cost module) into realize.TradeCostRow.Lines. The India adapter
// allowlists exactly these keys and rejects anything else as
// ErrUnknownCostLine (persistence D1 — no silent drop).
//
// Deliberately NO LineBrokerage. Brokerage is order-level, charged ONCE at
// terminal, and never appears in a TradeCostRow.Lines map (persistence D2
// + realize CLAUDE.md). An attempted "brokerage" key is treated as
// unknown by the India adapter — the safety valve.
const (
	LineSTT           = "stt"
	LineExchange      = "exchange"
	LineSEBI          = "sebi"
	LineStamp         = "stamp"
	LineGST           = "gst"
	LineAutosquareFee = "autosquare_fee"
)
