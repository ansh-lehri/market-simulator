package realize

import (
	"time"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/cost"
)

// SessionID is the broker-internal identifier for one live session. It does
// not cross the exchange↔broker seam, so it lives in economy — aliased
// locally in realize until a shared home is decided.
type SessionID = string

// TradeID identifies one closed round-trip. Optional on RealizationInput
// (empty when the caller has no upstream id); the accumulator carries the
// first non-empty id it sees across a round-trip's fills, and the Realizer
// generates one at flush time if none was supplied.
type TradeID = string

// ExitReason distinguishes voluntary closes from session-end auto-squares.
type ExitReason string

const (
	ExitVoluntary  ExitReason = "voluntary"
	ExitAutoSquare ExitReason = "auto_square"
)

// RealizationInput is one sell fill's payload, handed to OnSellFill by the
// caller (Convert's sell branch for voluntary closes; SettleSessionEnd
// builds it directly for auto-square). Every field is a value copy — no
// pointer into caller state crosses this seam.
type RealizationInput struct {
	AccountID    account.AccountID
	SessionID    SessionID
	InstrumentID contract.InstrumentID

	FillQty     int64
	MakerPrice  int64
	MatchedLots []account.LotMatch
	Cost        cost.CostBreakdown

	// PositionNowFlat is set by the caller from post-RemovePosition state.
	// When true, OnSellFill assembles and (subject to the gate) enqueues
	// the round-trip row, then drops the accumulator.
	PositionNowFlat bool

	// TradeID from the upstream fill event; empty when the caller has none.
	// Accumulator keeps the first non-empty value observed; the Realizer's
	// generator fills in a default at flush when all fills were empty.
	TradeID TradeID

	// Direction of the round-trip ("long" / "short"). Set by the caller who
	// knows whether this sell is closing a long or opening a short; realize
	// captures the first non-empty value observed and passes it through to
	// the assembled row. Prototype is long-only.
	Direction string

	// ExitAt is this fill's timestamp — the round-trip's ExitAt tracks the
	// latest across folded fills. Supplied by the caller so this package
	// holds no clock (auto-square stamps time.Now at its boundary).
	ExitAt time.Time
}

// RealizationResult is the per-fill triple ComputeRealizedPnl returns.
// Signed throughout; a loss is negative and is never clamped.
type RealizationResult struct {
	Gross int64
	Costs int64
	Net   int64
	Lines map[string]int64
}

// ParticipantTradeRow is one round-trip summary — the shape that lands in
// the participant_trades table via the drainer (elsewhere).
type ParticipantTradeRow struct {
	TradeID      TradeID
	AccountID    account.AccountID
	SessionID    SessionID
	InstrumentID contract.InstrumentID

	Direction  string // "long" — prototype long-only; stored for when short exists
	Quantity   int64
	EntryPrice int64 // weighted-average of the matched entries
	ExitPrice  int64 // qty-weighted exit across the partial sells

	EntryAt time.Time // earliest matched lot's EntryAt (round-trip start)
	ExitAt  time.Time // latest folded fill's ExitAt (round-trip close)

	GrossPnl   int64
	TotalCosts int64
	NetPnl     int64
	PnlPct     float64

	ExitReason ExitReason
}

// TradeCostRow is the itemized cost child (1:1 with ParticipantTradeRow via
// TradeID). Country-agnostic by construction: Lines is the accumulated
// per-line map handed up by the cost module (India keys: stt / stamp /
// exchange / sebi / gst / autosquare_fee; other countries emit their own),
// Total is the only universal figure callers depend on. Flattening these
// country-specific keys into fixed columns is the country-specific
// persistence adapter's job (schema-side), never realize's. Brokerage is
// not carried here — it is order-level, on its own persistence path.
type TradeCostRow struct {
	TradeID TradeID
	Lines   map[string]int64
	Total   int64
}

// ClosedTradeRow bundles the summary row and its cost child so the seam
// carries one logical event. Both 5.4 (voluntary + auto-square) and 5.2
// (if it ever enqueues) use this shape.
type ClosedTradeRow struct {
	Summary ParticipantTradeRow
	Cost    TradeCostRow
}
