package persistence

import (
	"time"

	"simulator/contract"
	"simulator/economy/account"
	"simulator/economy/realize"
)

// DurableAccount mirrors ONE row of the `accounts` table (schema §2.1). It is
// the flat DTO the SQL layer exchanges with the Postgres primitives; no
// domain type appears in loadAccount / saveAccount signatures.
//
// TradingCash is SIGNED — a negative value IS debt (Option B). There is no
// CHECK (>= 0) on this column by design; the DTO carries the value as-is.
// BankCash is >= 0 (ck_bank_nonneg).
//
// DripProgress is nullable (session-denominated progress to next tap; NULL
// when the drip is inactive), so it is a pointer.
type DurableAccount struct {
	AccountID    string
	OwnerType    string // "agent" | "human"
	OwnerID      string // agent_id or user_id — L5 does not own this table
	TradingCash  int64  // SIGNED coins
	BankCash     int64  // >= 0
	DripActive   bool
	DripProgress *int64 // nullable; nil ↔ NULL
}

// TradeInsert is the flattened row for ONE closed round-trip. Its
// participant_trades fields are country-agnostic (universal meaning per
// schema §2.2). The trade_costs payload stays opaque here: CostLines is the
// same map cost emits in CostBreakdown.Lines (India keys today; US keys when
// a US adapter arrives), unpacked into the country's trade_costs columns by
// the injected CountryCostAdapter at INSERT time (Stage 2). This keeps
// persistence's generic core free of any India column name.
//
// The mapping from realize.ClosedTradeRow → TradeInsert (translate.go) is
// country-agnostic; the country-specific flatten (india.go +
// IndiaCostAdapter) is the one place India column names live.
type TradeInsert struct {
	// participant_trades columns (universal — no country adapter needed).
	TradeID      string
	AccountID    string
	SessionID    string
	InstrumentID string
	Direction    string // "long" | "short" (prototype long-only)
	Quantity     int64
	EntryPrice   int64 // integer ticks
	ExitPrice    int64 // integer ticks
	EntryAt      time.Time
	ExitAt       time.Time
	GrossPnl     int64   // SIGNED
	TotalCosts   int64   // >= 0, sum of the country's cost stack
	NetPnl       int64   // SIGNED
	PnlPct       float64 // SIGNED, on net
	ExitReason   string  // "voluntary" | "auto_square" | ...

	// trade_costs payload — opaque here. Keys are country-specific line
	// names (India: stt/exchange/sebi/stamp/gst/autosquare_fee); the
	// CountryCostAdapter maps them to that country's column layout.
	// Total is the universal figure that lands in trade_costs.total
	// (denormalized == participant_trades.total_costs, schema §2.3).
	CostLines map[string]int64
	CostTotal int64
}

// SnapshotPayload is what the owner goroutine hands to writeSnapshot: a
// value-copy of ONE account's live state at a moment in time. persistence
// does not assemble it — the owner does, so single-writer discipline stays
// intact. Assembled from the live account.Account (Cash + Positions + drip)
// and the reserve.ReservationBook filtered to this account.
type SnapshotPayload struct {
	SessionID    string
	AccountID    string
	Cash         CashSnap
	Positions    []PositionSnap
	Reservations []ReservationSnap
}

// CashSnap combines the live split (Available/Reserved/Settled) plus BankCash
// and the drip pair, matching the l5:snap:{s}:acct:{a}:cash key value shape
// in §7. Available is SIGNED (Option B); the other cash fields are >= 0.
//
// The split lives ONLY in memory + this Redis mirror. Postgres holds one
// signed trading_cash figure that the split reconciles to (D7 — lifecycle
// does that reconciliation, not persistence).
type CashSnap struct {
	Available    int64 // SIGNED — may be negative (debt)
	Reserved     int64
	Settled      int64
	BankCash     int64
	DripActive   bool
	DripProgress *int64 // nullable, mirrors DurableAccount.DripProgress
}

// PositionSnap is one instrument's live holding for one account, mirroring
// the l5:snap:{s}:acct:{a}:pos:{instrument_id} key value in §7. OpenLots is
// FIFO in arrival order (append at tail, consume from head); write/read MUST
// preserve that order.
type PositionSnap struct {
	InstrumentID   string
	AvailableUnits int64
	ReservedUnits  int64
	OpenLots       []LotSnap
}

// LotSnap mirrors account.Lot exactly. EntryAt is the sole source of
// position_age; RemainingQty shrinks on RemovePosition (5.1).
type LotSnap struct {
	EntryPrice   int64
	EntryAt      time.Time
	RemainingQty int64
}

// ReservationSnap mirrors reserve.ReservationRecord's public fields (the
// link-back ids plus cost-lifecycle flags), so a crash-recovery load can
// hydrate the ReservationBook completely.
//
// Note: LAYER_5_PERSISTENCE §7 sketches this key with a smaller field set
// ({participant, side, is_market, reserved_cash|reserved_units,
// remaining_qty}). We include the full record because reserve's terminal
// paths need OrigQty / EverFilled / BrokerageCharged to charge brokerage
// exactly once — a summary snapshot cannot rehydrate that.
type ReservationSnap struct {
	OrderID          contract.OrderID
	AccountID        account.AccountID
	SessionID        realize.SessionID
	InstrumentID     contract.InstrumentID
	Side             contract.Side
	IsMarket         bool
	ReservedCash     int64
	ReservedUnits    int64
	RemainingQty     int64
	OrigQty          int64
	EverFilled       bool
	BrokerageCharged bool
}

// dirtyKey is the map key for the dirty-account set (D6). Held only inside
// Component (Stage 3); exported publicly as DirtyKey for the owner-facing
// DrainDirty() API added there.
type dirtyKey struct {
	SessionID string
	AccountID string
}
