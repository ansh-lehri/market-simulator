package funnel

import (
	"time"

	"simulator/contract"
)

// Reply-payload shapes for every command the funnel receives. Each
// request/response command (place, cancel, transfer, the four reads)
// has one reply struct here; OrderSpec and StopRecord sit alongside
// as the payload shapes carried in a command.
//
// Every live-read snapshot carries a Present bool. The shard handler
// returns Present:false with zero-valued payload when the account has
// no live session on this shard; the HTTP layer maps that to 409. On
// a live session, Present:true and the payload is a by-value snapshot
// read on the owner goroutine — nothing outside the shard holds a
// pointer into live Account / reservation state.

// OrderSpec is the broker-internal order intent, built by the HTTP layer
// from the wire body and threaded into a PlaceOrder command (BROKER_PHASE5
// §4.2 P5-D23). Minor-unit integers only — ticks for LimitPrice, whole
// units for Qty. LimitPrice must be > 0 iff Type == Limit (validated by
// validateSpec); it is 0 (ignored) for Market.
type OrderSpec struct {
	Instrument contract.InstrumentID
	Side       contract.Side
	Type       contract.OrdType
	TIF        contract.TIF
	Qty        contract.Qty
	LimitPrice contract.Ticks
}

// PlaceStatus discriminates the two PlaceOrder outcomes. Zero value is
// Unknown so an un-set field surfaces loudly in logs; a well-formed
// PlaceResult always carries PlaceAccepted or PlaceRejected.
type PlaceStatus int

const (
	PlaceStatusUnknown PlaceStatus = iota
	PlaceAccepted                  // reserved + punched; HTTP → 202 {order_id}
	PlaceRejected                  // pre-punch refusal; Reason explains why
)

// PlaceResult is the shard handler's reply to a PlaceOrder command
// (BROKER_PHASE5 §1.3). On PlaceAccepted, OrderID carries the minted
// "<AccountID>:<seq>" id and Reason is ReasonUnknown. On PlaceRejected,
// OrderID is empty and Reason explains the rejection; no reservation was
// made and nothing was punched (P5-D6).
type PlaceResult struct {
	Status  PlaceStatus
	OrderID OrderID
	Reason  RejectReason
}

// CancelStatus discriminates the two CancelOrder outcomes.
//
// CancelPending — the cancel punch was accepted LOCALLY (ownership + state
// pre-filter passed, flag set, message punched). It does NOT mean "cancelled"
// — the exchange is authoritative and the real outcome arrives async via
// settlement (OutcomeCancelled / OutcomeCancelRejected). The HTTP layer
// surfaces this as 202.
//
// CancelFailed — a pre-punch refusal (no session / unknown order / not
// cancellable). The HTTP layer surfaces this as 409 (BROKER_PHASE5 §4.5).
type CancelStatus int

const (
	CancelStatusUnknown CancelStatus = iota
	CancelPending
	CancelFailed
)

// CancelResult is the shard handler's reply to a CancelOrder command
// (BROKER_PHASE5 §2.3). Mirror of PlaceResult: on CancelPending the OrderID
// echoes the request's; on CancelFailed OrderID may be empty (unknown-order
// path) and Reason explains why.
type CancelResult struct {
	Status  CancelStatus
	OrderID OrderID
	Reason  RejectReason
}

// TransferResult is the reply to a Transfer command (BROKER_PHASE3 §1.3,
// P3-D5). Three outcomes, discriminated by (Present, Err):
//
//   - Present:false              — account is not live on this shard.
//     Internal-only signal; the HTTP handler
//     falls to Store.DurableTransfer. Never
//     surfaces to the client as a 409 (P3-D8).
//   - Present:true,  Err != nil  — account was live, MoveCash rejected
//     (e.g. account.ErrInsufficient). Nothing
//     moved — MoveCash fails before mutating.
//     HTTP maps to 422 (insufficient) or 500
//     (programming error).
//   - Present:true,  Err == nil  — success; Available/Bank are the live
//     post-move bucket values. HTTP → 200.
//
// Mode-blind {Available, Bank} shape (P3-D7): the on-demand vs canonical
// distinction never shows up on the wire — the handler persists or not
// based on PersistOnClose at session_save, not here.
type TransferResult struct {
	Present   bool
	Available int64
	Bank      int64
	Err       error
}

// SeedInventoryResult is the reply to a SeedInventory command
// (BROKER_SEED_INVENTORY §1). Two outcomes, discriminated by (Present, Err):
//
//   - Present:false         — no live session for this account on this
//     shard. Unlike Transfer there is NO durable
//     fallback for a mint (SI-D1) — the HTTP
//     handler maps this straight to 409.
//   - Present:true, Err set — CreditPosition rejected the mint. Its only
//     failure modes (Qty<=0 / EntryPrice<=0) are
//     already validated to 400 before the command
//     is built, so reaching this is a caller bug,
//     not a business outcome; HTTP maps it to 500
//     (SI-D2 — return, don't panic, on this path).
//   - Present:true, Err nil  — success; Available is the post-mint
//     PosAvailable for the seeded instrument.
type SeedInventoryResult struct {
	Present   bool
	Available int64
	Err       error
}

// BalanceSnapshot is the four-bucket Cash copy returned by the balance
// read. Present:false means the account has no live session on this
// shard (HTTP maps that to 409); the four int64 fields are zero in
// that case and must not be interpreted.
type BalanceSnapshot struct {
	Present   bool
	Available int64 // integer coins — may be negative (debt is legal)
	Reserved  int64
	Settled   int64
	Bank      int64
}

// PositionsSnapshot is the reply to the positions read. Positions is
// empty when the account is live but flat; Present:false signals no
// live session (→ 409). An unknown instrument filter on a live account
// returns Present:true with Positions=[] (→ 200 {positions:[]}).
type PositionsSnapshot struct {
	Present   bool
	Positions []PositionView
}

// PositionView is one holding row.
//
// WeightedAvgEntry is qty-weighted across the open lots; display-only,
// FIFO stays the realization basis. MarkAvailable is false whenever the
// market-price cache misses on LastPrice (the production stand-in
// always misses until the tape relay goes live); LTP and
// GrossUnrealized are zero in that case but the position itself still
// ships. GrossUnrealized uses zero incurred-entry-costs, so the "net"
// number is deferred — nothing accumulates per-position entry costs
// today, so a true net would misreport.
type PositionView struct {
	Instrument       InstrumentID
	Qty              int64 // free-to-sell units (Position.Available)
	ReservedQty      int64 // units locked behind resting sells
	WeightedAvgEntry int64 // qty-weighted over open lots; 0 iff flat
	MarkAvailable    bool
	LTP              int64 // 0 when !MarkAvailable
	GrossUnrealized  int64 // 0 when !MarkAvailable
}

// OpenOrdersSnapshot is the reply to the open-orders read. Orders is
// the account's working set (status ∈ {Working, PartiallyFilled});
// terminal orders are deleted from the shard's track map at settlement,
// so a Present:true snapshot is already filtered by construction. The
// shape's money / qty / side / price fields come from the reservation
// view — the funnel never mirrors reservation state onto OrderTrack.
// An order whose reservation has been removed (unusual — terminal state
// should drop the track too) still ships with the thin track fields;
// the money fields default to zero.
type OpenOrdersSnapshot struct {
	Present bool
	Orders  []OpenOrderView
}

// OpenOrderView is one working-order row. Fields split by source:
//
//   - OrderID / Instrument / Status / CancelInFlight come from the
//     thin OrderTrack (exchange-seam facts only);
//   - Side / IsMarket / LimitPrice / OrigQty / RemainingQty /
//     ReservedCash / ReservedUnits come from the reservation view
//     (a by-value copy out of the reservation book).
//
// LimitPrice is zero for market orders (the reserve layer sets it from
// Order.LimitPrice unconditionally, and validation guarantees it is
// zero exactly when the order is a market order). FilledQty is
// deliberately absent — the HTTP shaper derives OrigQty - RemainingQty
// at the edge so the funnel never carries a derived quantity.
type OpenOrderView struct {
	OrderID        OrderID
	Instrument     InstrumentID
	Side           contract.Side
	IsMarket       bool
	LimitPrice     int64 // integer ticks; 0 for market orders
	OrigQty        int64
	RemainingQty   int64
	ReservedCash   int64 // 0 for sell reservations (units are reserved instead)
	ReservedUnits  int64 // 0 for buy reservations (cash is reserved instead)
	Status         OrderStatus
	CancelInFlight bool
}

// ClosedTradeLive is one closed round-trip from the Realizer's in-memory ring,
// present only for on-demand (practice) sessions — canonical sessions never
// populate it (they write to Postgres instead). The field set mirrors
// store.TradeRow so shaping.go can emit identical JSON for both.
type ClosedTradeLive struct {
	TradeID    string
	Instrument string
	Direction  string
	Quantity   int64
	EntryPrice int64
	ExitPrice  int64
	EntryAt    time.Time
	ExitAt     time.Time
	GrossPnl   int64
	TotalCosts int64
	NetPnl     int64
	PnlPct     float64
	ExitReason string
}

// ObservationLive is the live half of the observation read — balance +
// positions + working orders assembled in one visit to the owning
// shard so the three sub-payloads cannot interleave with a fill. The
// durable half (recent closed trades) is merged in by the HTTP handler
// on a separate goroutine for canonical sessions; on-demand sessions
// use LiveTrades instead (Postgres is never written for those).
//
// Present:false → 409 (no live session); the other fields are zero.
type ObservationLive struct {
	Present    bool
	Balance    BalanceSnapshot
	Positions  []PositionView
	OpenOrders []OpenOrderView
	// LiveTrades is non-nil only for on-demand sessions. The HTTP handler
	// skips the Postgres read when this is non-empty.
	LiveTrades []ClosedTradeLive
}

// StopRecord is the payload of a FireStop command. Body owned by Tier 2.
type StopRecord struct{}
