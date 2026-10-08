package funnel

import "simulator/contract"

// Reply-payload shapes for the Tier-0 command model.
//
// Tier 0 defined the machine; Phase 5 filled place/cancel; Phase 6 fills
// the read-surface payloads (BalanceSnapshot / PositionsSnapshot /
// OpenOrdersSnapshot / ObservationLive and their row views). Transfer
// stays Phase-3-owned (TransferResult below is unchanged).
//
// Every live-read snapshot carries `Present bool`. The shard handler
// returns `Present:false` with zero-valued payload when the account has
// no live session on this shard; the HTTP layer maps that to 409. On a
// live session, `Present:true` and the payload is a by-value snapshot
// read from the owning shard goroutine (single-writer / copy-out).
//
// OrderSpec and StopRecord are the *payload* shapes carried in a command;
// OrderSpec is Phase-5-owned (filled below); StopRecord stays Tier-2-owned.

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

// BalanceSnapshot is the reply to a ReadBalance command (A8). Four-bucket
// copy-out of the owning account's Cash: Available, Reserved, Settled, Bank
// (BROKER_READ_SURFACES_tier4 §2 / BROKER_PHASE6 §2.3). Present=false means
// no live session on the shard — HTTP maps to 409; the four int64 fields
// are zero in that case and must not be interpreted.
type BalanceSnapshot struct {
	Present   bool
	Available int64 // integer coins
	Reserved  int64
	Settled   int64
	Bank      int64
}

// PositionsSnapshot is the reply to a ReadPositions command (A9). Positions
// is empty when the account is live but flat; Present=false signals "no
// live session" (→ 409). The optional ReadPositions.Instrument filter is
// applied by readPositions upstream — an unknown instrument on a live
// account returns Present=true with Positions=[] (→ 200 {positions:[]}).
type PositionsSnapshot struct {
	Present   bool
	Positions []PositionView
}

// PositionView is one row of PositionsSnapshot (BROKER_PHASE6 §3.3 /
// BROKER_READ_SURFACES_tier4 §3.3). Gross-only marks: ComputeUnrealizedMark
// is called with incurredEntryCosts=0 so GrossUnrealized is turnover-cost-
// free — the "net" number is a Phase-6 deferral (no per-position entry-cost
// accumulator yet; §3.8 "net-unrealized gap"). WeightedAvgEntry is
// qty-weighted across OpenLots; display-only, FIFO stays the realization
// basis. MarkAvailable=false when the shard's MarketPriceCache misses on
// LastPrice (production stand-in nopMarketCache always misses until Phase 7
// wires a real cache); LTP and GrossUnrealized are 0 in that case and the
// position + ReservedQty + WeightedAvgEntry still ship.
type PositionView struct {
	Instrument       InstrumentID
	Qty              int64 // Position.Available
	ReservedQty      int64 // Position.Reserved
	WeightedAvgEntry int64 // qty-weighted over OpenLots; 0 iff flat
	MarkAvailable    bool
	LTP              int64 // 0 when !MarkAvailable
	GrossUnrealized  int64 // 0 when !MarkAvailable
}

// OpenOrdersSnapshot is the reply to a ReadOpenOrders command (A11).
// Present=false → 409 (no live session). Orders is the account's working
// set (status ∈ {Working, PartiallyFilled}); terminal orders are deleted
// from shard.tracks at settlement, so a Present:true snapshot is already
// filtered by construction. The shape's money/qty/side/price fields are
// supplied by economy/reserve.ReservationView — the funnel never mirrors
// reservation state on OrderTrack (BROKER_FUNNEL CLAUDE "OrderTrack is
// thin"). An order whose ReservationView has been removed (unusual —
// terminal state should also drop the track) still ships with the thin
// OrderTrack fields; money fields default to zero.
type OpenOrdersSnapshot struct {
	Present bool
	Orders  []OpenOrderView
}

// OpenOrderView is one row of OpenOrdersSnapshot (BROKER_PHASE6 §4.4).
// Fields split by source:
//
//   - OrderID / Instrument / Status / CancelInFlight — the thin OrderTrack
//     (track.go: exchange-seam facts only).
//   - Side / IsMarket / LimitPrice / OrigQty / RemainingQty / ReservedCash
//     / ReservedUnits — reserve.ReservationView, by-value copy-out.
//
// LimitPrice is 0 for market orders (reserve.go sets it from Order.LimitPrice
// unconditionally; validateSpec guarantees LimitPrice==0 iff Type==Market).
// FilledQty is deliberately NOT a field — the HTTP shaper derives
// `OrigQty - RemainingQty` at the edge (A11 §2.5 "raw buckets" principle).
type OpenOrderView struct {
	OrderID        OrderID
	Instrument     InstrumentID
	Side           contract.Side
	IsMarket       bool
	LimitPrice     int64 // integer ticks; 0 for market orders
	OrigQty        int64
	RemainingQty   int64
	ReservedCash   int64 // coins; 0 for sell reservations (units are reserved instead)
	ReservedUnits  int64 // whole units; 0 for buy reservations
	Status         OrderStatus
	CancelInFlight bool
}

// ObservationLive is the reply to a ReadObservation command (A14). The
// LIVE half of an agent's self-snapshot: balance + positions + open orders
// read in ONE visit to the owning shard, so the three cannot interleave
// with a fill. The DURABLE half (recent closed trades from Store) is
// merged in by the HTTP handler, not here — different source, concurrent,
// degrade-don't-fail (BROKER_PHASE6 §5).
//
// Present=false → 409 (no live session); the three sub-payloads are
// zero in that case.
type ObservationLive struct {
	Present    bool
	Balance    BalanceSnapshot
	Positions  []PositionView
	OpenOrders []OpenOrderView
}

// StopRecord is the payload of a FireStop command. Body owned by Tier 2.
type StopRecord struct{}
