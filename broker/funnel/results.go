package funnel

import "simulator/contract"

// Reply-payload shapes for the Tier-0 command model.
//
// Tier 0 defines the machine; the Phase 5 stage fills the bodies for place
// and cancel. Transfer stays Phase-3-owned (TransferResult below is
// unchanged). Read-surface bodies (BalanceSnapshot / PositionsSnapshot) are
// still Tier-4-owned seams — zero-value for now.
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

// BalanceSnapshot is the reply to a ReadBalance command. Body owned by Tier 4.
type BalanceSnapshot struct{}

// PositionsSnapshot is the reply to a ReadPositions command. Body owned by Tier 4.
type PositionsSnapshot struct{}

// StopRecord is the payload of a FireStop command. Body owned by Tier 2.
type StopRecord struct{}
