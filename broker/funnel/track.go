package funnel

// OrderStatus is the closed set of states an OrderTrack transitions
// through (BROKER_TIER0 §Point-5). The shard flips Status as exchange
// results arrive; a terminal state removes the track from the shard's
// tracks map. Values are named, not numeric constants exposed on the
// wire — this enum is local to the funnel.
type OrderStatus int

const (
	// OrderStatusUnknown is the zero value; a well-formed track never
	// carries it. Present so unset fields are obviously wrong in logs.
	OrderStatusUnknown OrderStatus = iota
	// OrderStatusWorking — punched and live on the book.
	OrderStatusWorking
	// OrderStatusPartiallyFilled — one or more Convert calls but qty
	// still remaining.
	OrderStatusPartiallyFilled
	// OrderStatusFilled — terminal, RemainingQty == 0 via fills.
	OrderStatusFilled
	// OrderStatusCancelled — terminal, non-fill exit (user cancel,
	// expire, IOC/market remainder, FOK reject, auction lapse).
	OrderStatusCancelled
	// OrderStatusRejected — terminal, exchange rejected at admission.
	OrderStatusRejected
)

// OrderTrack is the broker's thin per-order record — EXCHANGE-SEAM
// FACTS ONLY (BROKER_TIER0 §Point-5). All reservation money-state
// (reserved cash / units, remaining qty, original qty, ever-filled,
// brokerage-charged) lives in L5's ReservationRecord and is read from
// L5; the funnel never mirrors it on OrderTrack. The rule prevents the
// broker from duplicating money truth.
//
// The five Tier-0 fields:
//   OrderID         the exchange's echoed key; the funnel mints it as
//                   "<AccountID>:<seq>" so result ingress can parse the
//                   owning account with no cross-shard lookup.
//   AccountID       which of this shard's many accounts owns the order
//                   (per-shard bookkeeping; NOT usable for cross-shard
//                   ingress routing — see the OrderID encoding).
//   Instrument      the routing key for a cancel — needed to punch the
//                   CancelRequest to the right instrument.
//   Status          state machine above.
//   CancelInFlight  a cancel was punched, awaiting the exchange's ack;
//                   set on cancel-punch, cleared on ack.
//
// A later tier (order intake) may extend OrderTrack with an IsMarketBuy
// flag (for the market-buy buffer true-up at settlement). That field is
// NOT Tier-0; do not add it here — extend when Tier-1 lands.
type OrderTrack struct {
	OrderID        OrderID
	AccountID      AccountID
	Instrument     InstrumentID
	Status         OrderStatus
	CancelInFlight bool
}
