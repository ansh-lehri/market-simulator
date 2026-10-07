package reserve

import "simulator/contract"

// ReservationView is a read-only, value-copy projection of a
// ReservationRecord. The accessor exists so the broker (Phase 5
// settlement, Phase 6 open-orders read) can inspect reservation state
// WITHOUT ever holding a pointer into the ReservationBook — all seven
// fields are value types (int64 / contract.Side / bool / string), so a
// consumer physically cannot reach into and mutate book state through a
// view.
//
// Field set is scoped to actual consumer needs:
//
//   - Phase-5 handleApplyResult (settleMakerFill): reads RemainingQty to
//     decide Filled vs PartiallyFilled on an incremental maker fill.
//   - Phase-6 open-orders read (A11, docs/broker/BROKER_READ_SURFACES
//     _tier4.md §4.2): reads Side / IsMarket / LimitPrice / OrigQty /
//     RemainingQty / ReservedCash / ReservedUnits — derives FilledQty =
//     OrigQty − RemainingQty at the call site (A11 §2.5 raw-buckets
//     principle). LimitPrice surfaces the resting-order price (0 for
//     market orders) so the open-orders panel can show "resting at ₹X".
//   - OrderID is included so a logging / debugging consumer can echo it
//     back without re-threading it from the caller frame.
//
// Deliberately NOT included:
//
//   - AccountID / SessionID / InstrumentID — the caller already owns
//     these via its OrderTrack / ownedSession; re-exposing them would be
//     noise.
//   - EverFilled / BrokerageCharged — internal cost-lifecycle flags
//     (reserve/book.go) with no consumer today; keep the view minimal.
//
// Grow the view later if a consumer actually needs more; adding a field
// is additive and backwards-compatible, removing one is not.
type ReservationView struct {
	OrderID       OrderID
	Side          contract.Side
	IsMarket      bool
	LimitPrice    int64 // integer ticks; 0 for market orders
	ReservedCash  int64
	ReservedUnits int64
	RemainingQty  int64
	OrigQty       int64
}

// ViewReservation returns a value-copy snapshot of the reservation
// record for orderID, or (zero, false) when the book has none.
//
// Pure read / projection: no money math, no mutation, no event, no
// MarkSnapshotDirty, no logger call. Matches the "never log inside pure
// code" rule and the "no synchronous store write on the hot path"
// rule — this method is safe to call from any read surface at any
// cadence without I/O cost.
//
// Single-writer discipline: called on the owning shard goroutine, same
// as the four write methods (ReserveIfAvailable / Convert / Release /
// ReleaseExcess). No lock, by construction.
//
// Panic policy: an unknown orderID is NOT an invariant violation here.
// Contrast with Convert / Release / ReleaseExcess, which panic on a
// missing record (those callers must have reserved first); the broker's
// settlement / read paths legitimately ask "do I still have a
// reservation for this order?" after a prior terminal step may have
// removed it. (RemainingQty==0 && both Reserved* == 0 is Convert's
// delete condition; a full Release / ReleaseExcess also removes.) So
// (zero, false) is the normal "no live reservation" answer, not a
// programming error.
func (service *ReservationService) ViewReservation(orderID OrderID) (ReservationView, bool) {
	record := service.book.get(orderID)
	if record == nil {
		return ReservationView{}, false
	}
	return ReservationView{
		OrderID:       record.OrderID,
		Side:          record.Side,
		IsMarket:      record.IsMarket,
		LimitPrice:    record.LimitPrice,
		ReservedCash:  record.ReservedCash,
		ReservedUnits: record.ReservedUnits,
		RemainingQty:  record.RemainingQty,
		OrigQty:       record.OrigQty,
	}, true
}
