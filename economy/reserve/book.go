package reserve

import (
	"simulator/contract"
	"simulator/economy/account"
)

// ReservationRecord is 5.2's per-order state — bookkeeping about a pending
// order, NOT money. Cash/positions live in Account (5.1's ledger); this
// record is the memory linking an admission to its later Convert / Release
// / ReleaseExcess. Held in the ReservationBook (Option 2, per doc §1.1),
// NEVER mixed into Account. AccountID / SessionID are link-back ids so the
// book can be filtered per account for the async snapshotter without
// Account knowing it exists — they are NOT pointers into an Account.
//
// The cost-lifecycle flags (OrigQty / EverFilled / BrokerageCharged) drive
// the once-per-executed-order brokerage rule (doc §3.4 / §5): 5.3 computes
// the amount, but the "fire exactly once at terminal, whether Convert or
// Release sees the terminal state" guard lives on this record.
type ReservationRecord struct {
	OrderID      OrderID
	AccountID    account.AccountID
	SessionID    SessionID
	InstrumentID contract.InstrumentID

	Side     contract.Side
	IsMarket bool // affects ReleaseExcess (market-buy buffer true-up)

	// LimitPrice — the user's set price (integer ticks) at admission. 0 for
	// market orders. Stored (never back-calculated) because the derived
	// `ReservedCash = LimitPrice*Qty` is lossy: partial fills decrement
	// ReservedCash at the execution price (convert.go), sells reserve units
	// (ReservedCash == 0), and market buys fold in a slippage buffer — so
	// `LimitPrice = ReservedCash / RemainingQty` is unreliable across the
	// record's lifetime. Read-only display fact; no hand mutates it after
	// admission. Keeps the pure/replayable rule intact (no clock/IO).
	// Consumed by the broker's Phase-6 open-orders read (A11) via
	// ReservationView.
	LimitPrice int64

	// ReservedCash — cash still locked for the unfilled qty (buy side).
	// 0 for sell records. Coins, always >= 0 while the record lives.
	ReservedCash int64

	// ReservedUnits — position units still locked for the unfilled qty
	// (sell side). 0 for buy records. Always >= 0.
	ReservedUnits int64

	// RemainingQty — qty not yet filled. Shrinks by fillQty in Convert;
	// on partial Release the record is retained with a decremented value.
	// Reaches 0 at the order's terminal step.
	RemainingQty int64

	// OrigQty — original order qty at admission. Fed into
	// cost.OrderCostInput.TotalQty so per-order brokerage tiers see the
	// full order size, not just the last fill's slice.
	OrigQty int64

	// EverFilled — set true on the first successful Convert. A never-filled
	// order (EverFilled == false at Release) is charged ZERO brokerage
	// (doc §5). Reading this flag is how ComputeOrderCost is told whether
	// the order actually executed.
	EverFilled bool

	// BrokerageCharged — set true the ONE time the per-executed-order
	// brokerage debit fires. Guards both terminal paths (Convert at
	// RemainingQty == 0, Release at partial-fill-then-cancel) so the
	// charge fires exactly once regardless of which method sees the
	// terminal state.
	BrokerageCharged bool
}

// ReservationBook is 5.2's private state — the whole-economy scope,
// keyed by OrderID (doc §1.1, Option 2). Held INSIDE the ReservationService
// (constructor creates it internally; see NewReservationService in stage 2).
// Callers reach book contents only through service methods, never directly.
//
// The map holds pointers because Convert / Release / ReleaseExcess mutate
// records in place across their lifecycle (shrinking Reserved*, flipping
// EverFilled / BrokerageCharged) — a value map would force each mutation
// to write back the whole struct.
type ReservationBook struct {
	records map[OrderID]*ReservationRecord
}

// newReservationBook builds an empty book. Unexported because the ONLY
// legitimate constructor path is via NewReservationService (stage 2),
// which creates and owns the book internally. Redis rehydration in a
// later phase will land as a separate internal seam (a private
// loadRecords or the snapshotter wiring), NOT by exporting this.
func newReservationBook() *ReservationBook {
	return &ReservationBook{records: map[OrderID]*ReservationRecord{}}
}

// put stores a record, keyed by its own OrderID. Overwrites any existing
// entry under the same key — reserve's callers never legitimately reserve
// twice under the same OrderID (Gate 4 admits an order once), so an
// overwrite is a caller bug, but the book itself does not police it;
// the service layer will (a duplicate OrderID at Reserve should surface
// as a programming-error panic once the service is wired in stage 2).
func (book *ReservationBook) put(record *ReservationRecord) {
	book.records[record.OrderID] = record
}

// get returns the record for orderID, or nil if the book has none.
// A nil return at Convert / Release / ReleaseExcess is an invariant
// violation (engine can only call those AFTER a successful reserve) —
// the service layer turns nil into a panic there. Returning nil rather
// than (record, ok) keeps callers uniform: nil-check is the invariant
// check.
func (book *ReservationBook) get(orderID OrderID) *ReservationRecord {
	return book.records[orderID]
}

// remove deletes the record for orderID. No-op on unknown keys — Go's
// delete-on-nonexistent-key is a no-op, and reserve's terminal paths
// (Convert fully-drained, Release full-cancel, ReleaseExcess completion)
// are the only callers; a second delete on the same key would already be
// a caller bug caught by the service layer's "record missing" check.
func (book *ReservationBook) remove(orderID OrderID) {
	delete(book.records, orderID)
}
