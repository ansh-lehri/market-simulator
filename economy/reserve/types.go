package reserve

import (
	"errors"

	"simulator/contract"
	"simulator/economy/account"
)

// SessionID is the broker-internal identifier for one live session. It does
// not cross the exchange↔broker seam, so it lives in economy. Aliased
// locally here (mirroring the same alias in realize/types.go) until a
// shared home is decided across economy packages.
type SessionID = string

// OrderID is the broker-side identifier for one admitted order. Opaque
// string; every ReservationRecord in the book is keyed by it and every
// engine call after Gate 4 references orders by this id.
type OrderID = string

// Order is the value the engine hands to ReserveIfAvailable at Admission
// Gate 4. It carries EVERYTHING reserve needs to decide fundability and
// build the ReservationRecord — no pointer into engine or account state
// crosses the seam.
//
// Fields are populated by the engine's Gate-4 caller before the reserve
// consult: identity ids (OrderID / AccountID / SessionID / InstrumentID),
// the order intent (Side / Qty / IsMarket / LimitPrice), and the
// market-buy sizing inputs the engine reads from its own state
// (BestAsk + SlippageBuffer, both used only when IsMarket && Side == Buy).
type Order struct {
	OrderID      OrderID
	AccountID    account.AccountID
	SessionID    SessionID
	InstrumentID contract.InstrumentID

	Side     contract.Side
	Qty      int64 // strictly positive — direction lives in Side
	IsMarket bool

	// LimitPrice is used only when !IsMarket. Integer ticks.
	LimitPrice int64

	// BestAsk + SlippageBuffer size the market-buy reservation:
	//   requiredCash = BestAsk*Qty + SlippageBuffer
	// Both integer coins/ticks. Ignored on sells and on buy-limit orders.
	BestAsk        int64
	SlippageBuffer int64
}

// Participant carries the identity + type of whoever placed the order.
// Gate 4 is parameterized by participant type (a human hits the three-pool
// account; an agent archetype may be budget-constrained or pass-through) —
// per-archetype branching is item 32, LOCKED as DEFERRED to Layer-2 agent
// design. Until then, reserve treats every participant identically; this
// struct is a passthrough value carrier so the seam is already in place
// when item 32 lands.
type Participant struct {
	ID string
}

// -----------------------------------------------------------------------------
// Rejection sentinels — the two typed reasons ReserveIfAvailable returns to
// the engine at Gate 4. Values, so callers dispatch with errors.Is on the
// sentinel directly (no struct payload needed at Gate 4 — the engine wants
// the reason, not have/want figures). Category-only surface, consistent
// with memory feedback_error_category_sentinels.
//
// These are DISTINCT from account.ErrInsufficientFunds / ErrInsufficientUnits:
// the account category sentinels describe a hand's bucket-underflow refusal
// (a should-never-fire backstop under single-writer); reserve's sentinels
// describe the Gate-4 admission decision returned BEFORE any hand runs.
// They live in different packages precisely because the read-compare in
// ReserveIfAvailable is a distinct concept from a hand's guard firing.
// -----------------------------------------------------------------------------

var (
	// ErrInsufficientFunds — returned by ReserveIfAvailable when a buy's
	// required cash (limit_price*qty, or best_ask*qty + slippage_buffer for
	// market) exceeds ReadAvailableCash. Option B: a negative Available
	// correctly fails any positive requirement here ("no buying power
	// underwater").
	ErrInsufficientFunds = errors.New("reserve: insufficient funds")

	// ErrInsufficientPosition — returned by ReserveIfAvailable when a sell's
	// requested qty exceeds ReadAvailablePosition. Missing instrument → 0 →
	// this sentinel returns naturally (no special case).
	ErrInsufficientPosition = errors.New("reserve: insufficient position")
)
