package account

import (
	"time"

	"simulator/contract"
)

// AccountID is the opaque broker/economy-side identifier for a participant's
// account. It does not cross the exchange↔broker seam (the exchange knows only
// contract.ParticipantID / contract.BrokerID), so it stays local to economy.
type AccountID = string

// Bucket names a cash bucket on Account.Cash.
type Bucket int

const (
	Available Bucket = iota // free trading cash — reservable; MAY be negative (Option B)
	Reserved                // cash locked behind resting buy orders; >= 0
	Settled                 // realized/spent trading cash this session; >= 0
	Bank                    // buffer pool copy, not trading-visible; >= 0
)

func (bucket Bucket) String() string {
	switch bucket {
	case Available:
		return "Available"
	case Reserved:
		return "Reserved"
	case Settled:
		return "Settled"
	case Bank:
		return "Bank"
	}
	return "Bucket(?)"
}

// PosBucket names a position bucket on Position. Deliberately no PosSettled —
// positions are session-scoped and auto-squared at close (economy §3.1).
type PosBucket int

const (
	PosAvailable PosBucket = iota // units held, free to sell; >= 0
	PosReserved                   // units locked behind a resting sell; >= 0
)

func (bucket PosBucket) String() string {
	switch bucket {
	case PosAvailable:
		return "PosAvailable"
	case PosReserved:
		return "PosReserved"
	}
	return "PosBucket(?)"
}

// Cash is the live trading-account cash split plus the live bank copy. The
// split lives ONLY in memory + its Redis mirror; Postgres holds one signed
// trading_cash figure that this split reconciles to.
type Cash struct {
	Available int64 // MAY go negative — only DebitCash can drive it there (Option B)
	Reserved  int64
	Settled   int64
	Bank      int64
}

// Lot is one priced batch of currently-held units — the cost-basis record.
// One structure serves the position, position_age (via EntryAt), and 5.4's
// FIFO lot-matching (persistence B8/B10/§4.1).
type Lot struct {
	EntryPrice   int64     // integer ticks — the MAKER price this fill executed at
	EntryAt      time.Time // when this fill happened — sole source of position_age
	RemainingQty int64     // units of THIS lot not yet sold; decremented by RemovePosition
}

// Position is the live per-instrument holding. Session-scoped: born empty,
// must end flat (auto-squared at close). NEVER persisted to Postgres.
// OpenLots is FIFO in ARRIVAL ORDER: append at tail, consume from head.
type Position struct {
	Available int64
	Reserved  int64
	OpenLots  []Lot
}

// LotMatch is one consumed slice of an open lot, returned by RemovePosition
// as the cost-basis breakdown handed to 5.4. RemovePosition matches the lots;
// 5.4 does the money math from these against the exit price.
type LotMatch struct {
	EntryPrice int64
	Qty        int64
	EntryAt    time.Time
}

// Account is the in-memory working state for ONE participant during ONE live
// session. Seeded at session start from the Redis snapshot (itself bootstrapped
// once from Postgres); authoritative and current-to-the-tick for the session.
// This package never reads or writes those stores — a separate snapshotter
// mirrors this state out.
type Account struct {
	ID        AccountID
	Cash      Cash
	Positions map[contract.InstrumentID]*Position

	// Write-back policy set by the Tier-2 lifecycle flow at load:
	//   canonical -> true  (durable figure written back to Postgres at session end)
	//   on-demand -> false (isolated copy, discarded at close, never written back)
	PersistOnClose bool

	// Durable drip state, carried for the (HELD) drip_tap flow. Not touched by
	// any hand in this package.
	DripActive   bool
	DripProgress *int64
}
