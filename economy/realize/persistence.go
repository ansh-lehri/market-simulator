package realize

import (
	"crypto/rand"
	"encoding/hex"

	"simulator/economy/account"
)

// Persistence is the one seam through which realize (and, later, reserve)
// hands work out for the async snapshotter and drainer. It is deliberately
// small: two methods, both non-blocking. Phase 1 wires an in-memory spy;
// the real Redis/Postgres implementation lands behind the same interface
// with zero changes to any compute code.
type Persistence interface {
	// MarkSnapshotDirty flags an account's live state so the background
	// snapshotter mirrors {cash, positions, reservations} to Redis on its
	// next tick. Realize does NOT call this — 5.2's reservation flows do.
	// The method lives on the interface for the shared seam; realize
	// simply never invokes it (Stage 3 tests assert that boundary).
	MarkSnapshotDirty(sessionID SessionID, accountID account.AccountID)

	// EnqueueClosedTrade pushes one round-trip's summary + itemized cost
	// child, bundled as ClosedTradeRow, onto the async closed-trade buffer.
	// Realize calls this on flush, gated by the account's PersistOnClose.
	EnqueueClosedTrade(closed ClosedTradeRow)
}

// AccountLedger is realize's read handle to accounts — one method,
// returning the live account or nil for unknown ids. Deliberately narrow:
// realize only needs to read PersistOnClose at accumulator birth (and, for
// auto-square in Stage 4, invoke the Tier-1 hands via the returned
// *account.Account). Defined locally in realize because no shared home
// exists yet; a future ledger package can implement this interface.
type AccountLedger interface {
	Get(accountID account.AccountID) *account.Account
}

// defaultTradeIDGenerator returns a random 128-bit hex string. Used by
// NewRealizer when the caller passes no upstream TradeID on a fill.
// crypto/rand failure is dev-fatal (root: broken invariant, panic).
func defaultTradeIDGenerator() TradeID {
	var randomBytes [16]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		panic("realize: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(randomBytes[:])
}
