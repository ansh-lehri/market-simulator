package funnel

import "simulator/contract"

// The seven Tier-0 command types, split by whether the caller waits for
// a reply (BROKER_TIER0 §3):
//
//   Request/response (carry a buffered(1) ReplyTo, caller blocks):
//     PlaceOrder      → Tier 1
//     CancelOrder     → Tier 1
//     BankMove        → Tier 3
//     ReadBalance     → Tier 4
//     ReadPositions   → Tier 4
//
//   Push (no reply, nobody waits):
//     ApplyResult     → Tier 1  (settle an exchange EngineResult)
//     FireStop        → Tier 2  (a stop triggered)
//
// Each command's Account() method returns the AccountID the router
// hashes; the router reads ONLY this method (BROKER_TIER0 §3, Point 3).
// Reply channels are always caller-created, buffered size 1 — the shard
// never blocks on the send, even after the caller has walked away
// (BROKER_TIER0 §7).

// PlaceOrder — Tier 1 order-intake command.
type PlaceOrder struct {
	AccountID AccountID
	SessionID SessionID
	Order     OrderSpec
	ReplyTo   chan PlaceResult // buffered(1); caller-owned
}

// Account returns the routing key for the funnel router.
func (cmd PlaceOrder) Account() AccountID { return cmd.AccountID }

// CancelOrder — Tier 1 cancel command.
type CancelOrder struct {
	AccountID AccountID
	OrderID   OrderID
	ReplyTo   chan CancelResult // buffered(1); caller-owned
}

// Account returns the routing key for the funnel router.
func (cmd CancelOrder) Account() AccountID { return cmd.AccountID }

// BankMove — Tier 3 bank ↔ trading move command. Amount is strictly
// positive; direction is carried by Dir, never by a negative amount
// (root value discipline).
type BankMove struct {
	AccountID AccountID
	Dir       BankDir
	Amount    int64
	ReplyTo   chan BankResult // buffered(1); caller-owned
}

// Account returns the routing key for the funnel router.
func (cmd BankMove) Account() AccountID { return cmd.AccountID }

// ReadBalance — Tier 4 live-balance read. Copy-out contract: the handler
// returns a by-value snapshot, never a pointer into the owned Account
// (BROKER_TIER0 §5, read rule).
type ReadBalance struct {
	AccountID AccountID
	ReplyTo   chan BalanceSnapshot // buffered(1); caller-owned
}

// Account returns the routing key for the funnel router.
func (cmd ReadBalance) Account() AccountID { return cmd.AccountID }

// ReadPositions — Tier 4 live-positions read. Copy-out contract; same
// rule as ReadBalance.
type ReadPositions struct {
	AccountID AccountID
	ReplyTo   chan PositionsSnapshot // buffered(1); caller-owned
}

// Account returns the routing key for the funnel router.
func (cmd ReadPositions) Account() AccountID { return cmd.AccountID }

// ApplyResult — push command carrying a settlement outcome from the
// exchange. Result is the whole contract.EngineResult; the AccountID is
// parsed from Result.OrderID upstream of the router (the OrderID shape
// is "<AccountID>:<seq>", BROKER_TIER0 §3, Point 3), so no cross-shard
// lookup is needed here.
type ApplyResult struct {
	AccountID AccountID
	Result    contract.EngineResult
}

// Account returns the routing key for the funnel router.
func (cmd ApplyResult) Account() AccountID { return cmd.AccountID }

// FireStop — push command emitted when a watched stop triggers.
type FireStop struct {
	AccountID AccountID
	Stop      StopRecord
}

// Account returns the routing key for the funnel router.
func (cmd FireStop) Account() AccountID { return cmd.AccountID }
