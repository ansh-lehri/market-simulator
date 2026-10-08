package funnel

import (
	"simulator/contract"
	"simulator/economy/account"
)

// The command types the funnel accepts, split by whether the caller
// waits for a reply.
//
//   Request/response (carry a buffered(1) ReplyTo, caller blocks):
//     PlaceOrder       — place a new order
//     CancelOrder      — cancel a working order
//     Transfer         — move money between the trading and bank buckets
//     ReadBalance      — read the account's four Cash buckets
//     ReadPositions    — read positions, optionally filtered by instrument
//     ReadOpenOrders   — read the account's working-order set
//     ReadObservation  — composite live snapshot (balance + positions + orders)
//
//   Push (no reply, nobody waits):
//     ApplyResult      — settle an exchange EngineResult
//     FireStop         — a stop triggered on a watched order
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

// Transfer — Tier 3 bank ↔ trading move command. Finalizes the Tier-0
// BankMove placeholder (BROKER_PHASE3 §1.2 / P3-D1): the command carries
// L5 cash BUCKETS (not a direction enum), so the shard-side handler calls
// account.Cash.MoveCash(From, To, Amount) with zero translation at the
// L5 seam.
//
// Two-vocabulary seam (P3-D2): the live path speaks buckets (here); the
// off-shard durable path speaks store.TransferDirection. The HTTP handler
// in broker/transfer maps the one request direction into BOTH, once —
// this struct does not carry a direction enum of its own.
//
// Amount is strictly positive (handler-validated before the command is
// built); direction is encoded entirely by which bucket pair is passed,
// never by a sign on Amount (root value discipline).
type Transfer struct {
	AccountID AccountID
	From      account.Bucket
	To        account.Bucket
	Amount    int64
	ReplyTo   chan TransferResult // buffered(1); caller-owned
}

// Account returns the routing key for the funnel router.
func (cmd Transfer) Account() AccountID { return cmd.AccountID }

// ReadBalance — Tier 4 live-balance read. Copy-out contract: the handler
// returns a by-value snapshot, never a pointer into the owned Account
// (BROKER_TIER0 §5, read rule).
type ReadBalance struct {
	AccountID AccountID
	ReplyTo   chan BalanceSnapshot // buffered(1); caller-owned
}

// Account returns the routing key for the funnel router.
func (cmd ReadBalance) Account() AccountID { return cmd.AccountID }

// ReadPositions is the live-positions read. Copy-out contract; same
// rule as ReadBalance. Instrument is an optional single-instrument
// filter: "" means every held instrument; a specific InstrumentID
// returns at most one row (empty slice if the account holds nothing
// in it). The HTTP layer parses ?instrument= and threads it in; the
// observation handler calls with "" to get them all.
type ReadPositions struct {
	AccountID  AccountID
	Instrument InstrumentID
	ReplyTo    chan PositionsSnapshot // buffered(1); caller-owned
}

// Account returns the routing key for the funnel router.
func (cmd ReadPositions) Account() AccountID { return cmd.AccountID }

// ReadOpenOrders is the live-open-orders read. Copy-out: the handler
// joins the shard's tracks map (status, cancel-in-flight) with the
// reservation view (money / qty / side / price) and returns a value-
// copy slice. No instrument filter — the account's whole working set.
type ReadOpenOrders struct {
	AccountID AccountID
	ReplyTo   chan OpenOrdersSnapshot // buffered(1); caller-owned
}

// Account returns the routing key for the funnel router.
func (cmd ReadOpenOrders) Account() AccountID { return cmd.AccountID }

// ReadObservation is the composite live snapshot — balance, positions
// and open orders assembled in one visit to the owning shard so the
// three sub-payloads cannot interleave with a fill. Three independent
// facade calls could interleave, which is the whole reason this is a
// dedicated command. The durable "recent closed trades" half of the
// observation body is fanned out concurrently by the HTTP handler and
// merged there, not here.
type ReadObservation struct {
	AccountID AccountID
	ReplyTo   chan ObservationLive // buffered(1); caller-owned
}

// Account returns the routing key for the funnel router.
func (cmd ReadObservation) Account() AccountID { return cmd.AccountID }

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
