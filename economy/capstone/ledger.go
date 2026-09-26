package capstone

import (
	"simulator/economy/account"
	"simulator/economy/realize"
	"simulator/economy/reserve"
)

// memoryLedger is the capstone-local AccountID -> *Account registry — a
// stand-in for the broker funnel's owner-side registry, which does not
// exist yet ([[project_orchestrator_role]]). Every session_load /
// ondemand_session_load result gets registered here via Put; the
// reserve service's mustGetAccount and realize's accumulator-birth read
// both resolve their account handles through Get.
//
// NOT a package. NOT reusable. Lives ONLY under economy/capstone/ until
// the broker funnel lands and owns the real registry.
//
// Structural typing: memoryLedger's method set satisfies BOTH
// reserve.AccountLedger and realize.AccountLedger — the compile-time
// asserts below hard-fail if either interface drifts.
type memoryLedger struct {
	accounts map[account.AccountID]*account.Account
}

// Compile-time proof memoryLedger satisfies both consumer interfaces.
// If reserve or realize adds a required method, Stage-4 wiring will
// stop compiling — surfacing the drift here before runtime.
var (
	_ reserve.AccountLedger = (*memoryLedger)(nil)
	_ realize.AccountLedger = (*memoryLedger)(nil)
)

// newMemoryLedger returns an empty ledger. Wire builds one per Bundle;
// nothing is registered until the caller runs a session_load flow and
// calls Put with the freshly loaded *Account.
func newMemoryLedger() *memoryLedger {
	return &memoryLedger{accounts: map[account.AccountID]*account.Account{}}
}

// Get returns the loaded account for accountID, or nil for an unknown
// id. Both reserve and realize treat a nil return as a broken ordering
// invariant (Gate 4 / accumulator-birth should never see an unloaded
// account) and panic — see reserve.mustGetAccount and
// realize.OnSellFill. This getter itself does not panic; it just
// reports absence.
func (ledger *memoryLedger) Get(accountID account.AccountID) *account.Account {
	return ledger.accounts[accountID]
}

// Put registers a freshly-loaded account. Called by the capstone test
// (and eventually the broker funnel) after every SessionLoad /
// OndemandSessionLoad and BEFORE the first order for that account.
//
// A re-Put of the same *Account is idempotent; a Put of a different
// *Account under the same id overwrites. Enforcement of "one live
// session per account" belongs to the broker funnel, not here — this
// stand-in is intentionally policy-free.
func (ledger *memoryLedger) Put(loaded *account.Account) {
	ledger.accounts[loaded.ID] = loaded
}
