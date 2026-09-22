package reserve

import (
	"simulator/economy/account"
	"simulator/economy/cost"
	"simulator/economy/realize"
)

// spyLedger is a minimal AccountLedger backed by an in-memory map. Tests
// pre-populate it with accounts they want the service to resolve; a Get
// on an unknown id returns nil so the service's mustGetAccount panics
// (which is what its ordering-invariant panic test asserts on).
type spyLedger struct {
	accounts map[account.AccountID]*account.Account
}

func newSpyLedger() *spyLedger {
	return &spyLedger{accounts: map[account.AccountID]*account.Account{}}
}

func (ledger *spyLedger) Get(accountID account.AccountID) *account.Account {
	return ledger.accounts[accountID]
}

// put installs an account under its own ID. Not part of the AccountLedger
// interface — a test-only affordance.
func (ledger *spyLedger) put(acct *account.Account) {
	ledger.accounts[acct.ID] = acct
}

// dirtyCall records one MarkSnapshotDirty invocation.
type dirtyCall struct {
	SessionID SessionID
	AccountID account.AccountID
}

// spyPersistence records every MarkSnapshotDirty call in arrival order.
// It never blocks (matches the "non-blocking" contract in seams.go's
// Persistence doc) and never touches Redis or Postgres. Reserve's tests
// assert exact call counts and (session, account) arguments.
type spyPersistence struct {
	dirtyCalls []dirtyCall
}

func newSpyPersistence() *spyPersistence { return &spyPersistence{} }

func (spy *spyPersistence) MarkSnapshotDirty(sessionID SessionID, accountID account.AccountID) {
	spy.dirtyCalls = append(spy.dirtyCalls, dirtyCall{SessionID: sessionID, AccountID: accountID})
}

// spyEvents records every Emit call in arrival order. Concrete event
// types (ReserveEvent / ConvertEvent / ReleaseEvent) are stored via the
// Event interface; tests type-assert to inspect the payload.
type spyEvents struct {
	emitted []Event
}

func newSpyEvents() *spyEvents { return &spyEvents{} }

func (spy *spyEvents) Emit(event Event) {
	spy.emitted = append(spy.emitted, event)
}

// spyRealizer records every OnSellFill invocation. Not exercised in
// stage 2 (ReserveIfAvailable does not touch realize), but wired into
// the test harness now so stages 3+ can reuse the same shape.
type spyRealizer struct {
	fills []realize.RealizationInput
}

func newSpyRealizer() *spyRealizer { return &spyRealizer{} }

func (spy *spyRealizer) OnSellFill(input realize.RealizationInput) {
	spy.fills = append(spy.fills, input)
}

// stubCostModule is a zero-cost stub — returns an empty CostBreakdown
// from both compute methods. Reserve stage 2 never calls the cost module
// (ReserveIfAvailable is pre-cost); the stub exists so the constructor
// can be satisfied with a valid CostModule interface value. Stages 3+
// swap this for cost.NewIndiaCostModule with pinned rates.
type stubCostModule struct{}

func (stubCostModule) ComputeFillCost(_ cost.FillCostInput) cost.CostBreakdown {
	return cost.CostBreakdown{Lines: map[string]int64{}, Total: 0}
}

func (stubCostModule) ComputeOrderCost(_ cost.OrderCostInput) cost.CostBreakdown {
	return cost.CostBreakdown{Lines: map[string]int64{}, Total: 0}
}

// harness bundles a service + its spies for a stage-2 test scenario.
// Tests build one with newHarness(...), pre-seed accounts via
// harness.ledger.put(...), then exercise service.ReserveIfAvailable(...)
// and assert on the spies + account state.
type harness struct {
	service *ReservationService
	ledger  *spyLedger
	persist *spyPersistence
	events  *spyEvents
	realiz  *spyRealizer
}

func newHarness() *harness {
	ledger := newSpyLedger()
	persist := newSpyPersistence()
	events := newSpyEvents()
	realiz := newSpyRealizer()
	return &harness{
		service: NewReservationService(ledger, stubCostModule{}, realiz, events, persist),
		ledger:  ledger,
		persist: persist,
		events:  events,
		realiz:  realiz,
	}
}
