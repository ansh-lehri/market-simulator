package reads

import (
	"context"
	"sync/atomic"
	"time"

	"simulator/broker/funnel"
	"simulator/broker/store"
)

// Shared stubs for the read-handler tests. Each stub satisfies one of
// the two ports and lets a test pin the value / error / delay the
// handler sees. Neither stub reaches for the real funnel or the real
// Store, so a test can drive branches (timeouts, bare errors, degrade
// paths) that are hard to force against the real implementations.

// stubBroker implements brokerPort. Fields are set per-test; a method
// without a configured outcome returns the zero snapshot + nil, which
// surfaces as Present:false → HTTP 409 (useful when a test wants to
// isolate one leg of a handler that calls multiple broker methods).
type stubBroker struct {
	// Balance leg
	balanceReply funnel.BalanceSnapshot
	balanceErr   error

	// Positions leg
	positionsReply      funnel.PositionsSnapshot
	positionsErr        error
	positionsInstrument funnel.InstrumentID // last seen filter — asserts the handler threads it

	// Orders leg
	ordersReply funnel.OpenOrdersSnapshot
	ordersErr   error

	// Observation leg
	observationReply funnel.ObservationLive
	observationErr   error

	// delay — if > 0, every method blocks on ctx.Done or delay before
	// returning. Lets a 504 test fire a tiny ReadTimeout and have the
	// stub land first on ctx.Done.
	delay time.Duration

	// Call counters so the observation-handler test can prove both the
	// live and the durable leg actually fired.
	observationCalls atomic.Int64
}

func (stub *stubBroker) ReadBalance(ctx context.Context, _ funnel.AccountID) (funnel.BalanceSnapshot, error) {
	if waitErr := stub.block(ctx); waitErr != nil {
		return funnel.BalanceSnapshot{}, waitErr
	}
	return stub.balanceReply, stub.balanceErr
}

func (stub *stubBroker) ReadPositions(ctx context.Context, _ funnel.AccountID, instrument funnel.InstrumentID) (funnel.PositionsSnapshot, error) {
	stub.positionsInstrument = instrument
	if waitErr := stub.block(ctx); waitErr != nil {
		return funnel.PositionsSnapshot{}, waitErr
	}
	return stub.positionsReply, stub.positionsErr
}

func (stub *stubBroker) ReadOpenOrders(ctx context.Context, _ funnel.AccountID) (funnel.OpenOrdersSnapshot, error) {
	if waitErr := stub.block(ctx); waitErr != nil {
		return funnel.OpenOrdersSnapshot{}, waitErr
	}
	return stub.ordersReply, stub.ordersErr
}

func (stub *stubBroker) ReadObservation(ctx context.Context, _ funnel.AccountID) (funnel.ObservationLive, error) {
	stub.observationCalls.Add(1)
	if waitErr := stub.block(ctx); waitErr != nil {
		return funnel.ObservationLive{}, waitErr
	}
	return stub.observationReply, stub.observationErr
}

// block sleeps for `delay`, cancelled early by ctx. Returns
// funnel.ErrBrokerTimeout on ctx cancellation so the stub reproduces
// the real broker facade's error shape — handler status mapping stays
// honestly tested without pulling a real shard goroutine into the test.
func (stub *stubBroker) block(ctx context.Context) error {
	if stub.delay <= 0 {
		return nil
	}
	select {
	case <-time.After(stub.delay):
		return nil
	case <-ctx.Done():
		return funnel.ErrBrokerTimeout
	}
}

// stubStore implements storePort. Fields are set per-test; a method
// with no configured reply returns the zero value, which the handlers
// treat as a legitimate empty-but-successful read (200 with [] / zero
// counts) — exactly the behaviour real Store methods have for new or
// trade-less accounts.
type stubStore struct {
	// RecentTrades leg (used by the observation endpoint).
	recentReply []store.TradeRow
	recentErr   error

	// TradePage + PnLSummary legs (trades + pnl endpoints).
	tradePageReply   []store.TradeRow
	tradePageErr     error
	pnlReply         store.PnLSummary
	pnlErr           error

	// Costs legs (per-trade + aggregate).
	costsPerTradeReply  []store.TradeCostBreakdown
	costsPerTradeErr    error
	costsAggregateReply store.CostAggregate
	costsAggregateErr   error

	// AccountCash leg — pre-session trading_cash + bank_cash pair.
	accountCashTrading int64
	accountCashBank    int64
	accountCashErr     error

	// Captured args so tests can assert the handler threaded them
	// through correctly (pagination cursor, country routing, etc.).
	tradePageLimit        int
	tradePageBefore       *time.Time
	costsPerTradeLimit    int
	costsPerTradeBefore   *time.Time

	// Applied uniformly by every method; a test that wants only one
	// leg slow should instead set its own stub via a wrapper.
	delay time.Duration

	// Call witnesses for fan-out proofs.
	recentCalls         atomic.Int64
	tradePageCalls      atomic.Int64
	pnlCalls            atomic.Int64
	costsPerTradeCalls  atomic.Int64
	costsAggregateCalls atomic.Int64
	accountCashCalls    atomic.Int64
}

func (stub *stubStore) AccountCash(ctx context.Context, _ store.AccountID) (int64, int64, error) {
	stub.accountCashCalls.Add(1)
	if waitErr := stub.waitOrCancel(ctx); waitErr != nil {
		return 0, 0, waitErr
	}
	return stub.accountCashTrading, stub.accountCashBank, stub.accountCashErr
}

func (stub *stubStore) RecentTrades(ctx context.Context, _ store.AccountID, _ int) ([]store.TradeRow, error) {
	stub.recentCalls.Add(1)
	if waitErr := stub.waitOrCancel(ctx); waitErr != nil {
		return nil, waitErr
	}
	return stub.recentReply, stub.recentErr
}

func (stub *stubStore) TradePage(ctx context.Context, _ store.AccountID, limit int, before *time.Time) ([]store.TradeRow, error) {
	stub.tradePageCalls.Add(1)
	stub.tradePageLimit = limit
	stub.tradePageBefore = before
	if waitErr := stub.waitOrCancel(ctx); waitErr != nil {
		return nil, waitErr
	}
	return stub.tradePageReply, stub.tradePageErr
}

func (stub *stubStore) PnLSummary(ctx context.Context, _ store.AccountID) (store.PnLSummary, error) {
	stub.pnlCalls.Add(1)
	if waitErr := stub.waitOrCancel(ctx); waitErr != nil {
		return store.PnLSummary{}, waitErr
	}
	return stub.pnlReply, stub.pnlErr
}

func (stub *stubStore) CostsPerTradeIN(ctx context.Context, _ store.AccountID, limit int, before *time.Time) ([]store.TradeCostBreakdown, error) {
	stub.costsPerTradeCalls.Add(1)
	stub.costsPerTradeLimit = limit
	stub.costsPerTradeBefore = before
	if waitErr := stub.waitOrCancel(ctx); waitErr != nil {
		return nil, waitErr
	}
	return stub.costsPerTradeReply, stub.costsPerTradeErr
}

func (stub *stubStore) CostsAggregateIN(ctx context.Context, _ store.AccountID) (store.CostAggregate, error) {
	stub.costsAggregateCalls.Add(1)
	if waitErr := stub.waitOrCancel(ctx); waitErr != nil {
		return store.CostAggregate{}, waitErr
	}
	return stub.costsAggregateReply, stub.costsAggregateErr
}

// waitOrCancel sleeps for delay, cancelled early by ctx. Returns the
// ctx error on cancel so a slow stub reproduces a real Store that
// errors out on ctx deadline (which the handler maps to 503).
func (stub *stubStore) waitOrCancel(ctx context.Context) error {
	if stub.delay <= 0 {
		return nil
	}
	select {
	case <-time.After(stub.delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Compile-time guarantees that the stubs still satisfy the ports — if
// a port grows a method, the build breaks here before any handler test
// runs.
var (
	_ brokerPort = (*stubBroker)(nil)
	_ storePort  = (*stubStore)(nil)
)

// defaultTestConfig is a healthy Config most tests reuse. The timeout
// tests shrink ReadTimeout so the ctx deadline fires before stub.delay.
var defaultTestConfig = Config{ReadTimeout: 2 * time.Second}
