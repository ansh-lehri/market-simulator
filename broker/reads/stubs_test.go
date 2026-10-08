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

// stubStore implements storePort. Only RecentTrades is actually
// exercised today (the observation endpoint is the only live-handler
// user of the store); the other four methods satisfy the interface
// with zero-value returns and are not called by any live handler yet.
// A durable-handler test that lands later will configure its own
// fields on this struct.
type stubStore struct {
	recentReply []store.TradeRow
	recentErr   error
	delay       time.Duration

	recentCalls atomic.Int64
}

func (stub *stubStore) RecentTrades(ctx context.Context, _ store.AccountID, _ int) ([]store.TradeRow, error) {
	stub.recentCalls.Add(1)
	if stub.delay > 0 {
		select {
		case <-time.After(stub.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return stub.recentReply, stub.recentErr
}

// Zero-value stubs for the four storePort methods that no handler
// calls today. They exist only so *stubStore satisfies the interface.
func (stub *stubStore) TradePage(context.Context, store.AccountID, int, *time.Time) ([]store.TradeRow, error) {
	return nil, nil
}
func (stub *stubStore) PnLSummary(context.Context, store.AccountID) (store.PnLSummary, error) {
	return store.PnLSummary{}, nil
}
func (stub *stubStore) CostsPerTradeIN(context.Context, store.AccountID, int, *time.Time) ([]store.TradeCostBreakdown, error) {
	return nil, nil
}
func (stub *stubStore) CostsAggregateIN(context.Context, store.AccountID) (store.CostAggregate, error) {
	return store.CostAggregate{}, nil
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
