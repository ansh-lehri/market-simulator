package conductor

import (
	"context"

	"simulator/broker/funnel"
	"simulator/broker/marketdata"
	"simulator/contract"
	"simulator/economy/lifecycle"
	"simulator/exchange/clock"
	"simulator/exchange/emit"
	"simulator/exchange/wire"
)

// BrokerPort lists ONLY the shipped funnel.Broker methods — the
// unbuilt-seam rule from conductor/CLAUDE.md. Later phases grow this:
//   - The teardown phases add EndSession(SessionEndCmd) and
//     FinalizeMarket(exchangeID) when they ship.
//
// Never list an unbuilt method here — the compile-time assertion below
// would fail.
//
// WireInstance attaches this broker to one exchange instance's private
// result stream, spawning the settlement consumer that drains it. One
// call per constructed exchange instance, invoked just before the
// exchange is started so the consumer is already parked when the first
// result lands. Non-blocking: returns as soon as the consumer goroutine
// is spawned. exchangeID is logging-only; routing is driven by the
// AccountID encoded on each EngineResult's OrderID.
type BrokerPort interface {
	EnsureAccount(ctx context.Context, ownerType lifecycle.OwnerType, ownerID string) (funnel.AccountID, error)
	StartSession(ctx context.Context, cmd funnel.SessionStartCmd) error
	QualifyOnDemand(ctx context.Context, accountID funnel.AccountID) (bool, error)
	WireInstance(exchangeID string, privateStream <-chan contract.EngineResult)
}

// MarketDataPort is the market-data holder's conductor-facing surface
// — per-instance WireInstance plus WSConfig for the WebSocket
// dispatcher the join phase will build.
type MarketDataPort interface {
	WireInstance(exchangeID marketdata.ExchangeID, tape <-chan emit.TapeEvent, market string, mode marketdata.Mode) *marketdata.FanOut
	WSConfig() marketdata.WSConfig
}

// ExchangeFactory wraps wire.NewExchange so tests can inject a fake
// without touching the real exchange runtime. The default implementation
// (defaultExchangeFactory) is a one-line pass-through.
type ExchangeFactory interface {
	NewExchange(numShards int, schedule clock.SessionSchedule, instruments []wire.InstrumentSpec) *wire.Exchange
}

// AgentOrchestrator is the agent-orchestrator layer's conductor-facing
// surface. The real orchestrator runs as a separate process reached
// over HTTP; adapters translate transport status into the typed error
// sentinels in errors.go.
//
// BringUp is synchronous and bounded by the caller's context deadline
// (the canonical bring-up flow derives the deadline from the Conductor
// Config's BringUpTimeout). It is called once per market bring-up,
// before any wiring, so a failure aborts cleanly with no exchange
// consumers spawned.
//
// A PLACEHOLDER stub implementation lives in stubs.go until the real
// orchestrator ships: the stub returns a randomized mix of nil and the
// three orchestrator-error sentinels so every spin-up code path is
// exercised in tests without manual swap-ins. Consequence (flag, not a
// defect): a market brought up against the stub has no real agents and
// will not truly trade.
type AgentOrchestrator interface {
	BringUp(ctx context.Context, market MarketTag, mode Mode, instruments []contract.InstrumentID) error
}

// LiquidityOperator is the liquidity operator's conductor-facing
// surface. The operator may run in-process or over HTTP; either way
// SeedLiquidity is fire-and-trust — the conductor hands over the specs
// and does not block on a response. Adapters that use the network run
// their own goroutine so this method returns promptly on the hot path.
//
// ctx is passed through so adapters can observe conductor shutdown and
// drop any in-flight transport work.
//
// A PLACEHOLDER stub implementation lives in stubs.go — it records a
// randomized mix of outcomes in observable in-package counters so tests
// can verify the stub exercised every branch.
type LiquidityOperator interface {
	SeedLiquidity(ctx context.Context, marketID MarketID, specs []LiquiditySpec)
}

// LiquiditySpec is the fire-and-trust payload shape. Minimal and
// placeholder — will change with the operator spec.
type LiquiditySpec struct {
	Instrument contract.InstrumentID
	Levels     int
	SpacingPct float64
	NearQty    int64
	Taper      float64
}

// Compile-time assertions — these MUST pass in the scaffold. If a
// later phase extends BrokerPort with an unbuilt funnel method, this
// file stops compiling and the mistake surfaces at build time rather
// than at first use.
var (
	_ BrokerPort     = (*funnel.Broker)(nil)
	_ MarketDataPort = (*marketdata.MarketData)(nil)
)
