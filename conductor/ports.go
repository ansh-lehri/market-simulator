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
//   - The canonical bring-up phase adds
//     WireInstance(exchangeID string, private <-chan contract.EngineResult)
//     when it ships that broker-side method.
//   - The canonical and on-demand teardown phases add
//     EndSession(SessionEndCmd) and FinalizeMarket(exchangeID) when
//     they ship.
//
// Never list an unbuilt method here — the compile-time assertion below
// would fail.
type BrokerPort interface {
	EnsureAccount(ctx context.Context, ownerType lifecycle.OwnerType, ownerID string) (funnel.AccountID, error)
	StartSession(ctx context.Context, cmd funnel.SessionStartCmd) error
	QualifyOnDemand(ctx context.Context, accountID funnel.AccountID) (bool, error)
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

// AgentOrchestrator is a PLACEHOLDER — the agent-orchestrator layer is
// undesigned. The real signature and behavior WILL change when the
// orchestrator is specified. The scaffold's stub implementation logs
// and returns nil (see stubs.go). Consequence (flag, not a defect): a
// spun-up market has no real agents until the orchestrator lands, so
// it will not truly trade; testable in isolation only.
type AgentOrchestrator interface {
	BringUp(market MarketTag, mode Mode, instruments []contract.InstrumentID) error
}

// LiquidityOperator is a PLACEHOLDER — the operator component is
// undesigned. SeedLiquidity is fire-and-trust (no return value).
// The scaffold's stub implementation logs and returns.
type LiquidityOperator interface {
	SeedLiquidity(marketID MarketID, specs []LiquiditySpec)
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
