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
// unbuilt-seam rule from conductor/CLAUDE.md.
//
// WireInstance attaches this broker to one exchange instance's private
// result stream, spawning the settlement consumer that drains it. One
// call per constructed exchange instance, invoked just before the
// exchange is started so the consumer is already parked when the first
// result lands. Non-blocking: returns as soon as the consumer goroutine
// is spawned. exchangeID is logging-only; routing is driven by the
// AccountID encoded on each EngineResult's OrderID.
//
// EndSession ends one live session on its owning shard. Canonical mode
// detaches the session binding (frees per-account XOR) and KEEPS the
// loaded account resident on the shard so a later rejoin can re-install
// over its carried positions and FinalizeMarket at EOD can persist +
// unload it. OnDemand mode is defined by the sibling on-demand teardown
// flow (not driven by this phase's handlers). Synchronous: returns when
// the shard has acked.
//
// FinalizeMarket is the single per-market close barrier the conductor
// waits on at closeAt. The broker fans out to every shard, each sweeping
// its own accounts bound to the given ExchangeID (persist via the
// lifecycle service, then unload). Synchronous: returns only after
// every shard replied OR the caller's ctx fired. A ctx fire surfaces as
// funnel.ErrBrokerTimeout; per-shard SessionSave failures surface as
// funnel.ErrFinalizeIncomplete wrapping the first cause.
type BrokerPort interface {
	EnsureAccount(ctx context.Context, ownerType lifecycle.OwnerType, ownerID string) (funnel.AccountID, error)
	StartSession(ctx context.Context, cmd funnel.SessionStartCmd) error
	QualifyOnDemand(ctx context.Context, accountID funnel.AccountID) (bool, error)
	WireInstance(exchangeID string, privateStream <-chan contract.EngineResult)
	EndSession(ctx context.Context, cmd funnel.SessionEndCmd) error
	FinalizeMarket(ctx context.Context, exchangeID funnel.ExchangeID) error
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
// WindDown signals the orchestrator that a market is closing — the
// agents for that market should flatten + persist their own state and
// exit. Fire-and-forget: the conductor does not wait on completion
// (the close-shape barrier is the broker's FinalizeMarket, not this).
// Signature WILL change when the real orchestrator design lands; the
// placeholder shape today takes the same {MarketTag, Mode} identifier
// pair as BringUp.
//
// A PLACEHOLDER stub implementation lives in stubs.go until the real
// orchestrator ships: BringUp returns a randomized mix of outcomes so
// every spin-up code path is exercised in tests; WindDown just records
// the call. Consequence (flag, not a defect): a market brought up
// against the stub has no real agents and will not truly trade.
type AgentOrchestrator interface {
	BringUp(ctx context.Context, market MarketTag, mode Mode, instruments []contract.InstrumentID) error
	WindDown(market MarketTag, mode Mode)
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
