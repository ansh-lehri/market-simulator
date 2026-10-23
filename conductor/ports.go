package conductor

import (
	"context"
	"time"

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

// AgentBringUpRequest is the conductor-side input to
// AgentOrchestrator.BringUp. It carries everything the real,
// HTTP-reached agent-orchestrator process needs to populate one run:
//
//   - Market is the market identity ("IN") — kept distinct from
//     ExchangeID because it is the stable, human-legible label tests
//     and logs key on; it does NOT uniquely identify a run (many
//     canonical sessions over time, or many concurrent on-demand
//     instances, share one Market).
//   - ExchangeID is the per-instance run id (canonical:
//     "<tag>:<session-date>"; on-demand: "od:<sessionID>") — the real
//     orchestrator's per-run namespace key. This is the field that
//     must round-trip unchanged between a BringUp call and the later
//     WindDown call for the same run.
//   - SessionStart/SessionEnd anchor the agent population's own
//     scheduler. For canonical these are the exchange's own
//     authoritative open/close instants. For on-demand these are a
//     provisional estimate taken just before this call (see
//     ondemand_spinup.go) — NOT necessarily identical to the
//     exchange's real T0, which is deliberately measured after this
//     call returns so a slow orchestrator round-trip doesn't eat into
//     the user's visible warm-up countdown.
//   - Scenario is only honored on on-demand sessions by the real
//     orchestrator. It carries the optional "condition" field from
//     POST /session/ondemand's request body (http_ondemand.go's
//     onDemandRequestBody.Condition), validated there against
//     Conductor.practiceConditions (Config.PracticeConditions, or
//     defaultPracticeConditions if left nil) before StartOnDemand ever
//     sets this field — empty string if the caller didn't pick one.
//
// Country and BrokerID are deliberately NOT here — they are constant
// for one conductor/orchestrator pairing (one broker, one country),
// not a per-call concept, so the production adapter stamps its own
// configured values onto the outbound wire request instead of
// threading them through every spin-up call site. See
// NewHTTPAgentOrchestrator in agent_client.go.
type AgentBringUpRequest struct {
	Market       MarketTag
	ExchangeID   MarketID
	Mode         Mode
	SessionStart time.Time
	SessionEnd   time.Time
	Instruments  []contract.InstrumentID
	Scenario     string
}

// AgentOrchestrator is the agent-orchestrator layer's conductor-facing
// surface. The real orchestrator runs as a separate process reached
// over HTTP (see agent_client.go's httpAgentOrchestrator); adapters
// translate transport status into the typed error sentinels in
// errors.go.
//
// BringUp is synchronous and bounded by the caller's context deadline
// (the canonical bring-up flow derives the deadline from the Conductor
// Config's BringUpTimeout). It is called once per market bring-up,
// before any wiring, so a failure aborts cleanly with no exchange
// consumers spawned.
//
// WindDown signals the orchestrator that a market is closing — the
// agents for that run should flatten + persist their own state and
// exit. Fire-and-forget in the sense that the conductor never blocks
// forward-progress on its outcome (the close-shape barrier is the
// broker's FinalizeMarket, not this) — but it IS now a real,
// error-returning network call; every call site logs-and-ignores a
// non-nil return rather than discarding it silently.
//
// A PLACEHOLDER stub implementation lives in stubs.go for tests and
// for any deployment that doesn't configure a real agent-server URL:
// BringUp returns a randomized mix of outcomes so every spin-up code
// path is exercised in tests; WindDown just records the call.
// Consequence of running against the stub (flag, not a defect): a
// market brought up against it has no real agents and will not truly
// trade.
type AgentOrchestrator interface {
	BringUp(ctx context.Context, request AgentBringUpRequest) error
	WindDown(ctx context.Context, exchangeID MarketID, market MarketTag, mode Mode) error
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
