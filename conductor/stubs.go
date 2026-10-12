package conductor

import (
	"log/slog"

	"simulator/contract"
	"simulator/exchange/clock"
	"simulator/exchange/wire"
)

// defaultExchangeFactory is the production ExchangeFactory — a one-line
// pass-through to wire.NewExchange so tests can swap in a fake while
// production never pays an indirection.
type defaultExchangeFactory struct{}

func (defaultExchangeFactory) NewExchange(numShards int, schedule clock.SessionSchedule, instruments []wire.InstrumentSpec) *wire.Exchange {
	return wire.NewExchange(numShards, schedule, instruments)
}

// stubAgentOrchestrator is a no-op AgentOrchestrator used until the
// agent-orchestrator layer is designed. Markets built with this stub
// will NOT trade (no agents), but every other conductor flow works —
// construct, wire, admit, teardown.
type stubAgentOrchestrator struct {
	log *slog.Logger
}

func (stub stubAgentOrchestrator) BringUp(market MarketTag, mode Mode, instruments []contract.InstrumentID) error {
	stub.log.Debug("conductor: stub AgentOrchestrator.BringUp (no-op — orchestrator undesigned)",
		"market", market,
		"mode", mode,
		"instrument_count", len(instruments),
	)
	return nil
}

// stubLiquidityOperator is a no-op LiquidityOperator used until the
// operator is designed. On-demand markets explicitly do without a
// liquidity operator for now; canonical markets will have a thin book
// until liquidity seeding ships.
type stubLiquidityOperator struct {
	log *slog.Logger
}

func (stub stubLiquidityOperator) SeedLiquidity(marketID MarketID, specs []LiquiditySpec) {
	stub.log.Debug("conductor: stub LiquidityOperator.SeedLiquidity (no-op — operator undesigned)",
		"market_id", marketID,
		"spec_count", len(specs),
	)
}

// Compile-time: the stubs satisfy their interfaces. Any port signature
// change (including the later agent-orchestrator overhaul) breaks the
// build here.
var (
	_ ExchangeFactory   = defaultExchangeFactory{}
	_ AgentOrchestrator = stubAgentOrchestrator{}
	_ LiquidityOperator = stubLiquidityOperator{}
)
