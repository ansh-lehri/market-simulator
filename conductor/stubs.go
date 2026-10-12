package conductor

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

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

// Stub outcome labels — the keys the stubs tally in outcomeCounts.
// Lowercase stable strings so tests and logs read the same vocabulary.
const (
	stubOutcomeSuccess     = "success"
	stubOutcomeUnavailable = "unavailable"
	stubOutcomeTimeout     = "timeout"
	stubOutcomeRejected    = "rejected"
)

// Cumulative upper bounds for the stub outcome distribution. A draw in
// [0, 100) maps to the first bucket whose upper bound it is strictly
// below. Chosen so every branch of the spin-up error path gets exercised
// over a handful of calls while the happy path still dominates: tests
// do not have to force outcomes to see nil, and do not have to swap
// stubs to see each error sentinel.
const (
	stubBoundSuccess     = 70  // [0,  70) → success
	stubBoundUnavailable = 85  // [70, 85) → unavailable
	stubBoundTimeout     = 95  // [85, 95) → timeout
	// [95, 100)                         → rejected
)

// stubDrawToOutcome maps a uniform draw in [0, 100) to one of the four
// outcome labels using the cumulative bounds above. Shared by both
// stubs so their distributions stay in lockstep.
func stubDrawToOutcome(draw int) string {
	switch {
	case draw < stubBoundSuccess:
		return stubOutcomeSuccess
	case draw < stubBoundUnavailable:
		return stubOutcomeUnavailable
	case draw < stubBoundTimeout:
		return stubOutcomeTimeout
	default:
		return stubOutcomeRejected
	}
}

// stubAgentOrchestrator simulates an out-of-process agent-orchestrator
// adapter. Each BringUp draws from a seeded pseudo-random source and
// returns one of {nil, ErrOrchestratorUnavailable, ErrOrchestratorTimeout,
// ErrAgentBringUpRejected} according to stubDrawToOutcome. A seed of 0
// at construction falls back to time.Now().UnixNano() so a production
// deployment that never injected a real orchestrator still sees varying
// outcomes across restarts.
//
// Concurrency: the random source and the outcome counter are both
// guarded by randomLock; math/rand.Rand is not safe for concurrent use.
//
// outcomeCounts is observable to in-package tests via
// outcomeCountsSnapshot. It is not part of the exported surface — the
// stub is unexported, and so is this accessor.
type stubAgentOrchestrator struct {
	log           *slog.Logger
	random        *rand.Rand
	randomLock    sync.Mutex
	outcomeCounts map[string]int
}

func newStubAgentOrchestrator(log *slog.Logger, seed int64) *stubAgentOrchestrator {
	if log == nil {
		log = slog.Default()
	}
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	return &stubAgentOrchestrator{
		log:           log,
		random:        rand.New(rand.NewSource(seed)),
		outcomeCounts: map[string]int{},
	}
}

func (stub *stubAgentOrchestrator) BringUp(ctx context.Context, market MarketTag, mode Mode, instruments []contract.InstrumentID) error {
	// Honor a context that was already cancelled or past its deadline
	// before we even drew — a real HTTP adapter would exit the same way.
	if contextErr := ctx.Err(); contextErr != nil {
		stub.randomLock.Lock()
		stub.outcomeCounts[stubOutcomeTimeout]++
		stub.randomLock.Unlock()
		stub.log.Warn("conductor: stub AgentOrchestrator.BringUp context already cancelled",
			"market", market,
			"mode", mode,
			"err", contextErr,
		)
		return fmt.Errorf("%w: %w", ErrOrchestratorTimeout, contextErr)
	}

	stub.randomLock.Lock()
	draw := stub.random.Intn(100)
	outcome := stubDrawToOutcome(draw)
	stub.outcomeCounts[outcome]++
	stub.randomLock.Unlock()

	switch outcome {
	case stubOutcomeSuccess:
		stub.log.Debug("conductor: stub AgentOrchestrator.BringUp success",
			"market", market,
			"mode", mode,
			"instrument_count", len(instruments),
		)
		return nil
	case stubOutcomeUnavailable:
		stub.log.Warn("conductor: stub AgentOrchestrator.BringUp unavailable",
			"market", market,
			"mode", mode,
		)
		return ErrOrchestratorUnavailable
	case stubOutcomeTimeout:
		stub.log.Warn("conductor: stub AgentOrchestrator.BringUp timeout",
			"market", market,
			"mode", mode,
		)
		return ErrOrchestratorTimeout
	default:
		stub.log.Warn("conductor: stub AgentOrchestrator.BringUp rejected",
			"market", market,
			"mode", mode,
		)
		return ErrAgentBringUpRejected
	}
}

// outcomeCountsSnapshot returns a copy of the current outcome tally.
// In-package tests use it to assert the stub exercised every branch
// over a seeded run. Returns a copy so the caller cannot mutate the
// stub's internal state.
func (stub *stubAgentOrchestrator) outcomeCountsSnapshot() map[string]int {
	stub.randomLock.Lock()
	defer stub.randomLock.Unlock()
	copied := make(map[string]int, len(stub.outcomeCounts))
	for outcome, count := range stub.outcomeCounts {
		copied[outcome] = count
	}
	return copied
}

// stubLiquidityOperator simulates the liquidity operator the same way
// stubAgentOrchestrator simulates the agent orchestrator — same seeded
// draw, same four-outcome distribution. SeedLiquidity is fire-and-trust,
// so the outcome is observable only via outcomeCountsSnapshot (and the
// stub's log line); callers get no return value either way.
type stubLiquidityOperator struct {
	log           *slog.Logger
	random        *rand.Rand
	randomLock    sync.Mutex
	outcomeCounts map[string]int
}

func newStubLiquidityOperator(log *slog.Logger, seed int64) *stubLiquidityOperator {
	if log == nil {
		log = slog.Default()
	}
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	return &stubLiquidityOperator{
		log:           log,
		random:        rand.New(rand.NewSource(seed)),
		outcomeCounts: map[string]int{},
	}
}

func (stub *stubLiquidityOperator) SeedLiquidity(ctx context.Context, marketID MarketID, specs []LiquiditySpec) {
	if contextErr := ctx.Err(); contextErr != nil {
		stub.randomLock.Lock()
		stub.outcomeCounts[stubOutcomeTimeout]++
		stub.randomLock.Unlock()
		stub.log.Warn("conductor: stub LiquidityOperator.SeedLiquidity context already cancelled",
			"market_id", marketID,
			"err", contextErr,
		)
		return
	}

	stub.randomLock.Lock()
	draw := stub.random.Intn(100)
	outcome := stubDrawToOutcome(draw)
	stub.outcomeCounts[outcome]++
	stub.randomLock.Unlock()

	switch outcome {
	case stubOutcomeSuccess:
		stub.log.Debug("conductor: stub LiquidityOperator.SeedLiquidity success",
			"market_id", marketID,
			"spec_count", len(specs),
		)
	case stubOutcomeUnavailable:
		stub.log.Warn("conductor: stub LiquidityOperator.SeedLiquidity unavailable",
			"market_id", marketID,
		)
	case stubOutcomeTimeout:
		stub.log.Warn("conductor: stub LiquidityOperator.SeedLiquidity timeout",
			"market_id", marketID,
		)
	default:
		stub.log.Warn("conductor: stub LiquidityOperator.SeedLiquidity rejected",
			"market_id", marketID,
		)
	}
}

func (stub *stubLiquidityOperator) outcomeCountsSnapshot() map[string]int {
	stub.randomLock.Lock()
	defer stub.randomLock.Unlock()
	copied := make(map[string]int, len(stub.outcomeCounts))
	for outcome, count := range stub.outcomeCounts {
		copied[outcome] = count
	}
	return copied
}

// Compile-time: the stubs satisfy their interfaces. Any port signature
// change breaks the build here.
var (
	_ ExchangeFactory   = defaultExchangeFactory{}
	_ AgentOrchestrator = (*stubAgentOrchestrator)(nil)
	_ LiquidityOperator = (*stubLiquidityOperator)(nil)
)
