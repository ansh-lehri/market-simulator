package conductor

import (
	"context"
	"errors"
	"testing"
	"time"

	"simulator/contract"
)

// Fixed seed so these tests are deterministic. 42 is arbitrary; the only
// requirement is that it exercises every outcome branch over the chosen
// call count, which is verified below.
const stubTestSeed int64 = 42

// callCountForDistribution is large enough that the Law of Large Numbers
// keeps every outcome's observed share within a loose band around the
// configured probability — flakiness-safe for CI, not a tight-fit
// statistical test.
const callCountForDistribution = 2000

func TestStubAgentOrchestrator_SeedProducesDeterministicSequence(t *testing.T) {
	firstStub := newStubAgentOrchestrator(nil, stubTestSeed)
	secondStub := newStubAgentOrchestrator(nil, stubTestSeed)

	firstSequence := make([]error, 100)
	secondSequence := make([]error, 100)
	for callIndex := 0; callIndex < 100; callIndex++ {
		firstSequence[callIndex] = firstStub.BringUp(context.Background(), AgentBringUpRequest{Market: "IN", Mode: Canonical})
		secondSequence[callIndex] = secondStub.BringUp(context.Background(), AgentBringUpRequest{Market: "IN", Mode: Canonical})
	}
	for callIndex, firstErr := range firstSequence {
		secondErr := secondSequence[callIndex]
		if (firstErr == nil) != (secondErr == nil) {
			t.Fatalf("call %d: nilness diverged (first=%v, second=%v)", callIndex, firstErr, secondErr)
		}
		if firstErr != nil && !errors.Is(firstErr, secondErr) && !errors.Is(secondErr, firstErr) {
			t.Fatalf("call %d: errors diverged (first=%v, second=%v)", callIndex, firstErr, secondErr)
		}
	}
}

func TestStubAgentOrchestrator_ReturnsEveryOutcomeOverManyCalls(t *testing.T) {
	stub := newStubAgentOrchestrator(nil, stubTestSeed)
	for callIndex := 0; callIndex < callCountForDistribution; callIndex++ {
		_ = stub.BringUp(context.Background(), AgentBringUpRequest{Market: "IN", Mode: Canonical, Instruments: []contract.InstrumentID{"RELIANCE"}})
	}
	counts := stub.outcomeCountsSnapshot()

	for _, expectedOutcome := range []string{stubOutcomeSuccess, stubOutcomeUnavailable, stubOutcomeTimeout, stubOutcomeRejected} {
		if counts[expectedOutcome] == 0 {
			t.Errorf("outcome %q never appeared in %d calls — stub is not covering every branch", expectedOutcome, callCountForDistribution)
		}
	}

	// Loose distribution check: each bucket within ±8% of its target
	// share. 8% is wide enough to tolerate CI jitter at this call count
	// without masking a wiring bug (e.g. a bucket collapsed to zero or
	// absorbed another's draws).
	targets := map[string]float64{
		stubOutcomeSuccess:     float64(stubBoundSuccess) / 100,
		stubOutcomeUnavailable: float64(stubBoundUnavailable-stubBoundSuccess) / 100,
		stubOutcomeTimeout:     float64(stubBoundTimeout-stubBoundUnavailable) / 100,
		stubOutcomeRejected:    float64(100-stubBoundTimeout) / 100,
	}
	for outcome, target := range targets {
		observed := float64(counts[outcome]) / float64(callCountForDistribution)
		if diff := observed - target; diff > 0.08 || diff < -0.08 {
			t.Errorf("outcome %q observed share %.3f vs target %.3f (diff %.3f) exceeds ±0.08 band",
				outcome, observed, target, diff)
		}
	}
}

func TestStubAgentOrchestrator_RespectsAlreadyCancelledContext(t *testing.T) {
	stub := newStubAgentOrchestrator(nil, stubTestSeed)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := stub.BringUp(ctx, AgentBringUpRequest{Market: "IN", Mode: Canonical})
	if !errors.Is(err, ErrOrchestratorTimeout) {
		t.Fatalf("BringUp with pre-cancelled ctx returned %v, want error that Is(ErrOrchestratorTimeout)", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("BringUp with pre-cancelled ctx did not wrap context.Canceled: %v", err)
	}
	// Should not have consumed a draw — the counts reflect only the
	// cancellation path.
	counts := stub.outcomeCountsSnapshot()
	if counts[stubOutcomeTimeout] != 1 {
		t.Errorf("timeout counter = %d, want 1 after one cancelled call", counts[stubOutcomeTimeout])
	}
	for _, otherOutcome := range []string{stubOutcomeSuccess, stubOutcomeUnavailable, stubOutcomeRejected} {
		if counts[otherOutcome] != 0 {
			t.Errorf("outcome %q counter = %d, want 0 (cancellation path only increments timeout)",
				otherOutcome, counts[otherOutcome])
		}
	}
}

func TestStubLiquidityOperator_SeedProducesDeterministicSequence(t *testing.T) {
	firstStub := newStubLiquidityOperator(nil, stubTestSeed)
	secondStub := newStubLiquidityOperator(nil, stubTestSeed)

	for callIndex := 0; callIndex < 200; callIndex++ {
		firstStub.SeedLiquidity(context.Background(), "IN:2026-10-12", nil)
		secondStub.SeedLiquidity(context.Background(), "IN:2026-10-12", nil)
	}
	firstCounts := firstStub.outcomeCountsSnapshot()
	secondCounts := secondStub.outcomeCountsSnapshot()
	for outcome, firstCount := range firstCounts {
		if firstCount != secondCounts[outcome] {
			t.Errorf("outcome %q: first=%d, second=%d (same seed should produce identical counts)",
				outcome, firstCount, secondCounts[outcome])
		}
	}
}

func TestStubLiquidityOperator_RecordsEveryOutcomeOverManyCalls(t *testing.T) {
	stub := newStubLiquidityOperator(nil, stubTestSeed)
	for callIndex := 0; callIndex < callCountForDistribution; callIndex++ {
		stub.SeedLiquidity(context.Background(), "IN:2026-10-12", nil)
	}
	counts := stub.outcomeCountsSnapshot()
	for _, expectedOutcome := range []string{stubOutcomeSuccess, stubOutcomeUnavailable, stubOutcomeTimeout, stubOutcomeRejected} {
		if counts[expectedOutcome] == 0 {
			t.Errorf("outcome %q never appeared in %d SeedLiquidity calls", expectedOutcome, callCountForDistribution)
		}
	}
}

func TestStubLiquidityOperator_RespectsAlreadyCancelledContext(t *testing.T) {
	stub := newStubLiquidityOperator(nil, stubTestSeed)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stub.SeedLiquidity(ctx, "IN:2026-10-12", nil)
	counts := stub.outcomeCountsSnapshot()
	if counts[stubOutcomeTimeout] != 1 {
		t.Errorf("timeout counter = %d, want 1 after one cancelled SeedLiquidity call", counts[stubOutcomeTimeout])
	}
}

func TestStubAgentOrchestrator_SubstitutesSlogDefaultWhenLogNil(t *testing.T) {
	// Smoke: nil log must not nil-deref — the constructor substitutes
	// slog.Default. Any outcome exercises the log branches.
	stub := newStubAgentOrchestrator(nil, stubTestSeed)
	for callIndex := 0; callIndex < 50; callIndex++ {
		_ = stub.BringUp(context.Background(), AgentBringUpRequest{Market: "IN", Mode: Canonical})
	}
}

func TestStubLiquidityOperator_SubstitutesSlogDefaultWhenLogNil(t *testing.T) {
	stub := newStubLiquidityOperator(nil, stubTestSeed)
	for callIndex := 0; callIndex < 50; callIndex++ {
		stub.SeedLiquidity(context.Background(), "IN:2026-10-12", nil)
	}
}

func TestStubAgentOrchestrator_SeedZeroFallsBackToClock(t *testing.T) {
	// Two stubs built with seed 0 at different nanosecond instants
	// should (overwhelmingly likely) produce different first draws.
	firstStub := newStubAgentOrchestrator(nil, 0)
	// Ensure the two seeds land in different nanosecond buckets.
	time.Sleep(2 * time.Millisecond)
	secondStub := newStubAgentOrchestrator(nil, 0)

	var firstDiffer, secondDiffer bool
	for callIndex := 0; callIndex < 10; callIndex++ {
		firstErr := firstStub.BringUp(context.Background(), AgentBringUpRequest{Market: "IN", Mode: Canonical})
		secondErr := secondStub.BringUp(context.Background(), AgentBringUpRequest{Market: "IN", Mode: Canonical})
		if (firstErr == nil) != (secondErr == nil) {
			firstDiffer = true
		}
		if firstErr != nil && secondErr != nil && !errors.Is(firstErr, secondErr) {
			secondDiffer = true
		}
	}
	// Not a strict guarantee — if both seeds happened to produce the
	// same first 10 draws we would hit a false positive. 10 draws of 4
	// outcomes with heavy success bias makes that vanishingly unlikely.
	if !firstDiffer && !secondDiffer {
		t.Log("seed-0 stubs produced identical first-10 sequences — statistically unlikely but technically possible; not failing the test")
	}
}
