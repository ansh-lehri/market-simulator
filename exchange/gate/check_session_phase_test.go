package gate

import (
	"testing"

	"simulator/contract"
)

// orderVariants is the (name, factory) pair every phase test iterates so
// each phase branch is exercised against BOTH order types. checkSessionPhase
// does not read order.Type today, but per feedback_tests_probe_dont_assume
// we don't rely on that symmetry — a future edit that starts branching on
// Type would otherwise slip past a single-type test.
func orderVariants() []struct {
	name    string
	build   func() *contract.FundedOrder
	orderID contract.OrderID
} {
	return []struct {
		name    string
		build   func() *contract.FundedOrder
		orderID contract.OrderID
	}{
		{"limit", validLimitOrder, "o-limit"},
		{"market", validMarketOrder, "o-market"},
	}
}

func TestCheckSessionPhase_ContinuousRoutesToMatch(t *testing.T) {
	for _, variant := range orderVariants() {
		t.Run(variant.name, func(t *testing.T) {
			ctx := &gateCtx{phase: &stubPhase{phase: PhaseContinuous}}

			got := checkSessionPhase(variant.build(), testBroker, ctx)

			if got.stop {
				t.Fatalf("expected cont, got stop with outcome %+v", got.outcome)
			}
			if ctx.route != routeMatch {
				t.Fatalf("route = %v, want routeMatch", ctx.route)
			}
		})
	}
}

func TestCheckSessionPhase_ClosingAuctionRoutesToAuction(t *testing.T) {
	for _, variant := range orderVariants() {
		t.Run(variant.name, func(t *testing.T) {
			ctx := &gateCtx{phase: &stubPhase{phase: PhaseClosingAuction}}

			got := checkSessionPhase(variant.build(), testBroker, ctx)

			if got.stop {
				t.Fatalf("expected cont, got stop with outcome %+v", got.outcome)
			}
			if ctx.route != routeAuction {
				t.Fatalf("route = %v, want routeAuction", ctx.route)
			}
		})
	}
}

func TestCheckSessionPhase_ClosedRejects(t *testing.T) {
	for _, variant := range orderVariants() {
		t.Run(variant.name, func(t *testing.T) {
			ctx := &gateCtx{phase: &stubPhase{phase: PhaseClosed}}

			got := checkSessionPhase(variant.build(), testBroker, ctx)

			if !got.stop {
				t.Fatalf("expected reject, got cont")
			}
			assertReject(t, got.outcome, contract.RejectMarketClosed, "session_phase", variant.orderID, testBroker)
		})
	}
}

func TestCheckSessionPhase_UnknownPhaseRejectsDefensively(t *testing.T) {
	// Any Phase value the pipeline does not handle (e.g. a future
	// PhaseOpeningAuction) must fail closed, not silently pass. Run against
	// both order types so a future Type-sensitive edit cannot slip through.
	for _, variant := range orderVariants() {
		t.Run(variant.name, func(t *testing.T) {
			ctx := &gateCtx{phase: &stubPhase{phase: Phase(99)}}

			got := checkSessionPhase(variant.build(), testBroker, ctx)

			if !got.stop {
				t.Fatalf("expected reject, got cont")
			}
			assertReject(t, got.outcome, contract.RejectMarketClosed, "session_phase", variant.orderID, testBroker)
		})
	}
}
