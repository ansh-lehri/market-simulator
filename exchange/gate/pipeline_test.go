package gate

import (
	"reflect"
	"testing"

	"simulator/contract"
	"simulator/exchange/book"
	"simulator/exchange/clock"
	"simulator/exchange/match"
)

// newPipelineCtx builds a fully-wired gateCtx for end-to-end RunPipeline
// tests. Returns the ctx plus handles on the stubs so a test can inspect
// side effects (auction calls, admitSeq counter, book state).
//
// Defaults are permissive: TickSize=1 (every price on-grid), band
// [0, 1_000_000] (every reasonable price in-band). Individual tests
// override these when they want to force a specific rejection.
func newPipelineCtx(
	t *testing.T,
	phase clock.Phase,
	tickSize contract.Ticks,
	bandLower, bandUpper contract.Ticks,
	orderBook *book.Book,
) (ctx *gateCtx, auction *stubAuction, admitSeq *monotonicSeq) {
	t.Helper()
	admitSeq = &monotonicSeq{}
	auction = &stubAuction{}
	matcher := match.NewMatcher(orderBook, allowAllClamp{}, &monotonicSeq{})
	ctx = &gateCtx{
		phase:   &stubPhase{phase: phase},
		rules:   Rulebook{TickSize: tickSize},
		book:    orderBook,
		band:    stubBounds{lower: bandLower, upper: bandUpper},
		seq:     admitSeq,
		matcher: matcher,
		auction: auction,
	}
	return ctx, auction, admitSeq
}

// --- Routing ---------------------------------------------------------------

func TestRunPipeline_ContinuousRoutesToMatcher(t *testing.T) {
	// A valid buy limit in PhaseContinuous survives every gate, gets a Seq,
	// and reaches the matcher — proved by the book holding a resting bid
	// afterward and the auction stub seeing zero calls.
	ctx, auction, admitSeq := newPipelineCtx(
		t, clock.PhaseContinuous, 1, 0, 1_000_000, book.NewBook("ACME"),
	)

	got := RunPipeline(validLimitOrder(), testBroker, ctx)

	if got.Outcome != contract.OutcomeMatched {
		t.Fatalf("Outcome = %v, want OutcomeMatched", got.Outcome)
	}
	if got.OrderID != "o-limit" || got.BrokerID != testBroker {
		t.Fatalf("echo mismatch: OrderID=%q BrokerID=%q", got.OrderID, got.BrokerID)
	}
	if got.Match == nil || got.Match.Rested == nil {
		t.Fatalf("expected Rested (GTC limit vs empty book), got %+v", got.Match)
	}
	if !ctx.book.Quote().HasBid {
		t.Fatal("book has no bid after continuous limit rested")
	}
	if len(auction.calls) != 0 {
		t.Fatalf("auction.Add called %d times, want 0 on continuous route", len(auction.calls))
	}
	if admitSeq.counter != 1 {
		t.Fatalf("admitSeq counter = %d, want 1", admitSeq.counter)
	}
}

func TestRunPipeline_ClosingAuctionRoutesToAuctionBatch(t *testing.T) {
	// Same order in PhaseClosingAuction diverts: auction receives one Add,
	// matcher never runs (book untouched), diverted outcome carries no Match.
	ctx, auction, admitSeq := newPipelineCtx(
		t, clock.PhaseClosingAuction, 1, 0, 1_000_000, book.NewBook("ACME"),
	)

	got := RunPipeline(validLimitOrder(), testBroker, ctx)

	if got.Outcome != contract.OutcomeDiverted {
		t.Fatalf("Outcome = %v, want OutcomeDiverted", got.Outcome)
	}
	if got.OrderID != "o-limit" || got.BrokerID != testBroker {
		t.Fatalf("echo mismatch: OrderID=%q BrokerID=%q", got.OrderID, got.BrokerID)
	}
	if got.Match != nil {
		t.Fatalf("Match non-nil on OutcomeDiverted: %+v", got.Match)
	}
	if len(auction.calls) != 1 {
		t.Fatalf("auction.Add called %d times, want 1", len(auction.calls))
	}
	if auction.calls[0].admissionSeq != contract.Seq(1) {
		t.Fatalf("auction received admissionSeq %d, want 1 (first Next())", auction.calls[0].admissionSeq)
	}
	if auction.calls[0].brokerID != testBroker {
		t.Fatalf("auction received brokerID %q, want %q", auction.calls[0].brokerID, testBroker)
	}
	if ctx.book.Quote().HasBid || ctx.book.Quote().HasAsk {
		t.Fatal("book mutated on auction route — matcher must not have run")
	}
	if admitSeq.counter != 1 {
		t.Fatalf("admitSeq counter = %d, want 1 (auction-routed orders must still get a stamped Seq)", admitSeq.counter)
	}
}

// --- Invariant: rejection spends no Seq, leaves book untouched -------------

func TestRunPipeline_RejectionConsumesNoSeqAndLeavesBookUntouched(t *testing.T) {
	// Every reject reason: after RunPipeline the admitSeq counter must be
	// zero (invariant 1) and the book must be unchanged. Auction must also
	// have seen no calls. Covers all five reject reasons.
	cases := []struct {
		name    string
		phase   clock.Phase
		build   func() *contract.FundedOrder
		reason  contract.RejectReason
		stage   string
		orderID contract.OrderID
	}{
		{
			name: "malformed", phase: clock.PhaseContinuous,
			build:  func() *contract.FundedOrder { order := validLimitOrder(); order.Qty = 0; return order },
			reason: contract.RejectMalformed, stage: "structural", orderID: "o-limit",
		},
		{
			name: "market closed", phase: clock.PhaseClosed,
			build:  validLimitOrder,
			reason: contract.RejectMarketClosed, stage: "session_phase", orderID: "o-limit",
		},
		{
			name: "no liquidity", phase: clock.PhaseContinuous,
			build:  validMarketOrder, // sell market vs empty bid
			reason: contract.RejectNoLiquidity, stage: "liquidity", orderID: "o-market",
		},
		{
			name: "bad tick", phase: clock.PhaseContinuous,
			build:  func() *contract.FundedOrder { order := validLimitOrder(); order.LimitPrice = 10021; return order },
			reason: contract.RejectBadTick, stage: "tick_grid", orderID: "o-limit",
		},
		{
			name: "beyond circuit", phase: clock.PhaseContinuous,
			// 99995 is on the size-5 grid and above the band's 10500 upper.
			build:  func() *contract.FundedOrder { order := validLimitOrder(); order.LimitPrice = 99995; return order },
			reason: contract.RejectBeyondCircuit, stage: "circuit_band", orderID: "o-limit",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// TickSize=5 so 10021 is off-grid; band [0, 10500] so 99995 is beyond.
			ctx, auction, admitSeq := newPipelineCtx(
				t, tc.phase, 5, 0, 10500, book.NewBook("ACME"),
			)

			got := RunPipeline(tc.build(), testBroker, ctx)

			if got.Outcome != contract.OutcomeRejected {
				t.Fatalf("Outcome = %v, want OutcomeRejected", got.Outcome)
			}
			if got.Rejection == nil {
				t.Fatalf("Rejection nil")
			}
			if got.Rejection.Reason != tc.reason {
				t.Fatalf("Reason = %v, want %v", got.Rejection.Reason, tc.reason)
			}
			if got.Rejection.Stage != tc.stage {
				t.Fatalf("Stage = %q, want %q", got.Rejection.Stage, tc.stage)
			}
			if got.OrderID != tc.orderID {
				t.Fatalf("OrderID = %q, want %q", got.OrderID, tc.orderID)
			}
			if got.BrokerID != testBroker {
				t.Fatalf("BrokerID = %q, want %q (must echo on rejection too)", got.BrokerID, testBroker)
			}
			if admitSeq.counter != 0 {
				t.Fatalf("admitSeq counter = %d, want 0 (rejection must not spend a Seq)", admitSeq.counter)
			}
			if len(auction.calls) != 0 {
				t.Fatalf("auction.Add called %d times, want 0 on rejection", len(auction.calls))
			}
			if ctx.book.Quote().HasBid || ctx.book.Quote().HasAsk {
				t.Fatal("book mutated on rejection")
			}
		})
	}
}

// --- Invariant: auction orders skip ONLY liquidity -------------------------

func TestRunPipeline_AuctionRouteStillEnforcesTickAndBand(t *testing.T) {
	// EXCHANGE_14 §8 invariant 5: auction-routed orders run every gate
	// except liquidity. Prove tick and band still reject even in closing
	// auction. (Liquidity skip is already proven by the routing test above
	// — a sell limit against an empty book in PhaseClosingAuction diverts
	// cleanly.)
	cases := []struct {
		name   string
		build  func() *contract.FundedOrder
		reason contract.RejectReason
		stage  string
	}{
		{
			name: "off-tick in auction still rejects",
			build: func() *contract.FundedOrder {
				order := validLimitOrder()
				order.LimitPrice = 10021 // off a size-5 grid
				return order
			},
			reason: contract.RejectBadTick, stage: "tick_grid",
		},
		{
			name: "beyond-band in auction still rejects",
			build: func() *contract.FundedOrder {
				order := validLimitOrder()
				order.LimitPrice = 99995 // on grid (divisible by 5), beyond band (> 10500)
				return order
			},
			reason: contract.RejectBeyondCircuit, stage: "circuit_band",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, auction, _ := newPipelineCtx(
				t, clock.PhaseClosingAuction, 5, 0, 10500, book.NewBook("ACME"),
			)

			got := RunPipeline(tc.build(), testBroker, ctx)

			if got.Outcome != contract.OutcomeRejected {
				t.Fatalf("Outcome = %v, want OutcomeRejected (auction route must still enforce %s)", got.Outcome, tc.stage)
			}
			if got.Rejection.Reason != tc.reason {
				t.Fatalf("Reason = %v, want %v", got.Rejection.Reason, tc.reason)
			}
			if len(auction.calls) != 0 {
				t.Fatalf("auction.Add called %d times on rejection, want 0", len(auction.calls))
			}
		})
	}
}

func TestRunPipeline_AuctionRouteSkipsLiquidityCheck(t *testing.T) {
	// A market order in PhaseClosingAuction against an empty book must NOT
	// hit RejectNoLiquidity — auction skips liquidity by design. It also
	// must not be blocked by any other stage: passes structural, no tick
	// (market), no band (market), gets a Seq, diverts.
	ctx, auction, _ := newPipelineCtx(
		t, clock.PhaseClosingAuction, 1, 0, 1_000_000, book.NewBook("ACME"),
	)

	got := RunPipeline(validMarketOrder(), testBroker, ctx)

	if got.Outcome != contract.OutcomeDiverted {
		t.Fatalf("Outcome = %v, want OutcomeDiverted (auction must skip liquidity)", got.Outcome)
	}
	if len(auction.calls) != 1 {
		t.Fatalf("auction.Add called %d times, want 1", len(auction.calls))
	}
}

// --- Determinism -----------------------------------------------------------

func TestRunPipeline_DeterministicAcrossRuns(t *testing.T) {
	// Running the same input through two fresh contexts must yield the same
	// EngineResult structure. Stateful bits reset with fresh stubs; the
	// pipeline itself introduces no nondeterminism. Uses reflect.DeepEqual
	// because MatchResult / RestingOrder contain pointer fields.
	run := func() contract.EngineResult {
		ctx, _, _ := newPipelineCtx(
			t, clock.PhaseContinuous, 1, 0, 1_000_000, book.NewBook("ACME"),
		)
		return RunPipeline(validLimitOrder(), testBroker, ctx)
	}

	first := run()
	second := run()

	if !reflect.DeepEqual(first, second) {
		t.Fatalf("nondeterministic result\n  first:  %+v\n  second: %+v", first, second)
	}
}
