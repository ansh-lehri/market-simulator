package gate

import (
	"testing"

	"simulator/contract"
	"simulator/exchange/book"
	"simulator/exchange/match"
)

// newRouteCtx builds a gateCtx wired for routeOrder tests: real book, real
// matcher (with allow-all clamp so the matcher's own circuit check never
// interferes), stub auction, and pre-set route + admissionSeq that
// routeOrder is meant to consume.
func newRouteCtx(t *testing.T, route routeTarget, admissionSeq contract.Seq) (*gateCtx, *book.Book, *stubAuction) {
	t.Helper()
	orderBook := book.NewBook("ACME")
	auction := &stubAuction{}
	matcher := match.NewMatcher(orderBook, allowAllClamp{}, &monotonicSeq{})
	ctx := &gateCtx{
		book:         orderBook,
		matcher:      matcher,
		auction:      auction,
		route:        route,
		admissionSeq: admissionSeq,
	}
	return ctx, orderBook, auction
}

func TestRouteOrder_MatchProducesMatchedEngineResult(t *testing.T) {
	// Iterate both order types per feedback_stage_tests_cross_cover_order_types.
	// The MatchResult contents differ (limit rests, market cancels on empty
	// book) — that's the matcher's concern; here we only assert the
	// EngineResult wrapper.
	for _, variant := range orderVariants() {
		t.Run(variant.name, func(t *testing.T) {
			ctx, _, auction := newRouteCtx(t, routeMatch, contract.Seq(42))

			got := routeOrder(variant.build(), testBroker, ctx)

			if !got.stop {
				t.Fatalf("expected stop (routeOrder is terminal), got cont")
			}
			if got.outcome == nil {
				t.Fatalf("outcome is nil")
			}
			if got.outcome.Outcome != contract.OutcomeMatched {
				t.Fatalf("Outcome = %v, want OutcomeMatched", got.outcome.Outcome)
			}
			if got.outcome.OrderID != variant.orderID {
				t.Fatalf("OrderID = %q, want %q", got.outcome.OrderID, variant.orderID)
			}
			if got.outcome.BrokerID != testBroker {
				t.Fatalf("BrokerID = %q, want %q", got.outcome.BrokerID, testBroker)
			}
			if got.outcome.Match == nil {
				t.Fatalf("Match is nil on OutcomeMatched")
			}
			if got.outcome.Rejection != nil {
				t.Fatalf("Rejection non-nil on OutcomeMatched: %+v", got.outcome.Rejection)
			}
			if len(auction.calls) != 0 {
				t.Fatalf("auction.Add called %d times on routeMatch, want 0", len(auction.calls))
			}
		})
	}
}

func TestRouteOrder_MatchPropagatesBrokerIDAndAdmissionSeq(t *testing.T) {
	// A GTC-limit against an empty book rests, and the resulting
	// RestingOrder preserves both BrokerID and Seq — that's how we prove
	// routeOrder actually passed those two values into matcher.Match
	// (limit-only by construction: only limits produce a Rested).
	const expectedSeq contract.Seq = 42
	ctx, orderBook, _ := newRouteCtx(t, routeMatch, expectedSeq)
	order := validLimitOrder() // GTC buy limit at 10020

	got := routeOrder(order, testBroker, ctx)

	if got.outcome.Match.Rested == nil {
		t.Fatalf("expected Rested (GTC limit vs empty book), got nil")
	}
	if got.outcome.Match.Rested.Seq != expectedSeq {
		t.Fatalf("Rested.Seq = %d, want %d (admissionSeq passed to matcher)",
			got.outcome.Match.Rested.Seq, expectedSeq)
	}
	if got.outcome.Match.Rested.BrokerID != testBroker {
		t.Fatalf("Rested.BrokerID = %q, want %q (brokerID passed to matcher)",
			got.outcome.Match.Rested.BrokerID, testBroker)
	}
	// Independent sanity: book now holds a resting bid.
	quote := orderBook.Quote()
	if !quote.HasBid {
		t.Fatalf("book has no bid after routeOrder rested a limit")
	}
}

func TestRouteOrder_AuctionInvokesAddAndProducesDivertedEngineResult(t *testing.T) {
	// Iterate both order types — the auction accepts both.
	for _, variant := range orderVariants() {
		t.Run(variant.name, func(t *testing.T) {
			const expectedSeq contract.Seq = 42
			ctx, _, auction := newRouteCtx(t, routeAuction, expectedSeq)
			order := variant.build()

			got := routeOrder(order, testBroker, ctx)

			if !got.stop {
				t.Fatalf("expected stop, got cont")
			}
			if got.outcome.Outcome != contract.OutcomeDiverted {
				t.Fatalf("Outcome = %v, want OutcomeDiverted", got.outcome.Outcome)
			}
			if got.outcome.OrderID != variant.orderID {
				t.Fatalf("OrderID = %q, want %q", got.outcome.OrderID, variant.orderID)
			}
			if got.outcome.BrokerID != testBroker {
				t.Fatalf("BrokerID = %q, want %q", got.outcome.BrokerID, testBroker)
			}
			if got.outcome.Match != nil {
				t.Fatalf("Match non-nil on OutcomeDiverted: %+v", got.outcome.Match)
			}

			// Auction received exactly one Add with all three arguments.
			if len(auction.calls) != 1 {
				t.Fatalf("auction.Add called %d times, want 1", len(auction.calls))
			}
			call := auction.calls[0]
			if call.order != order {
				t.Fatalf("auction received wrong order pointer: got %p, want %p", call.order, order)
			}
			if call.admissionSeq != expectedSeq {
				t.Fatalf("auction admissionSeq = %d, want %d", call.admissionSeq, expectedSeq)
			}
			if call.brokerID != testBroker {
				t.Fatalf("auction brokerID = %q, want %q", call.brokerID, testBroker)
			}
		})
	}
}

func TestRouteOrder_UnsetRoutePanics(t *testing.T) {
	// Invariant violation: routeOrder must never run without ctx.route set.
	// checkSessionPhase is the only writer; skipping it (or a future bug
	// that leaves route unset) surfaces here rather than silently defaulting.
	ctx, _, _ := newRouteCtx(t, routeMatch, 1)
	ctx.route = routeTarget(99) // simulate an unset / unknown route

	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic on unset route")
		}
	}()
	routeOrder(validLimitOrder(), testBroker, ctx)
}
