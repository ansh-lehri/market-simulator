package gate

import (
	"testing"

	"simulator/contract"
	"simulator/exchange/book"
)

// newBookWith seeds a fresh book with an optional resting bid and/or ask so
// tests can control HasBid / HasAsk precisely.
func newBookWith(t *testing.T, hasBid, hasAsk bool) *book.Book {
	t.Helper()
	orderBook := book.NewBook("ACME")
	if hasBid {
		orderBook.Insert(&contract.RestingOrder{
			OrderID: "m-bid", Side: contract.Buy, Price: 10019, Remaining: 50, Seq: 1,
		})
	}
	if hasAsk {
		orderBook.Insert(&contract.RestingOrder{
			OrderID: "m-ask", Side: contract.Sell, Price: 10023, Remaining: 40, Seq: 2,
		})
	}
	return orderBook
}

func TestCheckLiquidity_AuctionRouteSkipped(t *testing.T) {
	// Route-based short-circuit: regardless of order type/side or book
	// state, auction-routed orders bypass this stage. Both order types
	// covered per feedback_stage_tests_cross_cover_order_types.
	for _, variant := range orderVariants() {
		t.Run(variant.name, func(t *testing.T) {
			ctx := &gateCtx{
				route: routeAuction,
				book:  newBookWith(t, false, false), // deliberately empty; skip must not consult it
			}

			got := checkLiquidity(variant.build(), testBroker, ctx)

			if got.stop {
				t.Fatalf("expected cont (auction skip), got stop with outcome %+v", got.outcome)
			}
		})
	}
}

func TestCheckLiquidity_ContinuousLimitSkipped(t *testing.T) {
	// Type-based short-circuit: a limit against an empty book rests, so no
	// admission check. Both sides covered — the stage doesn't inspect
	// Side for limits, but we don't rely on that symmetry
	// (feedback_stage_tests_cross_cover_order_types).
	cases := []struct {
		name string
		side contract.Side
	}{
		{"buy limit", contract.Buy},
		{"sell limit", contract.Sell},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &gateCtx{
				route: routeMatch,
				book:  newBookWith(t, false, false),
			}
			order := validLimitOrder()
			order.Side = tc.side

			got := checkLiquidity(order, testBroker, ctx)

			if got.stop {
				t.Fatalf("expected cont (limit skip), got stop with outcome %+v", got.outcome)
			}
		})
	}
}

func TestCheckLiquidity_ContinuousMarketRejectsOnEmptyOpposite(t *testing.T) {
	// Market must find liquidity on the opposite side. Buy needs an ask;
	// sell needs a bid. Test both sides against a book that has ONLY the
	// same-side resting order — the opposite side is empty.
	cases := []struct {
		name    string
		side    contract.Side
		hasBid  bool
		hasAsk  bool
		orderID contract.OrderID
	}{
		{"buy vs empty ask (bid-only book)", contract.Buy, true, false, "o-market"},
		{"buy vs empty ask (fully empty book)", contract.Buy, false, false, "o-market"},
		{"sell vs empty bid (ask-only book)", contract.Sell, false, true, "o-market"},
		{"sell vs empty bid (fully empty book)", contract.Sell, false, false, "o-market"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &gateCtx{
				route: routeMatch,
				book:  newBookWith(t, tc.hasBid, tc.hasAsk),
			}
			order := validMarketOrder()
			order.Side = tc.side

			got := checkLiquidity(order, testBroker, ctx)

			if !got.stop {
				t.Fatalf("expected reject, got cont")
			}
			assertReject(t, got.outcome, contract.RejectNoLiquidity, "liquidity", tc.orderID, testBroker)
		})
	}
}

func TestCheckLiquidity_ContinuousMarketPassesWhenOppositePresent(t *testing.T) {
	// Both sides against a book that has the required opposite side. Prove
	// same-side presence is irrelevant (both variants of each side tested).
	cases := []struct {
		name   string
		side   contract.Side
		hasBid bool
		hasAsk bool
	}{
		{"buy with ask only", contract.Buy, false, true},
		{"buy with both sides", contract.Buy, true, true},
		{"sell with bid only", contract.Sell, true, false},
		{"sell with both sides", contract.Sell, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &gateCtx{
				route: routeMatch,
				book:  newBookWith(t, tc.hasBid, tc.hasAsk),
			}
			order := validMarketOrder()
			order.Side = tc.side

			got := checkLiquidity(order, testBroker, ctx)

			if got.stop {
				t.Fatalf("expected cont, got stop with outcome %+v", got.outcome)
			}
		})
	}
}
