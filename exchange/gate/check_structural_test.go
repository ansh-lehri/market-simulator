package gate

import (
	"testing"

	"simulator/contract"
)

// validLimitOrder / validMarketOrder are minimal well-formed orders; every
// structural case starts from one and mutates a single field.
func validLimitOrder() *contract.FundedOrder {
	return &contract.FundedOrder{
		OrderID:    "o-limit",
		Instrument: "ACME",
		Side:       contract.Buy,
		Type:       contract.Limit,
		TIF:        contract.GTC,
		Qty:        10,
		LimitPrice: 10020,
	}
}

func validMarketOrder() *contract.FundedOrder {
	return &contract.FundedOrder{
		OrderID:    "o-market",
		Instrument: "ACME",
		Side:       contract.Sell,
		Type:       contract.Market,
		TIF:        contract.IOC,
		Qty:        10,
	}
}

const testBroker contract.BrokerID = "B1"

func TestCheckStructural_Pass(t *testing.T) {
	cases := []struct {
		name  string
		order *contract.FundedOrder
	}{
		{"valid limit buy", validLimitOrder()},
		{"valid market sell", validMarketOrder()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := checkStructural(tc.order, testBroker, &gateCtx{})
			if got.stop {
				t.Fatalf("expected cont, got stop with outcome %+v", got.outcome)
			}
		})
	}
}

func TestCheckStructural_Reject(t *testing.T) {
	// Type-agnostic dimensions run against every order variant, per
	// feedback_stage_tests_cross_cover_order_types: checkStructural doesn't
	// short-circuit on Type for these, so both templates must reject.
	typeAgnosticCases := []struct {
		name   string
		mutate func(*contract.FundedOrder)
	}{
		{"zero qty", func(order *contract.FundedOrder) { order.Qty = 0 }},
		{"negative qty", func(order *contract.FundedOrder) { order.Qty = -5 }},
		{"unknown side", func(order *contract.FundedOrder) { order.Side = contract.Side(99) }},
	}
	for _, tc := range typeAgnosticCases {
		for _, variant := range orderVariants() {
			t.Run(tc.name+"/"+variant.name, func(t *testing.T) {
				order := variant.build()
				tc.mutate(order)
				got := checkStructural(order, testBroker, &gateCtx{})
				if !got.stop {
					t.Fatalf("expected reject, got cont")
				}
				assertReject(t, got.outcome, contract.RejectMalformed, "structural", variant.orderID, testBroker)
			})
		}
	}

	// Type-specific dimensions: the mutation is only meaningful for one
	// Type, so no cross-coverage needed. `unknown type` is orthogonal but
	// worth exercising from both starting templates to prove neither
	// starting LimitPrice value (10020 from limit, 0 from market) affects
	// the reject.
	typeSpecificCases := []struct {
		name    string
		build   func() *contract.FundedOrder
		mutate  func(*contract.FundedOrder)
		orderID contract.OrderID
	}{
		{"unknown type from limit template", validLimitOrder, func(order *contract.FundedOrder) { order.Type = contract.OrdType(99) }, "o-limit"},
		{"unknown type from market template", validMarketOrder, func(order *contract.FundedOrder) { order.Type = contract.OrdType(99) }, "o-market"},
		{"limit missing price", validLimitOrder, func(order *contract.FundedOrder) { order.LimitPrice = 0 }, "o-limit"},
		{"limit negative price", validLimitOrder, func(order *contract.FundedOrder) { order.LimitPrice = -1 }, "o-limit"},
	}
	for _, tc := range typeSpecificCases {
		t.Run(tc.name, func(t *testing.T) {
			order := tc.build()
			tc.mutate(order)
			got := checkStructural(order, testBroker, &gateCtx{})
			if !got.stop {
				t.Fatalf("expected reject, got cont")
			}
			assertReject(t, got.outcome, contract.RejectMalformed, "structural", tc.orderID, testBroker)
		})
	}
}

func TestCheckStructural_MarketWithPriceRejects(t *testing.T) {
	order := validMarketOrder()
	order.LimitPrice = 10020

	got := checkStructural(order, testBroker, &gateCtx{})

	if !got.stop {
		t.Fatalf("expected reject, got cont")
	}
	assertReject(t, got.outcome, contract.RejectMalformed, "structural", "o-market", testBroker)
}
