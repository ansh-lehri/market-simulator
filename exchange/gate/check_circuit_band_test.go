package gate

import (
	"testing"

	"simulator/contract"
)

func TestCheckCircuitBand_MarketSkipped(t *testing.T) {
	// Market has no price to admit-reject on; the walk-time clamp handles
	// it. Explicit case so a future refactor cannot silently start reading
	// LimitPrice (== 0) and false-rejecting every market order when the
	// band lower is > 0.
	ctx := &gateCtx{band: stubBounds{lower: 9500, upper: 10500}}

	got := checkCircuitBand(validMarketOrder(), testBroker, ctx)

	if got.stop {
		t.Fatalf("expected cont, got stop with outcome %+v", got.outcome)
	}
}

func TestCheckCircuitBand_LimitInsideBand(t *testing.T) {
	// Bounds are inclusive on both edges (EXCHANGE_15 §2) — the "at edge"
	// cases prove that explicitly, since off-by-one on inclusivity is the
	// classic mistake.
	cases := []struct {
		name  string
		price contract.Ticks
	}{
		{"at lower bound", 9500},
		{"one above lower", 9501},
		{"middle", 10000},
		{"one below upper", 10499},
		{"at upper bound", 10500},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &gateCtx{band: stubBounds{lower: 9500, upper: 10500}}
			order := validLimitOrder()
			order.LimitPrice = tc.price

			got := checkCircuitBand(order, testBroker, ctx)

			if got.stop {
				t.Fatalf("expected cont for price %d in [9500,10500], got stop with outcome %+v",
					tc.price, got.outcome)
			}
		})
	}
}

func TestCheckCircuitBand_LimitOutsideBandRejects(t *testing.T) {
	cases := []struct {
		name  string
		price contract.Ticks
	}{
		{"one below lower", 9499},
		{"far below lower", 100},
		{"one above upper", 10501},
		{"far above upper", 99999},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &gateCtx{band: stubBounds{lower: 9500, upper: 10500}}
			order := validLimitOrder()
			order.LimitPrice = tc.price

			got := checkCircuitBand(order, testBroker, ctx)

			if !got.stop {
				t.Fatalf("expected reject for price %d outside [9500,10500], got cont",
					tc.price)
			}
			assertReject(t, got.outcome, contract.RejectBeyondCircuit, "circuit_band", "o-limit", testBroker)
		})
	}
}
