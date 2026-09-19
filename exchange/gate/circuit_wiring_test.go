package gate

import (
	"testing"

	"simulator/contract"
	"simulator/exchange/circuit"
)

// Compile-time proof that the real *circuit.CircuitBand satisfies
// gate.CircuitBounds. If either interface signature or the concrete type
// ever drifts (rename, signature change), the build breaks here rather than
// at wiring/integration time — earliest possible signal.
var _ CircuitBounds = (*circuit.CircuitBand)(nil)

// TestCheckCircuitBand_RealBand plugs the real *circuit.CircuitBand into
// checkCircuitBand instead of stubBounds, proving runtime compatibility
// end-to-end. Unit tests keep using stubBounds for locality (pinning exact
// bounds without doing arithmetic) and independence from circuit's
// construction rules; this test's job is to catch a semantic mismatch a
// signature check alone would miss — e.g. Bounds() swapping lower/upper,
// or a divergence between the edges Bounds returns and PriceAllowed
// enforces (EXCHANGE_15 §6 invariant 1).
func TestCheckCircuitBand_RealBand(t *testing.T) {
	// reference 10000, ±10% → bounds [9000, 11000]. Verified independently
	// by circuit/band_test.go (TestNewCircuitBand_IntegerFloor).
	band := circuit.NewCircuitBand(10000, 1000)

	passCases := []struct {
		name  string
		price contract.Ticks
	}{
		{"at lower bound", 9000},
		{"at reference", 10000},
		{"at upper bound", 11000},
	}
	for _, tc := range passCases {
		t.Run("pass/"+tc.name, func(t *testing.T) {
			ctx := &gateCtx{band: band}
			order := validLimitOrder()
			order.LimitPrice = tc.price

			got := checkCircuitBand(order, testBroker, ctx)

			if got.stop {
				t.Fatalf("expected cont for price %d in [9000,11000], got stop with outcome %+v",
					tc.price, got.outcome)
			}
		})
	}

	rejectCases := []struct {
		name  string
		price contract.Ticks
	}{
		{"one below lower", 8999},
		{"one above upper", 11001},
	}
	for _, tc := range rejectCases {
		t.Run("reject/"+tc.name, func(t *testing.T) {
			ctx := &gateCtx{band: band}
			order := validLimitOrder()
			order.LimitPrice = tc.price

			got := checkCircuitBand(order, testBroker, ctx)

			if !got.stop {
				t.Fatalf("expected reject for price %d outside [9000,11000], got cont",
					tc.price)
			}
			assertReject(t, got.outcome, contract.RejectBeyondCircuit, "circuit_band", "o-limit", testBroker)
		})
	}

	// Market skip still holds with a real band — proves the Type
	// short-circuit does not accidentally consult the band and short-out
	// the wrong way when band is a concrete pointer instead of a value.
	t.Run("skip/market", func(t *testing.T) {
		ctx := &gateCtx{band: band}

		got := checkCircuitBand(validMarketOrder(), testBroker, ctx)

		if got.stop {
			t.Fatalf("expected cont (market skip), got stop with outcome %+v", got.outcome)
		}
	})
}
