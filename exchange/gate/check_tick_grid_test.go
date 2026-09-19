package gate

import (
	"testing"

	"simulator/contract"
)

func TestCheckTickGrid_MarketSkipped(t *testing.T) {
	// A market order carries no LimitPrice, so tick-grid does not apply
	// regardless of TickSize. Kept explicit so a future refactor cannot
	// silently start modulus-checking zero and false-passing/false-failing.
	ctx := &gateCtx{rules: Rulebook{TickSize: 5}}

	got := checkTickGrid(validMarketOrder(), testBroker, ctx)

	if got.stop {
		t.Fatalf("expected cont, got stop with outcome %+v", got.outcome)
	}
}

func TestCheckTickGrid_LimitOnGrid(t *testing.T) {
	ctx := &gateCtx{rules: Rulebook{TickSize: 5}}
	order := validLimitOrder()
	order.LimitPrice = 10020 // 10020 % 5 == 0

	got := checkTickGrid(order, testBroker, ctx)

	if got.stop {
		t.Fatalf("expected cont, got stop with outcome %+v", got.outcome)
	}
}

func TestCheckTickGrid_LimitOffGridRejects(t *testing.T) {
	cases := []struct {
		name     string
		tickSize contract.Ticks
		price    contract.Ticks
	}{
		{"1 off nickel grid", 5, 10021},
		{"3 off nickel grid", 5, 10023},
		{"any non-zero on 1-tick grid never rejects, so use a bigger grid", 10, 10025},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &gateCtx{rules: Rulebook{TickSize: tc.tickSize}}
			order := validLimitOrder()
			order.LimitPrice = tc.price

			got := checkTickGrid(order, testBroker, ctx)

			if !got.stop {
				t.Fatalf("expected reject for price %d on grid %d, got cont", tc.price, tc.tickSize)
			}
			assertReject(t, got.outcome, contract.RejectBadTick, "tick_grid", "o-limit", testBroker)
		})
	}
}
