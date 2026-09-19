package gate

import (
	"testing"

	"simulator/contract"
)

func TestStampSequence_WritesToContextAndAdvances(t *testing.T) {
	// stampSequence has no Type short-circuit — every variant iterates per
	// feedback_stage_tests_cross_cover_order_types.
	for _, variant := range orderVariants() {
		t.Run(variant.name, func(t *testing.T) {
			ctx := &gateCtx{seq: &monotonicSeq{}}
			order := variant.build()

			got := stampSequence(order, testBroker, ctx)

			if got.stop {
				t.Fatalf("expected cont, got stop with outcome %+v", got.outcome)
			}
			if ctx.admissionSeq != contract.Seq(1) {
				t.Fatalf("ctx.admissionSeq = %d, want 1 (first Next() from a fresh counter)", ctx.admissionSeq)
			}
		})
	}
}

func TestStampSequence_LeavesFundedOrderUntouched(t *testing.T) {
	// Guards against a future edit that reintroduces `order.Seq = ...` if
	// contract.FundedOrder ever grew a Seq field: compare every field before
	// and after. Any inadvertent mutation of the seam type fails here.
	for _, variant := range orderVariants() {
		t.Run(variant.name, func(t *testing.T) {
			ctx := &gateCtx{seq: &monotonicSeq{}}
			order := variant.build()
			before := *order

			stampSequence(order, testBroker, ctx)

			if *order != before {
				t.Fatalf("stampSequence mutated the FundedOrder\n  before: %+v\n  after:  %+v", before, *order)
			}
		})
	}
}

func TestStampSequence_ConsecutiveCallsAdvance(t *testing.T) {
	// Second call must yield a strictly-greater seq than the first — the
	// property that makes stampSequence a valid time-priority source.
	ctx := &gateCtx{seq: &monotonicSeq{}}

	stampSequence(validLimitOrder(), testBroker, ctx)
	firstSeq := ctx.admissionSeq
	stampSequence(validLimitOrder(), testBroker, ctx)
	secondSeq := ctx.admissionSeq

	if secondSeq <= firstSeq {
		t.Fatalf("expected strict advance: firstSeq=%d, secondSeq=%d", firstSeq, secondSeq)
	}
}
