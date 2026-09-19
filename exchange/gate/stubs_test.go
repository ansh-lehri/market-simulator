package gate

import "simulator/contract"

// stubPhase implements SessionPhase with a settable current phase.
type stubPhase struct {
	phase Phase
}

func (stub *stubPhase) Current() Phase { return stub.phase }

// auctionAddCall records the arguments of a single AuctionBatch.Add call so
// routing tests can assert the exact triple that reached the auction batch.
type auctionAddCall struct {
	order        *contract.FundedOrder
	admissionSeq contract.Seq
	brokerID     contract.BrokerID
}

// stubAuction implements AuctionBatch and records every Add for inspection.
type stubAuction struct {
	calls []auctionAddCall
}

func (stub *stubAuction) Add(order *contract.FundedOrder, admissionSeq contract.Seq, brokerID contract.BrokerID) {
	stub.calls = append(stub.calls, auctionAddCall{order: order, admissionSeq: admissionSeq, brokerID: brokerID})
}

// stubBounds implements CircuitBounds with a fixed inclusive [lower, upper].
type stubBounds struct {
	lower contract.Ticks
	upper contract.Ticks
}

func (stub stubBounds) Bounds() (lower, upper contract.Ticks) {
	return stub.lower, stub.upper
}

// monotonicSeq implements match.SeqSource as a counter starting at 1.
// Same shape as match's own in-package stub, kept here so gate builds and
// tests standalone.
type monotonicSeq struct {
	counter uint64
}

func (source *monotonicSeq) Next() contract.Seq {
	source.counter++
	return contract.Seq(source.counter)
}

// assertReject fails the test if outcome is not a well-formed Rejected
// EngineResult carrying the given reason, stage, orderID, and brokerID.
func assertReject(
	t helperT,
	outcome *contract.EngineResult,
	reason contract.RejectReason,
	stageName string,
	orderID contract.OrderID,
	brokerID contract.BrokerID,
) {
	t.Helper()
	if outcome == nil {
		t.Fatalf("expected reject outcome, got nil")
	}
	if outcome.Outcome != contract.OutcomeRejected {
		t.Fatalf("outcome = %v, want OutcomeRejected", outcome.Outcome)
	}
	if outcome.OrderID != orderID {
		t.Fatalf("OrderID = %q, want %q", outcome.OrderID, orderID)
	}
	if outcome.BrokerID != brokerID {
		t.Fatalf("BrokerID = %q, want %q", outcome.BrokerID, brokerID)
	}
	if outcome.Rejection == nil {
		t.Fatalf("Rejection is nil, want non-nil with reason %v / stage %q", reason, stageName)
	}
	if outcome.Rejection.Reason != reason {
		t.Fatalf("Rejection.Reason = %v, want %v", outcome.Rejection.Reason, reason)
	}
	if outcome.Rejection.Stage != stageName {
		t.Fatalf("Rejection.Stage = %q, want %q", outcome.Rejection.Stage, stageName)
	}
}

// helperT is the subset of *testing.T the assertion helpers use. Kept small
// so the helpers can be reused by both plain tests and sub-tests without
// depending on a specific *testing.T method surface beyond t.Helper /
// t.Fatalf.
type helperT interface {
	Helper()
	Fatalf(format string, args ...any)
}
