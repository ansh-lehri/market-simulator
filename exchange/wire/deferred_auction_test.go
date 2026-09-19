package wire

import (
	"testing"

	"simulator/contract"
	"simulator/exchange/gate"
)

// TestDeferredAuctionBatch_SatisfiesGateInterface is a runtime companion to
// the package-level compile-time assertion — belt-and-braces so a future
// signature change to gate.AuctionBatch is caught here too.
func TestDeferredAuctionBatch_SatisfiesGateInterface(t *testing.T) {
	var batch gate.AuctionBatch = deferredAuctionBatch{}
	_ = batch
}

// TestDeferredAuctionBatch_AddPanics confirms the stand-in aborts if the gate
// ever routes an order to it — the loud failure we want if the session
// schedule accidentally includes PhaseClosingAuction before the real auction
// package lands.
func TestDeferredAuctionBatch_AddPanics(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatalf("deferredAuctionBatch.Add did not panic")
		}
	}()
	order := &contract.FundedOrder{OrderID: "o-1"}
	deferredAuctionBatch{}.Add(order, 1, "B1")
}
