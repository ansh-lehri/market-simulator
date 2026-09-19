package wire

import (
	"fmt"

	"simulator/contract"
	"simulator/exchange/gate"
)

// deferredAuctionBatch is the stand-in wired into every InstrumentState until
// the real auction package (EXCHANGE_13) lands. It satisfies gate.AuctionBatch
// but panics if the gate ever routes an order to it. The gate only calls Add
// when the session clock reports PhaseClosingAuction, so keeping the schedule
// free of that phase is what keeps this dormant.
type deferredAuctionBatch struct{}

// Compile-time check: deferredAuctionBatch satisfies gate.AuctionBatch.
var _ gate.AuctionBatch = deferredAuctionBatch{}

func (deferredAuctionBatch) Add(order *contract.FundedOrder, admissionSeq contract.Seq, brokerID contract.BrokerID) {
	panic(fmt.Sprintf(
		"wire: closing auction not yet implemented; order %q (broker %q, admitSeq %d) "+
			"was routed to auction — the session schedule must not include PhaseClosingAuction "+
			"until the auction package lands",
		order.OrderID, brokerID, admissionSeq,
	))
}
