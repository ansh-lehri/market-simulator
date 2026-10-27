package contract

// CancelReason explains why a quantity did not fill. Attached to the cancelled
// remainder in a MatchResult. See Exchange_Broker_Contract §4.2.
type CancelReason int

const (
	ReasonNone            CancelReason = iota
	ReasonNoLiquidity                  // opposite book emptied mid-walk
	ReasonFundedCap                    // market buy hit its MaxSpend envelope
	ReasonCircuitClamp                 // walk reached the circuit band edge
	ReasonIOCLeftover                  // IOC order's unfilled remainder
	ReasonSelfMatch                    // STP cancelled the incoming (CancelIncoming/CancelBoth)
	ReasonAuctionUnfilled              // auction order that did not fill at the clearing price
)

// Trade is one fill print. Price is always the maker's resting price —
// price-improvement accrues to the taker; the resting order gets exactly what
// it posted. See Exchange_Broker_Contract §4.2.
type Trade struct {
	Seq              Seq
	Instrument       InstrumentID
	Price            Ticks // the MAKER's resting price — always
	Qty              Qty
	TakerOrder       OrderID
	MakerOrder       OrderID
	TakerSide        Side // maker side is the opposite
	TakerParticipant ParticipantID
	MakerParticipant ParticipantID
	MakerBrokerID    BrokerID // routes OutcomeMakerFill back to the maker's broker
}

// MatchResult is the matcher's whole output for one incoming order. One
// incoming order produces exactly one MatchResult with every print batched in
// Trades (the one-shot guarantee). Later maker fills of a rested remainder
// arrive as separate EngineResult events with Outcome == OutcomeMakerFill.
// See Exchange_Broker_Contract §4.2 and §7.
type MatchResult struct {
	Trades         []Trade       // every print, in walk order (may be empty)
	Rested         *RestingOrder // non-nil iff a GTC-limit remainder was placed in the book
	CancelledQty   Qty           // remainder that did not fill and did not rest (0 if none)
	CancelReason   CancelReason  // why CancelledQty was cancelled
	LastPrice      Ticks         // last-price after this order (valid iff any Trades)
	LastPriceMoved bool          // true iff at least one trade printed
}

// RestingOrder is the record for a GTC-limit remainder now working in the
// book. BrokerID is retained so a later maker fill can be routed back to the
// broker that placed it — see Exchange_Broker_Contract §8.2 (the guarantee
// the exchange will honour for maker-fill delivery).
type RestingOrder struct {
	OrderID     OrderID
	Participant ParticipantID
	BrokerID    BrokerID // stamped at submission; routes later maker fills
	Side        Side
	Price       Ticks // the resting (maker) price
	Remaining   Qty   // decremented as it fills; the order is removed at 0
	Seq         Seq   // time priority, fixed for the order's life
}
