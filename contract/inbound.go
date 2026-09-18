package contract

// InboundKind discriminates the two variants carried by an InboundMessage.
// See Exchange_Broker_Contract §3.1.
type InboundKind int

const (
	SubmitOrder InboundKind = iota // a new funded order to admit + match
	CancelOrder                    // pull a resting order by id
)

// InboundMessage is the single type the broker sends over the inbound channel.
// It multiplexes submits and cancels; Kind picks which pointer is non-nil.
// See Exchange_Broker_Contract §3.1.
type InboundMessage struct {
	Kind       InboundKind    // discriminator
	BrokerID   BrokerID       // who sent it; echoed on the private result
	Instrument InstrumentID   // routing key — ingress hashes this to a shard
	Submit     *FundedOrder   // non-nil iff Kind == SubmitOrder
	Cancel     *CancelRequest // non-nil iff Kind == CancelOrder
}

// Side of an order. See Exchange_Broker_Contract §3.2.
type Side int

const (
	Buy Side = iota
	Sell
)

// OrdType — the only order types the exchange sees. Stops live on the broker;
// a triggered stop reaches the exchange as an ordinary Market or Limit.
// See Exchange_Broker_Contract §3.2.
type OrdType int

const (
	Limit OrdType = iota
	Market
)

// TIF — time-in-force. GTC-limit rests; Market/IOC/FOK never rest.
// See Exchange_Broker_Contract §3.2.
type TIF int

const (
	GTC TIF = iota
	IOC
	FOK
)

// STPMode — self-match policy. Zero value is CancelIncoming (the real-exchange
// standard). See Exchange_Broker_Contract §3.2.
type STPMode int

const (
	CancelIncoming STPMode = iota // cancel the aggressor on self-cross (default)
	CancelResting                 // pull the resting own-order, continue the walk
	CancelBoth                    // cancel both own orders
)

// FundedOrder is the submit payload. The broker has already reserved funds
// before sending it; the exchange trusts it is funded and consults no account.
// MaxSpend is the pre-attached reservation cap used by the funded-cap walk on
// market buys. See Exchange_Broker_Contract §3.2.
type FundedOrder struct {
	OrderID     OrderID       // broker-assigned, opaque, echoed on all events
	Participant ParticipantID // opaque; self-match prevention only
	Instrument  InstrumentID
	Side        Side
	Type        OrdType
	TIF         TIF
	Qty         Qty     // requested quantity, integer, > 0
	LimitPrice  Ticks   // set for Limit; ignored for Market
	MaxSpend    Ticks   // market-BUY reservation cap; 0 for sells/limits
	STP         STPMode // zero value = CancelIncoming
}

// CancelRequest is the cancel payload. Whole-order pull only — the resting
// order is identified by OrderID; there is no partial-cancel primitive.
// See Exchange_Broker_Contract §3.3.
type CancelRequest struct {
	OrderID OrderID // the resting order to remove
}
