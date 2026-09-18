package contract

// Value-type aliases used across the seam. See Exchange_Broker_Contract §2.
//
// Money and quantities are integers. A price is an integer count of ticks;
// rupee/dollar values are only reconstructed for display (ticks * tick_size).
// A float never crosses the seam.
type (
	Ticks         = int64  // price, as an integer count of ticks
	Qty           = int64  // quantity in whole units; a live order's Qty is > 0
	OrderID       = string // broker-assigned, opaque, echoed on every event
	ParticipantID = string // opaque; self-match prevention only, never money
	InstrumentID  = string // opaque instrument key, e.g. "ACME"
	BrokerID      = string // opaque; who sent the message, echoed on the private result
	Seq           = uint64 // monotonic sequence (admission time-priority OR event tape order)
)
