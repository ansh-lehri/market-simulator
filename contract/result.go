package contract

// Outcome is the discriminator on every private result the broker receives.
// See Exchange_Broker_Contract §4.1. The contract doc's 6-value enum is
// authoritative; EXCHANGE_14 lists only the first three.
type Outcome int

const (
	OutcomeMatched        Outcome = iota // went to the continuous matcher (has Match)
	OutcomeDiverted                      // collected into the closing auction (resolves later)
	OutcomeRejected                      // failed admission; never entered book or auction
	OutcomeCancelled                     // a cancel succeeded
	OutcomeCancelRejected                // a cancel found nothing to remove
	OutcomeMakerFill                     // a RESTING order of yours was hit by a taker
)

// EngineResult is the single private-outcome shape delivered to the broker.
// OrderID and BrokerID are echoed on every outcome so the broker can route
// and reconcile. See Exchange_Broker_Contract §4.1.
type EngineResult struct {
	Outcome      Outcome
	OrderID      OrderID      // echoed on EVERY outcome
	BrokerID     BrokerID     // echoed so the private event routes back
	Match        *MatchResult // non-nil for OutcomeMatched and OutcomeMakerFill
	Rejection    *Rejection   // non-nil for OutcomeRejected
	CancelledQty Qty          // set for OutcomeCancelled — qty to release
}
