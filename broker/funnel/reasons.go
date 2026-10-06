package funnel

// RejectReason is the funnel's local enum of broker-side pre-punch rejection
// reasons, carried on both PlaceResult.Reason and CancelResult.Reason.
//
// Deliberately DISTINCT from contract.RejectReason (BROKER_PHASE5 §4.5): that
// one is the admission-stage reason the EXCHANGE echoes on
// EngineResult.Rejection, which the broker surfaces to the user via the
// settlement path's "Rejected" branch — not this enum. Funnel reasons
// describe decisions the broker made BEFORE the punch (structural validation,
// Gate 4 insufficient reserve, cancel ownership / state).
//
// Zero value is ReasonUnknown so an unset field is obviously wrong in logs; a
// well-formed PlaceResult / CancelResult never carries it. ReasonOK is only
// used by validateSpec's happy path to flag "no reason to reject"; it never
// crosses a reply channel.
//
// The String() form is a stable snake_case tag used verbatim in the HTTP
// error body — do not rename a case without updating integration tests that
// assert on the wire shape.
type RejectReason int

const (
	ReasonUnknown           RejectReason = iota
	ReasonOK                             // validateSpec happy path; never on the wire
	ReasonNoSession                      // s.sessions[AccountID] miss
	ReasonUnknownInstrument              // sess.knownInstrument(id) == false
	ReasonBadQty                         // Qty <= 0
	ReasonBadSide                        // Side neither Buy nor Sell
	ReasonBadPrice                       // Limit && LimitPrice <= 0
	ReasonBadType                        // Type neither Limit nor Market
	ReasonInsufficientFunds              // Gate 4: ErrInsufficientFunds OR ErrInsufficientPosition (P5-D28)
	ReasonNoMarketPrice                  // market-buy with no BestAsk cache hit
	ReasonUnknownOrder                   // cancel: track missing OR cross-account
	ReasonNotCancellable                 // cancel: terminal status (Filled / Cancelled / Rejected)
)

// String returns the stable wire-visible snake_case tag. The HTTP handler
// puts this string in the response body's "reason" field verbatim (P5-D27
// buckets, exact reason-string in body).
func (reason RejectReason) String() string {
	switch reason {
	case ReasonOK:
		return "ok"
	case ReasonNoSession:
		return "no_session"
	case ReasonUnknownInstrument:
		return "unknown_instrument"
	case ReasonBadQty:
		return "bad_qty"
	case ReasonBadSide:
		return "bad_side"
	case ReasonBadPrice:
		return "bad_price"
	case ReasonBadType:
		return "bad_type"
	case ReasonInsufficientFunds:
		return "insufficient_funds"
	case ReasonNoMarketPrice:
		return "no_market_price"
	case ReasonUnknownOrder:
		return "unknown_order"
	case ReasonNotCancellable:
		return "not_cancellable"
	}
	return "unknown"
}
