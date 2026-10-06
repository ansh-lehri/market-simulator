package funnel

import "testing"

// RejectReason.String returns the stable snake_case tag used verbatim in HTTP
// bodies (BROKER_PHASE5 §4.5 P5-D27). The table pins every named constant;
// an unknown / future value lands on the "unknown" default (not fatal — just
// a loud log tag).
func TestRejectReason_String(t *testing.T) {
	cases := []struct {
		reason RejectReason
		want   string
	}{
		{ReasonOK, "ok"},
		{ReasonNoSession, "no_session"},
		{ReasonUnknownInstrument, "unknown_instrument"},
		{ReasonBadQty, "bad_qty"},
		{ReasonBadSide, "bad_side"},
		{ReasonBadPrice, "bad_price"},
		{ReasonBadType, "bad_type"},
		{ReasonInsufficientFunds, "insufficient_funds"},
		{ReasonNoMarketPrice, "no_market_price"},
		{ReasonUnknownOrder, "unknown_order"},
		{ReasonNotCancellable, "not_cancellable"},
		{ReasonUnknown, "unknown"},
		{RejectReason(9999), "unknown"},
	}
	for _, tc := range cases {
		if got := tc.reason.String(); got != tc.want {
			t.Errorf("RejectReason(%d).String() = %q, want %q", int(tc.reason), got, tc.want)
		}
	}
}
