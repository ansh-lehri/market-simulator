package funnel

import "testing"

// isTerminal returns true for the three terminal OrderStatus values
// (Filled / Cancelled / Rejected) and false for everything else, including
// the zero value OrderStatusUnknown (a malformed track must NOT be silently
// swept by finalizeTrack).
func TestIsTerminal_Table(t *testing.T) {
	cases := []struct {
		status OrderStatus
		want   bool
	}{
		{OrderStatusUnknown, false},
		{OrderStatusWorking, false},
		{OrderStatusPartiallyFilled, false},
		{OrderStatusFilled, true},
		{OrderStatusCancelled, true},
		{OrderStatusRejected, true},
		{OrderStatus(9999), false}, // future-value defensiveness
	}
	for _, tc := range cases {
		if got := isTerminal(tc.status); got != tc.want {
			t.Errorf("isTerminal(%d) = %v, want %v", int(tc.status), got, tc.want)
		}
	}
}
