package funnel

import (
	"testing"

	"simulator/contract"
	"simulator/economy/account"
)

// TestAccountOf — table coverage for the OrderID → AccountID parser.
//
// Each case anchors a rule from BROKER_PHASE4 §2.4 (P4-D6, P4-D7):
//   - normal case: strip the last ":<seq>" suffix.
//   - LastIndex over SplitN (the memory-rule symmetry probe): an
//     AccountID containing ":" must still parse correctly.
//   - trailing-colon tightening: a broker-mint invariant violation
//     (empty seq) parses to parsed=false, not a bogus accountID with
//     empty seq slot. Doc §2.4's inline sketch accepts it; we tighten
//     deliberately — see account_of.go doc comment.
//   - every other malformed shape (empty, no colon, leading colon,
//     colon-only) → parsed=false.
func TestAccountOf(t *testing.T) {
	cases := []struct {
		name          string
		orderID       contract.OrderID
		wantAccountID account.AccountID
		wantParsed    bool
	}{
		{
			name:          "normal_account_and_seq",
			orderID:       "acc-1:42",
			wantAccountID: "acc-1",
			wantParsed:    true,
		},
		{
			name:          "account_id_contains_colons_last_colon_wins",
			orderID:       "acc:with:colons:7",
			wantAccountID: "acc:with:colons",
			wantParsed:    true,
		},
		{
			name:          "seq_zero_is_a_valid_seq",
			orderID:       "acc-1:0",
			wantAccountID: "acc-1",
			wantParsed:    true,
		},
		{
			name:          "empty_order_id_is_malformed",
			orderID:       "",
			wantAccountID: "",
			wantParsed:    false,
		},
		{
			name:          "missing_colon_is_malformed",
			orderID:       "no-colon-orderid",
			wantAccountID: "",
			wantParsed:    false,
		},
		{
			name:          "leading_colon_only_empty_account_prefix",
			orderID:       ":42",
			wantAccountID: "",
			wantParsed:    false,
		},
		{
			name:          "trailing_colon_only_empty_seq_slot",
			orderID:       "acc-1:",
			wantAccountID: "",
			wantParsed:    false,
		},
		{
			name:          "colon_only_both_empty",
			orderID:       ":",
			wantAccountID: "",
			wantParsed:    false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotAccountID, gotParsed := accountOf(tc.orderID)
			if gotAccountID != tc.wantAccountID {
				t.Errorf("accountOf(%q) accountID = %q, want %q",
					tc.orderID, gotAccountID, tc.wantAccountID)
			}
			if gotParsed != tc.wantParsed {
				t.Errorf("accountOf(%q) parsed = %v, want %v",
					tc.orderID, gotParsed, tc.wantParsed)
			}
		})
	}
}
