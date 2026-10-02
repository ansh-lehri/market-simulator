package funnel

import (
	"testing"

	"simulator/economy/account"
)

// Every Tier-0 command must implement Command by returning its stored
// AccountID from Account(). This is the "command interface totality"
// check from the Phase-0 test plan — a new command sneaking in without a
// correct Account() would break the router's dumb-ingress contract
// (BROKER_TIER0 §3, Point 3).
//
// The test is table-driven over one instance of each of the seven
// Tier-0 commands, constructed with a distinguishable AccountID so a
// wrong-field return would be caught. Every command implements Command
// implicitly; the compile-time assignment `var _ Command = ...` also
// forces a build break if any command's Account() method disappears.

var (
	_ Command = PlaceOrder{}
	_ Command = CancelOrder{}
	_ Command = Transfer{}
	_ Command = ReadBalance{}
	_ Command = ReadPositions{}
	_ Command = ApplyResult{}
	_ Command = FireStop{}
)

func TestCommand_AccountReturnsRoutingKey(t *testing.T) {
	const wantAccount AccountID = "acct-under-test"

	cases := []struct {
		name    string
		command Command
	}{
		{"PlaceOrder", PlaceOrder{AccountID: wantAccount}},
		{"CancelOrder", CancelOrder{AccountID: wantAccount}},
		{"Transfer", Transfer{AccountID: wantAccount, From: account.Bank, To: account.Available, Amount: 1}},
		{"ReadBalance", ReadBalance{AccountID: wantAccount}},
		{"ReadPositions", ReadPositions{AccountID: wantAccount}},
		{"ApplyResult", ApplyResult{AccountID: wantAccount}},
		{"FireStop", FireStop{AccountID: wantAccount}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotAccount := tc.command.Account()
			if gotAccount != wantAccount {
				t.Fatalf("%s.Account() = %q, want %q", tc.name, gotAccount, wantAccount)
			}
		})
	}
}

// A different AccountID on a second instance must yield the different
// value — guards against a hand-coded constant return sneaking in.
func TestCommand_AccountReflectsInstanceField(t *testing.T) {
	firstInstance := PlaceOrder{AccountID: "acct-first"}
	secondInstance := PlaceOrder{AccountID: "acct-second"}

	if firstInstance.Account() == secondInstance.Account() {
		t.Fatalf("PlaceOrder.Account() ignored instance field: both returned %q",
			firstInstance.Account())
	}
}
